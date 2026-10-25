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

const std = @import("std");
const Allocator = std.mem.Allocator;
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const api_codec = @import("../api/codec.zig");
const api_types = @import("../api/types.zig");
const artifacts_mod = @import("../artifacts/mod.zig");
const catalog_mod = @import("../catalog/mod.zig");
const document_projection = @import("../document_projection.zig");
const document_facts = @import("../build/document_facts.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const read_lease = @import("../manifest/read_lease.zig");
const manifest_mod = @import("../manifest/mod.zig");
const query_reader = @import("../query/indexed_reader.zig");
const wal_mod = @import("../wal/mod.zig");
const embedder_mod = @import("antfly_local_sources").storage_db_enrichment_embedder;
const chunking = @import("antfly_local_sources").chunking_mod;
const media_outputs = @import("inference_chunker").outputs;
const maintenance_cancellation = @import("../maintenance_cancellation.zig");
const objectstore = @import("objectstore");
const artifacts_object_store = @import("../artifacts/object_store.zig");
const manifest_object_store = @import("../manifest/object_store.zig");
const progress_object_store = @import("../catalog/object_progress_store.zig");
const wal_object_store = @import("../wal/object_store.zig");
const operation_identity = @import("operation_id.zig");
const media_checkpoint = @import("media_checkpoint.zig");
const media_audit = @import("media_audit.zig");
const build_limits = @import("../build/lake_build_limits.zig");

pub const EnrichmentRunStats = struct {
    enriched_namespaces: usize = 0,
    enriched_documents: usize = 0,
    wal_appends: usize = 0,
    model_documents: usize = 0,
    fallback_documents: usize = 0,
    failed_documents: usize = 0,
    stage_failures: usize = 0,
    idle_namespaces: usize = 0,
};

pub const lexical_sparse_enrichment_version: u32 = 1;
pub const chunk_preview_enrichment_version: u32 = 1;
pub const chunk_embeddings_enrichment_version: u32 = 1;
pub const rerank_terms_enrichment_version: u32 = 1;

pub const SparseEnricherConfig = struct {
    batch_size: usize = 32,
    /// Bound pending entries considered per pass, including failed documents.
    scan_batch_size: usize = 1024,
    max_source_read_bytes: u64 = 64 * 1024 * 1024,
    /// Hard per-document input/output and live-memory admission, shared with
    /// publication. The smaller source allowance above is a soft batch limit.
    document_limits: build_limits.Limits = .{},
    pipeline_version: u32 = lexical_sparse_enrichment_version,
    stage: catalog_mod.EnrichmentStage = .lexical_sparse,
    model_preference: catalog_mod.EnrichmentModelPreference = .prefer_model,
    failure_policy: catalog_mod.EnrichmentFailurePolicy = .skip_document,
    cancellation: CancellationToken = .none,
    media_output_max_attempts: u32 = 3,
    /// Maximum pages between WAL acknowledgements. Start with one page and
    /// grow with the durable prefix, preserving a hard replay-work bound.
    media_pages_per_document: usize = 8,
    media_checkpoint_interval_ms: u64 = 1000,
};

const SourcePin = struct {
    progress: *catalog_mod.ProgressStore,
    namespace: []const u8,
    version: u64,
    parent: ?maintenance_cancellation.Token,
    cancellation: CancellationToken,
    lease: read_lease.Lease,
    cache: read_lease.Cache = .{},

    fn check(self: *SourcePin) !void {
        try maintenance_cancellation.check(self.parent);
        try self.cancellation.check();
        try self.lease.check();
        if (self.lease.unix_deadline -| @import("antfly_platform").time.realtimeNs() < read_lease.reuse_min_ns)
            self.lease = try self.cache.acquire(self.progress, self.namespace, self.version);
    }

    fn token(self: *SourcePin) CancellationToken {
        const Callbacks = struct {
            fn run(ptr: *const anyopaque) !void {
                try @as(*SourcePin, @ptrCast(@alignCast(@constCast(ptr)))).check();
            }
            fn canceled(ptr: *const anyopaque) bool {
                run(ptr) catch return true;
                return false;
            }
        };
        return .{ .ptr = self, .check_fn = Callbacks.run, .is_cancelled_fn = Callbacks.canceled };
    }
};

const DerivedBodyResult = struct {
    body: ?[]u8 = null,
    used_model: bool = false,
    used_fallback: bool = false,
};

const EnrichmentError = error{
    MissingSparseEmbeddingName,
    MissingChunkEmbeddingName,
    RequiredSparseModelUnavailable,
    RequiredChunkEmbeddingModelUnavailable,
    SparseEmbeddingModelFailed,
    ChunkEmbeddingModelFailed,
};

/// One bounded audit's model-generation observation. Never retained across
/// namespaces or ticks, and never used as another document's source/plan proof.
const MediaGenerationSnapshot = struct {
    const Boundary = struct { model: []const u8, model_identity: []const u8, model_revision: []const u8, task_type: []const u8, dimensions: usize };
    arena: std.heap.ArenaAllocator,
    boundary: ?Boundary = null,
    identities: []const media_outputs.Result = &.{},
    fn observe(self: *@This(), temporary: Allocator, metadata: *const chunking.inference.RemoteMetadata) !void {
        if (self.boundary != null) return;
        const raw = metadata.outputs_json orelse return;
        if (raw.len > 1024 * 1024) return;
        var parsed = try std.json.parseFromSlice([]media_outputs.Result, temporary, raw, .{});
        defer parsed.deinit();
        if (parsed.value.len > 16) return;
        const a = self.arena.allocator();
        const identities = try a.alloc(media_outputs.Result, parsed.value.len);
        for (identities, parsed.value) |*dest, value| dest.* = .{ .name = try a.dupe(u8, value.name), .kind = value.kind, .required = value.required, .model = try a.dupe(u8, value.model), .model_identity = try a.dupe(u8, value.model_identity), .model_revision = try a.dupe(u8, value.model_revision), .recipe_id = try a.dupe(u8, value.recipe_id), .status = .skipped };
        self.identities = identities;
        self.boundary = .{ .model = try a.dupe(u8, metadata.model), .model_identity = try a.dupe(u8, metadata.model_identity), .model_revision = try a.dupe(u8, metadata.model_revision), .task_type = try a.dupe(u8, metadata.task_type), .dimensions = metadata.dimensions };
    }
    fn matches(self: *const @This(), a: Allocator, document: std.json.Value, recipe: []const u8, version: u32, legacy: bool) !bool {
        const boundary = self.boundary orelse return false;
        const state = document.object.get("_enrichment") orelse return false;
        if (state != .object or serverlessMediaPending(state)) return false;
        const old_recipe = state.object.get("chunk_embeddings_recipe") orelse return false;
        const old_version = state.object.get("chunk_embeddings_version") orelse return false;
        if (old_recipe != .string or !std.mem.eql(u8, old_recipe.string, recipe) or old_version != .integer or old_version.integer < version) return false;
        const vectors = document.object.get("chunk_embeddings") orelse return false;
        if (vectors != .array or vectors.array.items.len == 0) return false;
        for (vectors.array.items) |row| {
            if (row != .object) return false;
            const descriptor = row.object.get("_chunk") orelse return false;
            if (descriptor != .object) return false;
            inline for (.{ "model", "model_identity", "model_revision", "task_type" }) |field| {
                const value = descriptor.object.get(field) orelse return false;
                if (value != .string or !std.mem.eql(u8, value.string, @field(boundary, field))) return false;
            }
            const dimensions = descriptor.object.get("dimensions") orelse return false;
            if (dimensions != .integer or dimensions.integer < 0 or @as(u64, @intCast(dimensions.integer)) != boundary.dimensions) return false;
        }
        const cached = document.object.get("media_outputs") orelse return legacy;
        if (cached != .object or !cached.object.contains("chunks")) return false;
        const rows = try std.json.parseFromValueLeaky(struct { chunks: []const media_outputs.Row }, a, cached, .{ .ignore_unknown_fields = true });
        if (rows.chunks.len == 0 or self.identities.len == 0) return false;
        for (self.identities) |identity| {
            const target = media_outputs.Target{ .name = identity.name, .kind = identity.kind, .model = identity.model };
            for (rows.chunks) |row| if (needsServerlessOutput(target, row.outputs, self.identities)) return false;
        }
        return true;
    }
};

pub const SparseEnricher = struct {
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    progress: *catalog_mod.ProgressStore,
    wal: *wal_mod.WalStore,
    sparse_embedder: ?embedder_mod.SparseEmbedder = null,
    sparse_embedding_name: ?[]u8 = null,
    chunk_embedder: ?embedder_mod.DenseEmbedder = null,
    chunk_embedding_name: ?[]u8 = null,
    chunk_embedding_dims: u32 = 8,
    model_chunker: ?chunking.types.Config = null,
    model_chunk_provider: ?chunking.Provider = null,
    model_chunk_field: []const u8 = "",
    media_output_max_attempts: u32 = 3,
    media_page_limit: usize = 0,
    media_checkpoint_interval_ms: u64 = 1000,
    generation_snapshot: ?*MediaGenerationSnapshot = null,
    checkpoint_context: ?media_checkpoint.Context = null,

    pub fn init(
        alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        manifests: *manifest_mod.ManifestStore,
        progress: *catalog_mod.ProgressStore,
        wal: *wal_mod.WalStore,
    ) SparseEnricher {
        return .{
            .alloc = alloc,
            .artifacts = artifacts,
            .manifests = manifests,
            .progress = progress,
            .wal = wal,
        };
    }

    pub fn deinit(self: *SparseEnricher) void {
        if (self.sparse_embedder) |embedder| embedder.deinit(self.alloc);
        if (self.sparse_embedding_name) |name| self.alloc.free(name);
        if (self.chunk_embedder) |embedder| embedder.deinit(self.alloc);
        if (self.chunk_embedding_name) |name| self.alloc.free(name);
        self.clearModelChunker();
        self.* = undefined;
    }

    pub fn configureModelChunkerFromIndexes(self: *SparseEnricher, indexes: std.json.Value, index_name: []const u8, options: @import("antfly_local_sources").inference_managed_embedder.InitOptions) !void {
        if (indexes != .object) return error.InvalidChunkerConfig;
        const index = indexes.object.get(index_name) orelse {
            self.clearModelChunker();
            return;
        };
        if (index != .object) return error.InvalidChunkerConfig;
        const embedder = index.object.get("embedder") orelse {
            self.clearModelChunker();
            return;
        };
        if (embedder != .object) return error.InvalidChunkerConfig;
        const provider_kind = embedder.object.get("provider") orelse {
            self.clearModelChunker();
            return;
        };
        if (provider_kind != .string or !std.mem.eql(u8, provider_kind.string, "antfly")) {
            self.clearModelChunker();
            return;
        }
        const model = embedder.object.get("model") orelse return error.InvalidChunkerConfig;
        if (model != .string or model.string.len == 0) return error.InvalidChunkerConfig;
        var config = if (index.object.get("chunker")) |value| try chunking.types.parseConfigFromValue(self.alloc, value) else chunking.types.Config{ .provider = .antfly };
        defer config.deinit(self.alloc);
        const task = embedder.object.get("task_type") orelse std.json.Value{ .string = "RETRIEVAL_DOCUMENT" };
        const pin = embedder.object.get("model_identity");
        if (task != .string or (pin != null and pin.? != .string and pin.? != .null)) return error.InvalidChunkerConfig;
        if (config.embedding == null) config.embedding = .{ .model = try self.alloc.dupe(u8, model.string), .dimensions = self.chunk_embedding_dims };
        const selected = &config.embedding.?;
        if (!std.mem.eql(u8, selected.model, model.string) or (selected.dimensions != null and selected.dimensions.? != self.chunk_embedding_dims)) return error.InvalidChunkerConfig;
        if (selected.task_type) |configured| {
            if (!std.mem.eql(u8, configured, task.string)) return error.InvalidChunkerConfig;
        } else selected.task_type = try self.alloc.dupe(u8, task.string);
        if (pin) |value| if (value == .string) {
            if (selected.model_identity) |configured| {
                if (!std.mem.eql(u8, configured, value.string)) return error.InvalidChunkerConfig;
            } else selected.model_identity = try self.alloc.dupe(u8, value.string);
        };
        selected.dimensions = self.chunk_embedding_dims;
        if (config.api_url.len == 0) if (embedder.object.get("url")) |url| {
            if (url != .string) return error.InvalidChunkerConfig;
            if (url.string.len != 0 and !std.mem.eql(u8, url.string, "antfly:embedded")) config.api_url = try self.alloc.dupe(u8, url.string);
        };
        const field = index.object.get("field") orelse std.json.Value{ .string = "text" };
        if (field != .string or field.string.len == 0) return error.InvalidChunkerConfig;
        var provider = chunking.Provider{ .execution = .{ .default_endpoint = options.inference_api_url, .io = options.io, .capability_cache = options.remote_capability_cache, .routing = .{ .source_table = options.source_table } } };
        if (options.antfly_provider) |local| {
            provider.ptr = local.ptr;
            provider.boundary_dispatch = local.boundary_dispatch;
            provider.chunk_input_callback = if (local.chunk_input) |callback| @ptrCast(callback) else null;
            provider.chunk_input_with_context_callback = if (local.chunk_input_with_context) |callback| @ptrCast(callback) else null;
            provider.chunk_page_with_context_callback = if (local.chunk_page_with_context) |callback| @ptrCast(callback) else null;
        }
        try self.setModelChunker(config, field.string, provider);
    }

    pub fn setModelChunker(self: *SparseEnricher, config: chunking.types.Config, field: []const u8, provider: ?chunking.Provider) !void {
        if (config.provider != .antfly or config.embedding == null or field.len == 0) return error.InvalidChunkerConfig;
        var owned_config = try config.clone(self.alloc);
        errdefer owned_config.deinit(self.alloc);
        const owned_field = try self.alloc.dupe(u8, field);
        errdefer self.alloc.free(owned_field);
        var owned_provider = if (provider) |value| try value.ownExecutionStrings(self.alloc) else null;
        errdefer if (owned_provider) |*value| value.deinit();
        if (owned_provider) |*value| if (value.execution.io) |io| try value.attachOwnedHttpClient(io);
        self.clearModelChunker();
        self.model_chunker = owned_config;
        self.model_chunk_field = owned_field;
        self.model_chunk_provider = owned_provider;
    }

    pub fn clearModelChunker(self: *SparseEnricher) void {
        if (self.model_chunker) |*value| value.deinit(self.alloc);
        if (self.model_chunk_provider) |*value| value.deinit();
        if (self.model_chunk_field.len != 0) self.alloc.free(self.model_chunk_field);
        self.model_chunker = null;
        self.model_chunk_provider = null;
        self.model_chunk_field = "";
    }

    pub fn setSparseEmbedder(self: *SparseEnricher, embedder: embedder_mod.SparseEmbedder, embedding_name: []const u8) !void {
        // Ownership transfers only after all fallible preparation succeeds.
        // This keeps the current configuration usable on allocation failure
        // and leaves the caller responsible for the replacement on error.
        const owned_name = try self.alloc.dupe(u8, embedding_name);
        if (self.sparse_embedder) |current| current.deinit(self.alloc);
        if (self.sparse_embedding_name) |name| self.alloc.free(name);
        self.sparse_embedder = embedder;
        self.sparse_embedding_name = owned_name;
    }

    pub fn clearSparseEmbedder(self: *SparseEnricher) void {
        if (self.sparse_embedder) |current| current.deinit(self.alloc);
        if (self.sparse_embedding_name) |name| self.alloc.free(name);
        self.sparse_embedder = null;
        self.sparse_embedding_name = null;
    }

    pub fn setChunkEmbedder(self: *SparseEnricher, embedder: embedder_mod.DenseEmbedder, embedding_name: []const u8, dims: u32) !void {
        const owned_name = try self.alloc.dupe(u8, embedding_name);
        if (self.chunk_embedder) |current| current.deinit(self.alloc);
        if (self.chunk_embedding_name) |name| self.alloc.free(name);
        self.chunk_embedder = embedder;
        self.chunk_embedding_name = owned_name;
        self.chunk_embedding_dims = dims;
    }

    pub fn clearChunkEmbedder(self: *SparseEnricher) void {
        if (self.chunk_embedder) |current| current.deinit(self.alloc);
        if (self.chunk_embedding_name) |name| self.alloc.free(name);
        self.chunk_embedder = null;
        self.chunk_embedding_name = null;
    }

    pub fn runNamespace(self: *SparseEnricher, namespace: []const u8) !EnrichmentRunStats {
        return try self.runNamespaceWithConfig(namespace, .{});
    }

    pub fn runNamespaceWithConfig(self: *SparseEnricher, namespace: []const u8, cfg: SparseEnricherConfig) !EnrichmentRunStats {
        return try self.runNamespaceWithConfigUntil(namespace, cfg, null);
    }

    pub fn runNamespaceWithConfigUntil(
        self: *SparseEnricher,
        namespace: []const u8,
        cfg: SparseEnricherConfig,
        cancellation: ?maintenance_cancellation.Token,
    ) !EnrichmentRunStats {
        try cfg.document_limits.validate();
        if (cfg.batch_size == 0 or cfg.scan_batch_size == 0 or cfg.max_source_read_bytes == 0) return error.InvalidEnrichmentLimits;
        try maintenance_cancellation.check(cancellation);
        try cfg.cancellation.check();
        const head = self.progress.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => return .{ .idle_namespaces = 1 },
            else => return err,
        };
        var lease_cache: read_lease.Cache = .{};
        var pin: SourcePin = .{ .progress = self.progress, .namespace = namespace, .version = head, .parent = cancellation, .cancellation = cfg.cancellation, .lease = try lease_cache.acquire(self.progress, namespace, head) };
        try pin.check();
        var manifest = try self.manifests.getAlloc(namespace, head);
        defer manifest.deinit(self.alloc);
        const latest_lsn = try self.wal.latestLsn(namespace);
        if (latest_lsn != manifest.wal_end_lsn) {
            return .{ .idle_namespaces = 1 };
        }
        // Enrichment progress is a second durable write after WAL append.
        // The append must be both idempotent and conditioned on the immutable
        // manifest's WAL boundary. Otherwise an ingest that lands during model
        // work can be overwritten by a stale derived upsert at a newer LSN.
        if (!self.wal.supportsConditionalIdempotentAppend())
            return error.IdempotentAppendUnsupported;

        // Facts are the authoritative snapshot. The document segment is only
        // a compaction base; its latest-only mutation sidecar is not a history.
        const facts_index = findArtifactIndex(manifest, .document_facts) orelse return error.DocumentFactsNotFound;
        for (manifest.artifacts[facts_index + 1 ..]) |artifact| if (artifact.kind == .document_facts) return error.InvalidDocumentFactsRoot;
        var remaining = cfg.max_source_read_bytes;
        // Routing has its own bounded allowance. A tiny body batch must still
        // be able to load the root and seek directly to pending work.
        var routing_remaining: u64 = 64 * 1024 * 1024;
        var no_writes: u64 = 0;
        var pages: PageStore = .{ .domain = PageStore.namespaceDomain(namespace), .artifacts = self.artifacts, .cancellation = pin.token(), .remaining_read_bytes = &routing_remaining, .remaining_write_bytes = &no_writes };
        const facts = try document_facts.loadRoot(self.alloc, &pages, manifest.artifacts[facts_index]);
        if (facts.wal_end_lsn != manifest.wal_end_lsn or facts.document_count != manifest.stats.document_count) return error.DocumentFactsSourceChanged;
        if (try @import("../build/document_facts_builder.zig").needsRebuild(self.alloc, facts, manifest.stats.policy, manifest.stats.indexes_json)) return error.EnrichmentPolicyChanged;
        const stage_index: usize = @backingInt(cfg.stage) - 1;
        const pending_count = facts.counts[3 + stage_index];
        const media_scan = cfg.stage == .chunk_embeddings and self.model_chunker != null;
        const scan_count = if (media_scan) facts.document_count else pending_count;
        var scan_fingerprint = facts.policy_fingerprint;
        if (media_scan) {
            const canonical = try chunking.types.stringifyAlloc(self.alloc, self.model_chunker.?);
            defer self.alloc.free(canonical);
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update("serverless-media-audit-v2");
            hash.update(&facts.policy_fingerprint);
            hash.update(canonical);
            scan_fingerprint = hash.finalResult();
        }

        const pending_page = facts.pending_pages[stage_index];
        const policy = manifest.stats.policy;
        const enabled = switch (cfg.stage) {
            .lexical_sparse => policy.enrichment_enabled,
            .chunk_preview => policy.chunk_preview_enabled,
            .chunk_embeddings => policy.chunk_embeddings_enabled,
            .rerank_terms => policy.rerank_terms_enabled,
        };
        const policy_version = switch (cfg.stage) {
            .lexical_sparse => policy.enrichment_pipeline_version,
            .chunk_preview => policy.chunk_preview_pipeline_version,
            .chunk_embeddings => policy.chunk_embeddings_pipeline_version,
            .rerank_terms => policy.rerank_terms_pipeline_version,
        };
        if (!enabled or policy_version != cfg.pipeline_version) return error.EnrichmentPolicyChanged;
        if (pending_count != if (pending_page) |page| page.records else @as(u64, 0)) return error.InvalidDocumentFactsRoot;
        try pin.check();

        // Loading the immutable manifest can overlap another publication.
        // Do not initialize progress or emit work if publication moved while
        // it was in flight. The atomic stage state below prevents a stale
        // worker from regressing progress after a newer worker takes over.
        if ((try self.progress.getHead(namespace)) != head)
            return error.EnrichmentProgressChanged;

        var previous = try self.progress.getEnrichmentStageProgress(namespace, cfg.stage);
        defer if (previous) |*value| value.deinit(self.progress.allocator);
        if (previous) |value| if (value.head_version > head) return error.EnrichmentProgressChanged;
        const same_pipeline = if (previous) |value| value.pipeline_version == cfg.pipeline_version and
            std.mem.eql(u8, &value.policy_fingerprint, &scan_fingerprint) else false;
        if (same_pipeline) if (previous.?.cycle_upper_order_key) |key| {
            if (key.len <= 8 or std.mem.readInt(u64, key[0..8], .big) > facts.wal_end_lsn)
                return error.InvalidEnrichmentStageProgress;
        };
        const after = if (same_pipeline) previous.?.after_order_key else null;
        var next_key: ?[]u8 = if (after) |key| try self.alloc.dupe(u8, key) else null;
        defer if (next_key) |key| self.alloc.free(key);
        var cycle_upper: ?[]u8 = if (same_pipeline) upper: {
            break :upper if (previous.?.cycle_upper_order_key) |key| try self.alloc.dupe(u8, key) else null;
        } else null;
        defer if (cycle_upper) |key| self.alloc.free(key);
        var next_offset: u64 = if (same_pipeline and previous.?.head_version == head) previous.?.doc_offset else 0;
        var cycles: u64 = if (previous) |value| value.completed_cycles else 0;
        var stats = EnrichmentRunStats{};
        var generations = MediaGenerationSnapshot{ .arena = std.heap.ArenaAllocator.init(self.alloc) };
        defer generations.arena.deinit();
        var expected_latest_lsn = latest_lsn;
        if (scan_count != 0) {
            if (cycle_upper == null) {
                // Capture one finite cycle under the pinned source. Rank seek
                // is O(tree height), not a scan of the pending population.
                var tail = if (media_scan) try document_facts.mediaCursor(self.alloc, pages.store(), facts, "", scan_count - 1) else try document_facts.pendingCursorAtRank(self.alloc, pages.store(), facts, stage_index, pending_count - 1);
                defer tail.deinit();
                const last = (try tail.next()) orelse return error.InvalidDocumentFactsRoot;
                cycle_upper = try self.alloc.dupe(u8, last.order_key);
            }
            var cursor = if (media_scan) try document_facts.mediaCursor(self.alloc, pages.store(), facts, after orelse "", null) else try document_facts.pendingCursor(self.alloc, pages.store(), facts, stage_index, after orelse "");
            defer cursor.deinit();
            var scanned: usize = 0;
            while (scanned < cfg.scan_batch_size) {
                const maybe_record = cursor.next() catch |err| {
                    if (err == error.ArtifactReadBudgetExceeded and scanned > 0) break;
                    return err;
                };
                if (maybe_record == null or std.mem.order(u8, maybe_record.?.order_key, cycle_upper.?) == .gt) {
                    // New WAL records sort after this finite cycle across HEADs,
                    // irrespective of document ID ordering or arrival rate.
                    // Wrap on the next pass to revisit failures and updates.
                    if (next_key) |key| self.alloc.free(key);
                    next_key = null;
                    self.alloc.free(cycle_upper.?);
                    cycle_upper = null;
                    next_offset = 0;
                    cycles = try std.math.add(u64, cycles, 1);
                    break;
                }
                const record = maybe_record.?;
                if (after) |key| if (std.mem.eql(u8, record.order_key, key)) continue;
                try pin.check();
                const fact = try document_facts.Fact.decode(record.value);
                // The batch allowance is soft. The first pending body may use
                // the shared hard document limit; later bodies resume next pass.
                const body_bytes = try bodyPayloadBytes(fact.body.bytes);
                if (body_bytes > remaining and scanned > 0) break;
                // Checkpoints are output/replay objects and may exceed the
                // source input cap. Their base is admitted separately below.
                const physical_limit = if (fact.body_has_dependencies) cfg.document_limits.max_replay_bytes else cfg.document_limits.max_input_bytes;
                var body_read_remaining = std.math.add(u64, physical_limit, document_facts.body_header_bytes) catch return error.EnrichmentDocumentBudgetExceeded;
                var source_input_bytes = body_bytes;
                var body_pages = pages;
                body_pages.remaining_read_bytes = &body_read_remaining;
                const completed_key = try self.alloc.dupe(u8, record.order_key);
                var owns_completed_key = true;
                errdefer if (owns_completed_key) self.alloc.free(completed_key);
                expected_latest_lsn = self.processPendingDocument(namespace, cfg, &pin, &body_pages, fact, record.key, head, expected_latest_lsn, &stats, &generations, &source_input_bytes) catch |err| failed: {
                    if (!isRecoverableEnrichmentError(err) and err != error.EnrichmentDocumentBudgetExceeded) return err;
                    stats.failed_documents += 1;
                    if (cfg.failure_policy == .fail_stage) return err;
                    break :failed expected_latest_lsn;
                };
                if (next_key) |key| self.alloc.free(key);
                next_key = completed_key;
                owns_completed_key = false;
                next_offset = try std.math.add(u64, next_offset, 1);
                remaining -|= source_input_bytes;
                scanned += 1;
                if (stats.wal_appends >= cfg.batch_size) break;
            }
        } else {
            if (next_key) |key| self.alloc.free(key);
            next_key = null;
            if (cycle_upper) |key| self.alloc.free(key);
            cycle_upper = null;
            next_offset = 0;
        }
        try pin.check();
        const desired = catalog_mod.EnrichmentStageProgress{
            .head_version = head,
            .doc_offset = next_offset,
            .revision = try std.math.add(u64, if (previous) |value| value.revision else 0, 1),
            .pipeline_version = cfg.pipeline_version,
            .policy_fingerprint = scan_fingerprint,
            .after_order_key = next_key,
            .cycle_upper_order_key = cycle_upper,
            .completed_cycles = cycles,
            .failed_documents = try std.math.add(u64, if (previous) |value| value.failed_documents else 0, stats.failed_documents),
        };
        if (!try self.progress.compareAndSwapEnrichmentStageProgress(namespace, cfg.stage, previous, desired)) return error.EnrichmentProgressChanged;
        if (stats.enriched_documents == 0) {
            stats.idle_namespaces = 1;
        } else {
            stats.enriched_namespaces = 1;
        }
        return stats;
    }

    fn mediaRecipe(self: *SparseEnricher, source_digest: [32]u8, version: u32) ![64]u8 {
        var revision: ?[32]u8 = null;
        if (self.chunk_embedder) |provider| {
            const caps = provider.capabilities(self.alloc, self.chunk_embedding_name orelse return error.MissingChunkEmbeddingName) catch |err| switch (err) {
                error.EmbeddingCapabilitiesUnavailable => null,
                else => return err,
            };
            if (caps) |value| revision = value.model_revision;
        }
        return media_audit.recipe(self.alloc, self.model_chunker.?, self.model_chunk_field, source_digest, revision, version);
    }

    fn auditMatches(self: *SparseEnricher, cfg: SparseEnricherConfig, pages: *PageStore, fact: document_facts.Fact, generations: *MediaGenerationSnapshot) !bool {
        if (cfg.stage != .chunk_embeddings or self.model_chunker == null or generations.boundary == null) return false;
        const ref = fact.media_audit orelse return false;
        if (ref.bytes > media_audit.max_bytes + document_facts.body_header_bytes) return error.InvalidDocumentFact;
        var witness_remaining: u64 = media_audit.max_bytes + document_facts.body_header_bytes;
        var witness_pages = pages.*;
        witness_pages.remaining_read_bytes = &witness_remaining;
        const json = try document_facts.readStoredBodyAlloc(self.alloc, &witness_pages, ref);
        defer self.alloc.free(json);
        var parsed = try std.json.parseFromSlice(media_audit.Witness, self.alloc, json, .{});
        defer parsed.deinit();
        if (parsed.value.format != 1) return false;
        for (parsed.value.sources) |source| if (std.mem.eql(u8, source.field, self.model_chunk_field)) {
            const recipe = try self.mediaRecipe(source.digest, cfg.pipeline_version);
            return generations.matches(parsed.arena.allocator(), parsed.value.document, &recipe, cfg.pipeline_version, self.model_chunker.?.outputs_json.len == 0);
        };
        return false;
    }

    fn processPendingDocument(self: *SparseEnricher, namespace: []const u8, cfg: SparseEnricherConfig, pin: *SourcePin, pages: *PageStore, fact: document_facts.Fact, key: []const u8, head: u64, expected_lsn: u64, stats: *EnrichmentRunStats, generations: *MediaGenerationSnapshot, source_input_bytes: *u64) !u64 {
        const stored_limit = if (fact.body_has_dependencies) cfg.document_limits.max_replay_bytes else cfg.document_limits.max_input_bytes;
        if (try bodyPayloadBytes(fact.body.bytes) > stored_limit) return error.EnrichmentDocumentBudgetExceeded;
        if (try self.auditMatches(cfg, pages, fact, generations)) {
            source_input_bytes.* = try bodyPayloadBytes(fact.media_audit.?.bytes);
            return expected_lsn;
        }
        var working = try build_limits.WorkingSetAllocator.init(self.alloc, cfg.document_limits);
        var worker = self.*;
        worker.alloc = working.allocator();
        worker.generation_snapshot = generations;
        worker.media_output_max_attempts = @max(cfg.media_output_max_attempts, 1);
        worker.media_page_limit = @max(cfg.media_pages_per_document, 1);
        worker.media_checkpoint_interval_ms = @max(cfg.media_checkpoint_interval_ms, 1);
        return worker.processAdmittedDocument(namespace, cfg, pin, pages, fact, key, head, expected_lsn, stats, source_input_bytes) catch |err| {
            if (err == error.OutOfMemory and working.limit_exceeded) return error.EnrichmentDocumentBudgetExceeded;
            return err;
        };
    }

    fn processAdmittedDocument(self: *SparseEnricher, namespace: []const u8, cfg: SparseEnricherConfig, pin: *SourcePin, pages: *PageStore, fact: document_facts.Fact, key: []const u8, head: u64, expected_lsn: u64, stats: *EnrichmentRunStats, source_input_bytes: *u64) !u64 {
        const original_read_budget = pages.remaining_read_bytes;
        defer pages.remaining_read_bytes = original_read_budget;
        var replay_remaining = cfg.document_limits.max_replay_bytes;
        const stored = try document_facts.readStoredBodyAlloc(self.alloc, pages, fact.body);
        defer self.alloc.free(stored);
        var base = fact.body;
        const source = if (document_facts.isCheckpointBody(stored)) resolved: {
            var descriptor = try document_facts.parseCheckpointBody(self.alloc, stored);
            defer descriptor.deinit();
            base = descriptor.value.base;
            // Admission applies to the immutable base. The merged repair
            // state includes output replay and belongs to the replay/working
            // allowances, even when it serializes larger than that base.
            source_input_bytes.* = try bodyPayloadBytes(base.bytes);
            if (source_input_bytes.* > cfg.document_limits.max_input_bytes) return error.EnrichmentDocumentBudgetExceeded;
            replay_remaining = std.math.sub(u64, replay_remaining, try bodyPayloadBytes(fact.body.bytes)) catch return error.EnrichmentDocumentBudgetExceeded;
            // Charge the admitted base only to source reads. Preserve the
            // checkpoint charge when output ancestry starts replaying below.
            var base_remaining = base.bytes;
            var base_pages = pages.*;
            base_pages.remaining_read_bytes = &base_remaining;
            break :resolved try document_facts.resolveCheckpointStateAlloc(self.alloc, &base_pages, stored);
        } else try self.alloc.dupe(u8, stored);
        defer self.alloc.free(source);
        if (!document_facts.isCheckpointBody(stored)) source_input_bytes.* = source.len;
        if (source_input_bytes.* > cfg.document_limits.max_input_bytes) return error.EnrichmentDocumentBudgetExceeded;
        // The next WAL mutation links only an already-published, leased fact
        // body. No speculative upload can race namespace retention.
        if (cfg.stage == .chunk_embeddings and self.model_chunker != null) {
            var key_digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(key, &key_digest, .{});
            self.checkpoint_context = .{ .allocator = self.alloc, .pages = pages, .previous = fact.body, .previous_is_compact = document_facts.isCheckpointBody(stored), .key_digest = key_digest };
            // Output ancestry is durable replay, not another logical source
            // input. Its independent finite allowance also admits legacy full
            // body checkpoints during migration; working/output caps still
            // bound reconstruction and the final serialized document.
            pages.remaining_read_bytes = &replay_remaining;
        }
        const derived = try buildDerivedBodyAlloc(self, cfg.stage, source, cfg.pipeline_version, cfg.model_preference);
        defer if (derived.body) |body| self.alloc.free(body);
        try pin.check();
        const body = derived.body orelse return expected_lsn;
        if (body.len > cfg.document_limits.max_output_bytes) return error.EnrichmentDocumentBudgetExceeded;
        // Only incomplete media output pages use the internal compact mutation.
        // Completion is an ordinary self-contained document snapshot.
        var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, body, .{});
        defer parsed.deinit();
        const checkpoint = cfg.stage == .chunk_embeddings and (try media_checkpoint.get(parsed.arena.allocator(), parsed.value)) != null;
        const payload = if (checkpoint) try document_facts.encodeCheckpointBodyAlloc(self.alloc, base, body) else null;
        defer if (payload) |bytes| self.alloc.free(bytes);
        const encoded = try api_codec.encodeMutationAlloc(self.alloc, .{ .kind = if (checkpoint) .checkpoint else .upsert, .doc_id = key, .body = payload orelse body });
        defer self.alloc.free(encoded);
        if (encoded.len > cfg.document_limits.max_output_bytes) return error.EnrichmentDocumentBudgetExceeded;
        try pin.check();
        var operation_buffer: [128]u8 = undefined;
        const operation = try operation_identity.formatDocument(&operation_buffer, head, @backingInt(cfg.stage), key, cfg.pipeline_version);
        const timestamp = std.math.add(u64, fact.last_timestamp_ns, 1) catch return error.EnrichmentTimestampOverflow;
        const appended = (try self.wal.appendIdempotentIfLatest(namespace, timestamp, encoded, operation, expected_lsn)) orelse return error.EnrichmentProgressChanged;
        stats.enriched_documents += 1;
        stats.wal_appends += 1;
        if (derived.used_model) stats.model_documents += 1;
        if (derived.used_fallback) stats.fallback_documents += 1;
        return appended;
    }
};

fn bodyPayloadBytes(encoded_bytes: u64) !u64 {
    return std.math.sub(u64, encoded_bytes, document_facts.body_header_bytes) catch error.InvalidDocumentBody;
}

test "serverless enrichment body admission excludes authenticated envelope" {
    const limits: build_limits.Limits = .{};
    try std.testing.expectEqual(limits.max_input_bytes, try bodyPayloadBytes(limits.max_input_bytes + document_facts.body_header_bytes));
    try std.testing.expectEqual(@as(u64, 0), try bodyPayloadBytes(document_facts.body_header_bytes));
    try std.testing.expectError(error.InvalidDocumentBody, bodyPayloadBytes(document_facts.body_header_bytes - 1));
}

fn findArtifactIndex(manifest: manifest_mod.Manifest, kind: manifest_mod.ArtifactKind) ?usize {
    for (manifest.artifacts, 0..) |artifact, idx| {
        if (artifact.kind == kind) return idx;
    }
    return null;
}

fn buildDerivedBodyAlloc(
    self: *SparseEnricher,
    stage: catalog_mod.EnrichmentStage,
    body: []const u8,
    pipeline_version: u32,
    model_preference: catalog_mod.EnrichmentModelPreference,
) !DerivedBodyResult {
    var result = if (stage == .chunk_embeddings and self.model_chunker != null and model_preference != .deterministic_only)
        try buildModelAwareChunkBodyAlloc(self, body, pipeline_version)
    else switch (stage) {
        .lexical_sparse => try buildDerivedSparseBodyAlloc(self, body, pipeline_version, model_preference),
        .chunk_preview => DerivedBodyResult{ .body = try buildDerivedChunkPreviewBodyAlloc(self.alloc, body, pipeline_version) },
        .chunk_embeddings => try buildDerivedChunkEmbeddingsBodyAlloc(self, body, pipeline_version, model_preference),
        .rerank_terms => DerivedBodyResult{ .body = try buildDerivedRerankTermsBodyAlloc(self.alloc, body, pipeline_version) },
    };
    if (result.body) |produced| {
        defer self.alloc.free(produced);
        result.body = try preserveOtherEnrichmentOutputsAlloc(self.alloc, body, produced, stage);
    }
    return result;
}

fn preserveOtherEnrichmentOutputsAlloc(a: Allocator, original: []const u8, produced: []const u8, stage: catalog_mod.EnrichmentStage) ![]u8 {
    var before = try std.json.parseFromSlice(std.json.Value, a, original, .{});
    defer before.deinit();
    var after = try std.json.parseFromSlice(std.json.Value, a, produced, .{});
    defer after.deinit();
    if (before.value != .object or after.value != .object) return a.dupe(u8, produced);
    const scratch = after.arena.allocator();
    const owned = switch (stage) {
        .lexical_sparse => "sparse_embedding",
        .chunk_preview => "chunk_preview",
        .chunk_embeddings => "chunk_embeddings",
        .rerank_terms => "rerank_terms",
    };
    for ([_][]const u8{ "embedding", "sparse_embedding", "graph_edges", "chunk_preview", "chunk_embeddings", "rerank_terms", "media_outputs", "transcript", "transcript_spans" }) |name| {
        const media_owned = stage == .chunk_embeddings and (std.mem.eql(u8, name, "chunk_preview") or std.mem.eql(u8, name, "media_outputs") or std.mem.eql(u8, name, "transcript") or std.mem.eql(u8, name, "transcript_spans"));
        if (media_owned or std.mem.eql(u8, name, owned) or after.value.object.contains(name)) continue;
        if (before.value.object.get(name)) |value| try after.value.object.put(scratch, name, value);
    }
    const old_state = before.value.object.get("_enrichment") orelse return std.json.Stringify.valueAlloc(a, after.value, .{});
    if (old_state != .object) return std.json.Stringify.valueAlloc(a, after.value, .{});
    var state = after.value.object.get("_enrichment") orelse std.json.Value{ .object = .empty };
    if (state != .object) return error.InvalidEnrichmentStageProgress;
    const prefix = switch (stage) {
        .lexical_sparse => "lexical_sparse",
        .chunk_preview => "chunk_preview",
        .chunk_embeddings => "chunk_embeddings",
        .rerank_terms => "rerank_terms",
    };
    var it = old_state.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.startsWith(u8, entry.key_ptr.*, prefix) or state.object.contains(entry.key_ptr.*)) continue;
        try state.object.put(scratch, entry.key_ptr.*, entry.value_ptr.*);
    }
    try after.value.object.put(scratch, "_enrichment", state);
    return std.json.Stringify.valueAlloc(a, after.value, .{});
}

fn buildDerivedSparseBodyAlloc(
    self: *SparseEnricher,
    body: []const u8,
    pipeline_version: u32,
    model_preference: catalog_mod.EnrichmentModelPreference,
) !DerivedBodyResult {
    const alloc = self.alloc;
    var projection = try document_projection.parseAlloc(alloc, body);
    defer projection.deinit(alloc);
    if (projection.sparse_embedding != null and projection.lexical_sparse_version != null and projection.lexical_sparse_version.? >= pipeline_version) {
        return .{};
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJSONString(alloc, &out, "{");
    var wrote_field = false;
    try appendPreservedDocumentFieldsJSON(alloc, &out, body, &wrote_field);
    try appendJSONFieldName(alloc, &out, &wrote_field, "text");
    try appendJSONStringValue(alloc, &out, projection.text);
    if (projection.embedding) |embedding| {
        try appendEmbeddingJSON(alloc, &out, "embedding", embedding);
    }
    if (projection.graph_edges_json) |graph_edges_json| {
        try appendJSONString(alloc, &out, ",\"graph_edges\":");
        try appendJSONString(alloc, &out, graph_edges_json);
    }

    var used_model = false;
    var used_fallback = false;
    var encoded_sparse = false;
    if (shouldAttemptModel(model_preference)) {
        switch (model_preference) {
            .prefer_model => {
                if (self.sparse_embedder) |embedder| {
                    const embedding_name = self.sparse_embedding_name orelse return EnrichmentError.MissingSparseEmbeddingName;
                    const sparse = embedder.embedSparse(alloc, embedding_name, projection.text) catch null;
                    if (sparse) |value| {
                        var owned_sparse = value;
                        defer owned_sparse.deinit(alloc);
                        used_model = true;
                        try appendJSONString(alloc, &out, ",\"sparse_embedding\":{");
                        for (owned_sparse.indices, owned_sparse.values, 0..) |index, value2, idx| {
                            if (idx != 0) try appendJSONString(alloc, &out, ",");
                            const feature = try std.fmt.allocPrint(alloc, "f{d}", .{index});
                            defer alloc.free(feature);
                            try appendJSONStringValue(alloc, &out, feature);
                            try appendJSONString(alloc, &out, ":");
                            const num = try std.fmt.allocPrint(alloc, "{d}", .{value2});
                            defer alloc.free(num);
                            try appendJSONString(alloc, &out, num);
                        }
                        encoded_sparse = true;
                    } else {
                        used_fallback = true;
                    }
                } else {
                    used_fallback = true;
                }
            },
            .require_model => {
                const embedder = self.sparse_embedder orelse return EnrichmentError.RequiredSparseModelUnavailable;
                const embedding_name = self.sparse_embedding_name orelse return EnrichmentError.MissingSparseEmbeddingName;
                var sparse = embedder.embedSparse(alloc, embedding_name, projection.text) catch return EnrichmentError.SparseEmbeddingModelFailed;
                defer sparse.deinit(alloc);
                used_model = true;
                try appendJSONString(alloc, &out, ",\"sparse_embedding\":{");
                for (sparse.indices, sparse.values, 0..) |index, value, idx| {
                    if (idx != 0) try appendJSONString(alloc, &out, ",");
                    const feature = try std.fmt.allocPrint(alloc, "f{d}", .{index});
                    defer alloc.free(feature);
                    try appendJSONStringValue(alloc, &out, feature);
                    try appendJSONString(alloc, &out, ":");
                    const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
                    defer alloc.free(num);
                    try appendJSONString(alloc, &out, num);
                }
                encoded_sparse = true;
            },
            .deterministic_only => {},
        }
    }
    if (!encoded_sparse) {
        const normalized = try query_reader.normalizeAlloc(alloc, projection.text);
        defer alloc.free(normalized);
        if (normalized.len == 0) return .{};

        var counts = std.StringArrayHashMapUnmanaged(u32).empty;
        defer {
            for (counts.keys()) |term| alloc.free(term);
            counts.deinit(alloc);
        }

        var token_count: usize = 0;
        var iter = std.mem.tokenizeAny(u8, normalized, " ");
        while (iter.next()) |token| {
            token_count += 1;
            const owned = try alloc.dupe(u8, token);
            errdefer alloc.free(owned);
            const gop = try counts.getOrPut(alloc, owned);
            if (!gop.found_existing) {
                gop.value_ptr.* = 0;
            } else {
                alloc.free(owned);
            }
            gop.value_ptr.* += 1;
        }
        if (token_count == 0 or counts.count() == 0) return .{};

        try appendJSONString(alloc, &out, ",\"sparse_embedding\":{");
        for (counts.keys(), counts.values(), 0..) |term, count, idx| {
            if (idx != 0) try appendJSONString(alloc, &out, ",");
            try appendJSONStringValue(alloc, &out, term);
            try appendJSONString(alloc, &out, ":");
            const weight = @as(f32, @floatFromInt(count)) / @as(f32, @floatFromInt(token_count));
            const num = try std.fmt.allocPrint(alloc, "{d}", .{weight});
            defer alloc.free(num);
            try appendJSONString(alloc, &out, num);
        }
    }
    const version = try std.fmt.allocPrint(alloc, "{d}", .{pipeline_version});
    defer alloc.free(version);
    try appendJSONString(alloc, &out, "},\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":");
    try appendJSONString(alloc, &out, version);
    try appendJSONString(alloc, &out, "}}");
    return .{
        .body = try out.toOwnedSlice(alloc),
        .used_model = used_model,
        .used_fallback = used_fallback,
    };
}

fn serverlessMediaPending(state: std.json.Value) bool {
    if (state != .object) return false;
    const value = state.object.get("chunk_embeddings_pending") orelse return false;
    return value == .bool and value.bool;
}

fn needsServerlessOutput(target: media_outputs.Target, cached: []const media_outputs.Result, current: []const media_outputs.Result) bool {
    const identity = for (current) |value| {
        if (std.mem.eql(u8, value.name, target.name)) break value.recipe_id;
    } else return true;
    var seen = false;
    for (cached) |value| if (std.mem.eql(u8, value.name, target.name)) {
        seen = true;
        if (!std.mem.eql(u8, identity, value.recipe_id) or !value.reusable()) return true;
    };
    return !seen;
}

fn mergeServerlessOutputs(a: Allocator, fresh: []const media_outputs.Result, cached: []const media_outputs.Result, max_attempts: u32) ![]const media_outputs.Result {
    return media_outputs.mergeResults(a, fresh, cached, max_attempts);
}

// Older transports put the sole embedding failure on the parent metadata.
// Give that response the same durable recipe and retry lifecycle as named outputs.
fn serverlessOutputResults(a: Allocator, target: chunking.types.EmbeddingTarget, metadata: *const chunking.inference.RemoteMetadata) ![]const media_outputs.Result {
    if (metadata.outputs_json) |raw| return std.json.parseFromSliceLeaky([]media_outputs.Result, a, raw, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, metadata.model, target.model) or !std.mem.eql(u8, metadata.task_type, target.task_type orelse "RETRIEVAL_DOCUMENT")) return error.InvalidChunkerResponse;
    const selected = media_outputs.Target{ .name = "embedding", .kind = .embedding, .model = target.model, .model_identity = target.model_identity, .task_type = target.task_type orelse "RETRIEVAL_DOCUMENT", .dimensions = if (target.dimensions) |n| @intCast(n) else null };
    const recipe = try selected.recipeIdentity(a, metadata.model_identity, metadata.model_revision);
    const result = try a.alloc(media_outputs.Result, 1);
    result[0] = .{ .name = "embedding", .kind = .embedding, .required = true, .model = try a.dupe(u8, metadata.model), .model_identity = try a.dupe(u8, metadata.model_identity), .model_revision = try a.dupe(u8, metadata.model_revision), .recipe_id = try a.dupe(u8, &recipe), .status = if (metadata.embedding_error != null) .failed else if (metadata.embedding != null) .complete else .skipped, .code = if (metadata.embedding_error) |code| try a.dupe(u8, code) else null, .embedding = if (metadata.embedding) |vector| try a.dupe(f32, vector) else null, .start_ns = metadata.descriptor.coordinates.start_ns, .end_ns = metadata.descriptor.coordinates.end_ns };
    return result;
}

fn appendServerlessEmbeddingRows(a: Allocator, target: chunking.types.EmbeddingTarget, metadata: *const chunking.inference.RemoteMetadata, results: []const media_outputs.Result, text: []const u8, rows: *std.ArrayListUnmanaged(std.json.Value), previews: *std.ArrayListUnmanaged(std.json.Value)) !void {
    var matched = false;
    for (results) |output| if (output.kind == .embedding and std.mem.eql(u8, output.name, "embedding")) {
        matched = true;
        if (output.status != .complete) continue;
        if (!std.mem.eql(u8, output.model, target.model)) return error.InvalidChunkerResponse;
        const selected = media_outputs.Target{ .name = "embedding", .kind = .embedding, .model = target.model, .model_identity = target.model_identity, .task_type = target.task_type orelse "RETRIEVAL_DOCUMENT", .dimensions = if (target.dimensions) |n| @intCast(n) else null, .required = output.required };
        try output.validate(selected);
        const recipe = try selected.recipeIdentity(a, output.model_identity, output.model_revision);
        if (!std.mem.eql(u8, &recipe, output.recipe_id)) return error.InvalidChunkerResponse;
        try appendServerlessEmbeddingRow(a, metadata, output.embedding.?, output, text, rows, previews);
    };
    if (matched) return;
    // Legacy responses have one vector belonging to the boundary. It is only
    // eligible when its complete computation contract matches the index.
    if (!std.mem.eql(u8, metadata.model, target.model) or !std.mem.eql(u8, metadata.task_type, target.task_type orelse "RETRIEVAL_DOCUMENT")) return error.InvalidChunkerResponse;
    if (target.model_identity) |pin| if (!std.mem.eql(u8, pin, metadata.model_identity)) return error.EmbeddingIdentityMismatch;
    const vector = metadata.embedding orelse return error.IncompleteChunkCoverage;
    if (vector.len == 0 or (target.dimensions != null and target.dimensions.? != vector.len)) return error.InvalidEmbeddingDimensions;
    for (vector) |value| if (!std.math.isFinite(value)) return error.InvalidEmbeddingResponse;
    try appendServerlessEmbeddingRow(a, metadata, vector, null, text, rows, previews);
}

fn appendServerlessEmbeddingRow(a: Allocator, metadata: *const chunking.inference.RemoteMetadata, vector: []const f32, output: ?media_outputs.Result, text: []const u8, rows: *std.ArrayListUnmanaged(std.json.Value), previews: *std.ArrayListUnmanaged(std.json.Value)) !void {
    var row = std.json.ObjectMap.empty;
    try row.put(a, "chunk", .{ .string = try a.dupe(u8, text) });
    var descriptor = metadata.*;
    descriptor.embedding = null;
    descriptor.outputs_json = null;
    const encoded = try std.json.Stringify.valueAlloc(a, descriptor, .{ .emit_null_optional_fields = false });
    var recipe = try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{});
    if (output) |value| {
        try recipe.object.put(a, "output_name", .{ .string = value.name });
        try recipe.object.put(a, "output_sub_index", .{ .integer = value.sub_index });
        try recipe.object.put(a, "output_model_identity", .{ .string = value.model_identity });
        if (value.start_ns) |start| try recipe.object.put(a, "output_start_ns", .{ .integer = start });
        if (value.end_ns) |end| try recipe.object.put(a, "output_end_ns", .{ .integer = end });
    }
    try row.put(a, "_chunk", recipe);
    var values = std.ArrayList(std.json.Value).empty;
    for (vector) |value| try values.append(a, .{ .float = value });
    try row.put(a, "embedding", .{ .array = values.toManaged(a) });
    try rows.append(a, .{ .object = row });
    try previews.append(a, .{ .string = try a.dupe(u8, text) });
}

fn buildModelAwareChunkBodyAlloc(self: *SparseEnricher, body: []const u8, pipeline_version: u32) !DerivedBodyResult {
    const a = self.alloc;
    var source = try std.json.parseFromSlice(std.json.Value, a, body, .{});
    defer source.deinit();
    const scratch = source.arena.allocator();
    var document = source.value;
    if (document != .object) {
        var object = std.json.ObjectMap.empty;
        if (document == .string) try object.put(scratch, "text", document) else return error.InvalidChunkSource;
        document = .{ .object = object };
    }
    const field = document.object.get(self.model_chunk_field) orelse std.json.Value.null;
    const recipe_fingerprint = try self.mediaRecipe(try media_audit.sourceDigest(a, field), pipeline_version);
    const selected = try chunking.chunk.modelSourceFromValue(field);
    if (selected != .source) if (self.generation_snapshot) |snapshot| if (try snapshot.matches(scratch, document, &recipe_fingerprint, pipeline_version, self.model_chunker.?.outputs_json.len == 0)) return .{};
    var decoded: ?[]u8 = null;
    defer if (decoded) |bytes| a.free(bytes);
    const input: ?chunking.inference.RemoteInput = switch (selected) {
        .none => null,
        .text => |text| .{ .text = text },
        .source => |reference| .{ .source = reference },
        .media => |media| blk: {
            const length = try std.base64.standard.Decoder.calcSizeForSlice(media.data);
            const bytes = try a.alloc(u8, length);
            decoded = bytes;
            try std.base64.standard.Decoder.decode(bytes, media.data);
            break :blk .{ .binary = .{ .mime_type = media.mime_type, .data = bytes } };
        },
    };
    const wanted = self.model_chunker.?.embedding orelse return error.InvalidChunkerConfig;
    const base_config = self.model_chunker.?;
    var consumers: media_outputs.Config = if (base_config.outputs_json.len != 0) try std.json.parseFromSliceLeaky(media_outputs.Config, scratch, base_config.outputs_json, .{}) else media_outputs.Config{ .boundary_model = "embedding", .targets = &.{} };
    consumers = try consumers.includingEmbedding(scratch, .{ .name = "embedding", .kind = .embedding, .model = wanted.model, .model_identity = wanted.model_identity, .task_type = wanted.task_type orelse "RETRIEVAL_DOCUMENT", .dimensions = if (wanted.dimensions) |n| @intCast(n) else null });
    var cached_rows: []const media_outputs.Row = &.{};
    var current_identities: []const media_outputs.Result = &.{};
    // The provider's plan authenticates source and boundary identity. Output
    // recipes below authorize each reusable result independently, including
    // across consumer edits and pipeline-version changes.
    var legacy_current = false;
    var checkpoint = try media_checkpoint.get(scratch, document);
    const had_checkpoint = checkpoint != null;
    var fast_checkpoint: ?media_checkpoint.Group = null;
    var probe_identities: []const media_outputs.Result = &.{};
    var same_checkpoint_plan = false;
    if (checkpoint) |saved| {
        const context = self.checkpoint_context orelse return error.InvalidMediaCheckpoint;
        if (!std.mem.eql(u8, &saved.key_digest, &context.key_digest)) return error.InvalidMediaCheckpoint;
        if (input != null) {
            var probe = try chunking.inference.probeModelPageWithProvider(a, base_config, input.?, self.model_chunk_provider);
            defer probe.deinit(a);
            if (probe.observedMetadata() != null) {
                probe_identities = try serverlessOutputResults(scratch, wanted, probe.observedMetadata().?);
                const generation = try media_checkpoint.generations(consumers.targets, probe_identities);
                same_checkpoint_plan = std.mem.eql(u8, saved.plan_id, probe.plan_id);
                if (std.mem.eql(u8, saved.recipe, &recipe_fingerprint) and same_checkpoint_plan and std.mem.eql(u8, &saved.generation, &generation)) fast_checkpoint = saved;
            }
        }
        if (fast_checkpoint == null) {
            if (same_checkpoint_plan) {
                try media_checkpoint.expand(scratch, context, &document);
            } else {
                // A changed/cleared source or boundary invalidates the entire
                // inventory. Retire its ancestry without reading old payloads.
                var state = document.object.get("_enrichment").?;
                _ = state.object.swapRemove("chunk_embeddings_checkpoint");
                try document.object.put(scratch, "_enrichment", state);
            }
            checkpoint = null;
        }
    }
    var repair_base: ?document_facts.BodyRef = if (fast_checkpoint) |saved| saved.repair_base else null;
    const baseline_vectors = document.object.get("chunk_embeddings");
    const baseline_previews = document.object.get("chunk_preview");
    current_identities = probe_identities;
    const cached_document_outputs = document.object.get("media_outputs");
    const has_output_rows = if (cached_document_outputs) |value| value == .object and value.object.contains("chunks") else false;
    const legacy_rows = document.object.get("chunk_embeddings");
    if (input != null and (has_output_rows or legacy_rows != null)) {
        var probe = try chunking.inference.probeModelPageWithProvider(a, base_config, input.?, self.model_chunk_provider);
        defer probe.deinit(a);
        if (self.generation_snapshot) |snapshot| if (probe.observedMetadata() != null) try snapshot.observe(a, probe.observedMetadata() orelse return error.InvalidChunkerResponse);
        if (has_output_rows) {
            const parsed = try std.json.parseFromValueLeaky(struct { chunks: []const media_outputs.Row, plan_id: []const u8 }, scratch, cached_document_outputs.?, .{ .ignore_unknown_fields = true });
            if (std.mem.eql(u8, probe.plan_id, parsed.plan_id) and probe.observedMetadata() != null) {
                cached_rows = parsed.chunks;
                current_identities = try serverlessOutputResults(scratch, wanted, probe.observedMetadata().?);
            }
        } else if (base_config.outputs_json.len == 0 and probe.observedMetadata() != null) {
            // Legacy single-embedding providers have no output recipes. Their
            // persisted descriptors must still authenticate the deployed model.
            const metadata = probe.observedMetadata() orelse return error.InvalidChunkerResponse;
            legacy_current = legacy_rows.? == .array and legacy_rows.?.array.items.len != 0;
            if (legacy_current) for (legacy_rows.?.array.items) |row| {
                if (row != .object) {
                    legacy_current = false;
                    break;
                }
                const descriptor = row.object.get("_chunk") orelse {
                    legacy_current = false;
                    break;
                };
                if (descriptor != .object) {
                    legacy_current = false;
                    break;
                }
                const names = [_][]const u8{ "plan_id", "source_fingerprint", "source_version", "model_identity", "model_revision" };
                const values = [_][]const u8{ metadata.plan_id, metadata.source_fingerprint, metadata.source_version, metadata.model_identity, metadata.model_revision };
                for (names, values) |name, expected| {
                    const value = descriptor.object.get(name) orelse {
                        legacy_current = false;
                        break;
                    };
                    if (value != .string or !std.mem.eql(u8, value.string, expected)) {
                        legacy_current = false;
                        break;
                    }
                }
                if (!legacy_current) break;
            };
        }
    }
    if (repair_base == null and cached_rows.len != 0) if (self.checkpoint_context) |context| if (!context.previous_is_compact and !had_checkpoint) {
        repair_base = context.previous;
    };
    if (fast_checkpoint != null and repair_base != null and cached_rows.len == 0) return error.InvalidMediaCheckpoint;
    // A configuration fingerprint cannot certify mutable deployed aliases.
    // Probe every configured recipe before the completed-document fast path.
    const current_outputs = input == null or legacy_current or (cached_rows.len != 0 and for (consumers.targets) |target| {
        for (cached_rows) |row| {
            if (needsServerlessOutput(target, row.outputs, current_identities)) break;
        } else continue;
        break false;
    } else true);
    if (current_outputs) {
        if (document.object.get("_enrichment")) |state| if (state == .object) {
            const old_recipe = state.object.get("chunk_embeddings_recipe");
            if (state.object.get("chunk_embeddings_version")) |version| if (version == .integer and version.integer >= pipeline_version and !serverlessMediaPending(state) and old_recipe != null and old_recipe.? == .string and std.mem.eql(u8, old_recipe.?.string, &recipe_fingerprint)) return .{};
        };
    }
    var config = base_config;
    config.page = true;
    config.max_chunks = if (config.max_chunks == 0) 64 else @min(config.max_chunks, 64);
    var position: ?chunking.types.Replay = null;
    defer if (position) |cursor| {
        a.free(cursor.plan_id);
        a.free(cursor.source_fingerprint);
        a.free(cursor.source_version);
    };
    var embeddings: std.ArrayListUnmanaged(std.json.Value) = .empty;
    var previews: std.ArrayListUnmanaged(std.json.Value) = .empty;
    var output_rows: std.ArrayList(media_outputs.Row) = .empty;
    var plan_id: []const u8 = "";
    var pending_outputs = false;
    var preparation_complete = true;
    var pages: usize = 0;
    var completed_pages: usize = 0;
    if (fast_checkpoint) |saved_group| {
        const saved = document.object.get("_enrichment").?;
        const cursor = try std.json.parseFromValueLeaky(chunking.types.Replay, scratch, saved.object.get("chunk_embeddings_cursor") orelse return error.InvalidMediaCheckpoint, .{});
        if (cursor.ordinal != saved_group.end or !std.mem.eql(u8, cursor.plan_id, saved_group.plan_id)) return error.InvalidMediaCheckpoint;
        position = try chunking.types.cloneReplay(a, cursor);
        plan_id = try scratch.dupe(u8, cursor.plan_id);
        const count = saved.object.get("chunk_embeddings_pages") orelse return error.InvalidMediaCheckpoint;
        if (count != .integer or count.integer <= 0) return error.InvalidMediaCheckpoint;
        completed_pages = std.math.cast(usize, count.integer) orelse return error.InvalidMediaCheckpoint;
    }
    if (fast_checkpoint == null and (cached_rows.len != 0 or legacy_current)) if (document.object.get("_enrichment")) |saved| if (saved == .object) {
        const fingerprint = saved.object.get("chunk_embeddings_preparation_recipe");
        const raw_cursor = saved.object.get("chunk_embeddings_cursor");
        if (fingerprint != null and fingerprint.? == .string and std.mem.eql(u8, fingerprint.?.string, &recipe_fingerprint) and raw_cursor != null) {
            const cursor = try std.json.parseFromValueLeaky(chunking.types.Replay, scratch, raw_cursor.?, .{});
            const old_vectors = document.object.get("chunk_embeddings");
            const legacy_prefix_rows = if (old_vectors != null and old_vectors.? == .array) old_vectors.?.array.items else &.{};
            const cached = if (cached_rows.len != 0) document.object.get("media_outputs").?.object.get("plan_id").? else legacy_prefix_rows[0].object.get("_chunk").?.object.get("plan_id").?;
            const prefix_count = if (cached_rows.len != 0) cached_rows.len else legacy_prefix_rows.len;
            const prefix_current = current: {
                for (cached_rows[0..@min(cached_rows.len, @as(usize, @intCast(cursor.ordinal)))]) |row| {
                    for (consumers.targets) |target| if (needsServerlessOutput(target, row.outputs, current_identities)) break :current false;
                }
                break :current true;
            };
            if (prefix_current and cached == .string and std.mem.eql(u8, cursor.plan_id, cached.string) and cursor.ordinal <= prefix_count) {
                if (saved.object.get("chunk_embeddings_pages")) |count| if (count == .integer and count.integer > 0) {
                    completed_pages = std.math.cast(usize, count.integer) orelse return error.InvalidChunkerResponse;
                };
                position = try chunking.types.cloneReplay(a, cursor);
                plan_id = try scratch.dupe(u8, cursor.plan_id);
                for (cached_rows[0..@min(cached_rows.len, @as(usize, @intCast(cursor.ordinal)))]) |row| {
                    try output_rows.append(scratch, row);
                    for (row.outputs) |result| pending_outputs = pending_outputs or (result.status != .complete and !result.terminal);
                }
                if (document.object.get("chunk_embeddings")) |old| if (old == .array) for (old.array.items) |row| {
                    if (row != .object) continue;
                    const descriptor = row.object.get("_chunk") orelse continue;
                    if (descriptor != .object) continue;
                    const unit = descriptor.object.get("descriptor") orelse continue;
                    if (unit != .object) continue;
                    const ordinal = unit.object.get("ordinal") orelse continue;
                    if (ordinal != .integer or ordinal.integer < 0 or ordinal.integer >= cursor.ordinal) continue;
                    try embeddings.append(scratch, row);
                    try previews.append(scratch, row.object.get("chunk") orelse .{ .string = "" });
                };
            }
        }
    };
    const group_start = if (position) |cursor| cursor.ordinal else 0;
    var covered: u64 = group_start;
    var group_generation: ?[32]u8 = if (fast_checkpoint) |saved| saved.generation else null;
    const page_limit = @min(self.media_page_limit, @max(completed_pages, 1));
    const checkpoint_deadline = @import("antfly_platform").time.monotonicNs() +| (std.math.mul(u64, self.media_checkpoint_interval_ms, std.time.ns_per_ms) catch std.math.maxInt(u64));
    while (input != null) {
        config = base_config;
        config.page = true;
        config.max_chunks = if (config.max_chunks == 0) 64 else @min(config.max_chunks, 64);
        config.replay = position;
        if (cached_rows.len != 0) {
            const targets = try scratch.dupe(media_outputs.Target, consumers.targets);
            const first: usize = @intCast(if (position) |cursor| cursor.ordinal else 0);
            for (targets) |*target| {
                target.execute = false;
                for (@min(first, cached_rows.len)..@min(first +| config.max_chunks, cached_rows.len)) |ordinal| {
                    if (needsServerlessOutput(target.*, cached_rows[ordinal].outputs, current_identities)) {
                        target.execute = true;
                        break;
                    }
                }
                if (first >= cached_rows.len) target.execute = true;
                var skips = std.ArrayList(media_outputs.Target.Window).empty;
                const current_recipe = for (current_identities) |identity| {
                    if (std.mem.eql(u8, identity.name, target.name)) break identity.recipe_id;
                } else "";
                for (@min(first, cached_rows.len)..@min(first +| config.max_chunks, cached_rows.len)) |ordinal| {
                    for (cached_rows[ordinal].outputs) |value| {
                        if (skips.items.len < 4096 and std.mem.eql(u8, value.name, target.name) and std.mem.eql(u8, value.recipe_id, current_recipe) and value.reusable()) try skips.append(scratch, .{ .ordinal = ordinal, .sub_index = value.sub_index });
                    }
                }
                target.skip_windows = skips.items;
            }
            config.embedding = null;
            config.outputs_json = try std.json.Stringify.valueAlloc(scratch, media_outputs.Config{ .boundary_model = consumers.boundary_model, .targets = targets }, .{ .emit_null_optional_fields = false });
        }

        var page = try chunking.inference.chunkPageWithProvider(a, config, input.?, self.model_chunk_provider);
        defer page.deinit(a);
        for (page.chunks) |chunk| {
            const metadata = chunk.metadata orelse return error.InvalidChunkerResponse;
            if (plan_id.len == 0) plan_id = try scratch.dupe(u8, page.plan_id);
            const fresh = try serverlessOutputResults(scratch, wanted, metadata);
            // Every page is another deployed-generation observation. A model
            // can change after the planning probe or between page requests.
            if (self.checkpoint_context != null) {
                const generation = try media_checkpoint.generations(consumers.targets, fresh);
                if (group_generation) |previous_generation| {
                    if (!std.mem.eql(u8, &previous_generation, &generation)) return error.ModelConfigurationChanged;
                } else group_generation = generation;
            }
            if (fresh.len != 0) current_identities = fresh;
            const cached: []const media_outputs.Result = if (chunk.id < cached_rows.len and cached_rows[chunk.id].chunk_id == chunk.id) cached_rows[chunk.id].outputs else &.{};
            const independent = try mergeServerlessOutputs(scratch, fresh, cached, self.media_output_max_attempts);
            for (independent) |result| if (result.status != .complete and !result.terminal) {
                pending_outputs = true;
            };
            if (independent.len != 0) try output_rows.append(scratch, .{ .chunk_id = chunk.id, .outputs = independent });
            try appendServerlessEmbeddingRows(scratch, wanted, metadata, independent, chunk.text orelse "", &embeddings, &previews);
        }
        pages += 1;
        covered = page.covered_through_ordinal;
        preparation_complete = page.complete;
        if (page.complete) break;
        if (position) |cursor| {
            a.free(cursor.plan_id);
            a.free(cursor.source_fingerprint);
            a.free(cursor.source_version);
            position = null;
        }
        const plan = try a.dupe(u8, page.plan_id);
        errdefer a.free(plan);
        const fingerprint = try a.dupe(u8, page.source_fingerprint);
        errdefer a.free(fingerprint);
        position = .{ .plan_id = plan, .source_fingerprint = fingerprint, .source_version = try a.dupe(u8, page.source_version), .ordinal = page.covered_through_ordinal };
        if (self.media_page_limit != 0 and (pages >= page_limit or (self.media_checkpoint_interval_ms != 0 and @import("antfly_platform").time.monotonicNs() >= checkpoint_deadline))) {
            pending_outputs = true;
            break;
        }
    }
    // The labeled version below also distinguishes a retryable failed output
    // from a healthy preparation cap. Terminal receipts are reusable.
    const can_compact = self.checkpoint_context != null and (cached_rows.len == 0 or repair_base != null or fast_checkpoint != null) and !preparation_complete and position != null and group_generation != null and healthy: {
        for (output_rows.items) |row| for (consumers.targets) |target| if (needsServerlessOutput(target, row.outputs, current_identities)) break :healthy false;
        break :healthy true;
    };
    if (!can_compact) if (fast_checkpoint != null) {
        try media_checkpoint.expandPrefix(scratch, self.checkpoint_context.?, &document);
        const old_vectors = document.object.get("chunk_embeddings").?.array.items;
        const old_previews = document.object.get("chunk_preview").?.array.items;
        const old_rows = try std.json.parseFromValueLeaky(struct { chunks: []const media_outputs.Row }, scratch, document.object.get("media_outputs").?, .{ .ignore_unknown_fields = true });
        try embeddings.insertSlice(scratch, 0, old_vectors);
        try previews.insertSlice(scratch, 0, old_previews);
        try output_rows.insertSlice(scratch, 0, old_rows.chunks);
    };
    if (!can_compact and !preparation_complete and covered < cached_rows.len) {
        try output_rows.appendSlice(scratch, cached_rows[@intCast(covered)..]);
        const index_recipe = for (current_identities) |identity| {
            if (std.mem.eql(u8, identity.name, "embedding")) break identity.recipe_id;
        } else "";
        if (baseline_vectors) |old| {
            if (old != .array or baseline_previews == null or baseline_previews.? != .array or old.array.items.len != baseline_previews.?.array.items.len) return error.InvalidChunkerResponse;
            for (old.array.items, baseline_previews.?.array.items) |row, preview| {
                const ordinal = try media_checkpoint.vectorOrdinal(row);
                if (ordinal < covered or !media_checkpoint.healthyIndex(cached_rows, ordinal, index_recipe)) continue;
                try embeddings.append(scratch, row);
                try previews.append(scratch, preview);
            }
        }
    }
    // Mark a mixed-generation traversal pending even if its source tail was
    // reached. The next pass repairs only receipts outside the latest space.
    for (output_rows.items) |row| for (consumers.targets) |target| {
        if (needsServerlessOutput(target, row.outputs, current_identities)) pending_outputs = true;
    };
    try document.object.put(scratch, "chunk_embeddings", .{ .array = embeddings.toManaged(scratch) });
    try document.object.put(scratch, "chunk_preview", .{ .array = previews.toManaged(scratch) });
    // These fields belong to this stage. Removing a configured consumer must
    // retire its previous values along with updating the recipe fingerprint.
    for ([_][]const u8{ "media_outputs", "transcript", "transcript_spans" }) |name| _ = document.object.swapRemove(name);
    if (output_rows.items.len != 0) {
        const projection = try media_outputs.project(scratch, output_rows.items);
        const encoded_outputs = try std.json.Stringify.valueAlloc(scratch, .{ .plan_id = plan_id, .chunks = output_rows.items, .complete_chunks = projection.complete_chunks, .failed_chunks = projection.failed_chunks, .terminal_chunks = projection.terminal_chunks, .status = projection.status }, .{ .emit_null_optional_fields = false });
        try document.object.put(scratch, "media_outputs", try std.json.parseFromSliceLeaky(std.json.Value, scratch, encoded_outputs, .{}));
        try document.object.put(scratch, "transcript", .{ .string = projection.text });
        const encoded_spans = try std.json.Stringify.valueAlloc(scratch, projection.transcript_spans, .{ .emit_null_optional_fields = false });
        try document.object.put(scratch, "transcript_spans", try std.json.parseFromSliceLeaky(std.json.Value, scratch, encoded_spans, .{}));
    }
    var state = document.object.get("_enrichment") orelse std.json.Value{ .object = .empty };
    if (state != .object) return error.InvalidChunkSource;
    try state.object.put(scratch, "chunk_embeddings", .{ .bool = !pending_outputs });
    try state.object.put(scratch, "chunk_embeddings_pending", .{ .bool = pending_outputs });
    try state.object.put(scratch, "chunk_embeddings_preparation_recipe", .{ .string = &recipe_fingerprint });
    if (!preparation_complete and position != null) {
        const encoded_cursor = try std.json.Stringify.valueAlloc(scratch, position.?, .{});
        try state.object.put(scratch, "chunk_embeddings_cursor", try std.json.parseFromSliceLeaky(std.json.Value, scratch, encoded_cursor, .{}));
        try state.object.put(scratch, "chunk_embeddings_pages", .{ .integer = @intCast(try std.math.add(usize, completed_pages, pages)) });
    } else {
        _ = state.object.swapRemove("chunk_embeddings_cursor");
        _ = state.object.swapRemove("chunk_embeddings_pages");
    }
    if (pending_outputs) {
        _ = state.object.swapRemove("chunk_embeddings_version");
        _ = state.object.swapRemove("chunk_embeddings_recipe");
    } else {
        try state.object.put(scratch, "chunk_embeddings_version", .{ .integer = pipeline_version });
        try state.object.put(scratch, "chunk_embeddings_recipe", .{ .string = &recipe_fingerprint });
        try state.object.put(scratch, "chunk_embeddings_recipe_format", .{ .integer = 2 });
    }
    try document.object.put(scratch, "_enrichment", state);
    if (can_compact) {
        const context = self.checkpoint_context.?;
        const index_recipe = for (current_identities) |identity| {
            if (std.mem.eql(u8, identity.name, "embedding")) break identity.recipe_id;
        } else return error.InvalidChunkOutput;
        try media_checkpoint.compact(scratch, context, &document, .{ .format = if (repair_base != null) 2 else 1, .repair_base = repair_base, .index_recipe = if (repair_base != null) index_recipe else null, .key_digest = context.key_digest, .recipe = &recipe_fingerprint, .generation = group_generation.?, .plan_id = plan_id, .start = if (fast_checkpoint != null) group_start else 0, .end = position.?.ordinal, .parent = if (fast_checkpoint != null) context.previous else null, .vectors = embeddings.items, .previews = previews.items, .rows = output_rows.items });
    }
    return .{ .body = try std.json.Stringify.valueAlloc(a, document, .{}), .used_model = true };
}

fn buildDerivedChunkPreviewBodyAlloc(alloc: Allocator, body: []const u8, pipeline_version: u32) !?[]u8 {
    var projection = try document_projection.parseAlloc(alloc, body);
    defer projection.deinit(alloc);
    if (projection.chunk_preview_version != null and projection.chunk_preview_version.? >= pipeline_version) {
        return null;
    }

    const normalized = try query_reader.normalizeAlloc(alloc, projection.text);
    defer alloc.free(normalized);
    if (normalized.len == 0) return null;

    const chunks = try buildChunkPreviewAlloc(alloc, normalized, 8);
    defer {
        for (chunks) |chunk| alloc.free(chunk);
        alloc.free(chunks);
    }
    if (chunks.len == 0) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJSONString(alloc, &out, "{");
    var wrote_field = false;
    try appendPreservedDocumentFieldsJSON(alloc, &out, body, &wrote_field);
    try appendJSONFieldName(alloc, &out, &wrote_field, "text");
    try appendJSONStringValue(alloc, &out, projection.text);
    if (projection.embedding) |embedding| {
        try appendJSONString(alloc, &out, ",\"embedding\":[");
        for (embedding, 0..) |value, idx| {
            if (idx != 0) try appendJSONString(alloc, &out, ",");
            const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
            defer alloc.free(num);
            try appendJSONString(alloc, &out, num);
        }
        try appendJSONString(alloc, &out, "]");
    }
    if (projection.sparse_embedding) |weights| {
        try appendJSONString(alloc, &out, ",\"sparse_embedding\":{");
        for (weights, 0..) |weight, idx| {
            if (idx != 0) try appendJSONString(alloc, &out, ",");
            try appendJSONStringValue(alloc, &out, weight.term);
            try appendJSONString(alloc, &out, ":");
            const num = try std.fmt.allocPrint(alloc, "{d}", .{weight.weight});
            defer alloc.free(num);
            try appendJSONString(alloc, &out, num);
        }
        try appendJSONString(alloc, &out, "}");
    }
    if (projection.graph_edges_json) |graph_edges_json| {
        try appendJSONString(alloc, &out, ",\"graph_edges\":");
        try appendJSONString(alloc, &out, graph_edges_json);
    }
    try appendJSONString(alloc, &out, ",\"chunk_preview\":");
    try appendStringSliceArrayJSON(alloc, &out, chunks);
    const version = try std.fmt.allocPrint(alloc, "{d}", .{pipeline_version});
    defer alloc.free(version);
    try appendJSONString(alloc, &out, ",\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":");
    try appendJSONString(alloc, &out, version);
    if (projection.lexical_sparse_version) |lexical_version| {
        const lexical = try std.fmt.allocPrint(alloc, "{d}", .{lexical_version});
        defer alloc.free(lexical);
        try appendJSONString(alloc, &out, ",\"lexical_sparse\":true,\"lexical_sparse_version\":");
        try appendJSONString(alloc, &out, lexical);
    }
    try appendJSONString(alloc, &out, "}}");
    return try out.toOwnedSlice(alloc);
}

fn buildDerivedChunkEmbeddingsBodyAlloc(
    self: *SparseEnricher,
    body: []const u8,
    pipeline_version: u32,
    model_preference: catalog_mod.EnrichmentModelPreference,
) !DerivedBodyResult {
    const alloc = self.alloc;
    var projection = try document_projection.parseAlloc(alloc, body);
    defer projection.deinit(alloc);
    if (projection.chunk_embeddings_version != null and projection.chunk_embeddings_version.? >= pipeline_version) {
        return .{};
    }

    var owned_chunks: ?[][]u8 = null;
    const chunks = if (projection.chunk_preview) |chunks|
        chunks
    else blk: {
        const normalized = try query_reader.normalizeAlloc(alloc, projection.text);
        defer alloc.free(normalized);
        if (normalized.len == 0) return .{};
        owned_chunks = try buildChunkPreviewAlloc(alloc, normalized, 8);
        break :blk owned_chunks.?;
    };
    defer if (owned_chunks) |value| {
        for (value) |chunk| alloc.free(chunk);
        alloc.free(value);
    };
    if (chunks.len == 0) return .{};

    const chunk_embedding_result = try buildChunkEmbeddingsAlloc(
        self,
        chunks,
        self.chunk_embedding_dims,
        model_preference,
    );
    const chunk_embeddings = chunk_embedding_result.embeddings;
    defer {
        for (chunk_embeddings) |*chunk_embedding| chunk_embedding.deinit(alloc);
        alloc.free(chunk_embeddings);
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJSONString(alloc, &out, "{");
    var wrote_field = false;
    try appendPreservedDocumentFieldsJSON(alloc, &out, body, &wrote_field);
    try appendJSONFieldName(alloc, &out, &wrote_field, "text");
    try appendJSONStringValue(alloc, &out, projection.text);
    if (projection.embedding) |embedding| {
        try appendEmbeddingJSON(alloc, &out, "embedding", embedding);
    }
    if (projection.sparse_embedding) |weights| {
        try appendSparseEmbeddingJSON(alloc, &out, weights);
    }
    if (projection.graph_edges_json) |graph_edges_json| {
        try appendJSONString(alloc, &out, ",\"graph_edges\":");
        try appendJSONString(alloc, &out, graph_edges_json);
    }
    try appendJSONString(alloc, &out, ",\"chunk_preview\":");
    try appendStringSliceArrayJSON(alloc, &out, chunks);
    try appendJSONString(alloc, &out, ",\"chunk_embeddings\":");
    try appendChunkEmbeddingsJSON(alloc, &out, chunk_embeddings);
    const version = try std.fmt.allocPrint(alloc, "{d}", .{pipeline_version});
    defer alloc.free(version);
    try appendJSONString(alloc, &out, ",\"_enrichment\":{\"chunk_embeddings\":true,\"chunk_embeddings_version\":");
    try appendJSONString(alloc, &out, version);
    if (projection.lexical_sparse_version) |lexical_version| {
        const lexical = try std.fmt.allocPrint(alloc, "{d}", .{lexical_version});
        defer alloc.free(lexical);
        try appendJSONString(alloc, &out, ",\"lexical_sparse\":true,\"lexical_sparse_version\":");
        try appendJSONString(alloc, &out, lexical);
    }
    if (projection.chunk_preview_version) |chunk_version| {
        const chunk = try std.fmt.allocPrint(alloc, "{d}", .{chunk_version});
        defer alloc.free(chunk);
        try appendJSONString(alloc, &out, ",\"chunk_preview\":true,\"chunk_preview_version\":");
        try appendJSONString(alloc, &out, chunk);
    }
    try appendJSONString(alloc, &out, "}}");
    return .{
        .body = try out.toOwnedSlice(alloc),
        .used_model = chunk_embedding_result.used_model,
        .used_fallback = chunk_embedding_result.used_fallback,
    };
}

fn buildDerivedRerankTermsBodyAlloc(alloc: Allocator, body: []const u8, pipeline_version: u32) !?[]u8 {
    var projection = try document_projection.parseAlloc(alloc, body);
    defer projection.deinit(alloc);
    if (projection.rerank_terms_version != null and projection.rerank_terms_version.? >= pipeline_version) {
        return null;
    }

    const normalized = try query_reader.normalizeAlloc(alloc, projection.text);
    defer alloc.free(normalized);
    if (normalized.len == 0) return null;

    const rerank_terms = try buildRerankTermsAlloc(alloc, normalized, projection.sparse_embedding, 8);
    defer {
        for (rerank_terms) |term| alloc.free(term);
        alloc.free(rerank_terms);
    }
    if (rerank_terms.len == 0) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJSONString(alloc, &out, "{");
    var wrote_field = false;
    try appendPreservedDocumentFieldsJSON(alloc, &out, body, &wrote_field);
    try appendJSONFieldName(alloc, &out, &wrote_field, "text");
    try appendJSONStringValue(alloc, &out, projection.text);
    if (projection.embedding) |embedding| {
        try appendJSONString(alloc, &out, ",\"embedding\":[");
        for (embedding, 0..) |value, idx| {
            if (idx != 0) try appendJSONString(alloc, &out, ",");
            const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
            defer alloc.free(num);
            try appendJSONString(alloc, &out, num);
        }
        try appendJSONString(alloc, &out, "]");
    }
    if (projection.sparse_embedding) |weights| {
        try appendSparseEmbeddingJSON(alloc, &out, weights);
    }
    if (projection.graph_edges_json) |graph_edges_json| {
        try appendJSONString(alloc, &out, ",\"graph_edges\":");
        try appendJSONString(alloc, &out, graph_edges_json);
    }
    if (projection.chunk_preview) |chunks| {
        try appendJSONString(alloc, &out, ",\"chunk_preview\":");
        try appendStringSliceArrayJSON(alloc, &out, chunks);
    }
    if (projection.chunk_embeddings) |chunk_embeddings| {
        try appendJSONString(alloc, &out, ",\"chunk_embeddings\":");
        try appendChunkEmbeddingsJSON(alloc, &out, chunk_embeddings);
    }
    try appendJSONString(alloc, &out, ",\"rerank_terms\":[");
    try appendStringSliceArrayJSON(alloc, &out, rerank_terms);
    const version = try std.fmt.allocPrint(alloc, "{d}", .{pipeline_version});
    defer alloc.free(version);
    try appendJSONString(alloc, &out, "],\"_enrichment\":{\"rerank_terms\":true,\"rerank_terms_version\":");
    try appendJSONString(alloc, &out, version);
    if (projection.lexical_sparse_version) |lexical_version| {
        const lexical = try std.fmt.allocPrint(alloc, "{d}", .{lexical_version});
        defer alloc.free(lexical);
        try appendJSONString(alloc, &out, ",\"lexical_sparse\":true,\"lexical_sparse_version\":");
        try appendJSONString(alloc, &out, lexical);
    }
    if (projection.chunk_preview_version) |chunk_version| {
        const chunk = try std.fmt.allocPrint(alloc, "{d}", .{chunk_version});
        defer alloc.free(chunk);
        try appendJSONString(alloc, &out, ",\"chunk_preview\":true,\"chunk_preview_version\":");
        try appendJSONString(alloc, &out, chunk);
    }
    if (projection.chunk_embeddings_version) |chunk_embeddings_version| {
        const chunk_embeddings = try std.fmt.allocPrint(alloc, "{d}", .{chunk_embeddings_version});
        defer alloc.free(chunk_embeddings);
        try appendJSONString(alloc, &out, ",\"chunk_embeddings\":true,\"chunk_embeddings_version\":");
        try appendJSONString(alloc, &out, chunk_embeddings);
    }
    try appendJSONString(alloc, &out, "}}");
    return try out.toOwnedSlice(alloc);
}

fn buildChunkPreviewAlloc(alloc: Allocator, normalized: []const u8, words_per_chunk: usize) ![][]u8 {
    var chunks = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (chunks.items) |chunk| alloc.free(chunk);
        chunks.deinit(alloc);
    }

    var iter = std.mem.tokenizeAny(u8, normalized, " ");
    var current = std.ArrayListUnmanaged(u8).empty;
    defer current.deinit(alloc);
    var word_count: usize = 0;
    while (iter.next()) |token| {
        if (word_count != 0) try current.append(alloc, ' ');
        try current.appendSlice(alloc, token);
        word_count += 1;
        if (word_count >= words_per_chunk) {
            try chunks.append(alloc, try current.toOwnedSlice(alloc));
            current = .empty;
            word_count = 0;
        }
    }
    if (current.items.len != 0) {
        try chunks.append(alloc, try current.toOwnedSlice(alloc));
    }
    return try chunks.toOwnedSlice(alloc);
}

fn buildChunkEmbeddingsAlloc(
    self: *SparseEnricher,
    chunks: []const []const u8,
    dims: usize,
    model_preference: catalog_mod.EnrichmentModelPreference,
) !struct {
    embeddings: []document_projection.ChunkEmbedding,
    used_model: bool,
    used_fallback: bool,
} {
    const alloc = self.alloc;
    const out = try alloc.alloc(document_projection.ChunkEmbedding, chunks.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*chunk_embedding| chunk_embedding.deinit(alloc);
    }
    var used_model = false;
    var used_fallback = false;
    for (chunks, 0..) |chunk, idx| {
        const embedding_result = try buildDenseEmbeddingAlloc(self, chunk, dims, model_preference);
        out[idx] = .{
            .chunk = try alloc.dupe(u8, chunk),
            .embedding = embedding_result.embedding,
        };
        used_model = used_model or embedding_result.used_model;
        used_fallback = used_fallback or embedding_result.used_fallback;
        initialized += 1;
    }
    return .{
        .embeddings = out,
        .used_model = used_model,
        .used_fallback = used_fallback,
    };
}

fn shouldAttemptModel(model_preference: catalog_mod.EnrichmentModelPreference) bool {
    return switch (model_preference) {
        .deterministic_only => false,
        .prefer_model, .require_model => true,
    };
}

fn buildDenseEmbeddingAlloc(
    self: *SparseEnricher,
    text: []const u8,
    dims: usize,
    model_preference: catalog_mod.EnrichmentModelPreference,
) !struct {
    embedding: []f32,
    used_model: bool,
    used_fallback: bool,
} {
    const alloc = self.alloc;
    switch (model_preference) {
        .deterministic_only => return .{
            .embedding = try buildDeterministicEmbeddingAlloc(alloc, text, dims),
            .used_model = false,
            .used_fallback = false,
        },
        .prefer_model => {
            if (self.chunk_embedder) |embedder| {
                const embedding_name = self.chunk_embedding_name orelse return EnrichmentError.MissingChunkEmbeddingName;
                const model_result = embedder.embedDense(alloc, embedding_name, text, @intCast(dims)) catch null;
                if (model_result) |result| {
                    return .{ .embedding = result, .used_model = true, .used_fallback = false };
                }
                return .{
                    .embedding = try buildDeterministicEmbeddingAlloc(alloc, text, dims),
                    .used_model = false,
                    .used_fallback = true,
                };
            }
            return .{
                .embedding = try buildDeterministicEmbeddingAlloc(alloc, text, dims),
                .used_model = false,
                .used_fallback = true,
            };
        },
        .require_model => {
            const embedder = self.chunk_embedder orelse return EnrichmentError.RequiredChunkEmbeddingModelUnavailable;
            const embedding_name = self.chunk_embedding_name orelse return EnrichmentError.MissingChunkEmbeddingName;
            const result = embedder.embedDense(alloc, embedding_name, text, @intCast(dims)) catch return EnrichmentError.ChunkEmbeddingModelFailed;
            return .{ .embedding = result, .used_model = true, .used_fallback = false };
        },
    }
}

fn isRecoverableEnrichmentError(err: anyerror) bool {
    return err == EnrichmentError.MissingSparseEmbeddingName or
        err == EnrichmentError.MissingChunkEmbeddingName or
        err == EnrichmentError.RequiredSparseModelUnavailable or
        err == EnrichmentError.RequiredChunkEmbeddingModelUnavailable or
        err == EnrichmentError.SparseEmbeddingModelFailed or
        err == EnrichmentError.ChunkEmbeddingModelFailed;
}

const FailingDenseEmbedder = struct {
    fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
        return error.TestDenseModelFailure;
    }

    fn interface() embedder_mod.DenseEmbedder {
        return .{
            .ptr = undefined,
            .dense_embed_fn = embedDense,
            .deinit_fn = null,
        };
    }
};

const FailingSparseEmbedder = struct {
    fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !embedder_mod.SparseEmbedding {
        return error.TestSparseModelFailure;
    }

    fn interface() embedder_mod.SparseEmbedder {
        return .{
            .ptr = undefined,
            .sparse_embed_fn = embedSparse,
            .deinit_fn = null,
        };
    }
};

const TrackingEmbedder = struct {
    deinit_count: *usize,

    fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
        return error.UnexpectedEmbeddingCall;
    }

    fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !embedder_mod.SparseEmbedding {
        return error.UnexpectedEmbeddingCall;
    }

    pub fn deinit(ptr: *anyopaque, _: Allocator) void {
        const self: *TrackingEmbedder = @ptrCast(@alignCast(ptr));
        self.deinit_count.* += 1;
    }

    fn denseInterface(self: *TrackingEmbedder) embedder_mod.DenseEmbedder {
        return .{ .ptr = self, .dense_embed_fn = embedDense, .deinit_fn = deinit };
    }

    fn sparseInterface(self: *TrackingEmbedder) embedder_mod.SparseEmbedder {
        return .{ .ptr = self, .sparse_embed_fn = embedSparse, .deinit_fn = deinit };
    }
};

test "serverless sparse enricher embedder replacement is transactional on allocation failure" {
    var old_sparse_deinits: usize = 0;
    var replacement_sparse_deinits: usize = 0;
    var old_dense_deinits: usize = 0;
    var replacement_dense_deinits: usize = 0;
    var old_sparse = TrackingEmbedder{ .deinit_count = &old_sparse_deinits };
    var replacement_sparse = TrackingEmbedder{ .deinit_count = &replacement_sparse_deinits };
    var old_dense = TrackingEmbedder{ .deinit_count = &old_dense_deinits };
    var replacement_dense = TrackingEmbedder{ .deinit_count = &replacement_dense_deinits };

    var enricher = SparseEnricher.init(std.testing.allocator, undefined, undefined, undefined, undefined);
    defer enricher.deinit();
    try enricher.setSparseEmbedder(old_sparse.sparseInterface(), "old_sparse");
    try enricher.setChunkEmbedder(old_dense.denseInterface(), "old_dense", 32);

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    enricher.alloc = failing.allocator();
    try std.testing.expectError(error.OutOfMemory, enricher.setSparseEmbedder(replacement_sparse.sparseInterface(), "new_sparse"));
    replacement_sparse.sparseInterface().deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("old_sparse", enricher.sparse_embedding_name.?);
    try std.testing.expectEqual(@as(usize, 0), old_sparse_deinits);

    failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    enricher.alloc = failing.allocator();
    try std.testing.expectError(error.OutOfMemory, enricher.setChunkEmbedder(replacement_dense.denseInterface(), "new_dense", 64));
    replacement_dense.denseInterface().deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("old_dense", enricher.chunk_embedding_name.?);
    try std.testing.expectEqual(@as(u32, 32), enricher.chunk_embedding_dims);
    try std.testing.expectEqual(@as(usize, 0), old_dense_deinits);

    enricher.alloc = std.testing.allocator;
    enricher.clearSparseEmbedder();
    enricher.clearChunkEmbedder();
    try std.testing.expectEqual(@as(usize, 1), old_sparse_deinits);
    try std.testing.expectEqual(@as(usize, 1), replacement_sparse_deinits);
    try std.testing.expectEqual(@as(usize, 1), old_dense_deinits);
    try std.testing.expectEqual(@as(usize, 1), replacement_dense_deinits);
}
const CancelingSparseEmbedder = struct {
    requested: *std.atomic.Value(bool),

    fn embedSparse(
        ptr: *anyopaque,
        alloc: Allocator,
        _: []const u8,
        _: []const u8,
    ) !embedder_mod.SparseEmbedding {
        const self: *CancelingSparseEmbedder = @ptrCast(@alignCast(ptr));
        const indices = try alloc.dupe(u32, &.{1});
        errdefer alloc.free(indices);
        const values = try alloc.dupe(f32, &.{1.0});
        self.requested.store(true, .release);
        return .{ .indices = indices, .values = values };
    }

    fn interface(self: *CancelingSparseEmbedder) embedder_mod.SparseEmbedder {
        return .{
            .ptr = self,
            .sparse_embed_fn = embedSparse,
            .deinit_fn = null,
        };
    }
};

const CancelAfterAppendWal = struct {
    inner: *wal_mod.WalStore,
    requested: *std.atomic.Value(bool),

    fn walStore(self: *@This()) wal_mod.WalStore {
        return .{
            .allocator = self.inner.allocator,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable: wal_mod.WalStore.VTable = .{
        .deinit = deinit,
        .append = append,
        .read_from_alloc = readFromAlloc,
        .latest_lsn = latestLsn,
        .truncate_prefix = truncatePrefix,
    };

    pub fn deinit(_: Allocator, _: *anyopaque) void {}

    fn append(ptr: *anyopaque, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const lsn = try self.inner.append(namespace, timestamp_ns, payload);
        self.requested.store(true, .release);
        return lsn;
    }

    fn readFromAlloc(ptr: *anyopaque, alloc: Allocator, namespace: []const u8, start_lsn: u64) ![]wal_mod.Record {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.vtable.read_from_alloc(self.inner.ptr, alloc, namespace, start_lsn);
    }

    fn latestLsn(ptr: *anyopaque, namespace: []const u8) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.latestLsn(namespace);
    }

    fn truncatePrefix(ptr: *anyopaque, namespace: []const u8, keep_from_lsn: u64) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.truncatePrefix(namespace, keep_from_lsn);
    }
};

const InjectBeforeConditionalAppendWal = struct {
    inner: *wal_mod.WalStore,
    injected_payload: []const u8,
    injected: bool = false,

    fn walStore(self: *@This()) wal_mod.WalStore {
        return .{
            .allocator = self.inner.allocator,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable: wal_mod.WalStore.VTable = .{
        .deinit = deinit,
        .append = append,
        .append_idempotent_if_latest = appendIdempotentIfLatest,
        .read_from_alloc = readFromAlloc,
        .latest_lsn = latestLsn,
        .truncate_prefix = truncatePrefix,
    };

    pub fn deinit(_: Allocator, _: *anyopaque) void {}

    fn append(ptr: *anyopaque, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.append(namespace, timestamp_ns, payload);
    }

    fn appendIdempotentIfLatest(
        ptr: *anyopaque,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
        expected_latest_lsn: u64,
    ) !?u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!self.injected) {
            _ = try self.inner.append(namespace, timestamp_ns + 1, self.injected_payload);
            self.injected = true;
        }
        return try self.inner.appendIdempotentIfLatest(
            namespace,
            timestamp_ns,
            payload,
            operation_id,
            expected_latest_lsn,
        );
    }

    fn readFromAlloc(ptr: *anyopaque, alloc: Allocator, namespace: []const u8, start_lsn: u64) ![]wal_mod.Record {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.vtable.read_from_alloc(self.inner.ptr, alloc, namespace, start_lsn);
    }

    fn latestLsn(ptr: *anyopaque, namespace: []const u8) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.latestLsn(namespace);
    }

    fn truncatePrefix(ptr: *anyopaque, namespace: []const u8, keep_from_lsn: u64) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.truncatePrefix(namespace, keep_from_lsn);
    }
};

fn buildDeterministicEmbeddingAlloc(alloc: Allocator, text: []const u8, dims: usize) ![]f32 {
    const embedding = try alloc.alloc(f32, dims);
    @memset(embedding, 0);

    var token_count: usize = 0;
    var iter = std.mem.tokenizeAny(u8, text, " ");
    while (iter.next()) |token| {
        token_count += 1;
        for (0..dims) |dim| {
            var hasher = std.hash.Wyhash.init(@as(u64, @intCast(dim + 1)));
            hasher.update(token);
            const hashed = hasher.final();
            const low_bits: u32 = @truncate(hashed & 0xffff);
            const centered_i32: i32 = @as(i32, @intCast(low_bits)) - 32768;
            const centered = @as(f32, @floatFromInt(centered_i32));
            embedding[dim] += centered / 32768.0;
        }
    }
    if (token_count == 0) return embedding;
    const denom = @as(f32, @floatFromInt(token_count));
    var norm: f32 = 0;
    for (embedding) |*value| {
        value.* /= denom;
        norm += value.* * value.*;
    }
    if (norm > 0) {
        const scale = @as(f32, 1.0) / @sqrt(norm);
        for (embedding) |*value| value.* *= scale;
    }
    return embedding;
}

const WeightedTerm = struct {
    term: []const u8,
    weight: f32,
};

fn buildRerankTermsAlloc(
    alloc: Allocator,
    normalized: []const u8,
    sparse_embedding: ?[]const document_projection.SparseTermWeight,
    limit: usize,
) ![][]u8 {
    if (sparse_embedding) |weights| {
        var ranked = try alloc.alloc(WeightedTerm, weights.len);
        defer alloc.free(ranked);
        for (weights, 0..) |weight, idx| {
            ranked[idx] = .{ .term = weight.term, .weight = weight.weight };
        }
        std.mem.sort(WeightedTerm, ranked, {}, struct {
            fn lessThan(_: void, a: WeightedTerm, b: WeightedTerm) bool {
                if (a.weight == b.weight) return std.mem.lessThan(u8, a.term, b.term);
                return a.weight > b.weight;
            }
        }.lessThan);
        const count = @min(limit, ranked.len);
        const out = try alloc.alloc([]u8, count);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |term| alloc.free(term);
            alloc.free(out);
        }
        for (ranked[0..count], 0..) |entry, idx| {
            out[idx] = try alloc.dupe(u8, entry.term);
            initialized += 1;
        }
        return out;
    }

    var counts = std.StringArrayHashMapUnmanaged(u32).empty;
    defer {
        for (counts.keys()) |term| alloc.free(term);
        counts.deinit(alloc);
    }
    var iter = std.mem.tokenizeAny(u8, normalized, " ");
    while (iter.next()) |token| {
        const owned = try alloc.dupe(u8, token);
        errdefer alloc.free(owned);
        const gop = try counts.getOrPut(alloc, owned);
        if (!gop.found_existing) {
            gop.value_ptr.* = 0;
        } else {
            alloc.free(owned);
        }
        gop.value_ptr.* += 1;
    }
    var ranked = try alloc.alloc(WeightedTerm, counts.count());
    defer alloc.free(ranked);
    for (counts.keys(), counts.values(), 0..) |term, count, idx| {
        ranked[idx] = .{ .term = term, .weight = @floatFromInt(count) };
    }
    std.mem.sort(WeightedTerm, ranked, {}, struct {
        fn lessThan(_: void, a: WeightedTerm, b: WeightedTerm) bool {
            if (a.weight == b.weight) return std.mem.lessThan(u8, a.term, b.term);
            return a.weight > b.weight;
        }
    }.lessThan);
    const count = @min(limit, ranked.len);
    const out = try alloc.alloc([]u8, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |term| alloc.free(term);
        alloc.free(out);
    }
    for (ranked[0..count], 0..) |entry, idx| {
        out[idx] = try alloc.dupe(u8, entry.term);
        initialized += 1;
    }
    return out;
}

fn appendJSONString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try out.appendSlice(alloc, value);
}

fn appendJSONFieldName(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    wrote_field: *bool,
    key: []const u8,
) !void {
    if (wrote_field.*) try appendJSONString(alloc, out, ",");
    try appendJSONStringValue(alloc, out, key);
    try appendJSONString(alloc, out, ":");
    wrote_field.* = true;
}

fn appendJSONStringValue(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    defer writer.deinit();
    try std.json.Stringify.value(value, .{}, &writer.writer);
    try out.appendSlice(alloc, writer.written());
}

fn appendJSONValue(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: std.json.Value) !void {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    defer writer.deinit();
    try std.json.Stringify.value(value, .{}, &writer.writer);
    try out.appendSlice(alloc, writer.written());
}

fn appendPreservedDocumentFieldsJSON(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    body: []const u8,
    wrote_field: *bool,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .object) return;

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (isDerivedEnrichmentField(entry.key_ptr.*)) continue;
        try appendJSONFieldName(alloc, out, wrote_field, entry.key_ptr.*);
        try appendJSONValue(alloc, out, entry.value_ptr.*);
    }
}

fn isDerivedEnrichmentField(key: []const u8) bool {
    return std.mem.eql(u8, key, "text") or
        std.mem.eql(u8, key, "embedding") or
        std.mem.eql(u8, key, "sparse_embedding") or
        std.mem.eql(u8, key, "graph_edges") or
        std.mem.eql(u8, key, "chunk_preview") or
        std.mem.eql(u8, key, "chunk_embeddings") or
        std.mem.eql(u8, key, "rerank_terms") or
        std.mem.eql(u8, key, "_enrichment");
}

fn appendEmbeddingJSON(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), key: []const u8, embedding: []const f32) !void {
    try appendJSONString(alloc, out, ",\"");
    try appendJSONString(alloc, out, key);
    try appendJSONString(alloc, out, "\":[");
    for (embedding, 0..) |value, idx| {
        if (idx != 0) try appendJSONString(alloc, out, ",");
        const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(num);
        try appendJSONString(alloc, out, num);
    }
    try appendJSONString(alloc, out, "]");
}

fn appendSparseEmbeddingJSON(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    weights: []const document_projection.SparseTermWeight,
) !void {
    try appendJSONString(alloc, out, ",\"sparse_embedding\":{");
    for (weights, 0..) |weight, idx| {
        if (idx != 0) try appendJSONString(alloc, out, ",");
        try appendJSONStringValue(alloc, out, weight.term);
        try appendJSONString(alloc, out, ":");
        const num = try std.fmt.allocPrint(alloc, "{d}", .{weight.weight});
        defer alloc.free(num);
        try appendJSONString(alloc, out, num);
    }
    try appendJSONString(alloc, out, "}");
}

fn appendStringSliceArrayJSON(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const []const u8,
) !void {
    try appendJSONString(alloc, out, "[");
    for (values, 0..) |value, idx| {
        if (idx != 0) try appendJSONString(alloc, out, ",");
        try appendJSONStringValue(alloc, out, value);
    }
    try appendJSONString(alloc, out, "]");
}

fn appendChunkEmbeddingsJSON(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    chunk_embeddings: []const document_projection.ChunkEmbedding,
) !void {
    try appendJSONString(alloc, out, "[");
    for (chunk_embeddings, 0..) |chunk_embedding, idx| {
        if (idx != 0) try appendJSONString(alloc, out, ",");
        try appendJSONString(alloc, out, "{\"chunk\":");
        try appendJSONStringValue(alloc, out, chunk_embedding.chunk);
        try appendJSONString(alloc, out, ",\"embedding\":[");
        for (chunk_embedding.embedding, 0..) |value, emb_idx| {
            if (emb_idx != 0) try appendJSONString(alloc, out, ",");
            const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
            defer alloc.free(num);
            try appendJSONString(alloc, out, num);
        }
        try appendJSONString(alloc, out, "]");
        if (chunk_embedding.descriptor_json) |recipe| {
            try appendJSONString(alloc, out, ",\"_chunk\":");
            try appendJSONString(alloc, out, recipe);
        }
        try appendJSONString(alloc, out, "}");
    }
    try appendJSONString(alloc, out, "]");
}

test "serverless sparse enricher appends derived sparse mutation when published docs lack sparse features" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests");
    const wal_root = tmpPath(&wal_root_buf, "wal");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo alpha\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);

    const tail = try wal_store.readFromAlloc("docs", 2);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"sparse_embedding\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"alpha\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"lexical_sparse_version\":1") != null);
}

test "serverless enrichment fails closed before a non-idempotent append" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-cancel-after-append");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-cancel-after-append");
    const wal_root = tmpPath(&wal_root_buf, "wal-cancel-after-append");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();
    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();
    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]api_types.DocumentMutation{.{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"alpha bravo\"}",
    }};
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var requested = std.atomic.Value(bool).init(false);
    var canceling_impl = CancelAfterAppendWal{ .inner = &wal_store, .requested = &requested };
    var canceling_wal = canceling_impl.walStore();
    defer canceling_wal.deinit();
    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &canceling_wal);
    try std.testing.expectError(
        error.IdempotentAppendUnsupported,
        enricher.runNamespaceWithConfig("docs", .{
            .batch_size = 4,
            .pipeline_version = lexical_sparse_enrichment_version,
        }),
    );
    try std.testing.expectEqual(@as(?u64, null), try progress_store.getEnrichmentDocOffset("docs"));
    try std.testing.expectEqual(@as(u64, 1), try wal_store.latestLsn("docs"));
}

test "serverless enrichment preserves successive partial facts publications and graph edges" {
    const a = std.testing.allocator;
    var artifact_buf: [256]u8 = undefined;
    var manifest_buf: [256]u8 = undefined;
    var wal_buf: [256]u8 = undefined;
    const artifact_path = tmpPath(&artifact_buf, "facts-artifacts");
    const manifest_path = tmpPath(&manifest_buf, "facts-manifests");
    const wal_path = tmpPath(&wal_buf, "facts-wal");
    defer cleanupTmp(artifact_path);
    defer cleanupTmp(manifest_path);
    defer cleanupTmp(wal_path);
    var artifact_impl = try artifacts_mod.FsStore.init(a, std.mem.span(artifact_path));
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();
    var manifest_impl = try manifest_mod.FsStore.init(a, std.mem.span(manifest_path));
    var manifests = manifest_impl.manifestStore();
    defer manifests.deinit();
    var progress_impl = try catalog_mod.FsProgressStore.init(a, std.mem.span(manifest_path));
    var progress = progress_impl.progressStore();
    defer progress.deinit();
    var wal_impl = try wal_mod.FsStore.init(a, std.mem.span(wal_path));
    var wal = wal_impl.walStore();
    defer wal.deinit();
    var builder = @import("../build/builder.zig").Builder.init(a, &artifacts, &manifests, &progress, &wal);
    var api = @import("../api/service.zig").Service.init(a, &wal, &builder);
    const batches = [_][]const api_types.DocumentMutation{
        &.{ .{ .kind = .upsert, .doc_id = "a", .body = "{\"text\":\"original\"}" }, .{ .kind = .upsert, .doc_id = "b", .body = "{\"text\":\"before\"}" } },
        &.{.{ .kind = .upsert, .doc_id = "a", .body = "{\"text\":\"updated alpha\",\"graph_edges\":[{\"target\":\"c\",\"edge_type\":\"link\",\"weight\":2}]}" }},
        &.{.{ .kind = .upsert, .doc_id = "b", .body = "{\"text\":\"updated bravo\"}" }},
    };
    for (batches, 0..) |batch, i| {
        var ingested = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = i + 100, .mutations = batch });
        defer ingested.deinit(a);
        var built = try publishEnrichmentFixture(&builder);
        built.deinit(a);
    }
    const previous_lsn = try wal.latestLsn("docs");
    var enricher = SparseEnricher.init(a, &artifacts, &manifests, &progress, &wal);
    defer enricher.deinit();
    const stats = try enricher.runNamespaceWithConfig("docs", .{ .scan_batch_size = 8 });
    try std.testing.expectEqual(@as(usize, 2), stats.enriched_documents);
    try std.testing.expect((try progress.getManifestReadDeadline("docs", 3)) != null);
    const tail = try wal.readFromAlloc("docs", previous_lsn + 1);
    defer wal_mod.freeRecords(a, tail);
    try std.testing.expectEqual(@as(usize, 2), tail.len);
    var first = try api_codec.decodeMutationAlloc(a, tail[0].payload);
    defer first.deinit(a);
    var parsed = try @import("antfly-json").parseFromSlice(std.json.Value, a, first.body.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("a", first.doc_id);
    try std.testing.expectEqualStrings("updated alpha", parsed.value.object.get("text").?.string);
    try std.testing.expectEqualStrings("c", parsed.value.object.get("graph_edges").?.array.items[0].object.get("target").?.string);
    var published = try publishEnrichmentFixture(&builder);
    published.deinit(a);
    var current = try manifests.getAlloc("docs", try progress.getHead("docs"));
    defer current.deinit(a);
    var reads: u64 = 1024 * 1024;
    var writes: u64 = 0;
    var pages: PageStore = .{ .domain = PageStore.namespaceDomain("docs"), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const graph_root = try pages.loadRoot(a, current.artifacts[findArtifactIndex(current, .graph_segment).?]);
    var edges = try @import("../graph_segment/page_graph.zig").Cursor.adjacency(a, pages.store(), graph_root, "a", .outgoing, "link");
    defer edges.deinit();
    try std.testing.expectEqualStrings("c", (try edges.next()).?.target);
    try std.testing.expect(try edges.next() == null);

    // Completed documents are absent from the stage index: even a root-only
    // read allowance can determine completion without loading any body/page.
    const stable_lsn = try wal.latestLsn("docs");
    const first_scan = try enricher.runNamespaceWithConfig("docs", .{ .scan_batch_size = 1, .max_source_read_bytes = document_facts.Root.encoded_bytes });
    try std.testing.expectEqual(@as(usize, 0), first_scan.wal_appends);
    try std.testing.expectEqual(@as(u64, 0), (try progress.getEnrichmentStageProgress("docs", .lexical_sparse)).?.doc_offset);
    const second_scan = try enricher.runNamespaceWithConfig("docs", .{ .scan_batch_size = 1 });
    try std.testing.expectEqual(@as(usize, 0), second_scan.wal_appends);
    try std.testing.expectEqual(@as(u64, 0), (try progress.getEnrichmentStageProgress("docs", .lexical_sparse)).?.doc_offset);
    try std.testing.expectEqual(stable_lsn, try wal.latestLsn("docs"));
}

test "serverless enrichment pending key cursor admits large bodies and preserves fair retries across publications" {
    const a = std.testing.allocator;
    var artifact_buf: [256]u8 = undefined;
    var manifest_buf: [256]u8 = undefined;
    var wal_buf: [256]u8 = undefined;
    const artifact_path = tmpPath(&artifact_buf, "cursor-artifacts");
    const manifest_path = tmpPath(&manifest_buf, "cursor-manifests");
    const wal_path = tmpPath(&wal_buf, "cursor-wal");
    defer cleanupTmp(artifact_path);
    defer cleanupTmp(manifest_path);
    defer cleanupTmp(wal_path);
    var artifact_impl = try artifacts_mod.FsStore.init(a, std.mem.span(artifact_path));
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();
    var manifest_impl = try manifest_mod.FsStore.init(a, std.mem.span(manifest_path));
    var manifests = manifest_impl.manifestStore();
    defer manifests.deinit();
    var progress_impl = try catalog_mod.FsProgressStore.init(a, std.mem.span(manifest_path));
    var progress = progress_impl.progressStore();
    defer progress.deinit();
    var wal_impl = try wal_mod.FsStore.init(a, std.mem.span(wal_path));
    var wal = wal_impl.walStore();
    defer wal.deinit();
    var builder = @import("../build/builder.zig").Builder.init(a, &artifacts, &manifests, &progress, &wal);
    var api = @import("../api/service.zig").Service.init(a, &wal, &builder);
    const padding = try a.alloc(u8, 16 * 1024);
    defer a.free(padding);
    @memset(padding, 'x');
    const large = try std.fmt.allocPrint(a, "{{\"text\":\"alpha\",\"padding\":\"{s}\"}}", .{padding});
    defer a.free(large);
    var inserted = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 1, .mutations = &.{
        .{ .kind = .upsert, .doc_id = "a-large", .body = large },
        .{ .kind = .upsert, .doc_id = "z-small", .body = "{\"text\":\"bravo\"}" },
    } });
    inserted.deinit(a);
    var published = try publishEnrichmentFixture(&builder);
    published.deinit(a);
    var enricher = SparseEnricher.init(a, &artifacts, &manifests, &progress, &wal);
    defer enricher.deinit();
    var cfg = SparseEnricherConfig{ .scan_batch_size = 1, .batch_size = 1, .max_source_read_bytes = 1, .model_preference = .deterministic_only };
    // A hard admission failure is durably observable and advances the stable
    // key, without claiming the document complete or blocking the healthy tail.
    cfg.document_limits.max_working_set_bytes = 4096;
    const rejected = try enricher.runNamespaceWithConfig("docs", cfg);
    try std.testing.expectEqual(@as(usize, 1), rejected.failed_documents);
    try std.testing.expectEqual(@as(usize, 0), rejected.wal_appends);
    var checkpoint = (try progress.getEnrichmentStageProgress("docs", .lexical_sparse)).?;
    defer checkpoint.deinit(a);
    try std.testing.expectEqualStrings("a-large", checkpoint.after_order_key.?[8..]);
    try std.testing.expectEqualStrings("z-small", checkpoint.cycle_upper_order_key.?[8..]);
    try std.testing.expectEqual(@as(u64, 1), checkpoint.failed_documents);
    // Reopen durable progress and publish unrelated data before the next tick.
    var reopened_impl = try catalog_mod.FsProgressStore.init(a, std.mem.span(manifest_path));
    var reopened = reopened_impl.progressStore();
    defer reopened.deinit();
    enricher.progress = &reopened;
    var unrelated = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 2, .mutations = &.{
        .{ .kind = .upsert, .doc_id = "0-complete", .body = "{\"text\":\"already complete\",\"sparse_embedding\":{\"done\":1},\"_enrichment\":{\"lexical_sparse_version\":1}}" },
        .{ .kind = .upsert, .doc_id = "m", .body = "{\"text\":\"new tail\"}" },
    } });
    unrelated.deinit(a);
    var next_head = try publishEnrichmentFixture(&builder);
    next_head.deinit(a);
    cfg.document_limits.max_working_set_bytes = (build_limits.Limits{}).max_working_set_bytes;
    // The authenticated envelope is not part of publication's body limit.
    cfg.document_limits.max_input_bytes = large.len;
    const healthy = try enricher.runNamespaceWithConfig("docs", cfg);
    try std.testing.expectEqual(@as(usize, 1), healthy.enriched_documents);
    const healthy_tail = try wal.readFromAlloc("docs", try wal.latestLsn("docs"));
    defer wal_mod.freeRecords(a, healthy_tail);
    var healthy_mutation = try api_codec.decodeMutationAlloc(a, healthy_tail[0].payload);
    defer healthy_mutation.deinit(a);
    try std.testing.expectEqualStrings("z-small", healthy_mutation.doc_id);
    var with_healthy = try publishEnrichmentFixture(&builder);
    with_healthy.deinit(a);
    var arrival = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 3, .mutations = &.{.{ .kind = .upsert, .doc_id = "mm", .body = "{\"text\":\"new tail\"}" }} });
    arrival.deinit(a);
    var arrival_head = try publishEnrichmentFixture(&builder);
    arrival_head.deinit(a);
    // The old cycle wraps even though its range now has an ever-growing tail.
    // Capacity can recover without editing the rejected document.
    const wrapped = try enricher.runNamespaceWithConfig("docs", cfg);
    try std.testing.expectEqual(@as(usize, 0), wrapped.wal_appends);
    var wrapped_progress = (try reopened.getEnrichmentStageProgress("docs", .lexical_sparse)).?;
    defer wrapped_progress.deinit(a);
    try std.testing.expectEqual(@as(u64, 1), wrapped_progress.completed_cycles);
    try std.testing.expectEqual(null, wrapped_progress.cycle_upper_order_key);
    arrival = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 4, .mutations = &.{.{ .kind = .upsert, .doc_id = "mmm", .body = "{\"text\":\"new tail\"}" }} });
    arrival.deinit(a);
    arrival_head = try publishEnrichmentFixture(&builder);
    arrival_head.deinit(a);
    const retried = try enricher.runNamespaceWithConfig("docs", cfg);
    try std.testing.expectEqual(@as(usize, 1), retried.enriched_documents);
    try std.testing.expectEqual(@as(usize, 0), retried.failed_documents);
    const large_tail = try wal.readFromAlloc("docs", try wal.latestLsn("docs"));
    defer wal_mod.freeRecords(a, large_tail);
    var large_mutation = try api_codec.decodeMutationAlloc(a, large_tail[0].payload);
    defer large_mutation.deinit(a);
    try std.testing.expectEqualStrings("a-large", large_mutation.doc_id);
    try std.testing.expect(large_mutation.body.?.len > cfg.max_source_read_bytes);

    // An update behind the cursor must also be revisited within the finite
    // cycle while each subsequent pass adds another higher pending key.
    var recovered_head = try publishEnrichmentFixture(&builder);
    recovered_head.deinit(a);
    var update = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 5, .mutations = &.{.{ .kind = .upsert, .doc_id = "a-large", .body = "{\"text\":\"updated behind cursor\"}" }} });
    update.deinit(a);
    var saw_updated = false;
    for (4..9) |i| {
        const id_buf = @as([16]u8, @splat('m'));
        const id = id_buf[0..i];
        arrival = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = i + 10, .mutations = &.{.{ .kind = .upsert, .doc_id = id, .body = "{\"text\":\"new tail\"}" }} });
        arrival.deinit(a);
        arrival_head = try publishEnrichmentFixture(&builder);
        arrival_head.deinit(a);
        const pass = try enricher.runNamespaceWithConfig("docs", cfg);
        if (pass.wal_appends == 0) continue;
        const records = try wal.readFromAlloc("docs", try wal.latestLsn("docs"));
        defer wal_mod.freeRecords(a, records);
        var mutation = try api_codec.decodeMutationAlloc(a, records[0].payload);
        defer mutation.deinit(a);
        if (std.mem.eql(u8, mutation.doc_id, "a-large")) {
            try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "updated behind cursor") != null);
            saw_updated = true;
            break;
        }
    }
    try std.testing.expect(saw_updated);
    // A policy-version transition discards both coordinates of the old cycle,
    // even when that bound is beyond every key in the new pending source.
    recovered_head = try publishEnrichmentFixture(&builder);
    recovered_head.deinit(a);
    update = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 30, .mutations = &.{.{ .kind = .upsert, .doc_id = "a-large", .body = "{\"text\":\"new policy pending\"}" }} });
    update.deinit(a);
    recovered_head = try publishEnrichmentFixture(&builder);
    recovered_head.deinit(a);
    var before_reset = (try reopened.getEnrichmentStageProgress("docs", .lexical_sparse)).?;
    defer before_reset.deinit(a);
    var old_policy = before_reset;
    old_policy.revision += 1;
    old_policy.pipeline_version = cfg.pipeline_version + 1;
    old_policy.after_order_key = "\xff\xff\xff\xff\xff\xff\xff\xffzzzz";
    old_policy.cycle_upper_order_key = "\xff\xff\xff\xff\xff\xff\xff\xffzzzz";
    try std.testing.expect(try reopened.compareAndSwapEnrichmentStageProgress("docs", .lexical_sparse, before_reset, old_policy));
    const reset = try enricher.runNamespaceWithConfig("docs", cfg);
    try std.testing.expectEqual(@as(usize, 1), reset.enriched_documents);
    var after_reset = (try reopened.getEnrichmentStageProgress("docs", .lexical_sparse)).?;
    defer after_reset.deinit(a);
    try std.testing.expectEqualStrings("mmmm", after_reset.after_order_key.?[8..]);
    try std.testing.expect(std.mem.order(u8, after_reset.cycle_upper_order_key.?, old_policy.cycle_upper_order_key.?) == .lt);
    try std.testing.expectEqual(cfg.pipeline_version, after_reset.pipeline_version);
    recovered_head = try publishEnrichmentFixture(&builder);
    recovered_head.deinit(a);
    // Extraction-policy semantics can change without a pipeline version bump.
    // The facts fingerprint is an equally strong cycle reset boundary.
    var old_semantics = after_reset;
    old_semantics.revision += 1;
    old_semantics.policy_fingerprint[0] ^= 1;
    old_semantics.after_order_key = old_policy.after_order_key;
    old_semantics.cycle_upper_order_key = old_policy.cycle_upper_order_key;
    try std.testing.expect(try reopened.compareAndSwapEnrichmentStageProgress("docs", .lexical_sparse, after_reset, old_semantics));
    const semantic_reset = try enricher.runNamespaceWithConfig("docs", cfg);
    try std.testing.expectEqual(@as(usize, 1), semantic_reset.enriched_documents);
    var after_semantic_reset = (try reopened.getEnrichmentStageProgress("docs", .lexical_sparse)).?;
    defer after_semantic_reset.deinit(a);
    try std.testing.expectEqualStrings("mmmmm", after_semantic_reset.after_order_key.?[8..]);
    try std.testing.expectEqual(after_reset.policy_fingerprint, after_semantic_reset.policy_fingerprint);
}

test "serverless enrichment WAL fence rejects a user mutation that lands during model work" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-stale-wal-fence");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-stale-wal-fence");
    const wal_root = tmpPath(&wal_root_buf, "wal-stale-wal-fence");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();
    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();
    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const initial = [_]api_types.DocumentMutation{.{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"stale source\"}",
    }};
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &initial });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    const user_delete = api_types.DocumentMutation{ .kind = .delete, .doc_id = "doc-a" };
    const encoded_delete = try api_codec.encodeMutationAlloc(alloc, user_delete);
    defer alloc.free(encoded_delete);
    var injecting_impl = InjectBeforeConditionalAppendWal{
        .inner = &wal_store,
        .injected_payload = encoded_delete,
    };
    var injecting_wal = injecting_impl.walStore();
    defer injecting_wal.deinit();
    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &injecting_wal);
    try std.testing.expectError(
        error.EnrichmentProgressChanged,
        enricher.runNamespaceWithConfig("docs", .{
            .batch_size = 4,
            .pipeline_version = lexical_sparse_enrichment_version,
        }),
    );

    const tail = try wal_store.readFromAlloc("docs", 2);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expectEqual(api_types.MutationKind.delete, mutation.kind);
    try std.testing.expectEqual(@as(u64, 2), try wal_store.latestLsn("docs"));
}

test "serverless object enrichment writes a stable idempotent WAL identity" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var artifact_impl = try artifacts_object_store.ObjectStore.initWithClient(alloc, memory.client(), "enrichment-artifacts", "tenant");
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();
    var manifest_impl = try manifest_object_store.ObjectStore.initWithClient(alloc, memory.client(), "enrichment-manifests", "tenant");
    var manifests = manifest_impl.manifestStore();
    defer manifests.deinit();
    var progress_impl = try progress_object_store.ObjectProgressStore.initWithClient(alloc, memory.client(), "enrichment-progress", "tenant");
    var progress = progress_impl.progressStore();
    defer progress.deinit();
    var wal_impl = try wal_object_store.ObjectStore.initWithClient(alloc, memory.client(), "enrichment-wal", "tenant");
    var wal = wal_impl.walStore();
    defer wal.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifacts, &manifests, &progress, &wal);
    var api = @import("../api/service.zig").Service.init(alloc, &wal, &builder);
    const batch = [_]api_types.DocumentMutation{.{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"alpha bravo\"}",
    }};
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifacts, &manifests, &progress, &wal);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.wal_appends);
    const tail = try wal.readFromAlloc("docs", 2);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    try std.testing.expectEqual(@as(?u64, 1), try operation_identity.sourceHeadVersion(tail[0].operation_id));
    try std.testing.expect(std.mem.startsWith(u8, tail[0].operation_id.?, operation_identity.prefix));
    try std.testing.expectEqual(
        tail[0].lsn,
        try wal.appendIdempotent(
            "docs",
            tail[0].timestamp_ns,
            tail[0].payload,
            tail[0].operation_id.?,
        ),
    );
    try std.testing.expectEqual(@as(u64, 2), try wal.latestLsn("docs"));
}

test "serverless sparse enricher can append derived chunk preview mutation" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-preview");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-preview");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-preview");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie delta echo foxtrot golf hotel india\",\"graph_edges\":[{\"target\":\"doc-b\",\"edge_type\":\"cites\",\"weight\":1.0}]}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = chunk_preview_enrichment_version,
        .stage = .chunk_preview,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);

    const tail = try wal_store.readFromAlloc("docs", 2);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"chunk_preview\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"graph_edges\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"chunk_preview_version\":1") != null);
}

test "serverless sparse enricher can append derived rerank terms mutation" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-rerank-terms");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-rerank-terms");
    const wal_root = tmpPath(&wal_root_buf, "wal-rerank-terms");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo alpha charlie\",\"sparse_embedding\":{\"alpha\":0.9,\"charlie\":0.4,\"bravo\":0.5},\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1,\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = rerank_terms_enrichment_version,
        .stage = .rerank_terms,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);

    const tail = try wal_store.readFromAlloc("docs", 2);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"rerank_terms\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"rerank_terms_version\":1") != null);
}

test "serverless sparse enricher can append derived chunk embeddings mutation" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-embeddings");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-embeddings");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-embeddings");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie delta echo foxtrot golf hotel india\",\"sparse_embedding\":{\"alpha\":1.0},\"graph_edges\":[{\"target\":\"doc-b\",\"edge_type\":\"cites\",\"weight\":1.0}],\"chunk_preview\":[\"alpha bravo charlie delta echo foxtrot golf hotel\",\"india\"],\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1,\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = chunk_embeddings_enrichment_version,
        .stage = .chunk_embeddings,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);

    const tail = try wal_store.readFromAlloc("docs", 2);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"chunk_embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"graph_edges\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"chunk_embeddings_version\":1") != null);
}

test "serverless sparse enricher idles when unpublished tail already exists" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-tail");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-tail");
    const wal_root = tmpPath(&wal_root_buf, "wal-tail");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);
    var tail_ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 101, .mutations = &batch });
    defer tail_ingest.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 0), stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), stats.idle_namespaces);
}

test "serverless sparse enricher skips docs already enriched at current version" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-current");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-current");
    const wal_root = tmpPath(&wal_root_buf, "wal-current");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo alpha\",\"sparse_embedding\":{\"alpha\":0.66},\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 0), stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), stats.idle_namespaces);
    try std.testing.expectEqual(@as(u64, 1), try wal_store.latestLsn("docs"));
}

test "serverless sparse enricher can use model-backed dense and sparse embedders" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-model-backed");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-model-backed");
    const wal_root = tmpPath(&wal_root_buf, "wal-model-backed");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie delta echo foxtrot golf hotel india\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    defer enricher.deinit();
    try enricher.setSparseEmbedder(deterministic_sparse.interface(), "serverless_sparse");
    try enricher.setChunkEmbedder(deterministic_dense.interface(), "serverless_chunk", 6);

    const sparse_stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
        .stage = .lexical_sparse,
        .model_preference = .prefer_model,
    });
    try std.testing.expectEqual(@as(usize, 1), sparse_stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), sparse_stats.model_documents);
    try std.testing.expectEqual(@as(usize, 0), sparse_stats.fallback_documents);

    var build_after_sparse = try publishEnrichmentFixture(&builder);
    defer build_after_sparse.deinit(alloc);

    const chunk_stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = chunk_embeddings_enrichment_version,
        .stage = .chunk_embeddings,
        .model_preference = .prefer_model,
    });
    try std.testing.expectEqual(@as(usize, 1), chunk_stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), chunk_stats.model_documents);
    try std.testing.expectEqual(@as(usize, 0), chunk_stats.fallback_documents);

    const tail = try wal_store.readFromAlloc("docs", 3);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"chunk_embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"f") != null);

    const second_batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "{\"text\":\"juliet kilo lima\"}" },
    };
    var second_ingest = try api.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 200,
        .mutations = &second_batch,
    });
    defer second_ingest.deinit(alloc);
    var second_build = try publishEnrichmentFixture(&builder);
    defer second_build.deinit(alloc);

    var requested: std.atomic.Value(bool) = .init(false);
    var canceling_sparse = CancelingSparseEmbedder{ .requested = &requested };
    try enricher.setSparseEmbedder(canceling_sparse.interface(), "serverless_sparse");
    const before_cancel_lsn = try wal_store.latestLsn("docs");
    try std.testing.expectError(
        error.Canceled,
        enricher.runNamespaceWithConfigUntil("docs", .{
            .batch_size = 4,
            .pipeline_version = lexical_sparse_enrichment_version,
            .stage = .lexical_sparse,
            .model_preference = .prefer_model,
        }, .{
            .io = std.Options.debug_io,
            .requested = &requested,
        }),
    );
    try std.testing.expectEqual(before_cancel_lsn, try wal_store.latestLsn("docs"));
}

test "serverless sparse enricher prefers model but falls back deterministically when sparse model fails" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-sparse-fallback");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-sparse-fallback");
    const wal_root = tmpPath(&wal_root_buf, "wal-sparse-fallback");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    defer enricher.deinit();
    try enricher.setSparseEmbedder(FailingSparseEmbedder.interface(), "serverless_sparse");

    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
        .stage = .lexical_sparse,
        .model_preference = .prefer_model,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 0), stats.model_documents);
    try std.testing.expectEqual(@as(usize, 1), stats.fallback_documents);
    try std.testing.expectEqual(@as(usize, 0), stats.failed_documents);
}

test "serverless sparse enricher can require chunk embedding model and fail stage" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-require-chunk-model");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-require-chunk-model");
    const wal_root = tmpPath(&wal_root_buf, "wal-require-chunk-model");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie\",\"chunk_preview\":[\"alpha bravo\",\"charlie\"],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    defer enricher.deinit();
    try enricher.setChunkEmbedder(FailingDenseEmbedder.interface(), "serverless_chunk", 6);

    try std.testing.expectError(
        EnrichmentError.ChunkEmbeddingModelFailed,
        enricher.runNamespaceWithConfig("docs", .{
            .batch_size = 4,
            .pipeline_version = chunk_embeddings_enrichment_version,
            .stage = .chunk_embeddings,
            .model_preference = .require_model,
            .failure_policy = .fail_stage,
        }),
    );
}

test "serverless sparse enricher advances progress in batches" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-batch");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-batch");
    const wal_root = tmpPath(&wal_root_buf, "wal-batch");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\"}" },
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "{\"text\":\"bravo\"}" },
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "{\"text\":\"charlie\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const first = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 2,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 2), first.enriched_documents);
    var stage_progress = (try progress_store.getEnrichmentStageProgress("docs", .lexical_sparse)).?;
    defer stage_progress.deinit(progress_store.allocator);
    try std.testing.expectEqual(build.version, stage_progress.head_version);
    try std.testing.expectEqual(@as(u64, 2), stage_progress.doc_offset);
    // The atomic source-bound tuple is authoritative, including after reopen;
    // the old independent unscoped offset is no longer a producer output.
    var reopened_fs = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var reopened = reopened_fs.progressStore();
    defer reopened.deinit();
    var reopened_progress = (try reopened.getEnrichmentStageProgress("docs", .lexical_sparse)).?;
    defer reopened_progress.deinit(reopened.allocator);
    try std.testing.expectEqualDeep(stage_progress, reopened_progress);

    const second = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 2,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 0), second.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), second.idle_namespaces);
}

fn publishEnrichmentFixture(builder: *@import("../build/builder.zig").Builder) !@import("../build/builder.zig").BuildResult {
    return builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = .{ .published_search_sources = @import("../search_sources.zig").defaultPublishedSearchSources(), .include_graph = true },
        .policy = .{ .enrichment_enabled = true, .chunk_preview_enabled = true, .chunk_embeddings_enabled = true, .rerank_terms_enabled = true },
    });
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-enrichment-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "model aware media serverless chunks use the shared paged provider and retain recipes" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        first_calls: usize = 0,
        fail_second: bool = false,
        fn page(ptr: *anyopaque, alloc: Allocator, _: []const u8, input: chunking.inference.RemoteInput, cfg: chunking.types.Config, _: @import("antfly_inference_execution_context").RequestContext) anyerror!chunking.inference.ChunkPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try @import("std").testing.expect(input == .source);
            self.calls += 1;
            const ordinal = if (cfg.replay) |cursor| cursor.ordinal else 0;
            var executing = if (cfg.embedding) |target| target.execute orelse true else false;
            if (cfg.outputs_json.len != 0) {
                var targets = try std.json.parseFromSlice(media_outputs.Config, alloc, cfg.outputs_json, .{});
                defer targets.deinit();
                executing = (try targets.value.boundary()).wants(ordinal, 0);
            }
            if (executing and ordinal == 0) self.first_calls += 1;
            if (executing and ordinal == 1 and self.fail_second) return error.ChunkRequestFailed;
            const id = try std.fmt.allocPrint(alloc, "{s}:{d}", .{ "1111111111111111111111111111111111111111111111111111111111111111", ordinal });
            defer alloc.free(id);
            const chunks = try alloc.alloc(chunking.inference.RemoteChunk, 1);
            errdefer alloc.free(chunks);
            chunks[0] = .{ .id = @intCast(ordinal), .mime_type = "video/mp4" };
            chunks[0].metadata = try (chunking.inference.RemoteMetadata{ .descriptor = .{ .ordinal = ordinal, .input_tokens = 1, .coordinates = .{ .kind = .video, .start_ns = @intCast(ordinal * 100), .end_ns = @intCast((ordinal + 1) * 100) } }, .inference_id = id, .plan_id = "1111111111111111111111111111111111111111111111111111111111111111", .source_fingerprint = "2222222222222222222222222222222222222222222222222222222222222222", .source_version = "v1", .model = "model", .model_identity = "3333333333333333333333333333333333333333333333333333333333333333", .model_revision = "4444444444444444444444444444444444444444444444444444444444444444", .task_type = "RETRIEVAL_DOCUMENT", .dimensions = 2, .policy = .{}, .embedding = &.{ 0.6, 0.8 } }).clone(alloc);
            chunks[0].owns_metadata = true;
            errdefer chunks[0].deinit(alloc);
            if (cfg.outputs_json.len != 0) {
                var consumers = try std.json.parseFromSlice(media_outputs.Config, alloc, cfg.outputs_json, .{});
                defer consumers.deinit();
                const target = consumers.value.targets[0];
                if (cfg.observe_only) try std.testing.expect(!target.execute);
                const metadata = chunks[0].metadata.?;
                const recipe_id = try target.recipeIdentity(alloc, metadata.model_identity, metadata.model_revision);
                chunks[0].metadata.?.outputs_json = try std.json.Stringify.valueAlloc(alloc, [_]media_outputs.Result{.{ .name = target.name, .kind = target.kind, .required = target.required, .model = target.model, .model_identity = metadata.model_identity, .model_revision = metadata.model_revision, .recipe_id = &recipe_id, .status = if (executing) .complete else .skipped, .embedding = if (executing) &.{ 0.6, 0.8 } else null }}, .{ .emit_null_optional_fields = false });
            }

            const plan = try alloc.dupe(u8, "1111111111111111111111111111111111111111111111111111111111111111");
            errdefer alloc.free(plan);
            const source = try alloc.dupe(u8, "2222222222222222222222222222222222222222222222222222222222222222");
            errdefer alloc.free(source);
            return .{ .chunks = chunks, .plan_id = plan, .source_fingerprint = source, .source_version = try alloc.dupe(u8, "v1"), .start_ordinal = ordinal, .covered_through_ordinal = ordinal + 1, .complete = ordinal == 1 };
        }
    };
    var fake = Fake{};
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "model", .dimensions = 2 }, .max_chunks = 1 }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });
    const result = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, "{\"clip\":{\"type\":\"source\",\"uri\":\"s3://bucket/clip.mp4\",\"mime_type\":\"video/mp4\",\"version\":\"v1\",\"version_kind\":\"s3_version_id\",\"length\":100},\"media_outputs\":{},\"transcript\":\"stale speech\",\"transcript_spans\":[],\"_enrichment\":{\"lexical_sparse_version\":1}}", 2, .require_model);
    defer a.free(result.body.?);
    try std.testing.expect(result.used_model);
    var rebuilt = try std.json.parseFromSlice(std.json.Value, a, result.body.?, .{});
    defer rebuilt.deinit();
    try std.testing.expectEqualStrings("", rebuilt.value.object.get("transcript").?.string);
    try std.testing.expectEqual(@as(usize, 0), rebuilt.value.object.get("transcript_spans").?.array.items.len);
    for (rebuilt.value.object.get("media_outputs").?.object.get("chunks").?.array.items) |row| {
        for (row.object.get("outputs").?.array.items) |output| try std.testing.expectEqualStrings("embedding", output.object.get("name").?.string);
    }
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    var projection = try document_projection.parseAlloc(a, result.body.?);
    defer projection.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), projection.chunk_embeddings.?.len);
    try std.testing.expectEqual(@as(?u32, 1), projection.lexical_sparse_version);
    try std.testing.expectEqual(@as(?u32, 2), projection.chunk_embeddings_version);
    const recipe = projection.chunk_embeddings.?[1].descriptor_json.?;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, recipe, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("descriptor").?.object.get("ordinal").?.integer);
    try std.testing.expect(parsed.value.object.get("embedding") == null);
    const skipped = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, result.body.?, 2, .require_model);
    try std.testing.expect(skipped.body == null);
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    worker.model_chunker.?.max_chunks = 2;
    const repaged = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, result.body.?, 2, .require_model);
    try std.testing.expect(repaged.body == null);
    try std.testing.expectEqual(@as(usize, 4), fake.calls);
    // The worker publishes a bounded prefix to WAL before asking for more.
    worker.media_page_limit = 1;
    worker.model_chunker.?.max_chunks = 1;
    fake.first_calls = 0;
    const first_page = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, "{\"clip\":{\"type\":\"source\",\"uri\":\"s3://bucket/clip.mp4\",\"mime_type\":\"video/mp4\",\"version\":\"v1\",\"version_kind\":\"s3_version_id\",\"length\":100}}", 2, .require_model);
    defer a.free(first_page.body.?);
    var prefix = try document_projection.parseAlloc(a, first_page.body.?);
    defer prefix.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), prefix.chunk_embeddings.?.len);
    try std.testing.expect(prefix.chunk_embeddings_version == null);
    fake.fail_second = true;
    try std.testing.expectError(error.ChunkRequestFailed, buildDerivedBodyAlloc(&worker, .chunk_embeddings, first_page.body.?, 2, .require_model));
    fake.fail_second = false;
    const resumed = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, first_page.body.?, 2, .require_model);
    defer a.free(resumed.body.?);
    var complete = try document_projection.parseAlloc(a, resumed.body.?);
    defer complete.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), complete.chunk_embeddings.?.len);
    try std.testing.expectEqual(@as(?u32, 2), complete.chunk_embeddings_version);
    try std.testing.expectEqual(@as(usize, 1), fake.first_calls);
}

test "model aware media serverless enrichment stages preserve independent outputs and metadata" {
    const a = std.testing.allocator;
    const original = "{\"text\":\"hello\",\"chunk_embeddings\":[{\"vector\":[1,0],\"_chunk\":{\"plan_id\":\"plan\"}}],\"_enrichment\":{\"chunk_embeddings_version\":2,\"chunk_embeddings_recipe\":\"recipe\",\"lexical_sparse_version\":1}}";
    const produced = "{\"text\":\"hello\",\"sparse_embedding\":{\"x\":1},\"_enrichment\":{\"lexical_sparse_version\":3}}";
    const merged = try preserveOtherEnrichmentOutputsAlloc(a, original, produced, .lexical_sparse);
    defer a.free(merged);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, merged, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.contains("chunk_embeddings"));
    const state = parsed.value.object.get("_enrichment").?.object;
    try std.testing.expectEqual(@as(i64, 3), state.get("lexical_sparse_version").?.integer);
    try std.testing.expectEqualStrings("recipe", state.get("chunk_embeddings_recipe").?.string);
}

test "model aware media serverless named vectors and failed outputs resume independently to success or terminal" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var fake = Fake{};
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });
    const first = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, "{\"text\":\"\",\"clip\":\"audio source\"}", 1, .require_model);
    defer a.free(first.body.?);
    var projection = try document_projection.parseAlloc(a, first.body.?);
    defer projection.deinit(a);
    try std.testing.expect(projection.chunk_embeddings_pending);
    try std.testing.expect(projection.chunk_embeddings_version == null);
    try std.testing.expectEqual(@as(usize, 2), projection.chunk_embeddings.?.len);
    for (projection.chunk_embeddings.?) |row| try std.testing.expectEqualSlices(f32, &.{ 0, 1 }, row.embedding);
    const second = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, first.body.?, 1, .require_model);
    defer a.free(second.body.?);
    try std.testing.expectEqualDeep([_]usize{ 1, 1, 2 }, fake.calls);
    fake.fail = false;
    const success = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, second.body.?, 1, .require_model);
    defer a.free(success.body.?);
    var success_projection = try document_projection.parseAlloc(a, success.body.?);
    defer success_projection.deinit(a);
    try std.testing.expect(!success_projection.chunk_embeddings_pending);
    try std.testing.expectEqual(@as(?u32, 1), success_projection.chunk_embeddings_version);
    try std.testing.expectEqualDeep([_]usize{ 1, 1, 3 }, fake.calls);
    fake.fail = true;
    const terminal = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, second.body.?, 1, .require_model);
    defer a.free(terminal.body.?);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, terminal.body.?, .{});
    defer parsed.deinit();
    const outputs = parsed.value.object.get("media_outputs").?.object;
    const speech = outputs.get("chunks").?.array.items[0].object.get("outputs").?.array.items[3].object;
    try std.testing.expect(speech.get("terminal").?.bool);
    try std.testing.expectEqual(@as(i64, 3), speech.get("attempts").?.integer);
    try std.testing.expectEqualStrings("failed", speech.get("status").?.string);
    const settled = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, terminal.body.?, 1, .require_model);
    try std.testing.expect(settled.body == null);
}

test "model aware media serverless index contract retains tasks and model pins" {
    const a = std.testing.allocator;
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined, .chunk_embedding_dims = 2 };
    defer worker.deinit();
    const pin: [64]u8 = @splat('a');
    const encoded = try std.fmt.allocPrint(a, "{{\"dense\":{{\"field\":\"clip\",\"embedder\":{{\"provider\":\"antfly\",\"model\":\"model\",\"task_type\":\"CLASSIFICATION\",\"model_identity\":\"{s}\"}}}}}}", .{pin});
    defer a.free(encoded);
    var indexes = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
    defer indexes.deinit();
    try worker.configureModelChunkerFromIndexes(indexes.value, "dense", .{});
    try std.testing.expectEqualStrings("CLASSIFICATION", worker.model_chunker.?.embedding.?.task_type.?);
    try std.testing.expectEqualStrings(&pin, worker.model_chunker.?.embedding.?.model_identity.?);
    try std.testing.expectEqual(@as(?i64, 2), worker.model_chunker.?.embedding.?.dimensions);
    const conflicting = "{\"dense\":{\"embedder\":{\"provider\":\"antfly\",\"model\":\"model\",\"task_type\":\"CLASSIFICATION\"},\"chunker\":{\"provider\":\"antfly\",\"embedding\":{\"model\":\"model\",\"task_type\":\"RETRIEVAL_DOCUMENT\",\"dimensions\":2}}}}";
    var bad = try std.json.parseFromSlice(std.json.Value, a, conflicting, .{});
    defer bad.deinit();
    try std.testing.expectError(error.InvalidChunkerConfig, worker.configureModelChunkerFromIndexes(bad.value, "dense", .{}));
}

test "model aware media serverless legacy output attempts survive retry" {
    const Fake = struct {
        fn page(_: *anyopaque, a: Allocator, _: []const u8, _: chunking.inference.RemoteInput, _: chunking.types.Config, _: @import("antfly_inference_execution_context").RequestContext) anyerror!chunking.inference.ChunkPage {
            const plan = "1111111111111111111111111111111111111111111111111111111111111111";
            const fingerprint = "2222222222222222222222222222222222222222222222222222222222222222";
            const identity = "3333333333333333333333333333333333333333333333333333333333333333";
            const revision = "4444444444444444444444444444444444444444444444444444444444444444";
            const recipe = try (media_outputs.Target{ .name = "embedding", .kind = .embedding, .model = "model", .dimensions = 2 }).recipeIdentity(a, identity, revision);
            const encoded = try std.json.Stringify.valueAlloc(a, [_]media_outputs.Result{.{ .name = "embedding", .kind = .embedding, .required = true, .model = "model", .model_identity = identity, .model_revision = revision, .recipe_id = &recipe, .status = .failed, .code = "Failed" }}, .{ .emit_null_optional_fields = false });
            defer a.free(encoded);
            const chunks = try a.alloc(chunking.inference.RemoteChunk, 1);
            errdefer a.free(chunks);
            chunks[0] = .{ .id = 0, .mime_type = "audio/wav", .metadata = try (chunking.inference.RemoteMetadata{ .descriptor = .{ .ordinal = 0, .input_tokens = 1, .coordinates = .{ .kind = .audio, .start_ns = 0, .end_ns = 100 } }, .inference_id = plan ++ ":0", .plan_id = plan, .source_fingerprint = fingerprint, .source_version = "v1", .model = "model", .model_identity = identity, .model_revision = revision, .task_type = "RETRIEVAL_DOCUMENT", .dimensions = 2, .policy = .{}, .embedding_error = "Failed", .outputs_json = null }).clone(a), .owns_metadata = true };
            errdefer chunks[0].deinit(a);
            const owned_plan = try a.dupe(u8, plan);
            errdefer a.free(owned_plan);
            const owned_fingerprint = try a.dupe(u8, fingerprint);
            errdefer a.free(owned_fingerprint);
            return .{ .chunks = chunks, .plan_id = owned_plan, .source_fingerprint = owned_fingerprint, .source_version = try a.dupe(u8, "v1"), .start_ordinal = 0, .covered_through_ordinal = 1, .complete = true };
        }
    };
    const a = std.testing.allocator;
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    var fake: u8 = 0;
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "model", .dimensions = 2 } }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });
    const first = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, "{\"clip\":\"audio source\"}", 1, .require_model);
    defer a.free(first.body.?);
    const second = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, first.body.?, 1, .require_model);
    defer a.free(second.body.?);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, second.body.?, .{});
    defer parsed.deinit();
    const output = parsed.value.object.get("media_outputs").?.object.get("chunks").?.array.items[0].object.get("outputs").?.array.items[0].object;
    try std.testing.expectEqual(@as(i64, 2), output.get("attempts").?.integer);
    const third = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, second.body.?, 1, .require_model);
    defer a.free(third.body.?);
    var terminal = try std.json.parseFromSlice(std.json.Value, a, third.body.?, .{});
    defer terminal.deinit();
    const receipt = terminal.value.object.get("media_outputs").?.object.get("chunks").?.array.items[0].object.get("outputs").?.array.items[0].object;
    try std.testing.expectEqual(@as(i64, 3), receipt.get("attempts").?.integer);
    try std.testing.expect(receipt.get("terminal").?.bool);
    const settled = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, third.body.?, 1, .require_model);
    try std.testing.expect(settled.body == null);
}

test "model aware media serverless consumer edits preserve healthy model work" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var fake = Fake{};
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });
    const first = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, "{\"text\":\"\",\"clip\":\"audio source\"}", 1, .require_model);
    defer a.free(first.body.?);
    var projection = try document_projection.parseAlloc(a, first.body.?);
    defer projection.deinit(a);
    try std.testing.expect(projection.chunk_embeddings_pending);
    try std.testing.expect(projection.chunk_embeddings_version == null);
    try std.testing.expectEqual(@as(usize, 2), projection.chunk_embeddings.?.len);
    for (projection.chunk_embeddings.?) |row| try std.testing.expectEqualSlices(f32, &.{ 0, 1 }, row.embedding);
    const changed = try std.mem.replaceOwned(u8, a, worker.model_chunker.?.outputs_json, "\"model\":\"speech\"", "\"model\":\"speech\",\"language\":\"en\"");
    a.free(worker.model_chunker.?.outputs_json);
    worker.model_chunker.?.outputs_json = changed;
    const second = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, first.body.?, 1, .require_model);
    defer a.free(second.body.?);
    try std.testing.expectEqualDeep([_]usize{ 1, 1, 2 }, fake.calls);
    var cleared_source = try std.json.parseFromSlice(std.json.Value, a, second.body.?, .{});
    defer cleared_source.deinit();
    try cleared_source.value.object.put(cleared_source.arena.allocator(), "clip", .null);
    const cleared_body = try std.json.Stringify.valueAlloc(a, cleared_source.value, .{});
    defer a.free(cleared_body);
    const cleared = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, cleared_body, 1, .require_model);
    defer a.free(cleared.body.?);
    var empty = try document_projection.parseAlloc(a, cleared.body.?);
    defer empty.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), empty.chunk_embeddings.?.len);
    try std.testing.expect(!empty.chunk_embeddings_pending);
    try std.testing.expectEqualDeep([_]usize{ 1, 1, 2 }, fake.calls);
}

const ServerlessMediaTestProvider = struct {
    speech_revision_changed: bool = false,
    parent_count: usize = 1,
    change_speech_at: ?u64 = null,
    fail_ordinal: ?u64 = null,
    pages: usize = 0,
    boundary_revision_changed: bool = false,
    index_revision_changed: bool = false,
    calls: [3]usize = @splat(0),
    fail: bool = true,
    fn page(raw: *anyopaque, a: Allocator, _: []const u8, _: chunking.inference.RemoteInput, cfg: chunking.types.Config, _: @import("antfly_inference_execution_context").RequestContext) anyerror!chunking.inference.ChunkPage {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.pages += 1;
        const ordinal = if (cfg.replay) |cursor| cursor.ordinal else 0;
        if (self.fail_ordinal) |failed| if (cfg.replay != null and ordinal == failed) return error.ChunkRequestFailed;
        if (self.change_speech_at) |after| if (cfg.replay != null and ordinal >= after) {
            self.speech_revision_changed = true;
        };
        var config = try std.json.parseFromSlice(media_outputs.Config, a, cfg.outputs_json, .{});
        defer config.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var results = std.ArrayList(media_outputs.Result).empty;
        for (config.value.targets, 0..) |target, index| {
            if (target.execute) self.calls[index] += 1;
            const identity = "3333333333333333333333333333333333333333333333333333333333333333";
            const revision = if ((index == 2 and self.speech_revision_changed) or (index == 0 and self.boundary_revision_changed) or (index == 1 and self.index_revision_changed)) "5555555555555555555555555555555555555555555555555555555555555555" else "4444444444444444444444444444444444444444444444444444444444444444";
            const recipe = try target.recipeIdentity(scratch, identity, revision);
            const copies: usize = if (index == 1 and target.execute) 2 else 1;
            for (0..copies) |sub_index| {
                var result = media_outputs.Result{ .name = target.name, .kind = target.kind, .required = target.required, .model = target.model, .model_identity = identity, .model_revision = revision, .recipe_id = try scratch.dupe(u8, &recipe), .sub_index = @intCast(sub_index), .start_ns = @intCast(sub_index * 50), .end_ns = @intCast((sub_index + 1) * 50), .status = if (!target.execute) .skipped else if (index == 2 and self.fail) .failed else .complete };
                if (result.status == .failed) result.code = "SpeechUnavailable";
                if (result.status == .complete) {
                    if (target.kind == .embedding) result.embedding = if (index == 0) &.{ 1, 0 } else &.{ 0, 1 } else result.spans = &.{.{ .text = "hello", .start_ns = 0, .end_ns = 50 }};
                }
                try results.append(scratch, result);
            }
        }
        const encoded = try std.json.Stringify.valueAlloc(scratch, results.items, .{ .emit_null_optional_fields = false });
        const plan = if (self.boundary_revision_changed) "6666666666666666666666666666666666666666666666666666666666666666" else "1111111111111111111111111111111111111111111111111111111111111111";
        const inference_id = try std.fmt.allocPrint(scratch, "{s}:{d}", .{ plan, ordinal });
        const fingerprint = "2222222222222222222222222222222222222222222222222222222222222222";
        const chunks = try a.alloc(chunking.inference.RemoteChunk, 1);
        errdefer a.free(chunks);
        chunks[0] = .{ .id = @intCast(ordinal), .mime_type = "audio/wav", .metadata = try (chunking.inference.RemoteMetadata{ .descriptor = .{ .ordinal = ordinal, .input_tokens = 1, .coordinates = .{ .kind = .audio, .start_ns = 0, .end_ns = 100 } }, .inference_id = inference_id, .plan_id = plan, .source_fingerprint = fingerprint, .source_version = "v1", .model = "boundaryB", .model_identity = results.items[0].model_identity, .model_revision = results.items[0].model_revision, .task_type = "RETRIEVAL_DOCUMENT", .dimensions = 2, .policy = .{}, .embedding = if (config.value.targets[0].execute) &.{ 1, 0 } else null, .outputs_json = encoded }).clone(a), .owns_metadata = true };
        errdefer chunks[0].deinit(a);
        const owned_plan = try a.dupe(u8, plan);
        errdefer a.free(owned_plan);
        const owned_fingerprint = try a.dupe(u8, fingerprint);
        errdefer a.free(owned_fingerprint);
        return .{ .chunks = chunks, .plan_id = owned_plan, .source_fingerprint = owned_fingerprint, .source_version = try a.dupe(u8, "v1"), .start_ordinal = ordinal, .covered_through_ordinal = ordinal + 1, .complete = ordinal + 1 == self.parent_count };
    }
};
test "model aware media serverless skipped placeholder preserves every cached subwindow" {
    const a = std.testing.allocator;
    const first = media_outputs.Result{ .name = "embedding", .kind = .embedding, .required = true, .model = "model", .model_identity = "identity", .model_revision = "revision", .recipe_id = "recipe", .status = .complete, .embedding = &.{ 1, 0 } };
    var second = first;
    second.sub_index = 1;
    second.embedding = &.{ 0, 1 };
    var skipped = first;
    skipped.status = .skipped;
    skipped.embedding = null;
    const merged = try mergeServerlessOutputs(a, &.{skipped}, &.{ first, second }, 3);
    defer a.free(merged);
    try std.testing.expectEqual(@as(usize, 2), merged.len);
}

test "model aware media serverless settled outputs probe secondary generations before accepting completion" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var fake = Fake{ .fail = false };
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });

    const first = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, "{\"clip\":\"audio source\"}", 1, .require_model);
    defer a.free(first.body.?);
    const unchanged = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, first.body.?, 1, .require_model);
    defer if (unchanged.body) |body| a.free(body);
    try std.testing.expect(unchanged.body == null);
    try std.testing.expectEqualDeep([_]usize{ 1, 1, 1 }, fake.calls);
    fake.speech_revision_changed = true;
    const second = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, first.body.?, 1, .require_model);
    defer if (second.body) |body| a.free(body);
    try std.testing.expect(second.body != null);
    try std.testing.expectEqualDeep([_]usize{ 1, 1, 2 }, fake.calls);
    fake.boundary_revision_changed = true;
    const replanned = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, second.body.?, 1, .require_model);
    defer if (replanned.body) |body| a.free(body);
    try std.testing.expect(replanned.body != null);
    try std.testing.expectEqualDeep([_]usize{ 2, 2, 3 }, fake.calls);
}

test "model aware media completed namespace discovers secondary generations through bounded durable scans" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-embeddings");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-embeddings");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-embeddings");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    var fake = ServerlessMediaTestProvider{ .fail = false };
    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    defer enricher.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(ServerlessMediaTestProvider.page) = null });
    try enricher.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&ServerlessMediaTestProvider.page) });
    const completed = try buildDerivedBodyAlloc(&enricher, .chunk_embeddings, "{\"text\":\"audio source\",\"clip\":\"audio source\"}", 1, .require_model);
    defer alloc.free(completed.body.?);
    const batch = [_]api_types.DocumentMutation{ .{ .kind = .upsert, .doc_id = "doc-a", .body = completed.body.? }, .{ .kind = .upsert, .doc_id = "doc-b", .body = completed.body.? } };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try publishEnrichmentFixture(&builder);
    defer build.deinit(alloc);
    fake.calls = @splat(0);
    fake.pages = 0;
    const settled = try enricher.runNamespaceWithConfig("docs", .{ .batch_size = 4, .scan_batch_size = 3, .pipeline_version = 1, .stage = .chunk_embeddings });
    try std.testing.expectEqual(@as(usize, 0), settled.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), fake.pages);
    try std.testing.expectEqualDeep([_]usize{ 0, 0, 0 }, fake.calls);
    fake.speech_revision_changed = true;
    const repaired = try enricher.runNamespaceWithConfig("docs", .{ .batch_size = 4, .scan_batch_size = 1, .pipeline_version = 1, .stage = .chunk_embeddings });
    try std.testing.expectEqual(@as(usize, 1), repaired.enriched_documents);
    try std.testing.expectEqualDeep([_]usize{ 0, 0, 1 }, fake.calls);
    // The first repair advances HEAD. Durable ID-order progress must continue
    // to the next completed record rather than restart at the first document.
    var next_head = try publishEnrichmentFixture(&builder);
    defer next_head.deinit(alloc);
    const continued = try enricher.runNamespaceWithConfig("docs", .{ .batch_size = 4, .scan_batch_size = 1, .pipeline_version = 1, .stage = .chunk_embeddings });
    try std.testing.expectEqual(@as(usize, 1), continued.enriched_documents);
    try std.testing.expectEqualDeep([_]usize{ 0, 0, 2 }, fake.calls);
}

test "model aware media serverless prefix repairs changed secondary generations" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var fake = Fake{ .fail = false, .parent_count = 2 };
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });
    worker.media_page_limit = 1;
    const first = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, "{\"clip\":\"audio source\"}", 1, .require_model);
    defer a.free(first.body.?);
    fake.speech_revision_changed = true;
    const second = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, first.body.?, 1, .require_model);
    defer a.free(second.body.?);
    var pending = try std.json.parseFromSlice(std.json.Value, a, second.body.?, .{});
    defer pending.deinit();
    try std.testing.expect(pending.value.object.get("_enrichment").?.object.get("chunk_embeddings_pending").?.bool);
    const third = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, second.body.?, 1, .require_model);
    defer a.free(third.body.?);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, third.body.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("_enrichment").?.object.get("chunk_embeddings_version").?.integer);
    try std.testing.expectEqual([3]usize{ 2, 2, 3 }, fake.calls);
    for (parsed.value.object.get("media_outputs").?.object.get("chunks").?.array.items) |row| {
        for (row.object.get("outputs").?.array.items) |output| {
            if (std.mem.eql(u8, output.object.get("name").?.string, "speech")) try std.testing.expectEqualStrings("5555555555555555555555555555555555555555555555555555555555555555", output.object.get("model_revision").?.string);
        }
    }
}

test "model aware media serverless geometric checkpoints reduce body bytes with bounded replay work" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var fake = Fake{ .fail = false, .parent_count = 64 };
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/media-checkpoints", .{tmp.sub_path});
    var fs = try artifacts_mod.FsStore.init(a, path);
    defer fs.deinit();
    var artifacts = fs.artifactStore();
    var reads: u64 = 100 * 1024 * 1024;
    var writes: u64 = 100 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var reference: ?[]u8 = null;
    defer if (reference) |bytes| a.free(bytes);
    var totals: [4]usize = @splat(0);
    var commits: [4]usize = @splat(0);
    for ([_]usize{ 1, 8, 64, 8 }, 0..) |limit, mode| {
        fake = .{ .fail = false, .parent_count = 64 };
        worker.media_page_limit = limit;
        worker.media_checkpoint_interval_ms = 0;
        var body = try a.dupe(u8, "{\"clip\":\"audio source\"}");
        defer a.free(body);
        while (true) {
            if (mode == 3) worker.checkpoint_context = .{ .allocator = a, .pages = &pages, .previous = try document_facts.putBody(a, &pages, body), .key_digest = @splat(5) };
            const before = fake.calls[0];
            const next = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, body, 1, .require_model);
            try std.testing.expect(fake.calls[0] - before <= limit);
            const produced = next.body orelse break;
            a.free(body);
            body = produced;
            totals[mode] += body.len;
            commits[mode] += 1;
            if (mode == 3 and commits[mode] == 2) {
                var clear = try std.json.parseFromSlice(std.json.Value, a, body, .{});
                defer clear.deinit();
                try clear.value.object.put(clear.arena.allocator(), "clip", .null);
                const cleared_source = try std.json.Stringify.valueAlloc(a, clear.value, .{});
                defer a.free(cleared_source);
                const allowed = reads;
                reads = 0;
                const cleared = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, cleared_source, 1, .require_model);
                defer a.free(cleared.body.?);
                reads = allowed;
                var cleared_document = try std.json.parseFromSlice(std.json.Value, a, cleared.body.?, .{});
                defer cleared_document.deinit();
                try std.testing.expect(!cleared_document.value.object.get("_enrichment").?.object.contains("chunk_embeddings_checkpoint"));
            }
            var parsed = try std.json.parseFromSlice(std.json.Value, a, body, .{});
            defer parsed.deinit();
            if (!parsed.value.object.get("_enrichment").?.object.get("chunk_embeddings_pending").?.bool) break;
        }
        try std.testing.expectEqual([3]usize{ 64, 64, 64 }, fake.calls);
        var final = try std.json.parseFromSlice(std.json.Value, a, body, .{});
        defer final.deinit();
        try std.testing.expect(!final.value.object.get("_enrichment").?.object.contains("chunk_embeddings_checkpoint"));
        const settled = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, body, 1, .require_model);
        try std.testing.expect(settled.body == null);
        const output = try std.json.Stringify.valueAlloc(a, .{ .vectors = final.value.object.get("chunk_embeddings").?, .outputs = final.value.object.get("media_outputs").? }, .{});
        defer a.free(output);
        if (reference) |bytes| try std.testing.expectEqualStrings(bytes, output) else reference = try a.dupe(u8, output);
    }
    try std.testing.expect(totals[1] < totals[0] / 4);
    try std.testing.expect(totals[2] < totals[0] / 12);
    try std.testing.expect(totals[3] < totals[1] / 2);
    std.debug.print("media checkpoint bytes={any} commits={any}\n", .{ totals, commits });
}

test "model aware media serverless detects a secondary deployment during one page group" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var fake = Fake{ .fail = false, .parent_count = 2, .change_speech_at = 1 };
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });
    worker.media_page_limit = 0;
    const first = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, "{\"clip\":\"audio source\"}", 1, .require_model);
    defer a.free(first.body.?);
    var pending = try document_projection.parseAlloc(a, first.body.?);
    defer pending.deinit(a);
    try std.testing.expect(pending.chunk_embeddings_pending);
    try std.testing.expect(pending.chunk_embeddings_version == null);
    const repaired = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, first.body.?, 1, .require_model);
    defer a.free(repaired.body.?);
    var complete = try document_projection.parseAlloc(a, repaired.body.?);
    defer complete.deinit(a);
    try std.testing.expect(!complete.chunk_embeddings_pending);
    try std.testing.expectEqual(@as(?u32, 1), complete.chunk_embeddings_version);
    try std.testing.expectEqual([3]usize{ 2, 2, 3 }, fake.calls);
}

test "model aware media serverless grouped failure resumes only the unacknowledged tail after restart" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var fake = Fake{ .fail = false, .parent_count = 8 };
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });
    worker.media_page_limit = 8;
    worker.media_checkpoint_interval_ms = 0;
    const first = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, "{\"clip\":\"audio source\"}", 1, .require_model);
    defer a.free(first.body.?);
    const second = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, first.body.?, 1, .require_model);
    defer a.free(second.body.?);
    // The next group owns two pages. Fail its second page, after one new
    // computation, without losing the earlier two durable page receipts.
    fake.fail_ordinal = 3;
    try std.testing.expectError(error.ChunkRequestFailed, buildDerivedBodyAlloc(&worker, .chunk_embeddings, second.body.?, 1, .require_model));
    try std.testing.expectEqual([3]usize{ 3, 3, 3 }, fake.calls);
    fake.fail_ordinal = null;
    var restarted = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined, .media_page_limit = 8, .media_checkpoint_interval_ms = 0 };
    defer restarted.deinit();
    try restarted.setModelChunker(worker.model_chunker.?, "clip", worker.model_chunk_provider);
    const resumed = try buildDerivedBodyAlloc(&restarted, .chunk_embeddings, second.body.?, 1, .require_model);
    defer a.free(resumed.body.?);
    const complete = try buildDerivedBodyAlloc(&restarted, .chunk_embeddings, resumed.body.?, 1, .require_model);
    defer a.free(complete.body.?);
    var projection = try document_projection.parseAlloc(a, complete.body.?);
    defer projection.deinit(a);
    try std.testing.expectEqual(@as(?u32, 1), projection.chunk_embeddings_version);
    try std.testing.expectEqual(@as(usize, 16), projection.chunk_embeddings.?.len);
    // Only the one unacknowledged successful page executes twice.
    try std.testing.expectEqual([3]usize{ 9, 9, 9 }, fake.calls);
}

test "model aware media linked checkpoints authenticate the document and repair only changed generations" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var fake = Fake{ .fail = false, .parent_count = 2 };
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/media-checkpoints", .{tmp.sub_path});
    var fs = try artifacts_mod.FsStore.init(a, path);
    defer fs.deinit();
    var artifacts = fs.artifactStore();
    var reads: u64 = 100 * 1024 * 1024;
    var writes: u64 = 100 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    worker.media_page_limit = 1;
    worker.checkpoint_context = .{ .allocator = a, .pages = &pages, .previous = try document_facts.putBody(a, &pages, "{\"clip\":\"audio source\"}"), .key_digest = @splat(5) };
    const first = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, "{\"clip\":\"audio source\"}", 1, .require_model);
    defer a.free(first.body.?);
    worker.checkpoint_context.?.previous = try document_facts.putBody(a, &pages, first.body.?);
    worker.checkpoint_context.?.key_digest = @splat(6);
    try std.testing.expectError(error.InvalidMediaCheckpoint, buildDerivedBodyAlloc(&worker, .chunk_embeddings, first.body.?, 1, .require_model));
    worker.checkpoint_context.?.key_digest = @splat(5);
    fake.speech_revision_changed = true;
    const second = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, first.body.?, 1, .require_model);
    defer a.free(second.body.?);
    var pending = try std.json.parseFromSlice(std.json.Value, a, second.body.?, .{});
    defer pending.deinit();
    try std.testing.expect(pending.value.object.get("_enrichment").?.object.get("chunk_embeddings_pending").?.bool);
    worker.checkpoint_context.?.previous = try document_facts.putBody(a, &pages, second.body.?);
    const third = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, second.body.?, 1, .require_model);
    defer a.free(third.body.?);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, third.body.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("_enrichment").?.object.get("chunk_embeddings_version").?.integer);
    try std.testing.expectEqual([3]usize{ 2, 2, 3 }, fake.calls);
    for (parsed.value.object.get("media_outputs").?.object.get("chunks").?.array.items) |row| {
        for (row.object.get("outputs").?.array.items) |output| {
            if (std.mem.eql(u8, output.object.get("name").?.string, "speech")) try std.testing.expectEqualStrings("5555555555555555555555555555555555555555555555555555555555555555", output.object.get("model_revision").?.string);
        }
    }
}

test "model aware media serverless output-only checkpoints close within the original document input limit" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var fake = Fake{ .fail = false, .parent_count = 64 };
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/media-checkpoints", .{tmp.sub_path});
    var fs = try artifacts_mod.FsStore.init(a, path);
    defer fs.deinit();
    var artifacts = fs.artifactStore();
    var reads: u64 = 100 * 1024 * 1024;
    var writes: u64 = 100 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var reference: ?[]u8 = null;
    defer if (reference) |bytes| a.free(bytes);
    var totals: [1]usize = @splat(0);
    var commits: [1]usize = @splat(0);
    for ([_]usize{8}, 0..) |limit, mode| {
        fake = .{ .fail = false, .parent_count = 64 };
        worker.media_page_limit = limit;
        worker.media_checkpoint_interval_ms = 0;
        const padding = try a.alloc(u8, 256 * 1024);
        defer a.free(padding);
        @memset(padding, 'x');
        var body = try std.json.Stringify.valueAlloc(a, .{ .clip = "audio source", .padding = padding }, .{});
        defer a.free(body);
        const base = try document_facts.putBody(a, &pages, body);
        var previous = base;
        var full_body_bytes: usize = 0;
        while (true) {
            reads = 2 * 1024 * 1024;
            worker.checkpoint_context = .{ .allocator = a, .pages = &pages, .previous = previous, .key_digest = @splat(5) };
            const before = fake.calls[0];
            const next = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, body, 1, .require_model);
            try std.testing.expect(fake.calls[0] - before <= limit);
            const produced = next.body orelse break;
            a.free(body);
            body = produced;
            full_body_bytes += body.len;
            commits[mode] += 1;
            var parsed = try std.json.parseFromSlice(std.json.Value, a, body, .{});
            defer parsed.deinit();
            if (!parsed.value.object.get("_enrichment").?.object.get("chunk_embeddings_pending").?.bool) {
                totals[mode] += body.len;
                break;
            }
            const compact = try document_facts.encodeCheckpointBodyAlloc(a, base, body);
            defer a.free(compact);
            totals[mode] += compact.len;
            try std.testing.expect(compact.len < 128 * 1024);
            const encoded = try api_codec.encodeMutationAlloc(a, .{ .kind = .checkpoint, .doc_id = "doc", .body = compact });
            defer a.free(encoded);
            var decoded = try api_codec.decodeMutationAlloc(a, encoded);
            defer decoded.deinit(a);
            const materializer = @import("../query/materializer.zig");
            const replayed = try materializer.materializeWithBodiesAlloc(a, &.{}, &.{.{ .lsn = commits[mode], .timestamp_ns = 1, .kind = decoded.kind, .doc_id = decoded.doc_id, .body = decoded.body }}, .{ .artifacts = &artifacts, .namespace = "docs", .max_read_bytes = 2 * 1024 * 1024 });
            defer materializer.freeDocuments(a, replayed);
            try std.testing.expectEqualStrings(body, replayed[0].body);
            previous = try document_facts.putBody(a, &pages, replayed[0].stored_body.?);
            const hydrated = try document_facts.readBodyAlloc(a, &pages, previous);
            defer a.free(hydrated);
            try std.testing.expectEqualStrings(body, hydrated);
        }
        std.debug.print("checkpoint bytes compact={d} repeated_source={d} commits={d} final={d}\n", .{ totals[mode], full_body_bytes, commits[mode], body.len });
        try std.testing.expect(totals[mode] * 2 < full_body_bytes);
        try std.testing.expectEqual([3]usize{ 64, 64, 64 }, fake.calls);
        var final = try std.json.parseFromSlice(std.json.Value, a, body, .{});
        defer final.deinit();
        try std.testing.expect(!final.value.object.get("_enrichment").?.object.contains("chunk_embeddings_checkpoint"));
        const settled = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, body, 1, .require_model);
        try std.testing.expect(settled.body == null);
        const output = try std.json.Stringify.valueAlloc(a, .{ .vectors = final.value.object.get("chunk_embeddings").?, .outputs = final.value.object.get("media_outputs").? }, .{});
        defer a.free(output);
        if (reference) |bytes| try std.testing.expectEqualStrings(bytes, output) else reference = try a.dupe(u8, output);
    }
    std.debug.print("output-only checkpoint WAL body bytes={any} commits={any}\n", .{ totals, commits });
}

test "model aware media serverless compact WAL checkpoints publish resume and finish through fact roots" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-embeddings");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-embeddings");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-embeddings");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    var fake = ServerlessMediaTestProvider{ .fail = false, .parent_count = 4 };
    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    defer enricher.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(ServerlessMediaTestProvider.page) = null });
    try enricher.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&ServerlessMediaTestProvider.page) });
    const padding = try alloc.alloc(u8, 256 * 1024);
    defer alloc.free(padding);
    @memset(padding, 'x');
    const source = try std.json.Stringify.valueAlloc(alloc, .{ .text = "audio source", .clip = "audio source", .padding = padding }, .{});
    defer alloc.free(source);
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &.{.{ .kind = .upsert, .doc_id = "doc-a", .body = source }} });
    defer ingest.deinit(alloc);
    var initial = try publishEnrichmentFixture(&builder);
    defer initial.deinit(alloc);
    for (0..4) |page_index| {
        const before_lsn = try wal_store.latestLsn("docs");
        // A finite audit cycle may first retire its old upper boundary
        // after HEAD advances. That idle turn must not execute model work.
        for (0..3) |_| {
            const stats = try enricher.runNamespaceWithConfig("docs", .{ .batch_size = 1, .scan_batch_size = 1, .pipeline_version = 1, .stage = .chunk_embeddings, .media_pages_per_document = 1, .failure_policy = .fail_stage, .document_limits = .{ .max_input_bytes = 2 * 1024 * 1024, .max_replay_bytes = 2 * 1024 * 1024 } });
            try std.testing.expectEqual(@as(usize, 0), stats.failed_documents);
            if (stats.wal_appends == 1) break;
            try std.testing.expectEqual(@as(usize, 0), stats.wal_appends);
        } else return error.TestUnexpectedResult;
        const records = try wal_store.readFromAlloc("docs", before_lsn + 1);
        defer wal_mod.freeRecords(alloc, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);
        var mutation = try api_codec.decodeMutationAlloc(alloc, records[0].payload);
        defer mutation.deinit(alloc);
        try std.testing.expectEqual(if (page_index == 3) api_types.MutationKind.upsert else .checkpoint, mutation.kind);
        if (page_index < 3) try std.testing.expect(mutation.body.?.len < 64 * 1024);
        var published = try publishEnrichmentFixture(&builder);
        defer published.deinit(alloc);
        var manifest = try manifest_store.getAlloc("docs", try progress_store.getHead("docs"));
        defer manifest.deinit(alloc);
        const root_ref = manifest.artifacts[findArtifactIndex(manifest, .document_facts).?];
        const docs = try @import("../build/builder.zig").materializeManifestFactsAlloc(alloc, &artifact_store, manifest, root_ref, null);
        defer @import("../query/materializer.zig").freeDocuments(alloc, docs);
        try std.testing.expectEqual(@as(usize, 1), docs.len);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, docs[0].body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(padding, parsed.value.object.get("padding").?.string);
        try std.testing.expectEqual(page_index < 3, parsed.value.object.get("_enrichment").?.object.contains("chunk_embeddings_checkpoint"));
        // Each pass constructs its document context from the published fact,
        // with no in-memory output prefix surviving the publication boundary.
    }
    try std.testing.expectEqual([3]usize{ 4, 4, 4 }, fake.calls);
}

test "model aware media serverless secondary repair keeps healthy vectors and checkpoints only new output groups" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/repair-checkpoints", .{tmp.sub_path});
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    defer fs.deinit();
    var objects = fs.artifactStore();
    var reads: u64 = 256 * 1024 * 1024;
    var writes: u64 = 256 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &objects, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var totals: [2]usize = @splat(0);
    const Compare = struct {
        fn values(left: std.json.Value, right: std.json.Value) !void {
            try std.testing.expectEqual(std.meta.activeTag(left), std.meta.activeTag(right));
            switch (left) {
                .object => |object| {
                    try std.testing.expectEqual(object.count(), right.object.count());
                    var fields = object.iterator();
                    while (fields.next()) |entry| try values(entry.value_ptr.*, right.object.get(entry.key_ptr.*) orelse return error.TestExpectedEqual);
                },
                .array => |array| {
                    try std.testing.expectEqual(array.items.len, right.array.items.len);
                    for (array.items, right.array.items) |first, second| try values(first, second);
                },
                else => try std.testing.expectEqualDeep(left, right),
            }
        }
    };
    var final_body: ?[]u8 = null;
    defer if (final_body) |bytes| a.free(bytes);
    for (0..2) |mode| {
        var fake = Fake{ .fail = false, .parent_count = 64 };
        var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
        defer worker.deinit();
        const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
        try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });

        const padding = try a.alloc(u8, 256 * 1024);
        defer a.free(padding);
        @memset(padding, 'x');
        const source = try std.json.Stringify.valueAlloc(a, .{ .clip = "audio source", .padding = padding }, .{});
        defer a.free(source);

        const first = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, source, 1, .require_model);
        defer a.free(first.body.?);
        var stored = try a.dupe(u8, first.body.?);
        defer a.free(stored);
        const base = try document_facts.putBody(a, &pages, stored);
        var previous = base;
        fake.speech_revision_changed = true;
        worker.media_page_limit = 8;
        worker.model_chunker.?.max_chunks = 1;
        var commits: usize = 0;
        while (commits < 32) {
            if (mode == 1) worker.checkpoint_context = .{ .allocator = a, .pages = &pages, .previous = previous, .previous_is_compact = document_facts.isCheckpointBody(stored), .key_digest = @splat(5) };
            const input = if (document_facts.isCheckpointBody(stored)) try document_facts.resolveCheckpointStateAlloc(a, &pages, stored) else try a.dupe(u8, stored);
            defer a.free(input);
            const next = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, input, 1, .require_model);
            if (next.body == null) break;
            defer a.free(next.body.?);
            var parsed = try std.json.parseFromSlice(std.json.Value, a, next.body.?, .{});
            defer parsed.deinit();
            const compact = (try media_checkpoint.get(parsed.arena.allocator(), parsed.value)) != null;
            const payload = if (compact) try document_facts.encodeCheckpointBodyAlloc(a, base, next.body.?) else try a.dupe(u8, next.body.?);
            a.free(stored);
            stored = payload;
            totals[mode] += payload.len;
            commits += 1;
            previous = try document_facts.putBody(a, &pages, stored);
            const public = try document_facts.readBodyAlloc(a, &pages, previous);
            defer a.free(public);
            var projection = try document_projection.parseAlloc(a, public);
            defer projection.deinit(a);
            // Every pending publication retains all independently healthy vectors.
            try std.testing.expectEqual(@as(usize, 128), projection.chunk_embeddings.?.len);
            try std.testing.expectEqual(@as(usize, 64), fake.calls[0]);
            try std.testing.expectEqual(@as(usize, 64), fake.calls[1]);
            if (compact) {
                const group = (try media_checkpoint.get(parsed.arena.allocator(), parsed.value)).?;
                try std.testing.expectEqual(@as(u8, 2), group.format);
                try std.testing.expectEqual(base, group.repair_base.?);
                try std.testing.expect(group.rows.len <= 8);
                var descriptor = try document_facts.parseCheckpointBody(a, payload);
                defer descriptor.deinit();
                try std.testing.expectEqual(base, descriptor.value.base);
                // The source and full receipt inventory stay in the leased baseline.
                try std.testing.expect(payload.len < first.body.?.len / 2);
            }
        }
        try std.testing.expectEqual(@as(usize, 11), commits);
        try std.testing.expectEqualDeep([3]usize{ 64, 64, 128 }, fake.calls);
        var final = try std.json.parseFromSlice(std.json.Value, a, stored, .{});
        defer final.deinit();
        if (mode == 0) {
            final_body = try a.dupe(u8, stored);
        } else {
            var expected = try std.json.parseFromSlice(std.json.Value, a, final_body.?, .{});
            defer expected.deinit();
            try Compare.values(expected.value, final.value);
        }
        try std.testing.expectEqual(@as(i64, 1), final.value.object.get("_enrichment").?.object.get("chunk_embeddings_version").?.integer);
        std.debug.print("secondary repair compact={any} source={d} commits={d} serialized_bytes={d} final={d} calls={any}\n", .{ mode == 1, source.len, commits, totals[mode], stored.len, fake.calls });
    }
    try std.testing.expect(totals[1] < totals[0] / 3);
}

test "model aware media serverless repair overlays hide stale index generations and reject mismatched baselines" {
    const Fake = ServerlessMediaTestProvider;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/repair-generation", .{tmp.sub_path});
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    defer fs.deinit();
    var objects = fs.artifactStore();
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 64 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &objects, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var fake = Fake{ .fail = false, .parent_count = 8 };
    var worker = SparseEnricher{ .alloc = a, .artifacts = undefined, .manifests = undefined, .progress = undefined, .wal = undefined };
    defer worker.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(Fake.page) = null });
    try worker.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&Fake.page) });

    const padding = try a.alloc(u8, 256 * 1024);
    defer a.free(padding);
    @memset(padding, 'x');
    const source = try std.json.Stringify.valueAlloc(a, .{ .clip = "audio source", .padding = padding }, .{});
    defer a.free(source);

    const initial = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, source, 1, .require_model);
    defer a.free(initial.body.?);
    const base = try document_facts.putBody(a, &pages, initial.body.?);
    worker.checkpoint_context = .{ .allocator = a, .pages = &pages, .previous = base, .key_digest = @splat(5) };
    worker.media_page_limit = 1;
    worker.model_chunker.?.max_chunks = 1;
    fake.index_revision_changed = true;
    const repaired = try buildDerivedBodyAlloc(&worker, .chunk_embeddings, initial.body.?, 1, .require_model);
    defer a.free(repaired.body.?);
    const payload = try document_facts.encodeCheckpointBodyAlloc(a, base, repaired.body.?);
    defer a.free(payload);
    const public = try document_facts.resolveCheckpointBodyAlloc(a, &pages, payload);
    defer a.free(public);
    var projection = try document_projection.parseAlloc(a, public);
    defer projection.deinit(a);
    // Only the newly repaired index rows belong to the new vector space.
    try std.testing.expectEqual(@as(usize, 2), projection.chunk_embeddings.?.len);
    try std.testing.expectEqualDeep([3]usize{ 8, 9, 8 }, fake.calls);
    const wrong_base = try document_facts.putBody(a, &pages, "{}");
    const wrong = try document_facts.encodeCheckpointBodyAlloc(a, wrong_base, repaired.body.?);
    defer a.free(wrong);
    try std.testing.expectError(error.InvalidMediaCheckpoint, document_facts.resolveCheckpointBodyAlloc(a, &pages, wrong));
    try std.testing.expectError(error.InvalidMediaCheckpoint, document_facts.resolveCheckpointStateAlloc(a, &pages, wrong));
    worker.checkpoint_context.?.key_digest = @splat(6);
    const state = try document_facts.resolveCheckpointStateAlloc(a, &pages, payload);
    defer a.free(state);
    try std.testing.expectError(error.InvalidMediaCheckpoint, buildDerivedBodyAlloc(&worker, .chunk_embeddings, state, 1, .require_model));
}

test "model aware media serverless repair replay uses independent budget after admission" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-embeddings");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-embeddings");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-embeddings");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    var fake = ServerlessMediaTestProvider{ .fail = false, .parent_count = 8 };
    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    defer enricher.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(ServerlessMediaTestProvider.page) = null });
    try enricher.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&ServerlessMediaTestProvider.page) });

    const padding = try alloc.alloc(u8, 256 * 1024);
    defer alloc.free(padding);
    @memset(padding, 'x');
    const source = try std.json.Stringify.valueAlloc(alloc, .{ .text = "audio source", .clip = "audio source", .padding = padding }, .{});
    defer alloc.free(source);
    const completed = try buildDerivedBodyAlloc(&enricher, .chunk_embeddings, source, 1, .require_model);
    defer alloc.free(completed.body.?);
    const input_limit = completed.body.?.len + 100;
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &.{.{ .kind = .upsert, .doc_id = "doc-a", .body = completed.body.? }} });
    defer ingest.deinit(alloc);
    var initial = try publishEnrichmentFixture(&builder);
    defer initial.deinit(alloc);
    fake.speech_revision_changed = true;
    enricher.model_chunker.?.max_chunks = 1;
    const before = try wal_store.latestLsn("docs");
    for (0..3) |_| {
        const stats = try enricher.runNamespaceWithConfig("docs", .{ .batch_size = 1, .scan_batch_size = 1, .pipeline_version = 1, .stage = .chunk_embeddings, .media_pages_per_document = 1, .failure_policy = .fail_stage, .document_limits = .{ .max_input_bytes = input_limit, .max_output_bytes = 2 * 1024 * 1024, .max_replay_bytes = 4 * 1024 * 1024 } });
        if (stats.wal_appends == 1) break;
    } else return error.TestUnexpectedResult;
    var published = try publishEnrichmentFixture(&builder);
    defer published.deinit(alloc);
    const after_first = try wal_store.latestLsn("docs");
    try std.testing.expect(after_first > before);
    for (0..48) |_| {
        _ = try enricher.runNamespaceWithConfig("docs", .{ .batch_size = 1, .scan_batch_size = 1, .pipeline_version = 1, .stage = .chunk_embeddings, .media_pages_per_document = 1, .failure_policy = .fail_stage, .document_limits = .{ .max_input_bytes = input_limit, .max_output_bytes = 2 * 1024 * 1024, .max_replay_bytes = 4 * 1024 * 1024 } });
        var build = try publishEnrichmentFixture(&builder);
        build.deinit(alloc);
        if (fake.calls[2] == 16) break;
    } else return error.TestUnexpectedResult;
    try std.testing.expect((try wal_store.latestLsn("docs")) > after_first);
    try std.testing.expectEqualDeep([3]usize{ 8, 8, 16 }, fake.calls);
}

test "model aware media serverless settled audit witnesses avoid payload hydration and retain dependencies" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-embeddings");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-embeddings");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-embeddings");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    var fake = ServerlessMediaTestProvider{ .fail = false, .parent_count = 64 };
    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    defer enricher.deinit();
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct { chunk_page_with_context: ?*const @TypeOf(ServerlessMediaTestProvider.page) = null });
    try enricher.setModelChunker(.{ .provider = .antfly, .embedding = .{ .model = "indexA", .dimensions = 2 }, .outputs_json = "{\"boundary_model\":\"boundary\",\"targets\":[{\"name\":\"boundary\",\"kind\":\"embedding\",\"model\":\"boundaryB\",\"dimensions\":2},{\"name\":\"embedding\",\"kind\":\"embedding\",\"model\":\"indexA\",\"dimensions\":2},{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"speech\"}]}" }, "clip", .{ .ptr = &fake, .boundary_dispatch = Boundary.local_dispatch, .chunk_page_with_context_callback = @ptrCast(&ServerlessMediaTestProvider.page) });

    const padding = try alloc.alloc(u8, 256 * 1024);
    defer alloc.free(padding);
    @memset(padding, 'x');
    const source = try std.json.Stringify.valueAlloc(alloc, .{ .text = "audio source", .clip = "audio source", .padding = padding }, .{});
    defer alloc.free(source);
    const completed = try buildDerivedBodyAlloc(&enricher, .chunk_embeddings, source, 1, .require_model);
    defer alloc.free(completed.body.?);
    var mutations: [32]api_types.DocumentMutation = undefined;
    var names: [32][16]u8 = undefined;
    for (&mutations, &names, 0..) |*mutation, *name, i| mutation.* = .{ .kind = .upsert, .doc_id = try std.fmt.bufPrint(name, "doc-{d:0>2}", .{i}), .body = completed.body.? };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &mutations });
    defer ingest.deinit(alloc);
    var initial = try publishEnrichmentFixture(&builder);
    defer initial.deinit(alloc);
    const Count = struct {
        var body_bytes: usize = 0;
        var body_calls: usize = 0;
        var full_calls: usize = 0;
        fn range(raw: *anyopaque, a: Allocator, id: []const u8, offset: u64, len: usize) ![]u8 {
            return controlled(raw, a, id, offset, len, .none);
        }
        fn controlled(raw: *anyopaque, a: Allocator, id: []const u8, offset: u64, len: usize, cancel: CancellationToken) ![]u8 {
            const fs: *artifacts_mod.FsStore = @ptrCast(@alignCast(raw));
            const bytes = try fs.getRangeAllocWithCancellation(a, id, offset, len, cancel);
            if (std.mem.startsWith(u8, bytes, "AFDBODY1")) {
                body_bytes += bytes.len;
                body_calls += 1;
                if (bytes.len > 256 * 1024) full_calls += 1;
            }
            return bytes;
        }
    };
    var counted = artifact_store.vtable.*;
    counted.get_range_alloc = Count.range;
    counted.get_range_alloc_with_cancellation = Count.controlled;
    artifact_store.vtable = &counted;
    Count.body_bytes = 0;
    Count.body_calls = 0;
    Count.full_calls = 0;
    fake.calls = @splat(0);
    fake.pages = 0;
    const stats = try enricher.runNamespaceWithConfig("docs", .{ .batch_size = 32, .scan_batch_size = 64, .pipeline_version = 1, .stage = .chunk_embeddings });
    try std.testing.expectEqual(@as(usize, 0), stats.wal_appends);
    try std.testing.expectEqualDeep([3]usize{ 0, 0, 0 }, fake.calls);
    try std.testing.expectEqual(@as(usize, 32), Count.body_calls);
    try std.testing.expectEqual(@as(usize, 1), Count.full_calls);
    try std.testing.expect(Count.body_bytes < 1024 * 1024);
    var manifest = try manifest_store.getAlloc("docs", try progress_store.getHead("docs"));
    defer manifest.deinit(alloc);
    const root_ref = manifest.artifacts[findArtifactIndex(manifest, .document_facts).?];
    var read_remaining: u64 = 128 * 1024 * 1024;
    var write_remaining: u64 = 0;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .artifacts = &artifact_store, .remaining_read_bytes = &read_remaining, .remaining_write_bytes = &write_remaining };
    const root = try document_facts.loadRoot(alloc, &pages, root_ref);
    const fact = (try document_facts.lookup(alloc, pages.store(), root, "doc-00")).?;
    try std.testing.expect(fact.media_audit != null);
    var retained = std.StringHashMapUnmanaged(void).empty;
    defer {
        var keys = retained.keyIterator();
        while (keys.next()) |key| alloc.free(key.*);
        retained.deinit(alloc);
    }
    try document_facts.retainRoot(alloc, &pages, root_ref, &retained);
    try std.testing.expect(retained.contains(&try fact.media_audit.?.identity(pages.domain)));
    std.debug.print("audit witnesses settled_docs=32 source_bytes={d} body_bytes_each={d} object_store_body_reads={d} object_store_body_bytes={d} model_calls={any} generation_observations={d} WAL_appends={d}\n", .{ source.len, completed.body.?.len, Count.body_calls, Count.body_bytes, fake.calls, fake.pages, stats.wal_appends });
    // Every fast-path dependency invalidates without trusting completion flags.
    var snapshot = MediaGenerationSnapshot{ .arena = std.heap.ArenaAllocator.init(alloc) };
    defer snapshot.arena.deinit();
    var probe = try chunking.inference.probeModelPageWithProvider(alloc, enricher.model_chunker.?, .{ .text = "audio source" }, enricher.model_chunk_provider);
    defer probe.deinit(alloc);
    try snapshot.observe(alloc, probe.observedMetadata().?);
    const cfg = SparseEnricherConfig{ .pipeline_version = 1, .stage = .chunk_embeddings };
    try std.testing.expect(try enricher.auditMatches(cfg, &pages, fact, &snapshot));
    var next_cfg = cfg;
    next_cfg.pipeline_version = 2;
    try std.testing.expect(!try enricher.auditMatches(next_cfg, &pages, fact, &snapshot));
    var altered = try std.json.parseFromSlice(std.json.Value, alloc, completed.body.?, .{});
    defer altered.deinit();
    const scratch = altered.arena.allocator();
    try altered.value.object.put(scratch, "clip", .{ .string = "different source" });
    const changed = try std.json.Stringify.valueAlloc(alloc, altered.value, .{});
    defer alloc.free(changed);
    const changed_witness = (try media_audit.buildAlloc(alloc, changed)).?;
    defer alloc.free(changed_witness);
    write_remaining = 1024 * 1024;
    pages.attempt = @splat(4);
    var changed_fact = fact;
    changed_fact.media_audit = try document_facts.putBody(alloc, &pages, changed_witness);
    try std.testing.expect(!try enricher.auditMatches(cfg, &pages, changed_fact, &snapshot));
    fake.speech_revision_changed = true;
    var changed_probe = try chunking.inference.probeModelPageWithProvider(alloc, enricher.model_chunker.?, .{ .text = "audio source" }, enricher.model_chunk_provider);
    defer changed_probe.deinit(alloc);
    var changed_snapshot = MediaGenerationSnapshot{ .arena = std.heap.ArenaAllocator.init(alloc) };
    defer changed_snapshot.arena.deinit();
    try changed_snapshot.observe(alloc, changed_probe.observedMetadata().?);
    try std.testing.expect(!try enricher.auditMatches(cfg, &pages, fact, &changed_snapshot));
}
