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

const std = @import("std");
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
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
const embedder_mod = @import("../../storage/db/enrichment/embedder.zig");
const maintenance_cancellation = @import("../maintenance_cancellation.zig");
const objectstore = @import("objectstore");
const artifacts_object_store = @import("../artifacts/object_store.zig");
const manifest_object_store = @import("../manifest/object_store.zig");
const progress_object_store = @import("../catalog/object_progress_store.zig");
const wal_object_store = @import("../wal/object_store.zig");
const operation_identity = @import("operation_id.zig");
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
        self.* = undefined;
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
        const stage_index: usize = @intFromEnum(cfg.stage) - 1;
        const pending_count = facts.counts[3 + stage_index];
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
            std.mem.eql(u8, &value.policy_fingerprint, &facts.policy_fingerprint) else false;
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
        var expected_latest_lsn = latest_lsn;
        if (pending_count != 0) {
            if (cycle_upper == null) {
                // Capture one finite cycle under the pinned source. Rank seek
                // is O(tree height), not a scan of the pending population.
                var tail = try document_facts.pendingCursorAtRank(self.alloc, pages.store(), facts, stage_index, pending_count - 1);
                defer tail.deinit();
                const last = (try tail.next()) orelse return error.InvalidDocumentFactsRoot;
                cycle_upper = try self.alloc.dupe(u8, last.order_key);
            }
            var cursor = try document_facts.pendingCursor(self.alloc, pages.store(), facts, stage_index, after orelse "");
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
                var body_read_remaining = fact.body.bytes;
                var body_pages = pages;
                body_pages.remaining_read_bytes = &body_read_remaining;
                const completed_key = try self.alloc.dupe(u8, record.order_key);
                var owns_completed_key = true;
                errdefer if (owns_completed_key) self.alloc.free(completed_key);
                expected_latest_lsn = self.processPendingDocument(namespace, cfg, &pin, &body_pages, fact, record.key, head, expected_latest_lsn, &stats) catch |err| failed: {
                    if (!isRecoverableEnrichmentError(err) and err != error.EnrichmentDocumentBudgetExceeded) return err;
                    stats.failed_documents += 1;
                    if (cfg.failure_policy == .fail_stage) return err;
                    break :failed expected_latest_lsn;
                };
                if (next_key) |key| self.alloc.free(key);
                next_key = completed_key;
                owns_completed_key = false;
                next_offset = try std.math.add(u64, next_offset, 1);
                remaining -|= body_bytes;
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
            .policy_fingerprint = facts.policy_fingerprint,
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

    fn processPendingDocument(self: *SparseEnricher, namespace: []const u8, cfg: SparseEnricherConfig, pin: *SourcePin, pages: *PageStore, fact: document_facts.Fact, key: []const u8, head: u64, expected_lsn: u64, stats: *EnrichmentRunStats) !u64 {
        if (try bodyPayloadBytes(fact.body.bytes) > cfg.document_limits.max_input_bytes) return error.EnrichmentDocumentBudgetExceeded;
        var working = try build_limits.WorkingSetAllocator.init(self.alloc, cfg.document_limits);
        var worker = self.*;
        worker.alloc = working.allocator();
        return worker.processAdmittedDocument(namespace, cfg, pin, pages, fact, key, head, expected_lsn, stats) catch |err| {
            if (err == error.OutOfMemory and working.limit_exceeded) return error.EnrichmentDocumentBudgetExceeded;
            return err;
        };
    }

    fn processAdmittedDocument(self: *SparseEnricher, namespace: []const u8, cfg: SparseEnricherConfig, pin: *SourcePin, pages: *PageStore, fact: document_facts.Fact, key: []const u8, head: u64, expected_lsn: u64, stats: *EnrichmentRunStats) !u64 {
        const source = try document_facts.readBodyAlloc(self.alloc, pages, fact.body);
        defer self.alloc.free(source);
        const derived = try buildDerivedBodyAlloc(self, cfg.stage, source, cfg.pipeline_version, cfg.model_preference);
        defer if (derived.body) |body| self.alloc.free(body);
        try pin.check();
        const body = derived.body orelse return expected_lsn;
        if (body.len > cfg.document_limits.max_output_bytes) return error.EnrichmentDocumentBudgetExceeded;
        const encoded = try api_codec.encodeMutationAlloc(self.alloc, .{ .kind = .upsert, .doc_id = key, .body = body });
        defer self.alloc.free(encoded);
        if (encoded.len > cfg.document_limits.max_output_bytes) return error.EnrichmentDocumentBudgetExceeded;
        try pin.check();
        var operation_buffer: [128]u8 = undefined;
        const operation = try operation_identity.formatDocument(&operation_buffer, head, @intFromEnum(cfg.stage), key, cfg.pipeline_version);
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
    return switch (stage) {
        .lexical_sparse => try buildDerivedSparseBodyAlloc(self, body, pipeline_version, model_preference),
        .chunk_preview => .{ .body = try buildDerivedChunkPreviewBodyAlloc(self.alloc, body, pipeline_version) },
        .chunk_embeddings => try buildDerivedChunkEmbeddingsBodyAlloc(self, body, pipeline_version, model_preference),
        .rerank_terms => .{ .body = try buildDerivedRerankTermsBodyAlloc(self.alloc, body, pipeline_version) },
    };
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

    fn deinit(ptr: *anyopaque, _: Allocator) void {
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

    fn deinit(_: Allocator, _: *anyopaque) void {}

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

    fn deinit(_: Allocator, _: *anyopaque) void {}

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
        try appendJSONString(alloc, out, "]}");
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
        const id_buf = [_]u8{'m'} ** 16;
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
