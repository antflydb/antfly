// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
//! Replicated producer results. Local leases and replay sequences are not
//! authority: ingress verifies the lease, while apply checks stable source
//! values and a committed producer epoch. Payloads contain final values, never
//! local staging paths or promotion keys.
const std = @import("std");
const keys = @import("../internal_keys.zig");

pub const max_mutations = 4096;
pub const max_source_documents = 4096;
pub const max_payload_bytes = 64 * 1024 * 1024;
pub const Digest = [32]u8;
pub const Namespace = [24]u8;
pub const Position = @import("receipt_position.zig").Position;
pub fn namespaceFromBytes(value: Namespace) @import("doc_identity_namespace.zig").Namespace {
    return .{ .table_id = std.mem.readInt(u64, value[0..8], .big), .shard_id = std.mem.readInt(u64, value[8..16], .big), .range_id = std.mem.readInt(u64, value[16..24], .big) };
}
pub const Family = enum { base_vector, derived_vector, document_artifact, resolution, graph };
pub const authority_key = "\x00\x00__artifact_publication__:authority";
pub const Authority = struct { namespace: Namespace, epoch: u64, catalog_digest: Digest };

pub fn authority(txn: anytype) !?Authority {
    const raw = txn.get(authority_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try decodeAuthority(raw);
}

pub fn decodeAuthority(raw: []const u8) !Authority {
    if (raw.len != 100 or !std.mem.eql(u8, raw[0..4], "APA1")) return error.ArtifactCatalogCorrupt;
    var digest: Digest = undefined;
    std.crypto.hash.Blake3.hash(raw[0..68], &digest, .{});
    if (!std.mem.eql(u8, raw[68..100], &digest)) return error.ArtifactCatalogCorrupt;
    const result: Authority = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .catalog_digest = raw[36..68].* };
    if (result.epoch == 0 or std.mem.allEqual(u8, &result.namespace, 0) or std.mem.allEqual(u8, &result.catalog_digest, 0)) return error.ArtifactCatalogCorrupt;
    return result;
}

pub fn stageAuthority(txn: anytype, command: Command) !void {
    if (command.mode != .activate) return error.InvalidBatchRequest;
    try @import("artifact_activation_boundary.zig").stage(txn, .{ .namespace = command.namespace, .epoch = command.authority_epoch, .catalog_digest = command.catalog_digest });
    var raw: [100]u8 = undefined;
    @memcpy(raw[0..4], "APA1");
    @memcpy(raw[4..28], &command.namespace);
    std.mem.writeInt(u64, raw[28..36], command.authority_epoch, .little);
    @memcpy(raw[36..68], &command.catalog_digest);
    std.crypto.hash.Blake3.hash(raw[0..68], raw[68..100], .{});
    try txn.put(authority_key, &raw);
}

/// Local worker bookkeeping may continue after activation, but final artifact
/// bytes and terminal coverage must pass the ordered writer. This check runs
/// in the same backend write transaction as the legacy mutation, so authority
/// activation cannot race a provider that started before the barrier.
pub fn requiresOrderedMaterialization(key: []const u8) bool {
    const coverage_prefix = [_]u8{ keys.replay_namespace, 0xff, keys.derived_coverage_kind };
    return std.mem.startsWith(u8, key, &coverage_prefix) or std.mem.startsWith(u8, key, @import("artifact_coverage_epoch.zig").prefix) or
        keys.isEmbeddingArtifactKey(key) or keys.isDerivedEmbeddingArtifactKey(key) or
        keys.isChunkArtifactRecordKey(key) or keys.isAssetArtifactKey(key) or
        @import("artifact_generation_scope.zig").isKey(key) or
        @import("artifact_generation_scope.zig").isHead(key) or
        keys.isDocumentUnitArtifactRecordKey(key) or keys.isSummaryArtifactKey(key) or
        keys.isResolutionArtifactKey(key) or @import("online_graph_artifacts.zig").isKey(key);
}

pub fn requireLegacyMaterialization(txn: anytype, key: []const u8) !void {
    if (requiresOrderedMaterialization(key) and try authority(txn) != null) return error.ArtifactCatalogDrift;
}

/// Content and timestamp identify the prepared input; its commit position also
/// rejects delete/reinsert ABA even when both content and timestamp are reused.
pub const Source = struct {
    document_key: []const u8,
    exists: bool = true,
    content_digest: Digest,
    timestamp: u64,
    /// Null only for baseline rows predating producer authority activation.
    /// Native clocks and data-Raft positions remain distinct authority domains.
    input_position: ?Position,
};
pub const ArtifactSource = struct {
    key: []const u8,
    /// Null certifies absence, including a prior deletion's revision stamp.
    content_digest: ?Digest,
    input_position: ?Position,
    source_index: u32,
};
pub const Mutation = struct {
    family: Family,
    key: []const u8,
    value: ?[]const u8,
    source_index: u32,
};
/// A leader-discovered primary-key page. Discovery is not progress: only the
/// ordered owner transaction may install its obligations and advance cursor.
pub const BaselinePage = struct {
    observed_term: u64,
    observed_index: u64,
    expected_cursor: []const u8,
    next_cursor: []const u8,
    upper_bound: []const u8,
    row_keys: []const []const u8,
    at_end: bool,

    pub fn validate(self: BaselinePage) !void {
        if (self.observed_term == 0 or self.observed_index == 0 or self.row_keys.len > 128 or
            self.expected_cursor.len > 1024 * 1024 or self.next_cursor.len > 1024 * 1024 or self.upper_bound.len > 1024 * 1024 or
            std.mem.order(u8, self.next_cursor, self.upper_bound) == .gt)
            return error.InvalidBatchRequest;
        const order = std.mem.order(u8, self.next_cursor, self.expected_cursor);
        if (order == .lt or (!self.at_end and order != .gt)) return error.InvalidBatchRequest;
        var previous = self.expected_cursor;
        var remaining: usize = 2 * 1024 * 1024;
        for (self.row_keys) |key| {
            if (!keys.isStoredDocumentRowKey(key) or std.mem.order(u8, key, previous) != .gt or
                std.mem.order(u8, key, self.next_cursor) == .gt) return error.InvalidBatchRequest;
            remaining = std.math.sub(usize, remaining, key.len) catch return error.InvalidBatchRequest;
            previous = key;
        }
    }
};
/// An off-lock validation result, fenced by the durable mutation epoch and
/// exact scan cursor. Only the ordered transaction may advance this result.
pub const ValidationPage = struct {
    mutation_epoch: u64,
    expected_cursor: []const u8,
    next_cursor: []const u8,
    at_end: bool,
    repair_documents: []const []const u8,

    pub fn validate(self: ValidationPage) !void {
        if (self.expected_cursor.len > 1024 * 1024 or self.next_cursor.len > 1024 * 1024 or self.repair_documents.len > 128) return error.InvalidBatchRequest;
        const order = std.mem.order(u8, self.next_cursor, self.expected_cursor);
        if (order == .lt or (!self.at_end and order != .gt)) return error.InvalidBatchRequest;
        var remaining: usize = 2 * 1024 * 1024;
        for (self.repair_documents) |document| remaining = std.math.sub(usize, remaining, document.len) catch return error.InvalidBatchRequest;
    }
};
pub const CensusPage = struct {
    document_key: []const u8,
    chunk_name: []const u8,
    visits: u32 = 128,
    bytes: u32 = 64 * 1024,
    before: Digest,
    after: Digest,
    pub fn validate(self: CensusPage) !void {
        if (self.document_key.len == 0 or self.document_key.len > 1024 * 1024 or self.chunk_name.len == 0 or self.chunk_name.len > 1024 * 1024 or
            self.visits == 0 or self.visits > 128 or self.bytes == 0 or self.bytes > 64 * 1024 or std.mem.eql(u8, &self.before, &self.after)) return error.InvalidBatchRequest;
    }
};
/// A portable request to verify one bounded prefix of the required-stream plan.
/// Claims exclude receiver-local root and obligation counters; they never
/// substitute for that receiver's own durable evidence.
pub const CompletionPage = struct {
    document_key: []const u8,
    visits: u32,
    bytes: u32,
    before: Digest,
    after: Digest,
    pub fn validate(self: CompletionPage) !void {
        if (self.document_key.len == 0 or self.document_key.len > 1024 * 1024 or
            self.visits == 0 or self.visits > 128 or self.bytes == 0 or self.bytes > 64 * 1024 or
            std.mem.eql(u8, &self.before, &self.after)) return error.InvalidBatchRequest;
    }
};
pub const Command = struct {
    mode: enum { activate, publish, baseline, validate_inputs, census, complete_streams, reconcile_units } = .publish,
    producer_kind: enum { index, enrichment, resolver, graph, promotion } = .index,
    namespace: Namespace,
    authority_epoch: u64,
    catalog_digest: Digest,
    producer_name: []const u8,
    producer_generation: u64,
    producer_artifact_name: []const u8 = "",
    /// Empty for document-wide work; otherwise the exact logical chunk/unit
    /// input guard. Distinct units never replace each other's receipts.
    producer_scope_key: []const u8 = "",
    sources: []const Source,
    artifact_sources: []const ArtifactSource = &.{},
    /// Physical compare-and-swap read set, not inference dependencies. A
    /// successful publication changes these values itself.
    mutation_preconditions: []const ArtifactSource = &.{},
    mutations: []const Mutation,
    publication_digest: Digest,
    baseline: ?BaselinePage = null,
    validation: ?ValidationPage = null,
    census: ?CensusPage = null,
    completion: ?CompletionPage = null,

    /// Dependencies participate in the immutable read set but do not earn a
    /// producer receipt. Only documents owning an authored output (including
    /// an explicit deletion) represent completed work for this stream.
    pub fn outputSources(self: Command) !std.StaticBitSet(max_source_documents) {
        if (self.sources.len > max_source_documents or self.mutations.len == 0) return error.InvalidBatchRequest;
        var result = std.StaticBitSet(max_source_documents).initEmpty();
        for (self.mutations) |effect| {
            if (effect.source_index >= self.sources.len) return error.InvalidBatchRequest;
            result.set(effect.source_index);
        }
        return result;
    }

    pub fn jsonStringify(self: @This(), stream: anytype) !void {
        try @import("artifact_publication_wire.zig").write(self, stream);
    }
    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
        return @import("artifact_publication_wire.zig").parse(alloc, source, options);
    }
    pub fn jsonParseFromValue(alloc: std.mem.Allocator, value: std.json.Value, options: std.json.ParseOptions) !@This() {
        return @import("artifact_publication_wire.zig").parseValue(alloc, value, options);
    }

    pub fn digest(self: Command) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-artifact-publication-v1\x00");
        bytes(&hash, @tagName(self.mode));
        bytes(&hash, @tagName(self.producer_kind));
        hash.update(&self.namespace);
        number(&hash, self.authority_epoch);
        hash.update(&self.catalog_digest);
        bytes(&hash, self.producer_name);
        number(&hash, self.producer_generation);
        bytes(&hash, self.producer_artifact_name);
        bytes(&hash, self.producer_scope_key);
        number(&hash, self.sources.len);
        for (self.sources) |source| {
            bytes(&hash, source.document_key);
            hash.update(&.{@intFromBool(source.exists)});
            hash.update(&source.content_digest);
            number(&hash, source.timestamp);
            hash.update(&positionBytes(source.input_position));
        }
        number(&hash, self.artifact_sources.len);
        for (self.artifact_sources) |source| {
            bytes(&hash, source.key);
            number(&hash, source.source_index);
            hash.update(&.{@intFromBool(source.content_digest != null)});
            if (source.content_digest) |digest_value| hash.update(&digest_value);
            hash.update(&positionBytes(source.input_position));
        }
        number(&hash, self.mutation_preconditions.len);
        for (self.mutation_preconditions) |source| {
            bytes(&hash, source.key);
            number(&hash, source.source_index);
            hash.update(&.{@intFromBool(source.content_digest != null)});
            if (source.content_digest) |digest_value| hash.update(&digest_value);
            hash.update(&positionBytes(source.input_position));
        }
        number(&hash, self.mutations.len);
        for (self.mutations) |mutation| {
            bytes(&hash, @tagName(mutation.family));
            bytes(&hash, mutation.key);
            number(&hash, mutation.source_index);
            hash.update(&.{@intFromBool(mutation.value != null)});
            if (mutation.value) |value| bytes(&hash, value);
        }
        hash.update(&.{@intFromBool(self.baseline != null)});
        if (self.baseline) |page| {
            number(&hash, page.observed_term);
            number(&hash, page.observed_index);
            bytes(&hash, page.expected_cursor);
            bytes(&hash, page.next_cursor);
            bytes(&hash, page.upper_bound);
            hash.update(&.{@intFromBool(page.at_end)});
            number(&hash, page.row_keys.len);
            for (page.row_keys) |key| bytes(&hash, key);
        }
        hash.update(&.{@intFromBool(self.validation != null)});
        if (self.validation) |page| {
            number(&hash, page.mutation_epoch);
            bytes(&hash, page.expected_cursor);
            bytes(&hash, page.next_cursor);
            hash.update(&.{@intFromBool(page.at_end)});
            number(&hash, page.repair_documents.len);
            for (page.repair_documents) |document| bytes(&hash, document);
        }
        hash.update(&.{@intFromBool(self.census != null)});
        if (self.census) |page| {
            bytes(&hash, page.document_key);
            bytes(&hash, page.chunk_name);
            number(&hash, page.visits);
            number(&hash, page.bytes);
            hash.update(&page.before);
            hash.update(&page.after);
        }
        hash.update(&.{@intFromBool(self.completion != null)});
        if (self.completion) |page| {
            bytes(&hash, page.document_key);
            number(&hash, page.visits);
            number(&hash, page.bytes);
            hash.update(&page.before);
            hash.update(&page.after);
        }
        var out: Digest = undefined;
        hash.final(&out);
        return out;
    }

    /// Provider outputs are deliberately excluded. A winning result is
    /// canonical only for this complete immutable input read-set.
    pub fn inputDigest(self: Command) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-producer-input-set-v1\x00");
        number(&hash, self.sources.len);
        for (self.sources) |source| {
            bytes(&hash, source.document_key);
            hash.update(&.{@intFromBool(source.exists)});
            hash.update(&source.content_digest);
            number(&hash, source.timestamp);
            hash.update(&positionBytes(source.input_position));
        }
        number(&hash, self.artifact_sources.len);
        for (self.artifact_sources) |source| {
            bytes(&hash, source.key);
            number(&hash, source.source_index);
            hash.update(&.{@intFromBool(source.content_digest != null)});
            if (source.content_digest) |value| hash.update(&value);
            hash.update(&positionBytes(source.input_position));
        }
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }

    /// Structural validation is shared by proposal and apply. Family-specific
    /// codecs and catalog consumer authorization are checked by the owner too.
    pub fn validate(self: Command, alloc: std.mem.Allocator) !void {
        if (self.authority_epoch == 0 or std.mem.allEqual(u8, &self.namespace, 0) or
            std.mem.allEqual(u8, &self.catalog_digest, 0)) return error.InvalidBatchRequest;
        if (self.mode == .baseline) {
            try (self.baseline orelse return error.InvalidBatchRequest).validate();
        } else if (self.baseline != null) return error.InvalidBatchRequest;
        if (self.mode == .validate_inputs) {
            try (self.validation orelse return error.InvalidBatchRequest).validate();
        } else if (self.validation != null) return error.InvalidBatchRequest;
        if (self.mode == .complete_streams) {
            try (self.completion orelse return error.InvalidBatchRequest).validate();
        } else if (self.completion != null) return error.InvalidBatchRequest;
        if (self.mode == .census or self.mode == .reconcile_units) {
            try (self.census orelse return error.InvalidBatchRequest).validate();
            if (self.mode == .reconcile_units and (self.producer_kind != .enrichment or self.producer_scope_key.len != 0)) return error.InvalidBatchRequest;
            if ((self.producer_kind != .index and self.producer_kind != .enrichment) or self.producer_generation == 0 or
                self.producer_name.len == 0 or self.producer_name.len > 1024 * 1024 or
                self.producer_artifact_name.len == 0 or self.producer_artifact_name.len > 1024 * 1024 or
                self.producer_scope_key.len > 1024 * 1024 or self.sources.len != 0 or self.artifact_sources.len != 0 or
                self.mutation_preconditions.len != 0 or self.mutations.len != 0 or
                !std.mem.eql(u8, &self.publication_digest, &self.digest())) return error.InvalidBatchRequest;
            if (self.producer_kind == .index) {
                if (self.producer_scope_key.len != 0) return error.InvalidBatchRequest;
            } else {
                if (self.producer_generation != self.authority_epoch or
                    !std.mem.eql(u8, self.producer_name, self.producer_artifact_name) or
                    !std.mem.eql(u8, self.producer_name, self.census.?.chunk_name)) return error.InvalidBatchRequest;
            }
            return;
        } else if (self.census != null) return error.InvalidBatchRequest;
        if (self.mode != .publish) {
            if (self.producer_kind != .index) return error.InvalidBatchRequest;
            if (self.producer_scope_key.len != 0) return error.InvalidBatchRequest;
            if (self.mutation_preconditions.len != 0) return error.InvalidBatchRequest;
            if (self.producer_name.len != 0 or self.producer_artifact_name.len != 0 or self.producer_generation != 0 or self.sources.len != 0 or self.artifact_sources.len != 0 or self.mutations.len != 0 or
                !std.mem.eql(u8, &self.publication_digest, &self.digest())) return error.InvalidBatchRequest;
            return;
        }
        if ((self.producer_generation == 0 and self.producer_kind != .resolver) or
            self.producer_name.len == 0 or self.producer_artifact_name.len == 0 or self.sources.len == 0 or
            self.sources.len > max_source_documents or self.mutations.len == 0 or
            self.artifact_sources.len > max_source_documents or self.mutation_preconditions.len > max_source_documents - self.artifact_sources.len or
            self.mutations.len > max_mutations or
            std.mem.allEqual(u8, &self.namespace, 0) or
            std.mem.allEqual(u8, &self.catalog_digest, 0)) return error.InvalidBatchRequest;
        var remaining: usize = max_payload_bytes;
        try consume(&remaining, self.producer_name.len);
        try consume(&remaining, self.producer_artifact_name.len);
        try consume(&remaining, self.producer_scope_key.len);
        if (self.producer_scope_key.len != 0) {
            var scoped = (try @import("artifact_ids.zig").decodeArtifactRefAlloc(alloc, self.producer_scope_key)) orelse return error.InvalidBatchRequest;
            defer scoped.deinit(alloc);
            if (scoped.kind != .chunk and !(scoped.kind == .asset and scoped.unit_id != null)) return error.InvalidBatchRequest;
            const head = if (scoped.kind == .chunk)
                try @import("artifact_chunk_manifest.zig").keyForMemberAlloc(alloc, self.producer_scope_key)
            else
                try @import("artifact_extraction_generation.zig").headKeyAlloc(alloc, scoped.document_id, scoped.name);
            defer if (head) |key| alloc.free(key);
            if (scoped.kind == .chunk) if (head) |key| {
                key[keys.findComponentTerminator(key, 1).? + 2] = keys.producer_generation_head_kind;
            };
            const guarded = for (self.artifact_sources) |source| {
                if (std.mem.eql(u8, source.key, self.producer_scope_key)) break true;
                if (head) |key| if (source.content_digest != null and std.mem.eql(u8, source.key, key)) break true;
            } else false;
            if (!guarded) return error.InvalidBatchRequest;
        }
        var documents: std.StringHashMapUnmanaged(void) = .empty;
        defer documents.deinit(alloc);
        for (self.sources, 0..) |source, source_order| {
            if (source.document_key.len == 0) return error.InvalidBatchRequest;
            if (source.exists) {
                if (source.timestamp == 0) return error.InvalidBatchRequest;
            } else if (source.timestamp != 0 or !std.mem.allEqual(u8, &source.content_digest, 0) or source.input_position == null) return error.InvalidBatchRequest;
            if (source_order != 0 and std.mem.order(u8, self.sources[source_order - 1].document_key, source.document_key) != .lt) return error.InvalidBatchRequest;
            if (source.input_position) |position| position.requireNamespace(namespaceFromBytes(self.namespace)) catch return error.InvalidBatchRequest;
            try consume(&remaining, source.document_key.len);
            if ((try documents.getOrPut(alloc, source.document_key)).found_existing) return error.InvalidBatchRequest;
        }
        var mutations: std.StringHashMapUnmanaged(void) = .empty;
        defer mutations.deinit(alloc);
        for ([_][]const ArtifactSource{ self.artifact_sources, self.mutation_preconditions }) |guards| {
            mutations.clearRetainingCapacity();
            for (guards, 0..) |source, source_order| {
                if (source.source_index >= self.sources.len or !guardedArtifactKey(source.key)) return error.InvalidBatchRequest;
                if (source_order != 0 and std.mem.order(u8, guards[source_order - 1].key, source.key) != .lt) return error.InvalidBatchRequest;
                try consume(&remaining, source.key.len);
                if (source.input_position) |position| position.requireNamespace(namespaceFromBytes(self.namespace)) catch return error.InvalidBatchRequest;
                if ((try mutations.getOrPut(alloc, source.key)).found_existing) return error.InvalidBatchRequest;
                const document = try @import("artifact_publication_owner.zig").documentAlloc(alloc, source.key);
                defer alloc.free(document);
                if (!std.mem.eql(u8, document, self.sources[source.source_index].document_key)) return error.InvalidBatchRequest;
            }
        }
        mutations.clearRetainingCapacity();
        for (self.mutations) |mutation| {
            if (mutation.source_index >= self.sources.len or mutation.key.len == 0) return error.InvalidBatchRequest;
            try consume(&remaining, mutation.key.len);
            if (mutation.value) |value| try consume(&remaining, value.len);
            if ((try mutations.getOrPut(alloc, mutation.key)).found_existing) return error.InvalidBatchRequest;
            const document = try @import("artifact_publication_owner.zig").documentAlloc(alloc, mutation.key);
            defer alloc.free(document);
            if (!std.mem.eql(u8, document, self.sources[mutation.source_index].document_key)) return error.InvalidBatchRequest;
            // A document prefix alone never authorizes primary rows, identity,
            // leases, catalog metadata, or arbitrary system records.
            if (!validFamilyKey(mutation.family, mutation.key)) return error.InvalidBatchRequest;
            if (!self.sources[mutation.source_index].exists and mutation.value != null) {
                // A deleted chunk source can retain only an authenticated
                // empty inventory, never live members. Producer preparation
                // additionally checks ownership and complete tail retirement.
                if (self.producer_kind == .enrichment and mutation.family == .document_artifact and @import("artifact_chunk_manifest.zig").isKey(mutation.key)) {
                    const manifest = @import("artifact_chunk_manifest.zig").Manifest.decode(mutation.value.?) catch return error.InvalidBatchRequest;
                    if (manifest.count != 0) return error.InvalidBatchRequest;
                    continue;
                }
                // Graph cleanup retains exact empty state/count witnesses,
                // preventing retries from mistaking cleaned-up ownership for
                // an inventory that still needs reconstruction. Neither can
                // resurrect edges or unverified segmented state.
                if (self.producer_kind != .graph or mutation.family != .graph) return error.InvalidBatchRequest;
                if (keys.isGraphAssetStateRootKey(mutation.key) and
                    keys.matchesGraphAssetStateIndexName(mutation.key, self.producer_name) and
                    @import("graph_asset_state.zig").isEmptyForGeneration(mutation.value.?, self.producer_generation)) continue;
                const count_key = try keys.graphEdgeContenderCountKeyAlloc(alloc, document, self.producer_name);
                defer alloc.free(count_key);
                if (!std.mem.eql(u8, count_key, mutation.key)) return error.InvalidBatchRequest;
                const count = @import("graph_edge_contender.zig").decodeVisibleCount(mutation.value.?, self.producer_generation) catch return error.InvalidBatchRequest;
                if (count == null or count.? != 0) return error.InvalidBatchRequest;
            }
        }
        if (!std.mem.eql(u8, &self.publication_digest, &self.digest())) return error.InvalidBatchRequest;
    }
};

pub const Receipt = struct { publication_digest: Digest, sequence: u64 };
pub const Rejection = enum(u8) { stale_source = 1, stale_catalog = 2, sealed_source = 3, invalid_output = 4, baseline_pending = 5 };
pub const Rejected = struct { publication_digest: Digest, applied_index: u64, reason: Rejection };
const receipt_prefix = "\x00\x00__artifact_publication__:receipt:";
const receipt_bytes = 4 + 32 + 32 + 8 + 8 + Position.encoded_len + 32 + 32;
const input_prefix = "\x00\x00__artifact_publication__:input:";
const artifact_input_prefix = "\x00\x00__artifact_publication__:artifact-input:";
const materialization_prefix = "\x00\x00__artifact_publication__:materialization-input:";

/// Whole-document producer census witness, including absent/new unit scopes.
/// Unlike the primary input stamp, every visible owned artifact mutation moves
/// this position. Private upload/generation staging does not change visibility.
/// This is not a completion receipt or proof about cross-document dependencies.
pub fn materializationRevisionKey(namespace: Namespace, document: []const u8) [materialization_prefix.len + 32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly-artifact-materialization-input-v1\x00");
    hash.update(&namespace);
    bytes(&hash, document);
    var result: [materialization_prefix.len + 32]u8 = undefined;
    @memcpy(result[0..materialization_prefix.len], materialization_prefix);
    hash.final(result[materialization_prefix.len..]);
    return result;
}

/// Exact local replay boundary of this owner's last visible mutation. A raw
/// physical/import write without a replay record has no projection entitlement;
/// an unrelated older journal tip must never be substituted for one.
pub const Materialization = struct {
    position: Position,
    replay_sequence: ?u64,
    pub const encoded_len = 4 + Position.encoded_len + 8;

    pub fn encode(self: Materialization) ![encoded_len]u8 {
        if (self.replay_sequence == 0 or self.replay_sequence == std.math.maxInt(u64)) return error.InvalidBatchRequest;
        var raw: [encoded_len]u8 = undefined;
        @memcpy(raw[0..4], "AMR1");
        @memcpy(raw[4..][0..Position.encoded_len], &try self.position.encode());
        std.mem.writeInt(u64, raw[4 + Position.encoded_len ..][0..8], self.replay_sequence orelse 0, .little);
        return raw;
    }

    pub fn decode(raw: []const u8, namespace: Namespace) !Materialization {
        if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..4], "AMR1")) return error.ArtifactCatalogCorrupt;
        const position = Position.decode(raw[4..][0..Position.encoded_len]) catch return error.ArtifactCatalogCorrupt;
        position.requireNamespace(namespaceFromBytes(namespace)) catch return error.ArtifactCatalogCorrupt;
        const sequence = std.mem.readInt(u64, raw[4 + Position.encoded_len ..][0..8], .little);
        if (sequence == std.math.maxInt(u64)) return error.ArtifactCatalogCorrupt;
        return .{ .position = position, .replay_sequence = if (sequence == 0) null else sequence };
    }
};

test "ordered artifact inventory materialization boundary rejects invalid and foreign positions" {
    const namespace: Namespace = @splat(1);
    const state: Materialization = .{ .position = .{ .native = .{ .namespace = namespaceFromBytes(namespace), .sequence = 7 } }, .replay_sequence = 19 };
    const raw = try state.encode();
    try std.testing.expectEqualDeep(state, try Materialization.decode(&raw, namespace));
    var absent = state;
    absent.replay_sequence = null;
    try std.testing.expectEqualDeep(absent, try Materialization.decode(&try absent.encode(), namespace));
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Materialization.decode(&raw, @splat(2)));
    for (0..raw.len) |len| try std.testing.expectError(error.ArtifactCatalogCorrupt, Materialization.decode(raw[0..len], namespace));
    var corrupt = raw;
    corrupt[0] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Materialization.decode(&corrupt, namespace));
    corrupt = raw;
    @memset(corrupt[4 + Position.encoded_len ..], 255);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Materialization.decode(&corrupt, namespace));
    absent.replay_sequence = 0;
    try std.testing.expectError(error.InvalidBatchRequest, absent.encode());
    absent.replay_sequence = std.math.maxInt(u64);
    try std.testing.expectError(error.InvalidBatchRequest, absent.encode());
}

pub fn materializationState(txn: anytype, namespace: Namespace, document: []const u8) !?Materialization {
    const raw = txn.get(&materializationRevisionKey(namespace, document)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try Materialization.decode(raw, namespace);
}

pub fn materializationRevision(txn: anytype, namespace: Namespace, document: []const u8) !?Position {
    return if (try materializationState(txn, namespace, document)) |state| state.position else null;
}

pub fn artifactRevisionKey(namespace: Namespace, artifact_key: []const u8) [artifact_input_prefix.len + 32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly-artifact-revision-v1\x00");
    hash.update(&namespace);
    bytes(&hash, artifact_key);
    var result: [artifact_input_prefix.len + 32]u8 = undefined;
    @memcpy(result[0..artifact_input_prefix.len], artifact_input_prefix);
    hash.final(result[artifact_input_prefix.len..]);
    return result;
}

pub fn artifactRevision(txn: anytype, namespace: Namespace, artifact_key: []const u8) !?Position {
    const raw = txn.get(&artifactRevisionKey(namespace, artifact_key)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const position = Position.decode(raw) catch return error.ArtifactCatalogCorrupt;
    position.requireNamespace(namespaceFromBytes(namespace)) catch return error.ArtifactCatalogCorrupt;
    return position;
}

pub fn stageArtifactRevisions(txn: anytype, command: Command, position: Position) !void {
    try position.requireNamespace(namespaceFromBytes(command.namespace));
    const encoded = try position.encode();
    for (command.mutations) |mutation| try txn.put(&artifactRevisionKey(command.namespace, mutation.key), &encoded);
}

pub fn guardedArtifactKey(key_bytes: []const u8) bool {
    // Head bytes are valid read-set witnesses, not generic artifact mutations.
    // Only the staged-output publication path may change generation visibility.
    if (@import("artifact_generation_scope.zig").isHead(key_bytes)) return true;
    inline for (std.meta.tags(Family)) |family| if (validFamilyKey(family, key_bytes)) return true;
    return false;
}

test "ordered artifact inventory generation heads are read guards not generic mutation authority" {
    const alloc = std.testing.allocator;
    const chunks = @import("artifact_chunk_manifest.zig");
    const scope = try chunks.scopedKeyAlloc(alloc, "doc\x00", "producer\xff", "unit\x00");
    defer alloc.free(scope);
    const kind_offset = keys.findComponentTerminator(scope, 1).? + 2;
    scope[kind_offset] = keys.producer_generation_head_kind;
    try std.testing.expect(guardedArtifactKey(scope));
    try std.testing.expect(requiresOrderedMaterialization(scope));
    inline for (std.meta.tags(Family)) |family| try std.testing.expect(!validFamilyKey(family, scope));
    for ([_]u8{ keys.producer_generation_row_kind, keys.producer_generation_state_kind }) |kind| {
        scope[kind_offset] = kind;
        try std.testing.expect(!guardedArtifactKey(scope));
    }
    scope[kind_offset] = keys.producer_generation_head_kind;
    const malformed = try std.mem.concat(alloc, u8, &.{ scope, "suffix" });
    defer alloc.free(malformed);
    try std.testing.expect(!guardedArtifactKey(malformed));
}

pub fn validateArtifactSources(alloc: std.mem.Allocator, txn: anytype, namespace: Namespace, sources: []const Source, artifact_sources: []const ArtifactSource) !void {
    for (artifact_sources) |source| {
        if (source.source_index >= sources.len or !guardedArtifactKey(source.key)) return error.InvalidBatchRequest;
        const document = try @import("artifact_publication_owner.zig").documentAlloc(alloc, source.key);
        defer alloc.free(document);
        if (!std.mem.eql(u8, document, sources[source.source_index].document_key)) return error.InvalidBatchRequest;
        if (!std.meta.eql(try artifactRevision(txn, namespace, source.key), source.input_position)) return error.EnrichmentSourceChanged;
        const raw = txn.get(source.key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (raw) |value| {
            const expected = source.content_digest orelse return error.EnrichmentSourceChanged;
            var actual: Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &actual, .{});
            if (!std.mem.eql(u8, &expected, &actual)) return error.EnrichmentSourceChanged;
        } else if (source.content_digest != null) return error.EnrichmentSourceChanged;
    }
}

/// Capture one owned physical primary input in the caller's read snapshot.
/// AROW bytes remain physical here; cross-owner adoption uses logical proofs.
pub fn capturePrimarySource(alloc: std.mem.Allocator, txn: anytype, namespace: Namespace, document: []const u8) !Source {
    const doc_key = try keys.documentKeyAlloc(alloc, document);
    defer alloc.free(doc_key);
    const row_key = try keys.relationalRowKeyAlloc(alloc, document);
    defer alloc.free(row_key);
    const doc = txn.get(doc_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const row = txn.get(row_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (doc != null and row != null) return error.EnrichmentSourceChanged;
    const raw = doc orelse row orelse return error.EnrichmentSourceChanged;
    var digest_value: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &digest_value, .{});
    const ttl_key = try keys.ttlKeyAlloc(alloc, document);
    defer alloc.free(ttl_key);
    const ttl = txn.get(ttl_key) catch |err| switch (err) {
        error.NotFound => return error.EnrichmentSourceChanged,
        else => return err,
    };
    if (ttl.len != 8) return error.EnrichmentSourceChanged;
    const timestamp = std.mem.readInt(u64, ttl[0..8], .little);
    if (timestamp == 0) return error.EnrichmentSourceChanged;
    const position = try inputRevision(txn, namespace, document);
    return .{ .document_key = try alloc.dupe(u8, document), .content_digest = digest_value, .timestamp = timestamp, .input_position = position };
}

pub fn capturePrimaryTombstoneSource(alloc: std.mem.Allocator, txn: anytype, namespace: Namespace, document: []const u8) !Source {
    const position = (try inputRevision(txn, namespace, document)) orelse return error.EnrichmentSourceChanged;
    const source: Source = .{ .document_key = document, .exists = false, .content_digest = @splat(0), .timestamp = 0, .input_position = position };
    try validateSources(alloc, txn, namespace, (&source)[0..1]);
    return .{ .document_key = try alloc.dupe(u8, document), .exists = false, .content_digest = @splat(0), .timestamp = 0, .input_position = position };
}

pub fn inputRevisionKey(namespace: Namespace, document: []const u8) [input_prefix.len + 32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly-artifact-input-v1\x00");
    hash.update(&namespace);
    bytes(&hash, document);
    var result: [input_prefix.len + 32]u8 = undefined;
    @memcpy(result[0..input_prefix.len], input_prefix);
    hash.final(result[input_prefix.len..]);
    return result;
}

pub fn inputRevision(txn: anytype, namespace: Namespace, document: []const u8) !?Position {
    const key = inputRevisionKey(namespace, document);
    const raw = txn.get(&key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const revision = Position.decode(raw) catch return error.ArtifactCatalogCorrupt;
    revision.requireNamespace(namespaceFromBytes(namespace)) catch return error.ArtifactCatalogCorrupt;
    return revision;
}

fn positionBytes(position: ?Position) [Position.encoded_len]u8 {
    return if (position) |value| value.encode() catch @as([Position.encoded_len]u8, @splat(0)) else @splat(0);
}

/// One slot per namespace/producer incarnation/document, not one slot per
/// attempt. Retrying a lost response does not grow an unbounded receipt log.
pub fn receiptKey(command: Command, source: Source) [receipt_prefix.len + 32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly-artifact-publication-receipt-v1\x00");
    hash.update(&command.namespace);
    number(&hash, command.authority_epoch);
    bytes(&hash, @tagName(command.producer_kind));
    bytes(&hash, command.producer_name);
    number(&hash, command.producer_generation);
    bytes(&hash, command.producer_artifact_name);
    bytes(&hash, command.producer_scope_key);
    bytes(&hash, source.document_key);
    var result: [receipt_prefix.len + 32]u8 = undefined;
    @memcpy(result[0..receipt_prefix.len], receipt_prefix);
    hash.final(result[receipt_prefix.len..]);
    return result;
}

fn rejectionKey(command: Command) [receipt_prefix.len + 33]u8 {
    const source: Source = if (command.sources.len != 0) command.sources[0] else .{
        .document_key = "",
        .content_digest = @splat(0),
        .timestamp = 0,
        .input_position = null,
    };
    var key: [receipt_prefix.len + 33]u8 = undefined;
    @memcpy(key[0 .. key.len - 1], &receiptKey(command, source));
    key[key.len - 1] = 0xff;
    return key;
}

/// A separate bounded response slot must never overwrite accepted provenance
/// when a delayed old command is rejected after a newer source was published.
pub fn stageRejection(txn: anytype, command: Command, index: u64, reason: Rejection) !void {
    if (index == 0) return error.InvalidBatchRequest;
    var raw: [77]u8 = undefined;
    @memcpy(raw[0..4], "APX1");
    raw[4] = @intFromEnum(reason);
    @memcpy(raw[5..37], &command.publication_digest);
    std.mem.writeInt(u64, raw[37..45], index, .little);
    std.crypto.hash.Blake3.hash(raw[0..45], raw[45..77], .{});
    try txn.put(&rejectionKey(command), &raw);
}

pub fn rejected(txn: anytype, command: Command) !?Rejected {
    const raw = txn.get(&rejectionKey(command)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != 77 or !std.mem.eql(u8, raw[0..4], "APX1")) return error.ArtifactCatalogCorrupt;
    var digest: Digest = undefined;
    std.crypto.hash.Blake3.hash(raw[0..45], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[45..77])) return error.ArtifactCatalogCorrupt;
    const reason = std.enums.fromInt(Rejection, raw[4]) orelse return error.ArtifactCatalogCorrupt;
    const index = std.mem.readInt(u64, raw[37..45], .little);
    if (index == 0) return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, &command.publication_digest, raw[5..37])) return null;
    return .{ .publication_digest = raw[5..37].*, .applied_index = index, .reason = reason };
}

pub fn readReceipt(txn: anytype, command: Command, source: Source) !?Receipt {
    return ReceiptSet.init(command).read(txn, source);
}

/// One logical read-set digest per command, not per source receipt. Commands
/// with many source documents otherwise perform quadratic hashing under the
/// writer admission lock. The borrowed command must remain immutable.
pub const ReceiptSet = struct {
    command: Command,
    input_digest: Digest,

    pub fn init(command: Command) ReceiptSet {
        return .{ .command = command, .input_digest = command.inputDigest() };
    }

    pub fn read(self: ReceiptSet, txn: anytype, source: Source) !?Receipt {
        return readReceiptWithDigest(txn, self.command, source, self.input_digest);
    }
};

fn readReceiptWithDigest(txn: anytype, command: Command, source: Source, input_digest: Digest) !?Receipt {
    const key = receiptKey(command, source);
    const raw = txn.get(&key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != receipt_bytes or !std.mem.eql(u8, raw[0..4], "APR2")) return error.ArtifactCatalogCorrupt;
    var digest: Digest = undefined;
    std.crypto.hash.Blake3.hash(raw[0 .. receipt_bytes - 32], &digest, .{});
    if (!std.mem.eql(u8, raw[receipt_bytes - 32 ..], &digest)) return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, raw[36..68], &source.content_digest) or
        std.mem.readInt(u64, raw[68..76], .little) != source.timestamp or
        !std.mem.eql(u8, raw[84 .. 84 + Position.encoded_len], &positionBytes(source.input_position)) or
        !std.mem.eql(u8, raw[84 + Position.encoded_len ..][0..32], &input_digest)) return null;
    const sequence = std.mem.readInt(u64, raw[76..84], .little);
    if (sequence == 0) return error.ArtifactCatalogCorrupt;
    return .{ .publication_digest = raw[4..36].*, .sequence = sequence };
}

pub fn stageReceipts(txn: anytype, command: Command, sequence: u64) !void {
    if (sequence == 0) return error.InvalidBatchRequest;
    const owners = try command.outputSources();
    const input_digest = command.inputDigest();
    for (command.sources, 0..) |source, source_index| {
        if (!owners.isSet(source_index)) continue;
        const key = receiptKey(command, source);
        var raw: [receipt_bytes]u8 = undefined;
        @memcpy(raw[0..4], "APR2");
        @memcpy(raw[4..36], &command.publication_digest);
        @memcpy(raw[36..68], &source.content_digest);
        std.mem.writeInt(u64, raw[68..76], source.timestamp, .little);
        std.mem.writeInt(u64, raw[76..84], sequence, .little);
        @memcpy(raw[84 .. 84 + Position.encoded_len], &positionBytes(source.input_position));
        @memcpy(raw[84 + Position.encoded_len ..][0..32], &input_digest);
        std.crypto.hash.Blake3.hash(raw[0 .. receipt_bytes - 32], raw[receipt_bytes - 32 ..], .{});
        try txn.put(&key, &raw);
    }
    // A successful retry supersedes its temporary rejection, atomically with
    // acceptance. Do not erase another command's response in the shared slot.
    if (try rejected(txn, command) != null) try txn.delete(&rejectionKey(command));
}

pub fn validateRequest(alloc: std.mem.Allocator, request: anytype) !void {
    const command = request.artifact_publication orelse return;
    try command.validate(alloc);
    const defaults: @TypeOf(request) = .{};
    inline for (@typeInfo(@TypeOf(request)).@"struct".fields) |field| {
        if (comptime !std.mem.eql(u8, field.name, "artifact_publication") and
            !std.mem.eql(u8, field.name, "sync_level") and !std.mem.eql(u8, field.name, "timestamp"))
        {
            if (comptime @typeInfo(field.type) == .pointer and @typeInfo(field.type).pointer.size == .slice) {
                if (@field(request, field.name).len != 0) return error.InvalidBatchRequest;
            } else if (!std.meta.eql(@field(request, field.name), @field(defaults, field.name))) return error.InvalidBatchRequest;
        }
    }
}

/// Must run inside the same writer transaction as final publication. An empty
/// artifact source guard is never interpreted as a missing document permission.
pub fn validateSources(alloc: std.mem.Allocator, txn: anytype, namespace: Namespace, sources: []const Source) !void {
    for (sources) |source| {
        if (!std.meta.eql(try inputRevision(txn, namespace, source.document_key), source.input_position)) return error.EnrichmentSourceChanged;
        const primary_key = try keys.documentKeyAlloc(alloc, source.document_key);
        defer alloc.free(primary_key);
        const relational_key = try keys.relationalRowKeyAlloc(alloc, source.document_key);
        defer alloc.free(relational_key);
        const primary = txn.get(primary_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const relational = txn.get(relational_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (!source.exists) {
            if (source.input_position == null or primary != null or relational != null) return error.EnrichmentSourceChanged;
            continue;
        }
        if (primary != null and relational != null) return error.EnrichmentSourceChanged;
        const raw = primary orelse relational orelse return error.EnrichmentSourceChanged;
        var digest: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw, &digest, .{});
        if (!std.mem.eql(u8, &digest, &source.content_digest)) return error.EnrichmentSourceChanged;
        const timestamp_key = try keys.ttlKeyAlloc(alloc, source.document_key);
        defer alloc.free(timestamp_key);
        const timestamp = txn.get(timestamp_key) catch |err| switch (err) {
            error.NotFound => return error.EnrichmentSourceChanged,
            else => return err,
        };
        if (timestamp.len != 8 or std.mem.readInt(u64, timestamp[0..8], .little) != source.timestamp) return error.EnrichmentSourceChanged;
    }
}

pub const Dispatcher = struct {
    ptr: *anyopaque,
    /// No storage/apply lock may be held here. Success means bounded queue
    /// admission only; output remains pending until the local durable receipt.
    enqueue: *const fn (*anyopaque, Namespace, []const u8) anyerror!void,

    /// Submit an already validated, bounded producer command. Queue/resource
    /// admission and leadership are transient control state, never evidence
    /// that inference failed or that this stream completed. Keep that boundary
    /// shared across asset, vector, graph and resolver callbacks.
    pub fn submit(self: Dispatcher, namespace: Namespace, command: []const u8) !void {
        self.enqueue(self.ptr, namespace, command) catch |err| switch (err) {
            error.ResourceLimitExceeded, error.ResourceBudgetExceeded, error.ResourceTemporarilyUnavailable, error.NotLeader => return error.ArtifactPublicationPending,
            error.Canceled => return error.EnrichmentRetryAborted,
            else => return err,
        };
    }
};

test "ordered artifact inventory producer dispatch backpressure is not terminal inference failure" {
    const Harness = struct {
        failure: anyerror,
        fn enqueue(ptr: *anyopaque, _: Namespace, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.failure;
        }
    };
    var harness: Harness = .{ .failure = error.ResourceLimitExceeded };
    const dispatcher: Dispatcher = .{ .ptr = &harness, .enqueue = Harness.enqueue };
    for ([_]anyerror{ error.ResourceLimitExceeded, error.ResourceBudgetExceeded, error.ResourceTemporarilyUnavailable, error.NotLeader }) |failure| {
        harness.failure = failure;
        try std.testing.expectError(error.ArtifactPublicationPending, dispatcher.submit(@splat(1), "bounded command"));
    }
    harness.failure = error.Canceled;
    try std.testing.expectError(error.EnrichmentRetryAborted, dispatcher.submit(@splat(1), "bounded command"));
    harness.failure = error.OutOfMemory;
    try std.testing.expectError(error.OutOfMemory, dispatcher.submit(@splat(1), "bounded command"));
    harness.failure = error.InvalidBatchRequest;
    try std.testing.expectError(error.InvalidBatchRequest, dispatcher.submit(@splat(1), "bounded command"));
}

pub const PreparedEffects = struct {
    arena: std.heap.ArenaAllocator,
    batch: @import("derived/derived_types.zig").DerivedBatch,
    coverage: []const Coverage = &.{},
    chunk_fence: ?@import("artifact_chunk_publication.zig").Fence = null,
    chunk_vector_fence: ?@import("artifact_chunk_vector_publication.zig").Fence = null,
    asset_upstream_fences: []@import("artifact_asset_publication.zig").UpstreamFence = &.{},
    target_hints: ?[]const @import("derived/change_journal.zig").TargetHint = null,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const PreparedBaseVectors = PreparedEffects;
pub const Coverage = struct { index_name: []const u8, generation: u64, document_key: []const u8, artifact_names: []const []const u8 = &.{}, artifact_keys: []const []const u8 = &.{}, outcome: ?enum { produced, skipped } = null };

pub fn prepareEffects(alloc: std.mem.Allocator, command: Command, catalogs: @import("artifact_inventory.zig").Catalogs) !PreparedEffects {
    return switch (command.producer_kind) {
        .index => if (command.producer_scope_key.len != 0) @import("artifact_chunk_vector_publication.zig").prepare(alloc, command, catalogs) else prepareBaseVectors(alloc, command, catalogs),
        .resolver => @import("artifact_publication_resolution.zig").prepareResolution(alloc, command, catalogs),
        .graph => @import("artifact_graph_publication.zig").prepare(alloc, command, catalogs),
        .enrichment => @import("artifact_asset_publication.zig").prepare(alloc, command, catalogs),
        .promotion => error.OnlineMergeArtifactTailsUnsupported,
    };
}

/// Derive projection work from the authenticated artifact bytes and exact
/// ordered producer definition. Callers cannot supply arbitrary replay JSON.
pub fn prepareBaseVectors(alloc: std.mem.Allocator, command: Command, catalogs: @import("artifact_inventory.zig").Catalogs) !PreparedBaseVectors {
    if (command.producer_kind != .index) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const manager = @import("catalog/index_manager.zig");
    const configs = try manager.deserializeCatalog(scratch, catalogs.indexes);
    const producer_catalog = @import("catalog/enrichment_catalog.zig");
    const producers = if (catalogs.enrichments.len == 0) try scratch.alloc(producer_catalog.EnrichmentConfig, 0) else try producer_catalog.deserializeCatalog(scratch, catalogs.enrichments);
    const producer = for (configs) |config| {
        if (std.mem.eql(u8, config.name, command.producer_name)) break config;
    } else return error.EnrichmentSourceChanged;
    if (keys.derivedCoverageGenerationForConfig(producer.coverage_generation, producer.config_json) != command.producer_generation or
        (producer.kind != .dense_vector and producer.kind != .sparse_vector)) return error.EnrichmentSourceChanged;
    if (!try manager.onlineBaseGeneratedVectorConfig(scratch, producer, producers)) return error.OnlineMergeArtifactTailsUnsupported;
    const producer_names = try manager.baseVectorArtifactNamesAlloc(scratch, producer);
    const name = command.producer_artifact_name;
    const authorized = for (producer_names) |candidate| {
        if (std.mem.eql(u8, candidate, name)) break true;
    } else false;
    if (!authorized) return error.InvalidBatchRequest;
    const dims = if (producer.kind == .dense_vector) try manager.denseConfigDimensions(scratch, producer) else 0;
    const Consumer = struct { name: []const u8, generation: u64, artifact_names: []const []const u8 };
    var consumers: std.ArrayList(Consumer) = .empty;
    for (configs) |config| {
        if (config.kind != producer.kind) continue;
        if (config.kind == .dense_vector and try manager.denseConfigDimensions(scratch, config) != dims) continue;
        const names = try manager.baseVectorArtifactNamesAlloc(scratch, config);
        const consumes = for (names) |candidate| {
            if (std.mem.eql(u8, candidate, name)) break true;
        } else false;
        if (consumes) try consumers.append(scratch, .{ .name = config.name, .generation = keys.derivedCoverageGenerationForConfig(config.coverage_generation, config.config_json), .artifact_names = names });
    }
    const derived = @import("derived/derived_types.zig");
    const codec = @import("enrichment/artifact_codec.zig");
    var dense: std.ArrayList(derived.DerivedDenseEmbeddingWrite) = .empty;
    var sparse: std.ArrayList(derived.DerivedSparseEmbeddingWrite) = .empty;
    var changed: std.ArrayList([]const u8) = .empty;
    var deleted: std.ArrayList([]const u8) = .empty;
    var coverage: std.ArrayList(Coverage) = .empty;
    for (command.mutations) |mutation| {
        if (mutation.family != .base_vector) return error.OnlineMergeArtifactTailsUnsupported;
        @import("online_vector_artifacts.zig").validate(mutation.key, mutation.value) catch return error.InvalidBatchRequest;
        const identity = (try keys.parseEmbeddingArtifactKeyAlloc(scratch, mutation.key)) orelse return error.InvalidBatchRequest;
        if (!std.mem.eql(u8, identity.artifact_name, name)) return error.InvalidBatchRequest;
        const artifact_key = try scratch.dupe(u8, mutation.key);
        if (mutation.value) |value| {
            const header = try codec.decodeHeader(value);
            if (producer.kind == .dense_vector) {
                if (header.kind != .dense_embedding) return error.InvalidBatchRequest;
                const vector = try codec.decodeDenseEmbeddingAlloc(scratch, value);
                if (vector.len != dims) return error.InvalidBatchRequest;
                for (consumers.items) |consumer| try dense.append(scratch, .{ .index_name = consumer.name, .doc_key = identity.doc_key, .artifact_key = artifact_key, .vector = vector });
            } else {
                if (header.kind != .sparse_embedding) return error.InvalidBatchRequest;
                const vector = try codec.decodeSparseEmbeddingAlloc(scratch, value);
                for (consumers.items) |consumer| try sparse.append(scratch, .{ .index_name = consumer.name, .doc_key = identity.doc_key, .artifact_key = artifact_key, .indices = vector.indices, .values = vector.values });
            }
            try changed.append(scratch, artifact_key);
        } else try deleted.append(scratch, artifact_key);
        for (consumers.items) |consumer| try coverage.append(scratch, .{ .index_name = consumer.name, .generation = consumer.generation, .document_key = identity.doc_key, .artifact_names = consumer.artifact_names });
    }
    return .{ .arena = arena, .coverage = try coverage.toOwnedSlice(scratch), .batch = .{ .dense_embeddings = try dense.toOwnedSlice(scratch), .sparse_embeddings = try sparse.toOwnedSlice(scratch), .changed_artifact_keys = try changed.toOwnedSlice(scratch), .deleted_keys = try deleted.toOwnedSlice(scratch) } };
}

/// Point-only accounting in the final writer snapshot. Counter migration is
/// an admission/baseline responsibility: apply never starts a legacy full
/// scan, nor fabricates a zero counter over possibly existing markers.
pub fn stageCoverage(alloc: std.mem.Allocator, txn: anytype, command: Command, coverage: []const Coverage) !void {
    const epoch = @import("artifact_coverage_epoch.zig");
    const scope = epoch.forCommand(command);
    const outcomes = [_][]const u8{ "produced", "skipped", "terminal_failed" };
    for (coverage) |item| {
        var produced = false;
        for (item.artifact_keys) |key| {
            const present = for (command.mutations) |effect| {
                if (std.mem.eql(u8, effect.key, key)) break effect.value != null;
            } else try @import("artifact_producer_provenance.zig").currentArtifactProduced(alloc, txn, key);
            if (present) {
                produced = true;
                break;
            }
        }
        for (item.artifact_names) |name| {
            const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, item.document_key, name);
            defer alloc.free(key);
            const present = for (command.mutations) |effect| {
                if (std.mem.eql(u8, effect.key, key)) break effect.value != null;
            } else blk: {
                _ = txn.get(key) catch |err| switch (err) {
                    error.NotFound => break :blk false,
                    else => return err,
                };
                break :blk true;
            };
            if (present) {
                produced = true;
                break;
            }
        }
        const target: ?usize = if (tombstonedSource(command.sources, item.document_key)) null else if (item.outcome) |outcome| (if (outcome == .produced) @as(usize, 0) else 1) else if (produced) 0 else 1;
        const marker = try epoch.marker(alloc, scope, item.index_name, item.generation, item.document_key);
        defer alloc.free(marker);
        const previous_raw = txn.get(marker) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const previous: ?usize = if (previous_raw) |raw| for (outcomes, 0..) |name, index| {
            if (std.mem.eql(u8, name, raw)) break index;
        } else return error.InvalidDerivedCoverageOutcome else null;
        var counters: [3][]u8 = undefined;
        var initialized: usize = 0;
        defer for (counters[0..initialized]) |counter| alloc.free(counter);
        var counts: [3]u64 = undefined;
        for (outcomes, 0..) |name, index| {
            const counter = try epoch.counter(alloc, scope, item.index_name, item.generation, name);
            counters[index] = counter;
            initialized += 1;
            const raw = txn.get(counter) catch |err| switch (err) {
                error.NotFound => return error.ArtifactCoverageBaselinePending,
                else => return err,
            };
            counts[index] = try keys.decodeDerivedCoverageOutcomeCount(raw);
        }
        // Even an unchanged marker needs a complete accounting baseline.
        // Otherwise an old marker can certify a new publication while its
        // generation is still missing the counters required for drain/seal.
        if (previous == target) continue;
        for (counts, 0..) |old_count, index| {
            if (previous != index and target != index) continue;
            var count = old_count;
            if (previous == index) count = std.math.sub(u64, count, 1) catch return error.InvalidDerivedCoverageCounter;
            if (target == index) count = std.math.add(u64, count, 1) catch return error.InvalidDerivedCoverageCounter;
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded, count, .little);
            try txn.put(counters[index], &encoded);
        }
        if (target) |index| try txn.put(marker, outcomes[index]) else try txn.delete(marker);
    }
}

fn tombstonedSource(sources: []const Source, document: []const u8) bool {
    var lower: usize = 0;
    var upper = sources.len;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        switch (std.mem.order(u8, sources[middle].document_key, document)) {
            .lt => lower = middle + 1,
            .gt => upper = middle,
            .eq => return !sources[middle].exists,
        }
    }
    return false;
}

fn validFamilyKey(family: Family, key: []const u8) bool {
    return switch (family) {
        .base_vector => keys.isEmbeddingArtifactKey(key),
        .derived_vector => keys.isDerivedEmbeddingArtifactKey(key),
        .document_artifact => keys.isChunkArtifactRecordKey(key) or keys.isAssetArtifactKey(key) or keys.isDocumentUnitArtifactRecordKey(key) or keys.isSummaryArtifactKey(key) or @import("artifact_chunk_manifest.zig").isKey(key),
        .resolution => keys.isResolutionArtifactKey(key),
        .graph => @import("online_graph_artifacts.zig").isKey(key),
    };
}
fn consume(remaining: *usize, amount: usize) !void {
    remaining.* = std.math.sub(usize, remaining.*, amount) catch return error.TransactionTooLarge;
}
fn number(hash: *std.crypto.hash.Blake3, value: u64) void {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, value, .little);
    hash.update(&encoded);
}
fn bytes(hash: *std.crypto.hash.Blake3, value: []const u8) void {
    number(hash, value.len);
    hash.update(value);
}

test "ordered artifact inventory publication binds exact sources families values and authority" {
    _ = @import("../artifact_publication_dispatch.zig");
    const alloc = std.testing.allocator;
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    var mutations = [_]Mutation{.{ .family = .base_vector, .key = key, .value = "payload", .source_index = 0 }};
    var command: Command = .{
        .namespace = @splat(1),
        .authority_epoch = 1,
        .catalog_digest = @splat(2),
        .producer_name = "model",
        .producer_generation = 3,
        .producer_artifact_name = "model",
        .sources = &.{.{ .document_key = "doc", .content_digest = @splat(4), .timestamp = 5, .input_position = .{ .raft = .{ .term = 1, .index = 6 } } }},
        .mutations = &mutations,
        .publication_digest = @splat(0),
    };
    command.publication_digest = command.digest();
    try command.validate(alloc);
    const logical_input = command.inputDigest();
    const before_precondition = command.digest();
    command.mutation_preconditions = &.{.{ .key = key, .content_digest = null, .input_position = null, .source_index = 0 }};
    try std.testing.expectEqualSlices(u8, &logical_input, &command.inputDigest());
    try std.testing.expect(!std.mem.eql(u8, &before_precondition, &command.digest()));
    try std.testing.expectError(error.InvalidBatchRequest, command.validate(alloc));
    command.publication_digest = command.digest();
    try command.validate(alloc);
    command.mutation_preconditions = &.{};
    command.publication_digest = command.digest();
    const first_receipt = receiptKey(command, command.sources[0]);
    command.producer_artifact_name = "second-model";
    try std.testing.expect(!std.mem.eql(u8, &first_receipt, &receiptKey(command, command.sources[0])));
    command.producer_artifact_name = "model";
    mutations[0].value = "changed";
    try std.testing.expectError(error.InvalidBatchRequest, command.validate(alloc));
    command.publication_digest = command.digest();
    mutations[0].source_index = 1;
    try std.testing.expectError(error.InvalidBatchRequest, command.validate(alloc));
    mutations[0].source_index = 0;
    mutations[0].key = "\x00\x00__metadata__:indexes";
    command.publication_digest = command.digest();
    try std.testing.expectError(error.InvalidBatchRequest, command.validate(alloc));
}

test "ordered artifact inventory tombstone input requires exact position and cannot create live output" {
    const alloc = std.testing.allocator;
    const namespace: Namespace = @splat(1);
    const Fake = struct {
        revision_key: []const u8,
        revision: [Position.encoded_len]u8,
        live: bool = false,
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            if (std.mem.eql(u8, key, self.revision_key)) return &self.revision;
            if (self.live and keys.isStoredDocumentRowKey(key)) return "{}";
            return error.NotFound;
        }
    };
    const revision_key = inputRevisionKey(namespace, "doc");
    var txn: Fake = .{ .revision_key = &revision_key, .revision = try (Position{ .raft = .{ .term = 1, .index = 2 } }).encode() };
    const source = try capturePrimaryTombstoneSource(alloc, &txn, namespace, "doc");
    defer alloc.free(source.document_key);
    try std.testing.expect(!source.exists);
    try validateSources(alloc, &txn, namespace, (&source)[0..1]);
    txn.live = true;
    try std.testing.expectError(error.EnrichmentSourceChanged, validateSources(alloc, &txn, namespace, (&source)[0..1]));
    txn.live = false;
    txn.revision = try (Position{ .raft = .{ .term = 1, .index = 3 } }).encode();
    try std.testing.expectError(error.EnrichmentSourceChanged, validateSources(alloc, &txn, namespace, (&source)[0..1]));
    const output = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(output);
    var mutation: Mutation = .{ .family = .base_vector, .key = output, .value = null, .source_index = 0 };
    var command: Command = .{ .namespace = namespace, .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "model", .sources = (&source)[0..1], .mutations = (&mutation)[0..1], .publication_digest = @splat(0) };
    command.publication_digest = command.digest();
    try command.validate(alloc);
    mutation.value = "must not resurrect";
    command.publication_digest = command.digest();
    try std.testing.expectError(error.InvalidBatchRequest, command.validate(alloc));
}

test "ordered artifact inventory graph tombstones retain only exact empty ownership witnesses" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const states = @import("graph_asset_state.zig");
    var name: std.ArrayList(u8) = .empty;
    try keys.appendDocumentPrefix(&name, alloc, "doc");
    try name.append(alloc, keys.graph_asset_state_kind);
    try keys.appendEncodedComponent(&name, alloc, "graph");
    try keys.appendEncodedComponent(&name, alloc, "asset");
    const root = try name.toOwnedSlice(alloc);
    const edge = try keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc", "graph", "links", "other", "doc");
    const empty = try states.encodeAlloc(alloc, 17, @as([]const struct { key: []const u8 }, &.{}));
    const segmented = try states.encodeSegmentedRootAlloc(alloc, 17, 0, 0);
    const nonempty = try states.encodeAlloc(alloc, 17, @as([]const struct { key: []const u8 }, &.{.{ .key = edge }}));
    const nonempty_segmented = try states.encodeSegmentedRootAlloc(alloc, 17, 1, 1);
    const trailing = try std.mem.concat(alloc, u8, &.{ empty, "\x00" });
    const source: Source = .{ .document_key = "doc", .exists = false, .content_digest = @splat(0), .timestamp = 0, .input_position = .{ .raft = .{ .term = 1, .index = 1 } } };
    var effect: Mutation = .{ .family = .graph, .key = root, .value = empty, .source_index = 0 };
    var command: Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_kind = .graph, .producer_name = "graph", .producer_generation = 17, .producer_artifact_name = "asset", .sources = (&source)[0..1], .mutations = (&effect)[0..1], .publication_digest = @splat(0) };
    for ([_][]const u8{ empty, segmented }) |raw| {
        effect.value = raw;
        command.publication_digest = command.digest();
        try command.validate(alloc);
        command.producer_generation = 18;
        command.publication_digest = command.digest();
        try std.testing.expectError(error.InvalidBatchRequest, command.validate(alloc));
        command.producer_generation = 17;
    }
    for ([_][]const u8{ nonempty, nonempty_segmented, trailing, "AGS4", "AGB1" }) |raw| {
        effect.value = raw;
        command.publication_digest = command.digest();
        try std.testing.expectError(error.InvalidBatchRequest, command.validate(alloc));
    }
    effect.value = empty;
    command.producer_name = "other-graph";
    command.publication_digest = command.digest();
    try std.testing.expectError(error.InvalidBatchRequest, command.validate(alloc));
    command.producer_name = "graph";
    effect.key = try keys.graphAssetStateSegmentKeyAlloc(alloc, root, 0);
    command.publication_digest = command.digest();
    try std.testing.expectError(error.InvalidBatchRequest, command.validate(alloc));
}

test "ordered artifact inventory publication projects every shared consumer and rejects another producer stream" {
    const alloc = std.testing.allocator;
    const Config = @import("types.zig").IndexConfig;
    const configs = [_]Config{
        .{ .name = "first", .kind = .dense_vector, .config_json = "{\"field\":\"body\",\"dims\":2,\"embedding_name\":\"model\"}", .coverage_generation = 7 },
        .{ .name = "second", .kind = .dense_vector, .config_json = "{\"field\":\"body\",\"dims\":2,\"embedding_name\":\"model\"}", .coverage_generation = 9 },
        .{ .name = "other", .kind = .dense_vector, .config_json = "{\"field\":\"body\",\"dims\":2,\"embedding_name\":\"other-model\"}", .coverage_generation = 11 },
    };
    var catalog: std.ArrayList(u8) = .empty;
    defer catalog.deinit(alloc);
    try catalog.appendSlice(alloc, "AIDX\x02\x00\x00\x00\x03\x00\x00\x00");
    for (configs) |config| {
        var length: [4]u8 = undefined;
        std.mem.writeInt(u32, &length, @intCast(config.name.len), .little);
        try catalog.appendSlice(alloc, &length);
        try catalog.appendSlice(alloc, config.name);
        try catalog.append(alloc, @intFromEnum(config.kind));
        std.mem.writeInt(u32, &length, @intCast(config.config_json.len), .little);
        try catalog.appendSlice(alloc, &length);
        try catalog.appendSlice(alloc, config.config_json);
        var generation: [8]u8 = undefined;
        std.mem.writeInt(u64, &generation, config.coverage_generation, .little);
        try catalog.appendSlice(alloc, &generation);
    }
    const catalogs: @import("artifact_inventory.zig").Catalogs = .{
        .indexes = catalog.items,
        .enrichments = "[{\"name\":\"model\",\"kind\":\"embedding\",\"source_field\":\"body\",\"expected_dims\":2},{\"name\":\"other-model\",\"kind\":\"embedding\",\"source_field\":\"other\",\"expected_dims\":2}]",
    };
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const payload = try @import("enrichment/artifact_codec.zig").encodeDenseEmbeddingAlloc(alloc, 3, &.{ 1, 2 });
    defer alloc.free(payload);
    var command: Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = catalogs.digest(), .producer_name = "first", .producer_generation = 7, .producer_artifact_name = "model", .sources = &.{.{ .document_key = "doc", .content_digest = @splat(1), .timestamp = 2, .input_position = .{ .raft = .{ .term = 1, .index = 1 } } }}, .mutations = &.{.{ .family = .base_vector, .key = key, .value = payload, .source_index = 0 }}, .publication_digest = @splat(0) };
    command.publication_digest = command.digest();
    var prepared = try prepareBaseVectors(alloc, command, catalogs);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 2), prepared.batch.dense_embeddings.len);
    try std.testing.expectEqualStrings("first", prepared.batch.dense_embeddings[0].index_name);
    try std.testing.expectEqualStrings("second", prepared.batch.dense_embeddings[1].index_name);
    try std.testing.expectEqual(@as(usize, 2), prepared.coverage.len);
    try std.testing.expectEqual(@as(u64, 9), prepared.coverage[1].generation);
    command.producer_artifact_name = "other-model";
    command.publication_digest = command.digest();
    try std.testing.expectError(error.InvalidBatchRequest, prepareBaseVectors(alloc, command, catalogs));
}

test "ordered artifact inventory final materialization observes activation in its write snapshot" {
    const boundary = @import("artifact_activation_boundary.zig");
    const Txn = struct {
        raw: ?[100]u8 = null,
        baseline: ?boundary.Encoded = null,
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            if (std.mem.eql(u8, key, boundary.key)) return if (self.baseline) |*value| value else error.NotFound;
            if (!std.mem.eql(u8, key, authority_key)) return error.NotFound;
            return if (self.raw) |*value| value else error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            if (std.mem.eql(u8, key, boundary.key)) {
                self.baseline = value[0..@sizeOf(boundary.Encoded)].*;
                return;
            }
            try std.testing.expectEqualStrings(authority_key, key);
            self.raw = value[0..100].*;
        }
    };
    var txn: Txn = .{};
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(std.testing.allocator, "doc", "model");
    defer std.testing.allocator.free(key);
    try requireLegacyMaterialization(&txn, key);
    var activation: Command = .{ .mode = .activate, .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    try stageAuthority(&txn, activation);
    try std.testing.expectError(error.ArtifactCatalogDrift, requireLegacyMaterialization(&txn, key));
    const coverage = try keys.derivedCoverageOutcomeKeyAlloc(std.testing.allocator, "model", 1, "doc");
    defer std.testing.allocator.free(coverage);
    try std.testing.expectError(error.ArtifactCatalogDrift, requireLegacyMaterialization(&txn, coverage));
    try requireLegacyMaterialization(&txn, "private-worker-heartbeat");
}

test "ordered artifact inventory publication coverage is point bounded idempotent and accounts sibling streams" {
    const alloc = std.testing.allocator;
    const Txn = struct {
        rows: std.StringHashMap([]u8),
        fn deinit(self: *@This()) void {
            var iterator = self.rows.iterator();
            while (iterator.next()) |entry| {
                std.testing.allocator.free(entry.key_ptr.*);
                std.testing.allocator.free(entry.value_ptr.*);
            }
            self.rows.deinit();
        }
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            return self.rows.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const copy = try std.testing.allocator.dupe(u8, value);
            errdefer std.testing.allocator.free(copy);
            const slot = try self.rows.getOrPut(key);
            if (slot.found_existing) std.testing.allocator.free(slot.value_ptr.*) else slot.key_ptr.* = try std.testing.allocator.dupe(u8, key);
            slot.value_ptr.* = copy;
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            const old = self.rows.fetchRemove(key) orelse return error.NotFound;
            std.testing.allocator.free(old.key);
            std.testing.allocator.free(old.value);
        }
    };
    var txn: Txn = .{ .rows = std.StringHashMap([]u8).init(alloc) };
    defer txn.deinit();
    const first = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "a");
    defer alloc.free(first);
    const second = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "b");
    defer alloc.free(second);
    try txn.put(second, "existing sibling output");
    const transitions = [_]Coverage{.{ .index_name = "shared", .generation = 1, .document_key = "doc", .artifact_names = &.{ "a", "b" } }};
    var command: Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "shared", .producer_generation = 1, .producer_artifact_name = "a", .sources = &.{}, .mutations = &.{.{ .family = .base_vector, .key = first, .value = null, .source_index = 0 }}, .publication_digest = @splat(0) };
    const epoch = @import("artifact_coverage_epoch.zig");
    const scope = epoch.forCommand(command);
    try std.testing.expectError(error.ArtifactCoverageBaselinePending, stageCoverage(alloc, &txn, command, &transitions));
    for ([_][]const u8{ "produced", "skipped", "terminal_failed" }) |outcome| {
        const key = try epoch.counter(alloc, scope, "shared", 1, outcome);
        defer alloc.free(key);
        try txn.put(key, &@as([8]u8, @splat(0)));
    }
    try stageCoverage(alloc, &txn, command, &transitions);
    try stageCoverage(alloc, &txn, command, &transitions);
    const marker = try epoch.marker(alloc, scope, "shared", 1, "doc");
    defer alloc.free(marker);
    try std.testing.expectEqualStrings("produced", try txn.get(marker));
    const missing = try epoch.counter(alloc, scope, "shared", 1, "terminal_failed");
    defer alloc.free(missing);
    try txn.delete(missing);
    try std.testing.expectError(error.ArtifactCoverageBaselinePending, stageCoverage(alloc, &txn, command, &transitions));
    try txn.put(missing, &@as([8]u8, @splat(0)));
    command.mutations = &.{ .{ .family = .base_vector, .key = first, .value = null, .source_index = 0 }, .{ .family = .base_vector, .key = second, .value = null, .source_index = 0 } };
    try stageCoverage(alloc, &txn, command, &transitions);
    try stageCoverage(alloc, &txn, command, &transitions);
    try std.testing.expectEqualStrings("skipped", try txn.get(marker));
    const produced = try epoch.counter(alloc, scope, "shared", 1, "produced");
    defer alloc.free(produced);
    const skipped = try epoch.counter(alloc, scope, "shared", 1, "skipped");
    defer alloc.free(skipped);
    try std.testing.expectEqual(@as(u64, 0), try keys.decodeDerivedCoverageOutcomeCount(try txn.get(produced)));
    try std.testing.expectEqual(@as(u64, 1), try keys.decodeDerivedCoverageOutcomeCount(try txn.get(skipped)));
    command.sources = &.{.{ .document_key = "doc", .exists = false, .content_digest = @splat(0), .timestamp = 0, .input_position = .{ .raft = .{ .term = 1, .index = 2 } } }};
    try stageCoverage(alloc, &txn, command, &transitions);
    try std.testing.expectError(error.NotFound, txn.get(marker));
    try std.testing.expectEqual(@as(u64, 0), try keys.decodeDerivedCoverageOutcomeCount(try txn.get(skipped)));
    const sources = [_]Source{
        .{ .document_key = "doc", .content_digest = @splat(3), .timestamp = 1, .input_position = null },
        .{ .document_key = "other", .content_digest = @splat(4), .timestamp = 2, .input_position = null },
    };
    command.sources = &sources;
    command.publication_digest = command.digest();
    try command.validate(alloc);
    try stageRejection(&txn, command, 7, .baseline_pending);
    try stageReceipts(&txn, command, 8);
    const receipts = ReceiptSet.init(command);
    try std.testing.expectEqual(@as(u64, 8), (try receipts.read(&txn, sources[0])).?.sequence);
    try std.testing.expect((try receipts.read(&txn, sources[1])) == null);
    try std.testing.expect((try rejected(&txn, command)) == null);

    // A changed dependency invalidates even the unchanged first source's
    // receipt. A different command's rejection shares the response slot but
    // must not be removed by acceptance of this command.
    var changed_sources = sources;
    changed_sources[1].content_digest = @splat(5);
    var changed = command;
    changed.sources = &changed_sources;
    changed.publication_digest = changed.digest();
    try std.testing.expect((try ReceiptSet.init(changed).read(&txn, sources[0])) == null);
    try stageRejection(&txn, changed, 9, .stale_source);
    try stageReceipts(&txn, command, 10);
    try std.testing.expect((try rejected(&txn, changed)) != null);
}
