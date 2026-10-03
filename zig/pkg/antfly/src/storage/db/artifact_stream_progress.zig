// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Durable receiver-verified census progress. Preparation owns its bytes and
//! runs outside the writer; commit repeats causal fences and predecessor CAS.
//! Root-only scope closure is receiver-derived, not a sender claim. Neither
//! enumeration nor one closed stream is document discharge or replay ACK.
const std = @import("std");
const census = @import("artifact_stream_census.zig");
const checkpoints = @import("artifact_stream_checkpoint.zig");
const publication = @import("artifact_publication.zig");
const Observation = @import("artifact_stream_observation.zig").Observation;
const Request = @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest;
const Plan = @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot;
const prefix = "\x00\x00__artifact_publication__:verified-stream:";
pub const Key = [prefix.len + 24 + 8 + 16 + 32]u8;

pub fn key(authority: publication.Authority, root: u128, request: Request, plan: *const Plan) !Key {
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    const generation = plan.coverageGeneration(request.index_name) orelse return error.ArtifactCatalogDrift;
    if (generation == 0) return error.ArtifactCatalogDrift;
    const local = checkpoints.key(authority, request.doc_key, .{ .root_incarnation = root, .kind = .index, .name = request.index_name, .generation = generation, .artifact = @import("enrichment/enrichment_types.zig").requestEmbeddingName(request), .scope = request.artifact_name });
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], local[local.len - 80 ..]);
    return result;
}

pub const Record = struct {
    root: u128,
    progress: census.Progress,
    claim: census.Claim,
    digest: publication.Digest,
    /// Locally verified scope closure, never copied from a sender's claim.
    closed: bool,
};

fn decode(raw: []const u8, selected: *const Key, root: u128) !Record {
    if (raw.len < 343 or raw.len > 343 + 2 * checkpoints.max_cursor_bytes + @import("artifact_chunk_scan_position.zig").max_encoded_bytes or
        !std.mem.eql(u8, raw[0..4], "AVP2") or raw[68] > 1) return error.ArtifactCatalogCorrupt;
    var digest: publication.Digest = undefined;
    std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    const loaded = try checkpoints.decode(raw[69 .. raw.len - 32]);
    const state = loaded.state;
    if ((raw[68] == 1 and !state.enumerated) or root == 0 or state.root_incarnation != root or
        !std.mem.eql(u8, selected[0..prefix.len], prefix) or
        !std.mem.eql(u8, selected[prefix.len..][0..24], &state.observation.authority.namespace) or
        std.mem.readInt(u64, selected[prefix.len + 24 ..][0..8], .big) != state.observation.authority.epoch or
        std.mem.readInt(u128, selected[prefix.len + 32 ..][0..16], .big) != root) return error.ArtifactCatalogCorrupt;
    return .{
        .closed = raw[68] == 1,
        .root = root,
        .progress = .{ .observation = state.observation, .cursor = state.cursor, .scan_cursor = state.scan_cursor, .logical_scan_cursor = state.logical_scan_cursor, .members = state.members, .chain = state.chain, .enumerated = state.enumerated },
        .claim = .{ .before = raw[4..36].*, .after = raw[36..68].* },
        .digest = digest,
    };
}

/// Borrowed from txn; imports with another physical-root identity cannot be
/// read as local verification. Re-keyed or cross-producer records fail closed.
pub fn load(txn: anytype, root: u128, request: Request, plan: *const Plan) !?Record {
    const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    const selected = try key(authority, root, request, plan);
    const raw = txn.get(&selected) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const record = try decode(raw, &selected, root);
    if (!std.mem.eql(u8, &try census.boundDigest(request, plan, record.progress), &record.claim.after)) return error.ArtifactCatalogCorrupt;
    return record;
}

pub const Prepared = struct {
    alloc: std.mem.Allocator,
    encoded: []u8,
    selected: Key,
    expected: ?publication.Digest,
    record: Record,
    document: []u8,
    pub fn deinit(self: *Prepared) void {
        self.alloc.free(self.encoded);
        self.alloc.free(self.document);
        self.* = undefined;
    }
};

/// Receiver-local closure of ONE materialized chunk-vector stream. This is
/// not document completion: the owner must still certify every other required
/// catalog stream before discharging its exact-input work obligation.
pub const Closure = struct {
    alloc: std.mem.Allocator,
    root: u128,
    selected: Key,
    record_digest: publication.Digest,
    record_bytes: usize,
    requirement: publication.Digest,
    observation: Observation,
    document: []u8,

    pub fn deinit(self: *Closure) void {
        self.alloc.free(self.document);
        self.* = undefined;
    }

    /// No scans, provider calls or provenance decoding under the writer.
    /// Verified registry rows are immutable CAS values: the checksum was
    /// verified during preparation; its exact bytes identify that version.
    pub fn requireCurrent(self: Closure, txn: anytype, actual_root: u128) !void {
        if (actual_root == 0 or actual_root != self.root) return error.DurableRootIncarnationUnavailable;
        try self.observation.requireCurrent(txn, self.document);
        const raw = txn.get(&self.selected) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (raw.len != self.record_bytes or raw.len < 32 or
            !std.mem.eql(u8, raw[raw.len - 32 ..], &self.record_digest)) return error.EnrichmentSourceChanged;
    }
};

/// Prepare from the receiver's durable registry, never a sender's cursor or
/// an uncommitted Page. The root-only producer contract establishes a closed
/// scope set. Unit/extraction producers need their own upstream scope census;
/// until it is certified they cannot reuse this root boundary as closure.
pub fn prepareClosure(alloc: std.mem.Allocator, txn: anytype, root: u128, request: Request, plan: *const Plan) !Closure {
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    try census.requireStreamPlan(alloc, txn, request, plan, authority);
    const completion = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const requirement = try completion.providerFor(request);
    if (!try completion.rootChunkScopeClosed(request.artifact_name)) return error.ArtifactPublicationPending;
    const record = (try load(txn, root, request, plan)) orelse return error.ArtifactPublicationPending;
    if (!record.closed) return error.ArtifactPublicationPending;
    const observation = record.progress.observation;
    try observation.requireCurrent(txn, request.doc_key);
    const selected = try key(authority, root, request, plan);
    const raw = try txn.get(&selected);
    return .{
        .alloc = alloc,
        .root = root,
        .selected = selected,
        .record_digest = record.digest,
        .record_bytes = raw.len,
        .requirement = requirement.id,
        .observation = observation,
        .document = try alloc.dupe(u8, request.doc_key),
    };
}

/// A catalog-proven document scope has one accepted producer reference. Its
/// current causal proof (including explicit absence or a closed root chunk
/// replacement), not enumeration EOF or a transport receipt, grants closure.
/// Native effects and index projections remain independent requirements.
pub const DocumentClosure = struct {
    arena: std.heap.ArenaAllocator,
    root: u128,
    requirement: publication.Digest,
    observation: Observation,
    document: []const u8,
    reference: []const u8,
    record_digest: publication.Digest,
    record_bytes: usize,

    pub fn deinit(self: *DocumentClosure) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// No artifact bodies, proof decoding, scans or provider calls in the
    /// writer. Materialization revision fences every local output mutation;
    /// inherited foreign inputs additionally fence the global mutation epoch.
    pub fn requireCurrent(self: DocumentClosure, txn: anytype, actual_root: u128) !void {
        if (actual_root == 0 or actual_root != self.root) return error.DurableRootIncarnationUnavailable;
        try self.observation.requireCurrent(txn, self.document);
        const current = txn.get(self.reference) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (current.len != self.record_bytes or current.len < 32 or
            !std.mem.eql(u8, current[current.len - 32 ..], &self.record_digest)) return error.EnrichmentSourceChanged;
    }
};

/// An accepted extraction head commits an immutable unit directory, not the
/// completion of its downstream child producers. The completion plan keeps
/// those child streams as separate requirements. Retain the exact accepted
/// head reference and both small generation-state records so private staging
/// changes cannot invalidate a prepared witness without a visible row change.
pub const ExtractionClosure = struct {
    arena: std.heap.ArenaAllocator,
    root: u128,
    requirement: publication.Digest,
    observation: Observation,
    document: []const u8,
    certificate: @import("artifact_producer_provenance.zig").ArtifactCertificate,
    head_key: []const u8,
    head_value: []const u8,
    state_key: []const u8,
    state_value: []const u8,
    directory_key: []const u8,
    directory_value: []const u8,

    pub fn deinit(self: *ExtractionClosure) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn requireCurrent(self: ExtractionClosure, txn: anytype, actual_root: u128) !void {
        if (actual_root == 0 or actual_root != self.root) return error.DurableRootIncarnationUnavailable;
        try self.observation.requireCurrent(txn, self.document);
        try self.certificate.requireCurrent(txn);
        const head = txn.get(self.head_key) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (!std.mem.eql(u8, head, self.head_value)) return error.EnrichmentSourceChanged;
        const state = txn.get(self.state_key) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (!std.mem.eql(u8, state, self.state_value)) return error.EnrichmentSourceChanged;
        const directory = txn.get(self.directory_key) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (!std.mem.eql(u8, directory, self.directory_value)) return error.EnrichmentSourceChanged;
    }
};

pub fn prepareExtractionClosure(alloc: std.mem.Allocator, txn: anytype, root: u128, request: Request, plan: *const Plan) !ExtractionClosure {
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    if (request.doc_key.len == 0 or request.doc_key.len > checkpoints.max_cursor_bytes or request.artifact_name.len == 0 or request.kind != .asset) return error.InvalidBatchRequest;
    const authorized = (try @import("artifact_producer_input.zig").authorizeTemplate(alloc, txn, request, plan)) orelse return error.ArtifactCatalogDrift;
    if (authorized.requirement.scope != .producer_defined) return error.ArtifactCatalogDrift;
    const extraction = @import("artifact_extraction_generation.zig");
    const selected_scope = try @import("artifact_generation_scope.zig").extractionKeyAlloc(alloc, request.doc_key, request.artifact_name);
    defer alloc.free(selected_scope);
    var view = (try extraction.View(@typeInfo(@TypeOf(txn)).pointer.child).open(alloc, txn, selected_scope)) orelse return error.ArtifactPublicationPending;
    defer view.deinit();
    if (!std.meta.eql(view.plan.core.spec.authority, authorized.authority)) return error.ArtifactCatalogDrift;
    const encoded_head = view.plan.core.spec.encode();
    const provenance = @import("artifact_producer_provenance.zig");
    var accepted = (try provenance.readCurrentForArtifact(alloc, txn, view.plan.core.head_key, &encoded_head)) orelse return error.ArtifactPublicationPending;
    defer accepted.deinit();
    const proof = accepted.proof;
    if (proof.producer_kind != .enrichment or proof.producer_scope_key.len != 0 or
        proof.producer_generation != authorized.authority.epoch or
        !std.mem.eql(u8, proof.producer_name, request.artifact_name) or
        !std.mem.eql(u8, proof.producer_artifact_name, request.artifact_name) or
        !std.mem.eql(u8, &proof.input_digest, &view.plan.core.spec.input_digest)) return error.ArtifactCatalogCorrupt;
    const head_effect = for (proof.effects) |effect| {
        if (std.mem.eql(u8, effect.key, view.plan.core.head_key)) break effect;
    } else return error.ArtifactCatalogCorrupt;
    if (head_effect.source_index >= proof.sources.len or
        !std.mem.eql(u8, proof.sources[head_effect.source_index].document_key, request.doc_key)) return error.ArtifactCatalogCorrupt;
    const certificate = try provenance.captureCurrentArtifactCertificate(txn, authorized.authority, view.plan.core.head_key, proof.publication_digest);
    const state_value = try txn.get(view.plan.core.state_key);
    const directory_value = try txn.get(view.plan.progress);
    if (state_value.len > 512 or directory_value.len > 512) return error.ArtifactCatalogCorrupt;
    var observation = try Observation.capture(txn, request.doc_key);
    try observation.observeProof(request.doc_key, proof);
    try observation.requireCurrent(txn, request.doc_key);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const document = try owned.dupe(u8, request.doc_key);
    const head_key = try owned.dupe(u8, view.plan.core.head_key);
    const head_value = try owned.dupe(u8, &encoded_head);
    const state_key = try owned.dupe(u8, view.plan.core.state_key);
    const owned_state = try owned.dupe(u8, state_value);
    const directory_key = try owned.dupe(u8, view.plan.progress);
    const owned_directory = try owned.dupe(u8, directory_value);
    return .{
        .arena = arena,
        .root = root,
        .requirement = authorized.requirement.id,
        .observation = observation,
        .document = document,
        .certificate = certificate,
        .head_key = head_key,
        .head_value = head_value,
        .state_key = state_key,
        .state_value = owned_state,
        .directory_key = directory_key,
        .directory_value = owned_directory,
    };
}

/// Root chunk replacements and singleton asset outputs have a closed scope
/// set established by their catalog contract. A manifest with no accepted
/// provenance is only an inventory; extraction/unit producers need their own
/// scope census and cannot borrow this boundary.
pub fn prepareEnrichmentClosure(alloc: std.mem.Allocator, txn: anytype, root: u128, request: Request, plan: *const Plan) !DocumentClosure {
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    if (request.doc_key.len == 0 or request.doc_key.len > checkpoints.max_cursor_bytes) return error.InvalidBatchRequest;
    if (request.kind == .chunk_text and request.upstream_artifact_name.len != 0) return error.OnlineMergeArtifactTailsUnsupported;
    const authorized = (try @import("artifact_asset_publication.zig").authorize(alloc, txn, request, plan)) orelse return error.ArtifactCatalogDrift;
    if (authorized.requirement.scope != .document) return error.ArtifactCatalogDrift;
    const name = if (request.artifact_name.len != 0) request.artifact_name else request.index_name;
    const provenance = @import("artifact_producer_provenance.zig");
    var accepted = if (request.kind == .chunk_text)
        try @import("artifact_chunk_publication.zig").readAcceptedRoot(alloc, txn, request.doc_key, name)
    else blk: {
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const source = publication.capturePrimarySource(scratch.allocator(), txn, authorized.authority.namespace, request.doc_key) catch |err| switch (err) {
            error.EnrichmentSourceChanged => try publication.capturePrimaryTombstoneSource(scratch.allocator(), txn, authorized.authority.namespace, request.doc_key),
            else => return err,
        };
        const selector: publication.Command = .{ .namespace = authorized.authority.namespace, .authority_epoch = authorized.authority.epoch, .catalog_digest = authorized.authority.catalog_digest, .producer_kind = .enrichment, .producer_name = name, .producer_generation = authorized.authority.epoch, .producer_artifact_name = name, .sources = (&source)[0..1], .mutations = &.{}, .publication_digest = @splat(0) };
        const result = (try provenance.readCurrentForSource(alloc, txn, selector, source)) orelse return error.ArtifactPublicationPending;
        break :blk result.owned;
    };
    defer accepted.deinit();
    const source = for (accepted.proof.sources) |candidate| {
        if (std.mem.eql(u8, candidate.document_key, request.doc_key)) break candidate;
    } else return error.ArtifactCatalogCorrupt;
    // Root-manifest lookup validates current member/prefix revisions and all
    // inputs. Bind its producer's latest reference too: a superseded accepted
    // publication must not masquerade as the producer's complete output.
    const selected = provenance.referenceKey(accepted.proof.inputCommand(), source);
    const reference_raw = txn.get(&selected) catch |err| if (err == error.NotFound) return error.ArtifactPublicationPending else return err;
    if (reference_raw.len != 32) return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, reference_raw, &accepted.proof.publication_digest)) return error.EnrichmentSourceChanged;
    var observation = try Observation.capture(txn, request.doc_key);
    try observation.observeProof(request.doc_key, accepted.proof);
    try observation.requireCurrent(txn, request.doc_key);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const document = try arena.allocator().dupe(u8, request.doc_key);
    const reference = try arena.allocator().dupe(u8, &selected);
    return .{ .arena = arena, .root = root, .requirement = authorized.requirement.id, .observation = observation, .document = document, .reference = reference, .record_digest = accepted.proof.publication_digest, .record_bytes = 32 };
}

pub fn prepareDocumentClosure(alloc: std.mem.Allocator, txn: anytype, root: u128, request: Request, plan: *const Plan) !DocumentClosure {
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    if (request.doc_key.len == 0 or request.doc_key.len > checkpoints.max_cursor_bytes) return error.InvalidBatchRequest;
    const input = @import("artifact_producer_input.zig");
    const authorized = (try input.authorizeDocument(alloc, txn, request, plan)) orelse return error.ArtifactCatalogDrift;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const source = publication.capturePrimarySource(owned, txn, authorized.authority.namespace, request.doc_key) catch |err| switch (err) {
        error.EnrichmentSourceChanged => try publication.capturePrimaryTombstoneSource(owned, txn, authorized.authority.namespace, request.doc_key),
        else => return err,
    };
    const artifact = try @import("../internal_keys.zig").embeddingArtifactKeyForDocumentAlloc(owned, request.doc_key, @import("enrichment/enrichment_types.zig").requestEmbeddingName(request));
    if (try @import("artifact_authored_acceptance.zig").readCurrent(txn, root, request.doc_key, artifact)) |authored| {
        if (authored.matchesSource(source)) {
            const observation = try Observation.capture(txn, request.doc_key);
            const reference = try owned.dupe(u8, &authored.reference);
            return .{ .arena = arena, .root = root, .requirement = authorized.requirement, .observation = observation, .document = source.document_key, .reference = reference, .record_digest = authored.record_digest, .record_bytes = authored.record_bytes };
        }
    }
    const selector: publication.Command = .{
        .namespace = authorized.authority.namespace,
        .authority_epoch = authorized.authority.epoch,
        .catalog_digest = authorized.authority.catalog_digest,
        .producer_name = request.index_name,
        .producer_generation = authorized.generation,
        .producer_artifact_name = @import("enrichment/enrichment_types.zig").requestEmbeddingName(request),
        .sources = (&source)[0..1],
        .mutations = &.{},
        .publication_digest = @splat(0),
    };
    const provenance = @import("artifact_producer_provenance.zig");
    var accepted = (try provenance.readCurrentForSource(alloc, txn, selector, source)) orelse return error.ArtifactPublicationPending;
    defer accepted.deinit();
    var observation = try Observation.capture(txn, request.doc_key);
    try observation.observeProof(request.doc_key, accepted.owned.proof);
    try observation.requireCurrent(txn, request.doc_key);
    const reference = try owned.dupe(u8, &provenance.referenceKey(selector, source));
    return .{
        .arena = arena,
        .root = root,
        .requirement = authorized.requirement,
        .observation = observation,
        .document = source.document_key,
        .reference = reference,
        .record_digest = accepted.receipt.publication_digest,
        .record_bytes = 32,
    };
}

test "ordered artifact inventory authored vectors close provider scope without reinference" {
    const alloc = std.testing.allocator;
    const inventory = @import("artifact_inventory.zig");
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const keys = @import("../internal_keys.zig");
    const input = @import("artifact_producer_input.zig");
    const authored = @import("artifact_authored_acceptance.zig");
    const codec = @import("enrichment/artifact_codec.zig");
    for ([_]bool{ true, false }) |dense| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/authored-closure", .{tmp.sub_path});
        defer alloc.free(path);
        var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 }, .online_source_authority = .native, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        try db.addEnrichment(.{ .name = "model", .kind = .embedding, .field = "body", .expected_dims = if (dense) 2 else 0 });
        try db.addIndex(.{ .name = "vector", .kind = if (dense) .dense_vector else .sparse_vector, .config_json = if (dense) "{\"field\":\"dense\",\"dims\":2,\"embedding_name\":\"model\"}" else "{\"field\":\"sparse\",\"embedding_name\":\"model\"}" });
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        catalog.binding.effect_protocol = 15;
        const authority: publication.Authority = .{ .namespace = catalog.namespace, .epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest };
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try inventory.stageOrdered(alloc, &txn, catalog, 1);
            try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            try @import("artifact_producer_obligations.zig").begin(alloc, &txn, authority);
            try @import("artifact_producer_validation.zig").begin(alloc, &txn, authority);
            try txn.commit();
        }
        var pin = try db.core.index_manager.acquireWritePlanSnapshot();
        defer pin.release();
        var request = pin.plan().generated_templates[0];
        request.doc_key = "doc";
        const primary = try keys.documentKeyAlloc(alloc, "doc");
        defer alloc.free(primary);
        const ttl = try keys.ttlKeyAlloc(alloc, "doc");
        defer alloc.free(ttl);
        const output = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", @import("enrichment/enrichment_types.zig").requestEmbeddingName(request));
        defer alloc.free(output);
        const value = if (dense) try codec.encodeAuthoredDenseEmbeddingAlloc(alloc, &.{ 1, 2 }) else try codec.encodeAuthoredSparseEmbeddingAlloc(alloc, &.{1}, &.{2});
        defer alloc.free(value);
        const raw = "{\"body\":\"hello\"}";
        const writes = [_]@import("../docstore.zig").KVPair{ .{ .key = primary, .value = raw }, .{ .key = ttl, .value = &.{ 1, 0, 0, 0, 0, 0, 0, 0 } }, .{ .key = output, .value = value } };
        // Merely importing authored-flagged bytes does not authorize skipping
        // the provider, nor satisfy the document requirement.
        try db.core.store.putBatchWithReplayAndParticipant(null, &writes, &.{}, null, .{}, null);
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            var token = (try input.capture(alloc, &read, db.root_incarnation, request, pin.plan(), primary, raw, null)).?;
            defer token.deinit();
            try std.testing.expect(!try token.accepted(&read));
            try std.testing.expectError(error.ArtifactPublicationPending, prepareDocumentClosure(alloc, &read, db.root_incarnation, request, pin.plan()));
        }
        var ingress = try authored.Prepared.init(alloc, db.root_incarnation, writes[2..], &writes);
        defer ingress.deinit();
        try db.core.store.putBatchWithReplayAndParticipant(null, &writes, &.{}, null, .{}, ingress.participant());
        var closure = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const Probe = struct {
                txn: *@TypeOf(read),
                output: []const u8,
                pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
                    try std.testing.expect(!std.mem.eql(u8, selected, self.output));
                    return self.txn.get(selected);
                }
            };
            var probe: Probe = .{ .txn = &read, .output = output };
            var token = (try input.capture(alloc, &probe, db.root_incarnation, request, pin.plan(), primary, raw, null)).?;
            defer token.deinit();
            try std.testing.expect(try token.accepted(&probe));
            token.root += 1;
            try std.testing.expect(!try token.accepted(&probe));
            break :blk try prepareDocumentClosure(alloc, &probe, db.root_incarnation, request, pin.plan());
        };
        defer closure.deinit();
        {
            var writer = try db.core.store.beginWriteTxn();
            defer writer.abort();
            try closure.requireCurrent(&writer, db.root_incarnation);
            try std.testing.expectError(error.DurableRootIncarnationUnavailable, closure.requireCurrent(&writer, db.root_incarnation + 1));
        }
        // Same bytes at a new physical output revision revoke acceptance.
        try db.core.store.put(output, value);
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.EnrichmentSourceChanged, closure.requireCurrent(&read, db.root_incarnation));
            var token = (try input.capture(alloc, &read, db.root_incarnation, request, pin.plan(), primary, raw, null)).?;
            defer token.deinit();
            try std.testing.expect(!try token.accepted(&read));
            try std.testing.expectError(error.ArtifactPublicationPending, prepareDocumentClosure(alloc, &read, db.root_incarnation, request, pin.plan()));
        }
    }
}

test "ordered artifact inventory document stream closure requires current outputs including absence" {
    const alloc = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const input = @import("artifact_producer_input.zig");
    const keys = @import("../internal_keys.zig");
    for ([_]bool{ true, false }) |dense| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/document-closure", .{tmp.sub_path});
        defer alloc.free(path);
        const options: db_mod.OpenOptions = .{
            .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
            .online_source_authority = .raft,
            .primary_backend = .{ .lsm = .{} },
            .start_index_workers = false,
            .start_optional_runtimes = false,
        };
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        if (dense) {
            try db.setSchemaJson(alloc,
                \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"body":{"type":"string"},"dense":{"type":"embedding"}},"additionalProperties":false}}}}
            );
        } else try db.setSchemaJson(alloc, "{}");
        try db.addEnrichment(.{ .name = "model", .kind = .embedding, .field = "body", .expected_dims = if (dense) 2 else 0 });
        try db.addIndex(.{ .name = "vector", .kind = if (dense) .dense_vector else .sparse_vector, .config_json = if (dense) "{\"field\":\"dense\",\"dims\":2,\"embedding_name\":\"model\"}" else "{\"field\":\"sparse\",\"embedding_name\":\"model\"}" });
        try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"hello\"}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        catalog.binding.effect_protocol = 15;
        try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
        var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        activation.publication_digest = activation.digest();
        try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
        var pinned_plan = try db.core.index_manager.acquireWritePlanSnapshot();
        var plan_held = true;
        defer if (plan_held) pinned_plan.release();
        const plan = pinned_plan.plan();
        try std.testing.expectEqual(@as(usize, 1), plan.generated_templates.len);
        var request = plan.generated_templates[0];
        request.doc_key = "doc";
        const output = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", @import("enrichment/enrichment_types.zig").requestEmbeddingName(request));
        defer alloc.free(output);
        const codec = @import("enrichment/artifact_codec.zig");
        const value = if (dense) try codec.encodeDenseEmbeddingAlloc(alloc, 17, &.{ 1, 2 }) else try codec.encodeSparseEmbeddingAlloc(alloc, 17, &.{1}, &.{2});
        defer alloc.free(value);
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.ArtifactPublicationPending, prepareDocumentClosure(alloc, &read, db.root_incarnation, request, plan));
            try std.testing.expectError(error.DurableRootIncarnationUnavailable, prepareDocumentClosure(alloc, &read, 0, request, plan));
            var unsupported = request;
            unsupported.input_kind = .materialized_chunks;
            try std.testing.expectError(error.OnlineMergeArtifactTailsUnsupported, prepareDocumentClosure(alloc, &read, db.root_incarnation, unsupported, plan));
        }
        var previous: ?DocumentClosure = null;
        defer if (previous) |*closure| closure.deinit();
        for (0..3) |pass| {
            const index: u64 = 4 + @as(u64, @intCast(pass)) * 5;
            if (pass == 1) try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{}" }}, .timestamp_ns = 101 }, .{ .term = 1, .index = index - 1 });
            if (pass == 2) try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .deletes = &.{"doc"}, .timestamp_ns = 102 }, .{ .term = 1, .index = index - 1 });
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const source = blk: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                if (previous) |*closure| try std.testing.expectError(error.EnrichmentSourceChanged, closure.requireCurrent(&read, db.root_incarnation));
                _ = try input.authorizeDocument(alloc, &read, request, plan);
                break :blk if (pass == 2) try publication.capturePrimaryTombstoneSource(arena.allocator(), &read, catalog.namespace, "doc") else try publication.capturePrimarySource(arena.allocator(), &read, catalog.namespace, "doc");
            };
            const effects = [_]publication.Mutation{.{ .family = .base_vector, .key = output, .value = if (pass == 0) value else null, .source_index = 0 }};
            var command: publication.Command = .{ .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = request.index_name, .producer_generation = plan.coverageGeneration(request.index_name).?, .producer_artifact_name = @import("enrichment/enrichment_types.zig").requestEmbeddingName(request), .sources = (&source)[0..1], .mutations = &effects, .publication_digest = @splat(0) };
            command.publication_digest = command.digest();
            try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .artifact_publication = command }, .{ .term = 1, .index = index });
            try std.testing.expect(try @import("artifact_native_stream.zig").advance(alloc, db.core.store, db.root_incarnation, "doc", plan) == .closed);
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                try std.testing.expect((try publication.rejected(&read, command)) == null);
                if (previous) |*closure| closure.deinit();
                previous = null;
                const MetadataAndPrimary = struct {
                    txn: *@TypeOf(read),
                    output: []const u8,
                    pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
                        // Provenance/revision witnesses replace vector-body
                        // reads during preparation, not only final commit.
                        try std.testing.expect(!std.mem.eql(u8, selected, self.output));
                        return self.txn.get(selected);
                    }
                };
                var without_payload: MetadataAndPrimary = .{ .txn = &read, .output = output };
                previous = try prepareDocumentClosure(alloc, &without_payload, db.root_incarnation, request, plan);
                try std.testing.expectEqualDeep((try plan.completion_plan.?.provider(0)).id, previous.?.requirement);
                {
                    var refs: @import("artifact_producer_dispatch.zig").Buffer = undefined;
                    const retry = try @import("artifact_producer_retry.zig").prepare(alloc, &without_payload, db.root_incarnation, plan, 0, "doc", &refs);
                    try std.testing.expectEqual(@as(usize, 0), retry.items.len);
                    try std.testing.expect(retry.progress.complete);
                }
                {
                    const completion = @import("artifact_completion_progress.zig");
                    var verifier: completion.StreamVerifier = .{ .plan = plan };
                    var witness = (try verifier.verify(alloc, &read, db.root_incarnation, "doc", try plan.completion_plan.?.provider(0))).?;
                    defer witness.deinit();
                    try witness.requireCurrent(&read, db.root_incarnation);
                    var partial = try completion.prepare(alloc, &read, db.root_incarnation, &plan.completion_plan.?, "doc", &verifier, .{});
                    defer if (partial) |*page| page.deinit();
                    // Certifying the generated node must not bypass native
                    // effects or the separate index-projection requirement.
                    if (partial) |*page| {
                        try std.testing.expect(!page.atEnd());
                        var writer = try db.core.store.beginWriteTxn();
                        defer writer.abort();
                        try std.testing.expect(!try page.stage(&writer, db.root_incarnation, &plan.completion_plan.?));
                    }
                }
                const Counted = struct {
                    txn: *@TypeOf(read),
                    reads: usize = 0,
                    pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
                        // Closure commit rechecks only private metadata.
                        try std.testing.expect(std.mem.startsWith(u8, selected, "\x00\x00"));
                        self.reads += 1;
                        return self.txn.get(selected);
                    }
                };
                var counted: Counted = .{ .txn = &read };
                try previous.?.requireCurrent(&counted, db.root_incarnation);
                try std.testing.expectEqual(@as(usize, 3), counted.reads);
                try std.testing.expectError(error.DurableRootIncarnationUnavailable, previous.?.requireCurrent(&read, db.root_incarnation + 1));
                const Check = struct {
                    fn run(a: std.mem.Allocator, txn: *@TypeOf(read), root: u128, selected: Request, snapshot: *const Plan) !void {
                        var closure = try prepareDocumentClosure(a, txn, root, selected, snapshot);
                        defer closure.deinit();
                    }
                };
                try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ &read, db.root_incarnation, request, plan });
            }
            if (pass == 0) {
                // Unrelated writes must not restart an owner-local stream.
                try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .writes = &.{.{ .key = "other", .value = "{}" }}, .timestamp_ns = 200 }, .{ .term = 1, .index = index + 1 });
                {
                    var read = try db.core.store.beginReadTxn();
                    defer read.abort();
                    try previous.?.requireCurrent(&read, db.root_incarnation);
                    const primary = if (dense) try keys.relationalRowKeyAlloc(arena.allocator(), "doc") else try keys.documentKeyAlloc(arena.allocator(), "doc");
                    var token = (try input.capture(alloc, &read, db.root_incarnation, request, plan, primary, try read.get(primary), null)).?;
                    defer token.deinit();
                    try std.testing.expect(try token.accepted(&read));
                }
                // Even a same-byte overwrite loses the accepted physical
                // revision. The old transport receipt alone is not closure.
                var marker: [16]u8 = undefined;
                std.mem.writeInt(u64, marker[0..8], 1, .little);
                std.mem.writeInt(u64, marker[8..16], index + 2, .little);
                try db.core.store.putBatch(&.{ .{ .key = output, .value = value }, .{ .key = &keys.ordered_document_applied_entry_key, .value = &marker } }, &.{});
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                try std.testing.expect((try publication.readReceipt(&read, command, source)) != null);
                const primary = if (dense) try keys.relationalRowKeyAlloc(arena.allocator(), "doc") else try keys.documentKeyAlloc(arena.allocator(), "doc");
                var token = (try input.capture(alloc, &read, db.root_incarnation, request, plan, primary, try read.get(primary), null)).?;
                defer token.deinit();
                try std.testing.expectError(error.EnrichmentSourceChanged, token.accepted(&read));
                try std.testing.expectError(error.EnrichmentSourceChanged, previous.?.requireCurrent(&read, db.root_incarnation));
                try std.testing.expectError(error.EnrichmentSourceChanged, prepareDocumentClosure(alloc, &read, db.root_incarnation, request, plan));
                var refs: @import("artifact_producer_dispatch.zig").Buffer = undefined;
                const retry = try @import("artifact_producer_retry.zig").prepare(alloc, &read, db.root_incarnation, plan, 0, "doc", &refs);
                try std.testing.expectEqual(@as(usize, 1), retry.items.len);
                try std.testing.expectEqualStrings(request.index_name, retry.items[0].index_name);
            }
        }
        // The closure owns only its bounded identity/fence, not a plan or a
        // snapshot. Reopen derives the same acceptance from durable evidence.
        pinned_plan.release();
        plan_held = false;
        db.close();
        db = try db_mod.DB.open(alloc, path, options);
        var reopened_plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer reopened_plan.release();
        var reopened_request = reopened_plan.plan().generated_templates[0];
        reopened_request.doc_key = "doc";
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try previous.?.requireCurrent(&read, db.root_incarnation);
        var recovered = try prepareDocumentClosure(alloc, &read, db.root_incarnation, reopened_request, reopened_plan.plan());
        defer recovered.deinit();
        try std.testing.expectEqualDeep(previous.?.requirement, recovered.requirement);
        try std.testing.expectEqualDeep(previous.?.record_digest, recovered.record_digest);
    }
}

/// Resolve the exact immutable template, never a caller-supplied provider
/// definition. A catalog with an ambiguous producer identity fails closed.
pub fn prepareCommand(alloc: std.mem.Allocator, txn: anytype, root: u128, command: publication.Command, plan: *const Plan) !Prepared {
    if (command.mode != .census) return error.InvalidBatchRequest;
    const page = command.census orelse return error.InvalidBatchRequest;
    const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (active.epoch != command.authority_epoch or !std.mem.eql(u8, &active.namespace, &command.namespace) or
        !std.mem.eql(u8, &active.catalog_digest, &command.catalog_digest) or
        (plan.coverageGeneration(command.producer_name) orelse return error.ArtifactCatalogDrift) != command.producer_generation) return error.ArtifactCatalogDrift;
    var selected: ?Request = null;
    for (plan.generated_templates) |candidate| {
        if (!std.mem.eql(u8, candidate.index_name, command.producer_name) or
            !std.mem.eql(u8, candidate.artifact_name, page.chunk_name) or
            !std.mem.eql(u8, @import("enrichment/enrichment_types.zig").requestEmbeddingName(candidate), command.producer_artifact_name) or
            candidate.input_kind != .materialized_chunks or (candidate.kind != .dense_embedding and candidate.kind != .sparse_embedding)) continue;
        if (selected != null) return error.InvalidBatchRequest;
        selected = candidate;
    }
    var request = selected orelse return error.ArtifactCatalogDrift;
    request.doc_key = page.document_key;
    return (try prepare(alloc, txn, root, request, plan, .{ .visits = page.visits, .bytes = page.bytes }, .{ .before = page.before, .after = page.after })) orelse error.EnrichmentSourceChanged;
}

pub fn commandFor(prepared: *const Prepared, request: Request, plan: *const Plan, limits: census.Limits) !publication.Command {
    try limits.validate();
    var command: publication.Command = .{
        .mode = .census,
        .namespace = prepared.record.progress.observation.authority.namespace,
        .authority_epoch = prepared.record.progress.observation.authority.epoch,
        .catalog_digest = prepared.record.progress.observation.authority.catalog_digest,
        .producer_name = request.index_name,
        .producer_generation = plan.coverageGeneration(request.index_name) orelse return error.ArtifactCatalogDrift,
        .producer_artifact_name = @import("enrichment/enrichment_types.zig").requestEmbeddingName(request),
        .sources = &.{},
        .mutations = &.{},
        .publication_digest = @splat(0),
        .census = .{ .document_key = prepared.document, .chunk_name = request.artifact_name, .visits = @intCast(limits.visits), .bytes = @intCast(limits.bytes), .before = prepared.record.claim.before, .after = prepared.record.claim.after },
    };
    command.publication_digest = command.digest();
    return command;
}

fn own(alloc: std.mem.Allocator, raw: []u8, selected: Key, expected: ?publication.Digest, root: u128, document: []const u8) !Prepared {
    errdefer alloc.free(raw);
    const record = try decode(raw, &selected, root);
    return .{ .alloc = alloc, .encoded = raw, .selected = selected, .expected = expected, .record = record, .document = try alloc.dupe(u8, document) };
}

/// A claimed page is independently recomputed. A duplicate can reuse a
/// previously verified page only while its causal witness is still current.
/// A stale input observation restarts at the beginning, never at an imported
/// sender checkpoint. Null means this receiver already enumerated the stream.
pub fn prepare(alloc: std.mem.Allocator, txn: anytype, root: u128, request: Request, plan: *const Plan, limits: census.Limits, claimed: ?census.Claim) !?Prepared {
    try limits.validate();
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    try census.requireStreamPlan(alloc, txn, request, plan, authority);
    const selected = try key(authority, root, request, plan);
    const old = try load(txn, root, request, plan);
    var start: census.Progress = undefined;
    const current = if (old) |record| blk: {
        record.progress.observation.requireCurrent(txn, request.doc_key) catch |err| switch (err) {
            error.EnrichmentSourceChanged => break :blk false,
            else => return err,
        };
        break :blk true;
    } else false;
    if (current) {
        const record = old.?;
        if (claimed) |claim| if (std.meta.eql(claim, record.claim)) {
            const raw = try alloc.dupe(u8, try txn.get(&selected));
            return try own(alloc, raw, selected, record.digest, root, request.doc_key);
        };
        if (record.progress.enumerated) {
            if (claimed != null) return error.EnrichmentSourceChanged;
            return null;
        }
        start = record.progress;
    } else start = .{ .observation = try Observation.capture(txn, request.doc_key) };
    var page = if (claimed) |claim|
        try census.verify(alloc, txn, request, plan, start, limits, claim)
    else
        try census.scan(alloc, txn, request, plan, start, limits);
    defer page.deinit();
    // The receiver has just checked every member, the selected producer
    // boundary and their complete causal inputs. Only a root-only catalog
    // shape closes here; an upstream unit scope census must close separately.
    const completion = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const closed = page.progress.enumerated and try completion.rootChunkScopeClosed(request.artifact_name);
    return try own(alloc, try encodeAlloc(alloc, root, page.progress, page.claim, closed), selected, if (old) |record| record.digest else null, root, request.doc_key);
}

fn encodeAlloc(alloc: std.mem.Allocator, root: u128, progress: census.Progress, claim: census.Claim, closed: bool) ![]u8 {
    if (closed and !progress.enumerated) return error.InvalidBatchRequest;
    const state: checkpoints.State = .{
        .root_incarnation = root,
        .observation = progress.observation,
        .cursor = progress.cursor,
        .scan_cursor = progress.scan_cursor,
        .logical_scan_cursor = progress.logical_scan_cursor,
        .members = progress.members,
        .chain = progress.chain,
        .enumerated = progress.enumerated,
    };
    const encoded_state = try state.encodeAlloc(alloc);
    defer alloc.free(encoded_state);
    const raw = try alloc.alloc(u8, 101 + encoded_state.len);
    @memcpy(raw[0..4], "AVP2");
    @memcpy(raw[4..36], &claim.before);
    @memcpy(raw[36..68], &claim.after);
    raw[68] = @intFromBool(closed);
    @memcpy(raw[69 .. raw.len - 32], encoded_state);
    std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
    return raw;
}

/// Must share the ordered owner's writer transaction, ownership/catalog
/// fence, applied marker and outbox. No scan or provider work occurs here.
/// Callers must supply their actual physical-root identity, not a wire value.
pub fn stage(txn: anytype, root: u128, prepared: *const Prepared) !bool {
    if (root == 0 or root != prepared.record.root) return error.DurableRootIncarnationUnavailable;
    try prepared.record.progress.observation.requireCurrent(txn, prepared.document);
    const obligations = @import("artifact_producer_obligations.zig");
    const work = (try obligations.load(txn)) orelse return error.ArtifactCoverageBaselinePending;
    try work.requireAuthority(prepared.record.progress.observation.authority);
    if (work.sealed_attempt != null) return error.RetainedEffectsFenceMismatch;
    const raw = txn.get(&prepared.selected) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (raw) |value| {
        const previous = try decode(value, &prepared.selected, root);
        if (std.mem.eql(u8, &previous.digest, &prepared.record.digest)) return false;
        if (prepared.expected == null or !std.mem.eql(u8, &previous.digest, &prepared.expected.?)) return error.EnrichmentSourceChanged;
    } else if (prepared.expected != null) return error.EnrichmentSourceChanged;
    try txn.put(&prepared.selected, prepared.encoded);
    return true;
}

pub fn collectObsoletePage(alloc: std.mem.Allocator, store_handle: anytype, root: u128) !bool {
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root, .big);
    return @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store_handle, prefix, 48, 48, if (root == 0) null else &identity);
}

test "ordered artifact inventory verified census records authenticate root identity and all bytes" {
    const observation: Observation = .{
        .authority = .{ .namespace = @splat(1), .epoch = 2, .catalog_digest = @splat(3) },
        .document_digest = @splat(4),
        .revision = .{ .raft = .{ .term = 1, .index = 5 } },
        .validation_epoch = 7,
    };
    var selected: Key = @splat(0);
    @memcpy(selected[0..prefix.len], prefix);
    @memcpy(selected[prefix.len..][0..24], &observation.authority.namespace);
    std.mem.writeInt(u64, selected[prefix.len + 24 ..][0..8], 2, .big);
    std.mem.writeInt(u128, selected[prefix.len + 32 ..][0..16], 17, .big);
    const Check = struct {
        fn run(alloc: std.mem.Allocator, identity: Key, input: Observation) !void {
            const progress: census.Progress = .{ .observation = input, .scan_cursor = "scan\x00\xff" };
            const claim: census.Claim = .{ .before = @splat(8), .after = @splat(9) };
            const raw = try encodeAlloc(alloc, 17, progress, claim, false);
            defer alloc.free(raw);
            const decoded = try decode(raw, &identity, 17);
            try std.testing.expect(!decoded.closed);
            try std.testing.expectError(error.InvalidBatchRequest, encodeAlloc(alloc, 17, progress, claim, true));
            try std.testing.expectEqualDeep(progress, decoded.progress);
            try std.testing.expectEqualDeep(claim, decoded.claim);
            try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(raw, &identity, 18));
            for (0..raw.len) |index| {
                raw[index] ^= 1;
                try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(raw, &identity, 17));
                raw[index] ^= 1;
            }
            raw[68] = 1;
            std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
            try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(raw, &identity, 17));
            const closed_progress: census.Progress = .{ .observation = input, .enumerated = true };
            const closed = try encodeAlloc(alloc, 17, closed_progress, claim, true);
            defer alloc.free(closed);
            try std.testing.expect((try decode(closed, &identity, 17)).closed);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{ selected, observation });
}

test "ordered artifact inventory stream closure requires an unambiguous fixed root scope" {
    const completion = @import("artifact_completion_plan.zig");
    var producer: Request = .{ .kind = .chunk_text, .index_name = "", .artifact_name = "chunks", .doc_key = "", .source_field = "body" };
    var plan = try completion.Plan.init(std.testing.allocator, .{}, &.{producer});
    defer plan.deinit();
    try std.testing.expect(try plan.rootChunkScopeClosed("chunks"));
    producer.upstream_artifact_name = "document-units";
    var upstream = try completion.Plan.init(std.testing.allocator, .{}, &.{producer});
    defer upstream.deinit();
    try std.testing.expect(!try upstream.rootChunkScopeClosed("chunks"));
    producer.upstream_artifact_name = "";
    producer.neighbor_context_json = "{}";
    var neighbor = try completion.Plan.init(std.testing.allocator, .{}, &.{producer});
    defer neighbor.deinit();
    try std.testing.expect(!try neighbor.rootChunkScopeClosed("chunks"));
    producer.neighbor_context_json = "";
    var ambiguous = try completion.Plan.init(std.testing.allocator, .{}, &.{ producer, producer });
    defer ambiguous.deinit();
    try std.testing.expectError(error.ArtifactCatalogDrift, ambiguous.rootChunkScopeClosed("chunks"));
    try std.testing.expect(!std.mem.eql(u8, &plan.digest, &ambiguous.digest));
    try std.testing.expectError(error.ArtifactCatalogDrift, plan.rootChunkScopeClosed("other"));
}
