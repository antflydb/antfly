// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Accepted producer provenance contains the complete immutable read-set and
//! output digests, never a second copy of large artifact bodies. Native apply
//! constructs it from the validated command; transfer authenticates its bytes
//! together with source receipts and the immutable source snapshot/tail.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const binary = @import("relational_integrity_json.zig");
pub const prefix = "\x00\x00__artifact_publication__:proof:";

pub const Effect = struct {
    family: publication.Family,
    key: []const u8,
    source_index: u32,
    value_digest: ?publication.Digest,
    value_bytes: u64,
};

pub const Proof = struct {
    version: u8 = 1,
    namespace: publication.Namespace,
    authority_epoch: u64,
    catalog_digest: publication.Digest,
    producer_kind: @FieldType(publication.Command, "producer_kind"),
    producer_name: []const u8,
    producer_generation: u64,
    producer_artifact_name: []const u8,
    producer_scope_key: []const u8 = "",
    publication_digest: publication.Digest,
    input_digest: publication.Digest,
    sources: []const publication.Source,
    artifact_sources: []const publication.ArtifactSource,
    effects: []const Effect,

    pub fn jsonStringify(self: Proof, stream: anytype) @TypeOf(stream.*).Error!void {
        try binary.write(self, stream);
    }

    pub fn inputCommand(self: Proof) publication.Command {
        return .{ .namespace = self.namespace, .authority_epoch = self.authority_epoch, .catalog_digest = self.catalog_digest, .producer_kind = self.producer_kind, .producer_name = self.producer_name, .producer_generation = self.producer_generation, .producer_artifact_name = self.producer_artifact_name, .producer_scope_key = self.producer_scope_key, .sources = self.sources, .artifact_sources = self.artifact_sources, .mutations = &.{}, .publication_digest = self.publication_digest };
    }

    pub fn validate(self: Proof) !void {
        if (self.version != 1 or self.authority_epoch == 0 or (self.producer_generation == 0 and self.producer_kind != .resolver) or
            self.producer_name.len == 0 or self.producer_artifact_name.len == 0 or
            self.sources.len == 0 or self.sources.len > publication.max_source_documents or
            self.artifact_sources.len > publication.max_source_documents or self.effects.len == 0 or
            self.effects.len > publication.max_mutations or !std.mem.eql(u8, &self.input_digest, &self.inputCommand().inputDigest())) return error.ArtifactCatalogCorrupt;
        for (self.effects) |effect| if (effect.source_index >= self.sources.len or
            (effect.value_digest == null and effect.value_bytes != 0)) return error.ArtifactCatalogCorrupt;
    }

    /// Must be checked against the current owner's exact input snapshot.
    /// Cross-owner adoption first validates logical source equivalence and
    /// rewrites physical positions; copying donor authority is never valid.
    pub fn validateInputs(self: Proof, alloc: std.mem.Allocator, txn: anytype) !void {
        try self.validate();
        try publication.validateSources(alloc, txn, self.namespace, self.sources);
        try publication.validateArtifactSources(alloc, txn, self.namespace, self.sources, self.artifact_sources);
    }

    /// A consumer must retain the upstream's causal inputs, not merely its
    /// currently accepted output bytes. `command` has passed validate(), so
    /// binary searches use its canonical source/guard ordering without maps
    /// or allocations. Source ordinals may change when read sets are merged.
    pub fn requireInheritedBy(self: Proof, command: publication.Command) !void {
        try self.validate();
        if (self.authority_epoch != command.authority_epoch or !std.mem.eql(u8, &self.namespace, &command.namespace) or
            !std.mem.eql(u8, &self.catalog_digest, &command.catalog_digest)) return error.ArtifactCatalogDrift;
        for (self.sources) |source| {
            var lo: usize = 0;
            var hi = command.sources.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (std.mem.order(u8, command.sources[mid].document_key, source.document_key) == .lt) lo = mid + 1 else hi = mid;
            }
            if (lo == command.sources.len) return error.EnrichmentSourceChanged;
            const actual = command.sources[lo];
            if (!std.mem.eql(u8, actual.document_key, source.document_key) or actual.exists != source.exists or
                actual.timestamp != source.timestamp or !std.meta.eql(actual.content_digest, source.content_digest) or
                !std.meta.eql(actual.input_position, source.input_position)) return error.EnrichmentSourceChanged;
        }
        for (self.artifact_sources) |guard| {
            if (guard.source_index >= self.sources.len) return error.ArtifactCatalogCorrupt;
            var lo: usize = 0;
            var hi = command.artifact_sources.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (std.mem.order(u8, command.artifact_sources[mid].key, guard.key) == .lt) lo = mid + 1 else hi = mid;
            }
            if (lo == command.artifact_sources.len) return error.EnrichmentSourceChanged;
            const actual = command.artifact_sources[lo];
            if (actual.source_index >= command.sources.len or !std.mem.eql(u8, actual.key, guard.key) or
                !std.meta.eql(actual.content_digest, guard.content_digest) or !std.meta.eql(actual.input_position, guard.input_position) or
                !std.mem.eql(u8, command.sources[actual.source_index].document_key, self.sources[guard.source_index].document_key)) return error.EnrichmentSourceChanged;
        }
    }

    pub fn validateEffects(self: Proof, txn: anytype) !void {
        try self.validate();
        for (self.effects) |effect| {
            const raw = txn.get(effect.key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (effect.value_digest) |expected| {
                const value = raw orelse return error.EnrichmentSourceChanged;
                if (value.len != effect.value_bytes) return error.EnrichmentSourceChanged;
                var actual: publication.Digest = undefined;
                std.crypto.hash.sha2.Sha256.hash(value, &actual, .{});
                if (!std.mem.eql(u8, &actual, &expected)) return error.EnrichmentSourceChanged;
            } else if (raw != null) return error.EnrichmentSourceChanged;
        }
    }
};

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    proof: Proof,
    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

// Binary JSON expands each key byte to at most four decimal characters plus
// a delimiter. Artifact bodies are replaced by fixed-size digests. Preparation
// must charge this actual allocation before entering the apply transaction.
pub const max_encoded_bytes = publication.max_payload_bytes * 6 + publication.max_mutations * 256;

pub fn encodeAlloc(alloc: std.mem.Allocator, proof: Proof) ![]u8 {
    try proof.validate();
    const json = try std.json.Stringify.valueAlloc(alloc, proof, .{});
    defer alloc.free(json);
    if (json.len > max_encoded_bytes - 40) return error.TransactionTooLarge;
    const result = try alloc.alloc(u8, 40 + json.len);
    @memcpy(result[0..4], "APF1");
    std.mem.writeInt(u32, result[4..8], @intCast(json.len), .little);
    @memcpy(result[8 .. result.len - 32], json);
    std.crypto.hash.sha2.Sha256.hash(result[0 .. result.len - 32], result[result.len - 32 ..][0..32], .{});
    return result;
}

pub fn decodeAlloc(alloc: std.mem.Allocator, raw: []const u8) !Owned {
    if (raw.len < 40 or raw.len > max_encoded_bytes or !std.mem.eql(u8, raw[0..4], "APF1") or
        std.mem.readInt(u32, raw[4..8], .little) != raw.len - 40) return error.ArtifactCatalogCorrupt;
    var digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const proof = std.json.parseFromSliceLeaky(Proof, arena.allocator(), raw[8 .. raw.len - 32], .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ArtifactCatalogCorrupt,
    };
    try proof.validate();
    return .{ .arena = arena, .proof = proof };
}

pub fn fromCommand(alloc: std.mem.Allocator, command: publication.Command) !Owned {
    try command.validate(alloc);
    if (command.mode != .publish) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const sources = try owned.dupe(publication.Source, command.sources);
    for (sources) |*source| source.document_key = try owned.dupe(u8, source.document_key);
    const artifact_sources = try owned.dupe(publication.ArtifactSource, command.artifact_sources);
    for (artifact_sources) |*source| source.key = try owned.dupe(u8, source.key);
    const effects = try owned.alloc(Effect, command.mutations.len);
    for (command.mutations, effects) |mutation, *effect| {
        const digest: ?publication.Digest = if (mutation.value) |value| blk: {
            var hash: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &hash, .{});
            break :blk hash;
        } else null;
        effect.* = .{ .family = mutation.family, .key = try owned.dupe(u8, mutation.key), .source_index = mutation.source_index, .value_digest = digest, .value_bytes = if (mutation.value) |value| value.len else 0 };
    }
    const proof: Proof = .{
        .namespace = command.namespace,
        .authority_epoch = command.authority_epoch,
        .catalog_digest = command.catalog_digest,
        .producer_kind = command.producer_kind,
        .producer_name = try owned.dupe(u8, command.producer_name),
        .producer_generation = command.producer_generation,
        .producer_artifact_name = try owned.dupe(u8, command.producer_artifact_name),
        .producer_scope_key = try owned.dupe(u8, command.producer_scope_key),
        .publication_digest = command.publication_digest,
        .input_digest = command.inputDigest(),
        .sources = sources,
        .artifact_sources = artifact_sources,
        .effects = effects,
    };
    return .{ .arena = arena, .proof = proof };
}

pub fn key(namespace: publication.Namespace, digest: publication.Digest) [prefix.len + 24 + 32]u8 {
    var result: [prefix.len + 24 + 32]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..][0..24], &namespace);
    @memcpy(result[prefix.len + 24 ..], &digest);
    return result;
}

const reference_prefix = "\x00\x00__artifact_publication__:proof_ref:";
const count_prefix = "\x00\x00__artifact_publication__:proof_count:";
const artifact_prefix = "\x00\x00__artifact_publication__:artifact_proof:";

fn artifactReferenceKey(authority: publication.Authority, artifact: []const u8) [artifact_prefix.len + 24 + 8 + 32]u8 {
    var result: [artifact_prefix.len + 24 + 8 + 32]u8 = undefined;
    @memcpy(result[0..artifact_prefix.len], artifact_prefix);
    @memcpy(result[artifact_prefix.len..][0..24], &authority.namespace);
    std.mem.writeInt(u64, result[artifact_prefix.len + 24 ..][0..8], authority.epoch, .big);
    std.crypto.hash.sha2.Sha256.hash(artifact, result[result.len - 32 ..][0..32], .{});
    return result;
}

/// Caller holds one immutable storage snapshot through graph planning. A
/// matching cached value is insufficient: validate the accepted output's
/// position and ALL original primary/derived inputs, including absences.
pub fn readCurrentForArtifact(alloc: std.mem.Allocator, txn: anytype, artifact: []const u8, expected_value: ?[]const u8) !?Owned {
    return readCurrentArtifactProof(alloc, txn, artifact, expected_value, true);
}

/// Metadata-only census read. The caller independently enumerates the physical
/// key and checks whether its proof describes presence or absence. Revision
/// witnesses and all causal inputs are validated; no vector/blob body is read.
/// This is not a cross-owner adoption certificate.
pub fn readCurrentArtifactMetadata(alloc: std.mem.Allocator, txn: anytype, artifact: []const u8) !?Owned {
    return readCurrentArtifactProof(alloc, txn, artifact, null, false);
}

/// Receiver-local preparation evidence, never a portable producer receipt.
/// Final apply already revalidates the inherited primary/artifact read set;
/// this point fence preserves the exact accepted proof selected off-lock.
pub const ArtifactCertificate = struct {
    reference: [artifact_prefix.len + 24 + 8 + 32]u8,
    value: [32 + publication.Position.encoded_len]u8,
    pub fn requireCurrent(self: ArtifactCertificate, txn: anytype) !void {
        const current = txn.get(&self.reference) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (!std.mem.eql(u8, current, &self.value)) return error.EnrichmentSourceChanged;
    }
};

pub fn certifyInheritedArtifact(alloc: std.mem.Allocator, txn: anytype, artifact: []const u8, expected_value: ?[]const u8, consumer: publication.Command) !ArtifactCertificate {
    var accepted = (try readCurrentForArtifact(alloc, txn, artifact, expected_value)) orelse return error.ArtifactCoverageBaselinePending;
    defer accepted.deinit();
    try accepted.proof.requireInheritedBy(consumer);
    const reference = artifactReferenceKey(.{ .namespace = consumer.namespace, .epoch = consumer.authority_epoch, .catalog_digest = consumer.catalog_digest }, artifact);
    const raw = try txn.get(&reference);
    if (raw.len != 32 + publication.Position.encoded_len) return error.ArtifactCatalogCorrupt;
    return .{ .reference = reference, .value = raw[0 .. 32 + publication.Position.encoded_len].* };
}

/// Availability for authoritative projection accounting. Exact revision
/// witnesses avoid reading or hashing large output bodies; the complete
/// causal input set must still be current. Legacy/stale bytes earn no credit.
pub fn currentArtifactProduced(alloc: std.mem.Allocator, txn: anytype, artifact: []const u8) !bool {
    var owned = (readCurrentArtifactMetadata(alloc, txn, artifact) catch |err| switch (err) {
        error.EnrichmentSourceChanged => return false,
        else => return err,
    }) orelse return false;
    defer owned.deinit();
    for (owned.proof.effects) |effect| if (std.mem.eql(u8, effect.key, artifact)) return effect.value_digest != null;
    return error.ArtifactCatalogCorrupt;
}

fn readCurrentArtifactProof(alloc: std.mem.Allocator, txn: anytype, artifact: []const u8, expected_value: ?[]const u8, comptime verify_value: bool) !?Owned {
    const authority = (try publication.authority(txn)) orelse return null;
    const reference = artifactReferenceKey(authority, artifact);
    const raw = txn.get(&reference) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != 32 + publication.Position.encoded_len) return error.ArtifactCatalogCorrupt;
    const digest: publication.Digest = raw[0..32].*;
    const position = publication.Position.decode(raw[32..]) catch return error.ArtifactCatalogCorrupt;
    if (!std.meta.eql(try publication.artifactRevision(txn, authority.namespace, artifact), @as(?publication.Position, position))) return error.EnrichmentSourceChanged;
    var owned = try decodeAlloc(alloc, try txn.get(&key(authority.namespace, digest)));
    errdefer owned.deinit();
    const proof = owned.proof;
    if (proof.authority_epoch != authority.epoch or !std.mem.eql(u8, &proof.namespace, &authority.namespace) or
        !std.mem.eql(u8, &proof.catalog_digest, &authority.catalog_digest) or !std.mem.eql(u8, &proof.publication_digest, &digest)) return error.ArtifactCatalogCorrupt;
    try proof.validateInputs(alloc, txn);
    const effect = for (proof.effects) |candidate| {
        if (std.mem.eql(u8, candidate.key, artifact)) break candidate;
    } else return error.ArtifactCatalogCorrupt;
    if (verify_value) if (effect.value_digest) |expected| {
        const value = expected_value orelse return error.EnrichmentSourceChanged;
        if (value.len != effect.value_bytes) return error.EnrichmentSourceChanged;
        var actual: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(value, &actual, .{});
        if (!std.mem.eql(u8, &actual, &expected)) return error.EnrichmentSourceChanged;
    } else if (expected_value != null) return error.EnrichmentSourceChanged;
    return owned;
}

pub const Accepted = struct {
    owned: Owned,
    receipt: publication.Receipt,

    pub fn deinit(self: *Accepted) void {
        self.owned.deinit();
        self.* = undefined;
    }
};

/// Resolve one required producer stream by its stable receipt identity. The
/// selector supplies producer identity, not a guessed dependency read-set:
/// dependencies come from the accepted proof and are all revalidated. This
/// also works for absence/cleanup publications with no surviving output.
///
/// Output revision witnesses avoid rereading large vector/asset bodies. A
/// same-byte overwrite still invalidates acceptance, as does replacing just
/// one output of a multi-output publication. The caller must hold one snapshot
/// for the entire call, and may not treat one accepted stream as whole-document
/// completion without enumerating the immutable catalog's remaining streams.
/// This is a strict current-output check, not the provider retry fast path:
/// shared graph count/contender outputs can legitimately be superseded, and
/// must be reconciled by their owning projection rather than reinferred.
pub fn readCurrentForSource(alloc: std.mem.Allocator, txn: anytype, selector: publication.Command, source: publication.Source) !?Accepted {
    return readSource(alloc, txn, selector, source, false);
}

/// A completion read, not a provider retry or an entire-document certificate.
/// Private stream output stays revision-exact. Shared graph winner/count keys
/// may instead be owned by a later accepted publication of the same projection.
/// All checks use the caller's one immutable snapshot; no artifact body is read.
pub fn readConvergedForSource(alloc: std.mem.Allocator, txn: anytype, selector: publication.Command, source: publication.Source) !?Accepted {
    return readSource(alloc, txn, selector, source, true);
}

const Projection = struct {
    alloc: std.mem.Allocator,
    owned: Owned,
    effects: std.StringHashMapUnmanaged(Effect),

    fn deinit(self: *Projection) void {
        self.effects.deinit(self.alloc);
        self.owned.deinit();
    }
};

fn readSource(alloc: std.mem.Allocator, txn: anytype, selector: publication.Command, source: publication.Source, comptime reconcile_shared: bool) !?Accepted {
    const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (authority.epoch != selector.authority_epoch or !std.mem.eql(u8, &authority.namespace, &selector.namespace) or
        !std.mem.eql(u8, &authority.catalog_digest, &selector.catalog_digest)) return error.ArtifactCatalogDrift;
    const reference = referenceKey(selector, source);
    const raw = txn.get(&reference) catch |err| switch (err) {
        error.NotFound => {
            try publication.validateSources(alloc, txn, selector.namespace, (&source)[0..1]);
            return null;
        },
        else => return err,
    };
    if (raw.len != 32) return error.ArtifactCatalogCorrupt;
    const digest: publication.Digest = raw[0..32].*;
    const encoded = txn.get(&key(authority.namespace, digest)) catch |err| switch (err) {
        error.NotFound => return error.ArtifactCatalogCorrupt,
        else => return err,
    };
    var owned = try decodeAlloc(alloc, encoded);
    errdefer owned.deinit();
    const proof = owned.proof;
    if (proof.authority_epoch != authority.epoch or !std.mem.eql(u8, &proof.namespace, &authority.namespace) or
        !std.mem.eql(u8, &proof.catalog_digest, &authority.catalog_digest) or !std.mem.eql(u8, &proof.publication_digest, &digest)) return error.ArtifactCatalogCorrupt;
    const accepted_index = for (proof.sources, 0..) |candidate, index| {
        if (std.mem.eql(u8, candidate.document_key, source.document_key)) break index;
    } else return error.ArtifactCatalogCorrupt;
    const accepted_source = proof.sources[accepted_index];
    for (proof.effects) |effect| {
        if (effect.source_index == accepted_index) break;
    } else return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, &reference, &referenceKey(proof.inputCommand(), accepted_source))) return error.ArtifactCatalogCorrupt;
    if (accepted_source.exists != source.exists or accepted_source.timestamp != source.timestamp or
        !std.mem.eql(u8, &accepted_source.content_digest, &source.content_digest) or
        !std.meta.eql(accepted_source.input_position, source.input_position)) return error.EnrichmentSourceChanged;
    try proof.validateInputs(alloc, txn);
    const receipt = (try publication.readReceipt(txn, proof.inputCommand(), accepted_source)) orelse return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, &receipt.publication_digest, &digest)) return error.ArtifactCatalogCorrupt;
    var projections: std.AutoHashMapUnmanaged(publication.Digest, Projection) = .empty;
    defer {
        var it = projections.valueIterator();
        while (it.next()) |value| value.deinit();
        projections.deinit(alloc);
    }
    for (proof.effects) |effect| {
        const witness = txn.get(&artifactReferenceKey(authority, effect.key)) catch |err| switch (err) {
            error.NotFound => return error.EnrichmentSourceChanged,
            else => return err,
        };
        if (witness.len != 32 + publication.Position.encoded_len) return error.ArtifactCatalogCorrupt;
        const projection_digest: publication.Digest = witness[0..32].*;
        const position = publication.Position.decode(witness[32..]) catch return error.ArtifactCatalogCorrupt;
        if (!std.meta.eql(try publication.artifactRevision(txn, authority.namespace, effect.key), @as(?publication.Position, position))) return error.EnrichmentSourceChanged;
        if (!std.mem.eql(u8, &projection_digest, &digest)) {
            if (!reconcile_shared or !try sharedGraphOutput(alloc, proof, effect)) return error.EnrichmentSourceChanged;
            const slot = try projections.getOrPut(alloc, projection_digest);
            if (!slot.found_existing) {
                // Remove the uninitialized slot on any failure before defer
                // visits the cache. Reuse each validated proof across all
                // winner/count keys from that publication, not once per edge.
                errdefer _ = projections.remove(projection_digest);
                const raw_projection = txn.get(&key(authority.namespace, projection_digest)) catch |err| switch (err) {
                    error.NotFound => return error.ArtifactCatalogCorrupt,
                    else => return err,
                };
                var projection = try decodeAlloc(alloc, raw_projection);
                errdefer projection.deinit();
                const current = projection.proof;
                if (current.authority_epoch != authority.epoch or !std.mem.eql(u8, &current.namespace, &authority.namespace) or
                    !std.mem.eql(u8, &current.catalog_digest, &authority.catalog_digest) or !std.mem.eql(u8, &current.publication_digest, &projection_digest)) return error.ArtifactCatalogCorrupt;
                try current.validateInputs(alloc, txn);
                // The revision-exact artifact witness is installed only by
                // an accepted transaction and retains this immutable proof.
                // Its producer's latest receipt may have moved to another
                // scope/output since then; it is not an acceptance-history
                // lookup and must not invalidate a still-current projection.
                var effects: std.StringHashMapUnmanaged(Effect) = .empty;
                errdefer effects.deinit(alloc);
                try effects.ensureTotalCapacity(alloc, @intCast(current.effects.len));
                for (current.effects) |candidate| {
                    const inserted = effects.getOrPutAssumeCapacity(candidate.key);
                    if (inserted.found_existing) return error.ArtifactCatalogCorrupt;
                    inserted.value_ptr.* = candidate;
                }
                slot.value_ptr.* = .{ .alloc = alloc, .owned = projection, .effects = effects };
            }
            const current = slot.value_ptr.owned.proof;
            if (current.producer_kind != .graph or current.producer_generation != proof.producer_generation or
                !std.mem.eql(u8, current.producer_name, proof.producer_name)) return error.EnrichmentSourceChanged;
            const replacement = slot.value_ptr.effects.get(effect.key) orelse return error.ArtifactCatalogCorrupt;
            if (!try sharedGraphOutput(alloc, current, replacement)) return error.EnrichmentSourceChanged;
        }
    }
    return .{ .owned = owned, .receipt = receipt };
}

fn sharedGraphOutput(alloc: std.mem.Allocator, proof: Proof, effect: Effect) !bool {
    if (proof.producer_kind != .graph or effect.family != .graph) return false;
    const keys = @import("../internal_keys.zig");
    if (keys.isGraphEdgeArtifactKey(effect.key)) return keys.matchesGraphEdgeIndexName(effect.key, proof.producer_name);
    // Never relax per-source contender or graph-asset state records. The
    // exact visible-count sentinel is the only other shared projection key.
    if (!keys.isGraphEdgeContenderKey(effect.key)) return false;
    const count = try keys.graphEdgeContenderCountKeyAlloc(alloc, proof.sources[effect.source_index].document_key, proof.producer_name);
    defer alloc.free(count);
    return std.mem.eql(u8, count, effect.key);
}

fn countKey(namespace: publication.Namespace, digest: publication.Digest) [count_prefix.len + 24 + 32]u8 {
    var result: [count_prefix.len + 24 + 32]u8 = undefined;
    @memcpy(result[0..count_prefix.len], count_prefix);
    @memcpy(result[count_prefix.len..][0..24], &namespace);
    @memcpy(result[count_prefix.len + 24 ..], &digest);
    return result;
}

pub fn referencePrefix(authority: publication.Authority) [reference_prefix.len + 32]u8 {
    var result: [reference_prefix.len + 32]u8 = undefined;
    @memcpy(result[0..reference_prefix.len], reference_prefix);
    @memcpy(result[reference_prefix.len..][0..24], &authority.namespace);
    std.mem.writeInt(u64, result[result.len - 8 ..], authority.epoch, .big);
    return result;
}

pub fn referenceKey(command: publication.Command, source: publication.Source) [reference_prefix.len + 24 + 8 + 32]u8 {
    const receipt = publication.receiptKey(command, source);
    var result: [reference_prefix.len + 24 + 8 + 32]u8 = undefined;
    @memcpy(result[0..reference_prefix.len], reference_prefix);
    @memcpy(result[reference_prefix.len..][0..24], &command.namespace);
    std.mem.writeInt(u64, result[reference_prefix.len + 24 ..][0..8], command.authority_epoch, .big);
    @memcpy(result[reference_prefix.len + 24 + 8 ..], receipt[receipt.len - 32 ..]);
    return result;
}

/// One bounded maintenance page can release old-epoch references independently
/// of proof size. The caller supplies a key obtained from the private prefix
/// scan and must prove that epoch is no longer the active producer authority.
pub fn retireReference(txn: anytype, reference: []const u8, current: publication.Authority) !void {
    const artifact = std.mem.startsWith(u8, reference, artifact_prefix);
    const prefix_len: usize = if (artifact) artifact_prefix.len else reference_prefix.len;
    if (reference.len != prefix_len + 24 + 8 + 32 or
        (!artifact and !std.mem.startsWith(u8, reference, reference_prefix))) return error.ArtifactCatalogCorrupt;
    const namespace: publication.Namespace = reference[prefix_len..][0..24].*;
    const epoch = std.mem.readInt(u64, reference[prefix_len + 24 ..][0..8], .big);
    if (!std.mem.eql(u8, &namespace, &current.namespace) or epoch >= current.epoch) return error.ArtifactCatalogScopeChanged;
    const digest = try txn.get(reference);
    if (digest.len != 32 + @as(usize, if (artifact) publication.Position.encoded_len else 0)) return error.ArtifactCatalogCorrupt;
    const owned_digest: publication.Digest = digest[0..32].*;
    try changeReferences(txn, namespace, owned_digest, false);
    try txn.delete(reference);
}

fn changeReferences(txn: anytype, namespace: publication.Namespace, digest: publication.Digest, increment: bool) !void {
    const count_key = countKey(namespace, digest);
    const raw = txn.get(&count_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (raw != null and raw.?.len != 8) return error.ArtifactCatalogCorrupt;
    const old: u64 = if (raw) |value| std.mem.readInt(u64, value[0..8], .little) else 0;
    const next = if (increment) std.math.add(u64, old, 1) catch return error.ArtifactCatalogCorrupt else std.math.sub(u64, old, 1) catch return error.ArtifactCatalogCorrupt;
    if (next == 0) {
        try txn.delete(&count_key);
        try txn.delete(&key(namespace, digest));
    } else {
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(u64, &encoded, next, .little);
        try txn.put(&count_key, &encoded);
    }
}

/// The caller prepares encoded proof bytes outside apply and stages these
/// references in the same writer transaction as accepted receipts/effects.
/// One immutable proof serves all sources. Superseded source receipts release
/// their references, reclaiming the proof when its last current receipt moves.
/// Epoch retirement still needs a bounded reference walk; it cannot drop only
/// the authority key and strand these references.
pub fn stage(txn: anytype, command: publication.Command, encoded_proof: []const u8, position: publication.Position) !void {
    const owners = try command.outputSources();
    const proof_key = key(command.namespace, command.publication_digest);
    const old = txn.get(&proof_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (old) |value| {
        if (!std.mem.eql(u8, value, encoded_proof)) return error.ArtifactCatalogCorrupt;
    } else try txn.put(&proof_key, encoded_proof);
    for (command.sources, 0..) |source, source_index| {
        if (!owners.isSet(source_index)) continue;
        const reference = referenceKey(command, source);
        const previous = txn.get(&reference) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (previous) |digest| {
            if (digest.len != 32) return error.ArtifactCatalogCorrupt;
            if (std.mem.eql(u8, digest, &command.publication_digest)) continue;
            // Copy before mutating the transaction: borrowed values may be
            // invalidated by the first subsequent write.
            const old_digest: publication.Digest = digest[0..32].*;
            try changeReferences(txn, command.namespace, old_digest, false);
        }
        try changeReferences(txn, command.namespace, command.publication_digest, true);
        try txn.put(&reference, &command.publication_digest);
    }
    const authority: publication.Authority = .{ .namespace = command.namespace, .epoch = command.authority_epoch, .catalog_digest = command.catalog_digest };
    const encoded_position = try position.encode();
    var value: [32 + publication.Position.encoded_len]u8 = undefined;
    @memcpy(value[0..32], &command.publication_digest);
    @memcpy(value[32..], &encoded_position);
    for (command.mutations) |effect| {
        const reference = artifactReferenceKey(authority, effect.key);
        const previous = txn.get(&reference) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (previous) |old_value| {
            if (old_value.len != value.len) return error.ArtifactCatalogCorrupt;
            if (std.mem.eql(u8, old_value, &value)) continue;
            const old_digest: publication.Digest = old_value[0..32].*;
            try changeReferences(txn, command.namespace, old_digest, false);
        }
        try changeReferences(txn, command.namespace, command.publication_digest, true);
        try txn.put(&reference, &value);
    }
}

test "ordered artifact inventory provenance shares receipts and reclaims superseded proof" {
    const Fake = struct {
        values: std.StringHashMap([]u8),
        fn get(self: *@This(), name: []const u8) anyerror![]const u8 {
            return self.values.get(name) orelse error.NotFound;
        }
        fn put(self: *@This(), name: []const u8, value: []const u8) !void {
            const owned_value = try std.testing.allocator.dupe(u8, value);
            errdefer std.testing.allocator.free(owned_value);
            const entry = try self.values.getOrPut(name);
            if (entry.found_existing) std.testing.allocator.free(entry.value_ptr.*) else entry.key_ptr.* = try std.testing.allocator.dupe(u8, name);
            entry.value_ptr.* = owned_value;
        }
        fn delete(self: *@This(), name: []const u8) !void {
            const entry = self.values.fetchRemove(name) orelse return error.NotFound;
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        fn deinit(self: *@This()) void {
            var it = self.values.iterator();
            while (it.next()) |entry| {
                std.testing.allocator.free(entry.key_ptr.*);
                std.testing.allocator.free(entry.value_ptr.*);
            }
            self.values.deinit();
        }
    };
    var txn: Fake = .{ .values = std.StringHashMap([]u8).init(std.testing.allocator) };
    defer txn.deinit();
    const sources = [_]publication.Source{
        .{ .document_key = "a", .content_digest = @splat(1), .timestamp = 1, .input_position = null },
        .{ .document_key = "b", .content_digest = @splat(2), .timestamp = 1, .input_position = null },
        .{ .document_key = "read-only-neighbor", .content_digest = @splat(9), .timestamp = 1, .input_position = null },
    };
    const mutations = [_]publication.Mutation{
        .{ .family = .base_vector, .key = "a-output", .value = null, .source_index = 0 },
        .{ .family = .base_vector, .key = "b-output", .value = null, .source_index = 1 },
    };
    var command: publication.Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "model", .sources = &sources, .mutations = &mutations, .publication_digest = @splat(3) };
    const original_key = key(command.namespace, command.publication_digest);
    const position: publication.Position = .{ .raft = .{ .term = 1, .index = 1 } };
    try stage(&txn, command, "first-proof", position);
    try stage(&txn, command, "first-proof", position);
    // Preparation evidence survives unrelated writes, but not replacement or
    // retirement of the exact accepted artifact proof. Check the whole value:
    // an identical digest at a different publication position is not a match.
    const prepared_reference = artifactReferenceKey(.{ .namespace = command.namespace, .epoch = command.authority_epoch, .catalog_digest = command.catalog_digest }, mutations[0].key);
    const prepared_raw = try txn.get(&prepared_reference);
    const certificate: ArtifactCertificate = .{ .reference = prepared_reference, .value = prepared_raw[0 .. 32 + publication.Position.encoded_len].* };
    try certificate.requireCurrent(&txn);
    try txn.put("unrelated", "write");
    try certificate.requireCurrent(&txn);
    var changed_position = certificate.value;
    changed_position[changed_position.len - 1] ^= 1;
    try txn.put(&prepared_reference, &changed_position);
    try std.testing.expectError(error.EnrichmentSourceChanged, certificate.requireCurrent(&txn));
    try txn.delete(&prepared_reference);
    try std.testing.expectError(error.EnrichmentSourceChanged, certificate.requireCurrent(&txn));
    try txn.put(&prepared_reference, &certificate.value);
    try certificate.requireCurrent(&txn);
    try std.testing.expectError(error.NotFound, txn.get(&referenceKey(command, sources[2])));
    try std.testing.expectEqual(@as(u64, 4), std.mem.readInt(u64, (try txn.get(&countKey(command.namespace, command.publication_digest)))[0..8], .little));
    command.sources = sources[0..1];
    command.mutations = mutations[0..1];
    command.publication_digest = @splat(4);
    try stage(&txn, command, "second-proof", position);
    try std.testing.expectError(error.EnrichmentSourceChanged, certificate.requireCurrent(&txn));
    try std.testing.expectEqualStrings("first-proof", try txn.get(&original_key));
    command.sources = sources[1..2];
    var second = mutations[1];
    second.source_index = 0;
    command.mutations = (&second)[0..1];
    try stage(&txn, command, "second-proof", position);
    try std.testing.expectError(error.NotFound, txn.get(&original_key));
    try std.testing.expectEqual(@as(u64, 4), std.mem.readInt(u64, (try txn.get(&countKey(command.namespace, command.publication_digest)))[0..8], .little));
    try std.testing.expectError(error.ArtifactCatalogCorrupt, stage(&txn, command, "different-proof", position));
    const reference_b = referenceKey(command, sources[1]);
    const authority: publication.Authority = .{ .namespace = command.namespace, .epoch = 1, .catalog_digest = command.catalog_digest };
    try std.testing.expectError(error.ArtifactCatalogScopeChanged, retireReference(&txn, &reference_b, authority));
    var next_authority = authority;
    next_authority.epoch = 2;
    try retireReference(&txn, &reference_b, next_authority);
    try std.testing.expectEqualStrings("second-proof", try txn.get(&key(command.namespace, command.publication_digest)));
    try retireReference(&txn, &referenceKey(command, sources[0]), next_authority);
    for (mutations) |effect| try retireReference(&txn, &artifactReferenceKey(authority, effect.key), next_authority);
    try std.testing.expectError(error.NotFound, txn.get(&key(command.namespace, command.publication_digest)));
}

test "ordered artifact inventory provenance preserves complete binary read set without output duplication" {
    const alloc = std.testing.allocator;
    const keys = @import("../internal_keys.zig");
    const output = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc\xff", "model");
    defer alloc.free(output);
    const input = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc\xff", "input");
    defer alloc.free(input);
    var command: publication.Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "model", .sources = &.{.{ .document_key = "doc\xff", .content_digest = @splat(3), .timestamp = 1, .input_position = null }}, .artifact_sources = &.{.{ .key = input, .content_digest = null, .input_position = null, .source_index = 0 }}, .mutations = &.{.{ .family = .base_vector, .key = output, .value = "large-output-replaced-by-digest", .source_index = 0 }}, .publication_digest = @splat(0) };
    command.publication_digest = command.digest();
    var proof = try fromCommand(alloc, command);
    defer proof.deinit();
    const encoded = try encodeAlloc(alloc, proof.proof);
    defer alloc.free(encoded);
    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(proof.proof, decoded.proof);
    try std.testing.expectEqualSlices(u8, input, decoded.proof.artifact_sources[0].key);
    try std.testing.expect(decoded.proof.artifact_sources[0].content_digest == null);
    encoded[encoded.len - 1] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeAlloc(alloc, encoded));
}
