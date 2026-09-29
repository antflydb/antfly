// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Authenticated source-copy provenance. These are inert donor proof bodies
//! plus selected-output bitmaps, never copied receipt or authority records.
const std = @import("std");
const provenance = @import("artifact_producer_provenance.zig");
const publication = @import("artifact_publication.zig");
const inventory = @import("artifact_inventory.zig");
const catalog_view = @import("artifact_catalog_view.zig");

pub const import_prefix = "\x00\x00__metadata__:source_proof:";
pub const merge_prefix = "\x00\x00__metadata__:merge_source_proof:";
pub const witness_prefix = "\x00\x00__metadata__:merge_source_proof_witness:";
pub const max_entries: usize = 65536;
pub const max_bytes: usize = @import("../backup_codec.zig").max_block_payload_bytes;
const record_magic = "SPR1";

pub fn importKey(namespace: publication.Namespace, digest: publication.Digest) [import_prefix.len + 24 + 32]u8 {
    var result: [import_prefix.len + 24 + 32]u8 = undefined;
    @memcpy(result[0..import_prefix.len], import_prefix);
    @memcpy(result[import_prefix.len..][0..24], &namespace);
    @memcpy(result[import_prefix.len + 24 ..], &digest);
    return result;
}

/// Merge evidence is cut-scoped so a later attempt cannot overwrite the
/// selected-output bitmap from a different certified source pin.
pub fn mergeKey(namespace: publication.Namespace, pin: [32]u8, digest: publication.Digest) [merge_prefix.len + 24 + 32 + 32]u8 {
    var result: [merge_prefix.len + 24 + 32 + 32]u8 = undefined;
    @memcpy(result[0..merge_prefix.len], merge_prefix);
    @memcpy(result[merge_prefix.len..][0..24], &namespace);
    @memcpy(result[merge_prefix.len + 24 ..][0..32], &pin);
    @memcpy(result[merge_prefix.len + 56 ..], &digest);
    return result;
}

/// Small immutable CAS record committed with the certified APF3 body. The
/// ordered adopter can compare it under the writer lock without rehashing a
/// potentially large source proof there.
pub fn witnessKey(namespace: publication.Namespace, pin: [32]u8, digest: publication.Digest) [witness_prefix.len + 24 + 32 + 32]u8 {
    var result: [witness_prefix.len + 24 + 32 + 32]u8 = undefined;
    @memcpy(result[0..witness_prefix.len], witness_prefix);
    @memcpy(result[witness_prefix.len..][0..24], &namespace);
    @memcpy(result[witness_prefix.len + 24 ..][0..32], &pin);
    @memcpy(result[witness_prefix.len + 56 ..], &digest);
    return result;
}

/// APF3 decode has already verified its physical checksum over the entire
/// proof. Bind that checksum to the selected bitmap without hashing the large
/// proof a second time on every source import and candidate read.
pub fn recordDigest(proof_checksum: publication.Digest, bitmap: []const u8) publication.Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly-source-proof-record-v1\x00");
    hash.update(&proof_checksum);
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, bitmap.len, .little);
    hash.update(&size);
    hash.update(bitmap);
    var result: publication.Digest = undefined;
    hash.final(&result);
    return result;
}

test "ordered artifact inventory source proof witness binds checksum and selected bitmap" {
    const checksum: publication.Digest = @splat(1);
    const original = recordDigest(checksum, &.{1});
    try std.testing.expectEqualDeep(original, recordDigest(checksum, &.{1}));
    try std.testing.expect(!std.mem.eql(u8, &original, &recordDigest(checksum, &.{2})));
    try std.testing.expect(!std.mem.eql(u8, &original, &recordDigest(@splat(2), &.{1})));
}

/// Only inert evidence keys use this namespace. Donor receipt/authority keys
/// are never legal merge-page effects.
pub fn transferDigest(namespace: publication.Namespace, pin: [32]u8, key: []const u8) !publication.Digest {
    if (key.len != merge_prefix.len + 24 + 32 + 32 or !std.mem.startsWith(u8, key, merge_prefix) or
        !std.mem.eql(u8, key[merge_prefix.len..][0..24], &namespace) or
        !std.mem.eql(u8, key[merge_prefix.len + 24 ..][0..32], &pin)) return error.InvalidMergePage;
    return key[merge_prefix.len + 56 ..][0..32].*;
}

pub fn encodeValueAlloc(alloc: std.mem.Allocator, bitmap: []const u8, proof: []const u8) ![]u8 {
    if (bitmap.len == 0 or bitmap.len > publication.max_source_documents / 8 or proof.len > provenance.max_encoded_bytes or
        bitmap.len +| proof.len +| 6 > max_bytes - 44) return error.ResourceLimitExceeded;
    const value = try alloc.alloc(u8, 6 + bitmap.len + proof.len);
    @memcpy(value[0..4], record_magic);
    std.mem.writeInt(u16, value[4..6], @intCast(bitmap.len), .little);
    @memcpy(value[6..][0..bitmap.len], bitmap);
    @memcpy(value[6 + bitmap.len ..], proof);
    return value;
}

pub const Decoded = struct {
    /// Proof slices and bitmap borrow the certified batch value.
    proof: provenance.Owned,
    bitmap: []const u8,
    proof_checksum: publication.Digest,
    record_digest: publication.Digest,
    pub fn deinit(self: *@This()) void {
        self.proof.deinit();
        self.* = undefined;
    }
};

pub const ReceiverEffect = struct {
    effect: provenance.Effect,
    input_position: ?publication.Position,
};

/// The certified source cut and APF3 body remain bound to an owned candidate
/// after the transfer buffer is released. This identity is not local producer
/// authority; a later ordered command must still fence its active catalog.
pub const DonorIdentity = struct {
    source_pin: publication.Digest,
    namespace: publication.Namespace,
    binding: inventory.Binding,
    publication_digest: publication.Digest,
    input_digest: publication.Digest,
    proof_checksum: publication.Digest,
    record_digest: publication.Digest,
    producer_kind: @FieldType(publication.Command, "producer_kind"),
    producer_name: []const u8,
    producer_generation: u64,
    producer_artifact_name: []const u8,
    producer_scope_key: []const u8,
    selected_bitmap: []const u8,
};

/// Off-lock candidate evidence for a receiver-local publication. Physical
/// positions are captured from the destination, never copied from APF3. This
/// is not an adoption certificate: apply must revalidate it in its own writer
/// transaction before staging receipts or activating a producer capability.
pub const ReceiverCandidate = struct {
    arena: std.heap.ArenaAllocator,
    donor: DonorIdentity,
    receiver: struct { namespace: publication.Namespace, binding: inventory.Binding },
    /// Physical producer generation is receiver-owned. Null means this
    /// producer family still needs a dedicated catalog mapping before it can
    /// be adopted; donor generations must never be used as a fallback.
    receiver_producer_generation: ?u64,
    sources: []const publication.Source,
    artifact_sources: []const publication.ArtifactSource,
    effects: []const ReceiverEffect,

    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Exact bindings certify each physical catalog; their semantic digests
/// certify the cross-owner definition match. One plan serves all bounded
/// proofs at a certified cut: index/graph physical generations, enrichment
/// authority epochs, and resolver definition generations are mapped by name
/// without re-parsing catalogs per proof. Graph outputs still require value/key
/// rebinding before this identity is usable.
pub const ReceiverProducerPlan = struct {
    const IndexBinding = struct { kind: u8, donor: u64, receiver: u64 };
    const GenerationBinding = struct { donor: u64, receiver: u64 };
    arena: std.heap.ArenaAllocator,
    donor_binding: inventory.Binding,
    receiver_binding: inventory.Binding,
    indexes: std.StringHashMapUnmanaged(IndexBinding),
    enrichments: std.StringHashMapUnmanaged(GenerationBinding),
    resolvers: std.StringHashMapUnmanaged(GenerationBinding),

    fn familyGenerations(scratch: std.mem.Allocator, owned: std.mem.Allocator, raw: []const u8, default_generation: ?u64) !std.StringHashMapUnmanaged(u64) {
        var result: std.StringHashMapUnmanaged(u64) = .empty;
        if (raw.len == 0) return result;
        var parsed = std.json.parseFromSlice(std.json.Value, scratch, raw, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.SourceSnapshotCorrupt,
        };
        defer parsed.deinit();
        if (parsed.value != .array) return error.SourceSnapshotCorrupt;
        for (parsed.value.array.items) |item| {
            if (item != .object) return error.SourceSnapshotCorrupt;
            const name = item.object.get("name") orelse return error.SourceSnapshotCorrupt;
            if (name != .string or name.string.len == 0) return error.SourceSnapshotCorrupt;
            const producer_generation = default_generation orelse blk: {
                const value = item.object.get("config_generation") orelse break :blk 0;
                if (value != .integer or value.integer < 0) return error.SourceSnapshotCorrupt;
                break :blk std.math.cast(u64, value.integer) orelse return error.SourceSnapshotCorrupt;
            };
            if (result.contains(name.string)) return error.SourceSnapshotCorrupt;
            const inserted = try result.getOrPut(owned, try owned.dupe(u8, name.string));
            if (inserted.found_existing) return error.SourceSnapshotCorrupt;
            inserted.value_ptr.* = producer_generation;
        }
        return result;
    }

    fn mapFamily(
        scratch: std.mem.Allocator,
        owned: std.mem.Allocator,
        donor_raw: []const u8,
        receiver_raw: []const u8,
        donor_epoch: ?u64,
        receiver_epoch: ?u64,
    ) !std.StringHashMapUnmanaged(GenerationBinding) {
        const donor = try familyGenerations(scratch, owned, donor_raw, donor_epoch);
        const receiver = try familyGenerations(scratch, owned, receiver_raw, receiver_epoch);
        if (donor.count() != receiver.count()) return error.SourceSnapshotCorrupt;
        var mapped: std.StringHashMapUnmanaged(GenerationBinding) = .empty;
        var iterator = donor.iterator();
        while (iterator.next()) |entry| {
            const target = receiver.get(entry.key_ptr.*) orelse return error.SourceSnapshotCorrupt;
            if (donor_epoch == null and entry.value_ptr.* != target) return error.SourceSnapshotCorrupt;
            try mapped.put(owned, entry.key_ptr.*, .{ .donor = entry.value_ptr.*, .receiver = target });
        }
        return mapped;
    }

    pub fn init(alloc: std.mem.Allocator, donor_catalog: inventory.Catalogs, donor_binding: inventory.Binding, receiver_catalog: inventory.Catalogs, receiver_binding: inventory.Binding) !ReceiverProducerPlan {
        if (!std.mem.eql(u8, &donor_catalog.digest(), &donor_binding.digest) or
            !std.mem.eql(u8, &receiver_catalog.digest(), &receiver_binding.digest) or
            !std.mem.eql(u8, &try donor_catalog.semanticDigest(alloc), &donor_binding.semantic_digest) or
            !std.mem.eql(u8, &try receiver_catalog.semanticDigest(alloc), &receiver_binding.semantic_digest) or
            !donor_binding.compatible(receiver_binding)) return error.SourceSnapshotCorrupt;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var indexes: std.StringHashMapUnmanaged(IndexBinding) = .empty;
        var receiver_indexes: std.StringHashMapUnmanaged(catalog_view.Entry) = .empty;
        if (receiver_catalog.indexes.len != 0) {
            var entries = try catalog_view.Iterator.init(receiver_catalog.indexes);
            while (try entries.next()) |entry| {
                const result = try receiver_indexes.getOrPut(owned, entry.name);
                if (result.found_existing or entry.generation == 0) return error.SourceSnapshotCorrupt;
                result.value_ptr.* = entry;
            }
        }
        if (donor_catalog.indexes.len != 0) {
            var entries = try catalog_view.Iterator.init(donor_catalog.indexes);
            while (try entries.next()) |entry| {
                const target = receiver_indexes.get(entry.name) orelse return error.SourceSnapshotCorrupt;
                if (target.kind != entry.kind or entry.generation == 0) return error.SourceSnapshotCorrupt;
                const result = try indexes.getOrPut(owned, try owned.dupe(u8, entry.name));
                if (result.found_existing) return error.SourceSnapshotCorrupt;
                result.value_ptr.* = .{ .kind = entry.kind, .donor = entry.generation, .receiver = target.generation };
            }
        }
        if (indexes.count() != receiver_indexes.count()) return error.SourceSnapshotCorrupt;
        const enrichments = try mapFamily(alloc, owned, donor_catalog.enrichments, receiver_catalog.enrichments, donor_binding.epoch, receiver_binding.epoch);
        const resolvers = try mapFamily(alloc, owned, donor_catalog.resolvers, receiver_catalog.resolvers, null, null);
        return .{ .arena = arena, .donor_binding = donor_binding, .receiver_binding = receiver_binding, .indexes = indexes, .enrichments = enrichments, .resolvers = resolvers };
    }

    /// The merge checkpoint committed the donor layout under the exact copy
    /// attempt before importing any proof. Build this once per receiver pass
    /// from that durable record and the current ordered receiver catalog;
    /// neither a live donor lookup nor per-proof catalog parsing is needed.
    pub fn initForMerge(alloc: std.mem.Allocator, txn: anytype, progress: @import("merge_page_contract.zig").Progress) !ReceiverProducerPlan {
        try progress.validate();
        const donor_binding = progress.source.artifact_catalog orelse return error.ArtifactCatalogDrift;
        if (!progress.source.provenance_required or donor_binding.effect_protocol != 15) return error.ArtifactCatalogDrift;
        var source = try @import("merge_artifact_catalog.zig").load(alloc, txn, progress);
        defer source.deinit();
        var receiver = (try inventory.load(alloc, txn)) orelse return error.ArtifactCatalogDrift;
        defer receiver.deinit();
        var receiver_namespace: publication.Namespace = undefined;
        @import("doc_identity.zig").encodeNamespace(&receiver_namespace, progress.receiver_namespace);
        if (!std.mem.eql(u8, &receiver.value.command.namespace, &receiver_namespace)) return error.ArtifactCatalogDrift;
        const receiver_catalog = try inventory.catalogs(txn);
        const local = try inventory.local(txn);
        if (!std.mem.eql(u8, &receiver_catalog.digest(), &receiver.value.command.binding.digest) or
            !std.mem.eql(u8, &local.digest, &receiver.value.command.binding.digest)) return error.ArtifactCatalogDrift;
        return init(alloc, source.value.catalogs, donor_binding, receiver_catalog, receiver.value.command.binding);
    }

    pub fn deinit(self: *ReceiverProducerPlan) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn generation(self: *const ReceiverProducerPlan, proof: provenance.Proof) !?u64 {
        if (proof.producer_kind == .enrichment or proof.producer_kind == .resolver) {
            const mapping = if (proof.producer_kind == .enrichment) self.enrichments.get(proof.producer_name) else self.resolvers.get(proof.producer_name);
            const entry = mapping orelse return error.SourceSnapshotCorrupt;
            if (entry.donor != proof.producer_generation) return error.SourceSnapshotCorrupt;
            return entry.receiver;
        }
        if (proof.producer_kind != .index and proof.producer_kind != .graph) return null;
        const entry = self.indexes.get(proof.producer_name) orelse return error.SourceSnapshotCorrupt;
        if ((entry.kind == 3) != (proof.producer_kind == .graph) or entry.donor != proof.producer_generation)
            return error.SourceSnapshotCorrupt;
        return entry.receiver;
    }
};

/// Return null when a donor read-set or selected output is no longer exact at
/// the receiver. The caller can regenerate that stream instead of silently
/// adopting a stale result. One proof is bounded by the APF3 block limit;
/// the arena owns all remapped keys after the source buffer is released.
pub fn prepareReceiverCandidate(
    alloc: std.mem.Allocator,
    txn: anytype,
    receiver_namespace: publication.Namespace,
    donor_range: @import("../byte_range.zig").ByteRange,
    source_pin: publication.Digest,
    producer_plan: *const ReceiverProducerPlan,
    decoded: Decoded,
) !?ReceiverCandidate {
    if (std.mem.allEqual(u8, &source_pin, 0)) return error.SourceSnapshotCorrupt;
    var arena = std.heap.ArenaAllocator.init(alloc);
    var returned = false;
    defer if (!returned) arena.deinit();
    const owned = arena.allocator();
    const proof = decoded.proof.proof;
    const donor_binding = producer_plan.donor_binding;
    if (donor_binding.effect_protocol != 15 or donor_binding.epoch != proof.authority_epoch or
        !std.mem.eql(u8, &donor_binding.digest, &proof.catalog_digest)) return error.SourceSnapshotCorrupt;
    const witness = txn.get(&witnessKey(proof.namespace, source_pin, proof.publication_digest)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (!std.mem.eql(u8, witness, &decoded.record_digest)) return null;
    var ordered = (try inventory.load(alloc, txn)) orelse return null;
    defer ordered.deinit();
    const receiver_binding = ordered.value.command.binding;
    if (!std.mem.eql(u8, &ordered.value.command.namespace, &receiver_namespace) or
        !std.meta.eql(producer_plan.receiver_binding, receiver_binding)) return null;
    const active = (try publication.authority(txn)) orelse return null;
    if (!std.mem.eql(u8, &active.namespace, &receiver_namespace) or active.epoch != receiver_binding.epoch or
        !std.mem.eql(u8, &active.catalog_digest, &receiver_binding.digest)) return null;
    const receiver_producer_generation = try producer_plan.generation(proof);
    for (proof.sources, 0..) |source, index| if (decoded.bitmap[index / 8] & (@as(u8, 1) << @intCast(index % 8)) != 0 and
        !donor_range.contains(source.document_key)) return error.SourceSnapshotCorrupt;
    const sources = try owned.alloc(publication.Source, proof.sources.len);
    for (proof.sources, sources) |donor, *receiver| {
        receiver.* = if (donor.exists)
            publication.capturePrimarySource(owned, txn, receiver_namespace, donor.document_key) catch |err| switch (err) {
                error.EnrichmentSourceChanged => return null,
                else => return err,
            }
        else
            publication.capturePrimaryTombstoneSource(owned, txn, receiver_namespace, donor.document_key) catch |err| switch (err) {
                error.EnrichmentSourceChanged => return null,
                else => return err,
            };
        if (receiver.exists != donor.exists or receiver.timestamp != donor.timestamp or
            !std.mem.eql(u8, &receiver.content_digest, &donor.content_digest)) return null;
    }
    const artifact_sources = try owned.alloc(publication.ArtifactSource, proof.artifact_sources.len);
    for (proof.artifact_sources, artifact_sources) |donor, *receiver| {
        const raw = txn.get(donor.key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (raw) |value| {
            const expected = donor.content_digest orelse return null;
            var actual: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &actual, .{});
            if (!std.mem.eql(u8, &actual, &expected)) return null;
        } else if (donor.content_digest != null) return null;
        receiver.* = .{
            .key = try owned.dupe(u8, donor.key),
            .content_digest = donor.content_digest,
            .input_position = try publication.artifactRevision(txn, receiver_namespace, donor.key),
            .source_index = donor.source_index,
        };
    }
    const selected_effects = try owned.alloc(ReceiverEffect, proof.effects.len);
    var selected_count: usize = 0;
    for (proof.effects) |effect| {
        if (decoded.bitmap[effect.source_index / 8] & (@as(u8, 1) << @intCast(effect.source_index % 8)) == 0) continue;
        const raw = txn.get(effect.key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (raw) |value| {
            const expected = effect.value_digest orelse return null;
            if (value.len != effect.value_bytes) return null;
            var actual: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &actual, .{});
            if (!std.mem.eql(u8, &actual, &expected)) return null;
        } else if (effect.value_digest != null) return null;
        const input_position = try publication.artifactRevision(txn, receiver_namespace, effect.key);
        // A present imported postimage needs a receiver-owned physical
        // revision. Only an absent output may acquire one at ordered adoption.
        if (raw != null and input_position == null) return null;
        var local = effect;
        local.key = try owned.dupe(u8, effect.key);
        selected_effects[selected_count] = .{
            .effect = local,
            .input_position = input_position,
        };
        selected_count += 1;
    }
    const producer_name = try owned.dupe(u8, proof.producer_name);
    const producer_artifact_name = try owned.dupe(u8, proof.producer_artifact_name);
    const producer_scope_key = try owned.dupe(u8, proof.producer_scope_key);
    const selected_bitmap = try owned.dupe(u8, decoded.bitmap);
    returned = true;
    return .{
        .arena = arena,
        .receiver = .{ .namespace = receiver_namespace, .binding = receiver_binding },
        .receiver_producer_generation = receiver_producer_generation,
        .donor = .{
            .source_pin = source_pin,
            .namespace = proof.namespace,
            .binding = donor_binding,
            .publication_digest = proof.publication_digest,
            .input_digest = proof.input_digest,
            .proof_checksum = decoded.proof_checksum,
            .record_digest = decoded.record_digest,
            .producer_kind = proof.producer_kind,
            .producer_name = producer_name,
            .producer_generation = proof.producer_generation,
            .producer_artifact_name = producer_artifact_name,
            .producer_scope_key = producer_scope_key,
            .selected_bitmap = selected_bitmap,
        },
        .sources = sources,
        .artifact_sources = artifact_sources,
        .effects = selected_effects[0..selected_count],
    };
}

/// The writer transaction must repeat this check before installing any local
/// receipt. A successful off-lock preparation is only a snapshot observation:
/// equal bytes at a newer physical revision are still a stale candidate.
/// Returning false lets the caller regenerate instead of accepting donor
/// authority or overwriting a concurrent receiver publication.
pub fn revalidateReceiverCandidate(
    alloc: std.mem.Allocator,
    txn: anytype,
    receiver_namespace: publication.Namespace,
    candidate: ReceiverCandidate,
) !bool {
    if (!std.mem.eql(u8, &candidate.receiver.namespace, &receiver_namespace) or
        !candidate.donor.binding.compatible(candidate.receiver.binding)) return false;
    const witness = txn.get(&witnessKey(candidate.donor.namespace, candidate.donor.source_pin, candidate.donor.publication_digest)) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (!std.mem.eql(u8, witness, &candidate.donor.record_digest)) return false;
    var ordered = (try inventory.load(alloc, txn)) orelse return false;
    defer ordered.deinit();
    if (!std.mem.eql(u8, &ordered.value.command.namespace, &receiver_namespace) or
        !std.meta.eql(ordered.value.command.binding, candidate.receiver.binding)) return false;
    const active = (try publication.authority(txn)) orelse return false;
    if (!std.mem.eql(u8, &active.namespace, &receiver_namespace) or active.epoch != candidate.receiver.binding.epoch or
        !std.mem.eql(u8, &active.catalog_digest, &candidate.receiver.binding.digest)) return false;
    publication.validateSources(alloc, txn, receiver_namespace, candidate.sources) catch |err| switch (err) {
        error.EnrichmentSourceChanged => return false,
        else => return err,
    };
    publication.validateArtifactSources(alloc, txn, receiver_namespace, candidate.sources, candidate.artifact_sources) catch |err| switch (err) {
        error.EnrichmentSourceChanged => return false,
        else => return err,
    };
    for (candidate.effects) |selected| {
        if (!std.meta.eql(try publication.artifactRevision(txn, receiver_namespace, selected.effect.key), selected.input_position)) return false;
        const raw = txn.get(selected.effect.key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (raw) |value| {
            const expected = selected.effect.value_digest orelse return false;
            if (value.len != selected.effect.value_bytes) return false;
            var actual: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &actual, .{});
            if (!std.mem.eql(u8, &actual, &expected)) return false;
        } else if (selected.effect.value_digest != null) return false;
    }
    return true;
}

/// Prepare the receiver-owned proof outside serialized apply. Its logical
/// slices borrow the candidate, while `encoded` is separately owned by the
/// caller. Writer apply must revalidate the candidate and stage this proof,
/// its selected receipts and references in one transaction. This function
/// grants no authority by itself.
pub const AdoptedProof = struct {
    proof: provenance.Proof,
    encoded: []u8,
    effects: []provenance.Effect,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.encoded);
        alloc.free(self.effects);
        self.* = undefined;
    }
};

pub fn buildAdoptedProofAlloc(alloc: std.mem.Allocator, candidate: *const ReceiverCandidate) !AdoptedProof {
    // Graph key/value generations must first be rebound and compared against
    // their receiver postimages. Non-index producer generations need their
    // own catalog identity mapping. Never inherit the donor's generation.
    if (candidate.donor.producer_kind != .index) return error.ArtifactAdoptionUnsupported;
    const generation = candidate.receiver_producer_generation orelse return error.ArtifactAdoptionUnsupported;
    const effects = try alloc.alloc(provenance.Effect, candidate.effects.len);
    errdefer alloc.free(effects);
    for (candidate.effects, effects) |selected, *effect| {
        if (selected.effect.family == .graph) return error.ArtifactAdoptionUnsupported;
        effect.* = selected.effect;
    }
    var proof: provenance.Proof = .{
        .namespace = candidate.receiver.namespace,
        .authority_epoch = candidate.receiver.binding.epoch,
        .catalog_digest = candidate.receiver.binding.digest,
        .producer_kind = candidate.donor.producer_kind,
        .producer_name = candidate.donor.producer_name,
        .producer_generation = generation,
        .producer_artifact_name = candidate.donor.producer_artifact_name,
        .producer_scope_key = candidate.donor.producer_scope_key,
        .publication_digest = @splat(0),
        .input_digest = undefined,
        .sources = candidate.sources,
        .artifact_sources = candidate.artifact_sources,
        .effects = effects,
        .origin = .{
            .source_pin = candidate.donor.source_pin,
            .namespace = candidate.donor.namespace,
            .binding = candidate.donor.binding,
            .publication_digest = candidate.donor.publication_digest,
            .input_digest = candidate.donor.input_digest,
            .proof_checksum = candidate.donor.proof_checksum,
            .selected_bitmap = candidate.donor.selected_bitmap,
        },
    };
    proof.input_digest = proof.inputCommand().inputDigest();
    proof.publication_digest = proof.adoptionDigest();
    try proof.validatePortableShape(alloc);
    const encoded = try provenance.encodeAlloc(alloc, proof);
    return .{ .proof = proof, .encoded = encoded, .effects = effects };
}

test "ordered artifact inventory receiver producer mapping rejects donor identity drift" {
    const alloc = std.testing.allocator;
    const prefix = "AIDX\x02\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00g\x03\x02\x00\x00\x00{}";
    const donor: inventory.Catalogs = .{ .indexes = prefix ++ "\x05\x00\x00\x00\x00\x00\x00\x00" };
    const receiver: inventory.Catalogs = .{ .indexes = prefix ++ "\x09\x00\x00\x00\x00\x00\x00\x00" };
    const donor_binding: inventory.Binding = .{ .epoch = 3, .digest = donor.digest(), .semantic_digest = try donor.semanticDigest(alloc), .effect_protocol = 15 };
    const receiver_binding: inventory.Binding = .{ .epoch = 4, .digest = receiver.digest(), .semantic_digest = try receiver.semanticDigest(alloc), .effect_protocol = 15 };
    var plan = try ReceiverProducerPlan.init(alloc, donor, donor_binding, receiver, receiver_binding);
    defer plan.deinit();
    const source = publication.Source{ .document_key = "d", .content_digest = @splat(1), .timestamp = 1, .input_position = null };
    const effect = provenance.Effect{ .family = .graph, .key = "effect", .source_index = 0, .value_digest = null, .value_bytes = 0 };
    var proof: provenance.Proof = .{ .namespace = @splat(1), .authority_epoch = 3, .catalog_digest = donor_binding.digest, .producer_kind = .graph, .producer_name = "g", .producer_generation = 5, .producer_artifact_name = "g", .publication_digest = @splat(2), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
    proof.input_digest = proof.inputCommand().inputDigest();
    try std.testing.expectEqual(@as(?u64, 9), try plan.generation(proof));
    proof.producer_generation = 9;
    try std.testing.expectError(error.SourceSnapshotCorrupt, plan.generation(proof));
    proof.producer_generation = 5;
    proof.producer_name = "missing";
    try std.testing.expectError(error.SourceSnapshotCorrupt, plan.generation(proof));
    proof.producer_name = "g";
    proof.producer_kind = .index;
    try std.testing.expectError(error.SourceSnapshotCorrupt, plan.generation(proof));
    proof.producer_kind = .graph;
    var forged = donor_binding;
    forged.digest[0] ^= 1;
    try std.testing.expectError(error.SourceSnapshotCorrupt, ReceiverProducerPlan.init(alloc, donor, forged, receiver, receiver_binding));
    const AllocationCheck = struct {
        fn run(a: std.mem.Allocator, source_catalog: inventory.Catalogs, source_binding: inventory.Binding, target_catalog: inventory.Catalogs, target_binding: inventory.Binding) !void {
            var mapped = try ReceiverProducerPlan.init(a, source_catalog, source_binding, target_catalog, target_binding);
            defer mapped.deinit();
            try std.testing.expectEqual(@as(usize, 1), mapped.indexes.count());
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{ donor, donor_binding, receiver, receiver_binding });
}

test "ordered artifact inventory receiver maps enrichment epochs and resolver definitions" {
    const alloc = std.testing.allocator;
    const donor: inventory.Catalogs = .{
        .enrichments = "[{\"name\":\"asset\",\"source_field\":\"body\"}]",
        .resolvers = "[{\"name\":\"resolve\",\"config_generation\":7},{\"name\":\"default\"}]",
    };
    const receiver: inventory.Catalogs = .{
        .enrichments = "[{\"source_field\":\"body\",\"name\":\"asset\"}]",
        .resolvers = "[{\"name\":\"default\"},{\"config_generation\":7,\"name\":\"resolve\"}]",
    };
    const donor_binding: inventory.Binding = .{ .epoch = 3, .digest = donor.digest(), .semantic_digest = try donor.semanticDigest(alloc), .effect_protocol = 15 };
    const receiver_binding: inventory.Binding = .{ .epoch = 4, .digest = receiver.digest(), .semantic_digest = try receiver.semanticDigest(alloc), .effect_protocol = 15 };
    var plan = try ReceiverProducerPlan.init(alloc, donor, donor_binding, receiver, receiver_binding);
    defer plan.deinit();
    var proof: provenance.Proof = .{
        .namespace = @splat(1),
        .authority_epoch = donor_binding.epoch,
        .catalog_digest = donor_binding.digest,
        .producer_kind = .enrichment,
        .producer_name = "asset",
        .producer_generation = donor_binding.epoch,
        .producer_artifact_name = "asset",
        .publication_digest = @splat(2),
        .input_digest = @splat(3),
        .sources = &.{},
        .artifact_sources = &.{},
        .effects = &.{},
    };
    try std.testing.expectEqual(@as(?u64, receiver_binding.epoch), try plan.generation(proof));
    proof.producer_generation = receiver_binding.epoch;
    try std.testing.expectError(error.SourceSnapshotCorrupt, plan.generation(proof));
    proof.producer_kind = .resolver;
    proof.producer_name = "resolve";
    proof.producer_generation = 7;
    try std.testing.expectEqual(@as(?u64, 7), try plan.generation(proof));
    proof.producer_generation = 8;
    try std.testing.expectError(error.SourceSnapshotCorrupt, plan.generation(proof));
    proof.producer_name = "default";
    proof.producer_generation = 0;
    try std.testing.expectEqual(@as(?u64, 0), try plan.generation(proof));
    proof.producer_name = "missing";
    try std.testing.expectError(error.SourceSnapshotCorrupt, plan.generation(proof));
    proof.producer_kind = .promotion;
    try std.testing.expect((try plan.generation(proof)) == null);
    const drifted: inventory.Catalogs = .{ .enrichments = donor.enrichments, .resolvers = "[{\"name\":\"resolve\",\"config_generation\":8}]" };
    const drifted_binding: inventory.Binding = .{ .epoch = 4, .digest = drifted.digest(), .semantic_digest = try drifted.semanticDigest(alloc), .effect_protocol = 15 };
    try std.testing.expectError(error.SourceSnapshotCorrupt, ReceiverProducerPlan.init(alloc, donor, donor_binding, drifted, drifted_binding));
    const AllocationCheck = struct {
        fn run(a: std.mem.Allocator, source: inventory.Catalogs, source_binding: inventory.Binding, target: inventory.Catalogs, target_binding: inventory.Binding) !void {
            var mapped = try ReceiverProducerPlan.init(a, source, source_binding, target, target_binding);
            defer mapped.deinit();
            try std.testing.expectEqual(@as(usize, 1), mapped.enrichments.count());
            try std.testing.expectEqual(@as(usize, 2), mapped.resolvers.count());
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{ donor, donor_binding, receiver, receiver_binding });
}

test "ordered artifact inventory receiver plan pins durable merge source catalog" {
    const alloc = std.testing.allocator;
    const pages = @import("merge_page_contract.zig");
    const source_catalog = @import("merge_artifact_catalog.zig");
    const donor: inventory.Catalogs = .{ .indexes = "AIDX\x02\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00g\x03\x02\x00\x00\x00{}\x05\x00\x00\x00\x00\x00\x00\x00" };
    const receiver: inventory.Catalogs = .{ .indexes = "AIDX\x02\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00g\x03\x02\x00\x00\x00{}\x09\x00\x00\x00\x00\x00\x00\x00" };
    const donor_binding: inventory.Binding = .{ .epoch = 1, .digest = donor.digest(), .semantic_digest = try donor.semanticDigest(alloc), .effect_protocol = 15 };
    const receiver_binding: inventory.Binding = .{ .epoch = 1, .digest = receiver.digest(), .semantic_digest = try receiver.semanticDigest(alloc), .effect_protocol = 15 };
    const donor_namespace: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 };
    const receiver_namespace: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 };
    const progress: pages.Progress = .{
        .version = 2,
        .transition_id = 17,
        .donor_group_id = 2,
        .receiver_group_id = 3,
        .receiver_namespace = receiver_namespace,
        .attempt = .{ .donor_term = 1, .sequence = 1 },
        .source = .{ .namespace = donor_namespace, .pin_digest = @splat(7), .applied_index = 5, .retention = .{ .epoch = 1, .after_sequence = 0 }, .artifact_catalog = donor_binding, .provenance_required = true },
        .provenance_pending = true,
    };
    const source_raw = (try source_catalog.encode(alloc, .{ .kind = @as(enum { begin_copy, accept }, .begin_copy), .page_source = @as(?pages.Source, progress.source), .page_receiver_namespace = @as(?@TypeOf(receiver_namespace), receiver_namespace), .page_source_catalogs = @as(?inventory.Catalogs, donor) }, progress)).?;
    defer alloc.free(source_raw);
    var receiver_namespace_bytes: publication.Namespace = undefined;
    @import("doc_identity.zig").encodeNamespace(&receiver_namespace_bytes, receiver_namespace);
    const ordered: inventory.Ordered = .{ .command = .{ .namespace = receiver_namespace_bytes, .binding = receiver_binding, .catalogs = receiver }, .applied_index = 4 };
    const ordered_raw = try std.json.Stringify.valueAlloc(alloc, ordered, .{ .emit_strings_as_arrays = true });
    defer alloc.free(ordered_raw);
    const Fake = struct {
        source_raw: []const u8,
        ordered_raw: []const u8,
        indexes: []const u8,
        pub fn get(self: *@This(), key_bytes: []const u8) anyerror![]const u8 {
            if (std.mem.eql(u8, key_bytes, source_catalog.key)) return self.source_raw;
            if (std.mem.eql(u8, key_bytes, inventory.ordered_key)) return self.ordered_raw;
            if (std.mem.eql(u8, key_bytes, inventory.index_key)) return self.indexes;
            return error.NotFound;
        }
    };
    var txn: Fake = .{ .source_raw = source_raw, .ordered_raw = ordered_raw, .indexes = receiver.indexes };
    var plan = try ReceiverProducerPlan.initForMerge(alloc, &txn, progress);
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 1), plan.indexes.count());
    try std.testing.expectEqual(@as(u64, 9), plan.indexes.get("g").?.receiver);
    var changed_attempt = progress;
    changed_attempt.attempt.sequence += 1;
    try std.testing.expectError(error.ArtifactCatalogDrift, ReceiverProducerPlan.initForMerge(alloc, &txn, changed_attempt));
    txn.indexes = donor.indexes;
    try std.testing.expectError(error.ArtifactCatalogDrift, ReceiverProducerPlan.initForMerge(alloc, &txn, progress));
}

pub fn decodeValue(alloc: std.mem.Allocator, namespace: publication.Namespace, digest: publication.Digest, raw: []const u8) !Decoded {
    if (raw.len < 6 + 40 or raw.len > max_bytes - 44 or !std.mem.eql(u8, raw[0..4], record_magic)) return error.SourceSnapshotCorrupt;
    const bitmap_len = std.mem.readInt(u16, raw[4..6], .little);
    if (bitmap_len == 0 or bitmap_len > publication.max_source_documents / 8 or raw.len < 6 + @as(usize, bitmap_len) + 40)
        return error.SourceSnapshotCorrupt;
    const bitmap = raw[6..][0..bitmap_len];
    var proof = provenance.decodeBorrowed(alloc, raw[6 + bitmap_len ..]) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.SourceSnapshotCorrupt,
    };
    errdefer proof.deinit();
    if (!std.mem.eql(u8, &proof.proof.namespace, &namespace) or !std.mem.eql(u8, &proof.proof.publication_digest, &digest) or
        bitmap.len != (proof.proof.sources.len + 7) / 8) return error.SourceSnapshotCorrupt;
    proof.proof.validatePortableShape(alloc) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.SourceSnapshotCorrupt,
    };
    var outputs = std.StaticBitSet(publication.max_source_documents).initEmpty();
    for (proof.proof.effects) |effect| outputs.set(effect.source_index);
    var selected = false;
    for (bitmap, 0..) |bits, byte_index| {
        for (0..8) |bit_index| {
            if (bits & (@as(u8, 1) << @intCast(bit_index)) == 0) continue;
            const ordinal = byte_index * 8 + bit_index;
            if (ordinal >= proof.proof.sources.len or !outputs.isSet(ordinal)) return error.SourceSnapshotCorrupt;
            selected = true;
        }
    }
    if (!selected) return error.SourceSnapshotCorrupt;
    const proof_checksum: publication.Digest = raw[raw.len - 32 ..][0..32].*;
    return .{ .proof = proof, .bitmap = bitmap, .proof_checksum = proof_checksum, .record_digest = recordDigest(proof_checksum, bitmap) };
}

pub const Entry = struct { digest: publication.Digest, value: []const u8 };
pub const Position = struct { object: u32, offset: u64 = 0, remaining: u32 = 0 };
pub const Descriptor = struct {
    digest: publication.Digest,
    object: u32,
    value_offset: u64,
    value_len: u32,
    next_position: Position,

    pub fn read(self: Descriptor, reader: anytype, offset: u64, out: []u8) !void {
        if (offset > self.value_len or out.len > self.value_len - offset) return error.SourceSnapshotCorrupt;
        try exact(reader, self.object, self.value_offset + offset, out);
    }
};

fn exact(reader: anytype, object: u32, offset: u64, out: []u8) !void {
    var done: usize = 0;
    while (done < out.len) {
        const count = try reader.readAt(object, offset + done, out[done..]);
        if (count == 0 or count > out.len - done) return error.SourceSnapshotCorrupt;
        done += count;
    }
}

fn readWord(reader: anytype, object: u32, offset: u64) !u32 {
    var bytes: [4]u8 = undefined;
    try exact(reader, object, offset, &bytes);
    return std.mem.readInt(u32, &bytes, .little);
}

/// Locate one record without copying its potentially large APF3 body. A
/// source-certificate verifier authenticates the complete object before the
/// merge driver uses these offsets; the receiver validates the record again
/// before granting any local adoption evidence.
pub fn descriptor(reader: anytype, object_size: u64, position: Position) !?Descriptor {
    if (object_size < 4 or object_size > max_bytes) return error.SourceSnapshotCorrupt;
    var next = position;
    if (next.offset == 0) {
        if (next.remaining != 0) return error.SourceSnapshotCorrupt;
        next.remaining = try readWord(reader, next.object, 0);
        if ((next.remaining == 0 and object_size != 4) or next.remaining > max_entries or next.remaining > (object_size - 4) / 86)
            return error.SourceSnapshotCorrupt;
        next.offset = 4;
    }
    if (next.offset < 4 or next.offset > object_size or next.remaining > max_entries) return error.SourceSnapshotCorrupt;
    if (next.remaining == 0) {
        if (next.offset != object_size) return error.SourceSnapshotCorrupt;
        return null;
    }
    if (object_size - next.offset < 40) return error.SourceSnapshotCorrupt;
    if (try readWord(reader, next.object, next.offset) != 32) return error.SourceSnapshotCorrupt;
    var digest: publication.Digest = undefined;
    try exact(reader, next.object, next.offset + 4, &digest);
    const value_len = try readWord(reader, next.object, next.offset + 36);
    if (value_len < 46 or value_len > max_bytes - 44 or value_len > object_size - next.offset - 40)
        return error.SourceSnapshotCorrupt;
    const value_offset = next.offset + 40;
    const end = value_offset + value_len;
    next.offset = end;
    next.remaining -= 1;
    if ((next.remaining == 0 and end != object_size) or next.remaining > (object_size - end) / 86)
        return error.SourceSnapshotCorrupt;
    return .{ .digest = digest, .object = position.object, .value_offset = value_offset, .value_len = value_len, .next_position = next };
}

pub const Reader = struct {
    bytes: []const u8,
    offset: usize = 4,
    remaining: u32,

    pub fn init(bytes: []const u8) !Reader {
        if (bytes.len < 4 or bytes.len > max_bytes) return error.SourceSnapshotCorrupt;
        const count = std.mem.readInt(u32, bytes[0..4], .little);
        if ((count == 0 and bytes.len != 4) or count > max_entries or count > (bytes.len - 4) / 8) return error.SourceSnapshotCorrupt;
        return .{ .bytes = bytes, .remaining = count };
    }

    fn take(self: *Reader, length: usize) ![]const u8 {
        if (length > self.bytes.len -| self.offset) return error.SourceSnapshotCorrupt;
        const result = self.bytes[self.offset..][0..length];
        self.offset += length;
        return result;
    }

    pub fn next(self: *Reader) !?Entry {
        if (self.remaining == 0) {
            if (self.offset != self.bytes.len) return error.SourceSnapshotCorrupt;
            return null;
        }
        const key_len = std.mem.readInt(u32, (try self.take(4))[0..4], .little);
        if (key_len != 32) return error.SourceSnapshotCorrupt;
        const digest: publication.Digest = (try self.take(32))[0..32].*;
        const value_len = std.mem.readInt(u32, (try self.take(4))[0..4], .little);
        if (value_len < 46 or value_len > max_bytes - 44) return error.SourceSnapshotCorrupt;
        const value = try self.take(value_len);
        self.remaining -= 1;
        return .{ .digest = digest, .value = value };
    }
};

test "ordered artifact inventory source proof batch rejects forged bitmap and count without granting authority" {
    const alloc = std.testing.allocator;
    const effect_key = try @import("../internal_keys.zig").embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "index");
    defer alloc.free(effect_key);
    const source = publication.Source{ .document_key = "doc", .content_digest = @splat(3), .timestamp = 1, .input_position = null };
    const effect = provenance.Effect{ .family = .base_vector, .key = effect_key, .source_index = 0, .value_digest = null, .value_bytes = 0 };
    var proof: provenance.Proof = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_kind = .index, .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "asset", .publication_digest = @splat(4), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
    proof.input_digest = proof.inputCommand().inputDigest();
    const bytes = try provenance.encodeAlloc(alloc, proof);
    defer alloc.free(bytes);
    const value = try encodeValueAlloc(alloc, &.{1}, bytes);
    defer alloc.free(value);
    var decoded = try decodeValue(alloc, proof.namespace, proof.publication_digest, value);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &.{1}, decoded.bitmap);
    const forged = try alloc.dupe(u8, value);
    defer alloc.free(forged);
    forged[6] = 2;
    try std.testing.expectError(error.SourceSnapshotCorrupt, decodeValue(alloc, proof.namespace, proof.publication_digest, forged));
    const foreign_key = try @import("../internal_keys.zig").embeddingArtifactKeyForDocumentAlloc(alloc, "other", "index");
    defer alloc.free(foreign_key);
    const foreign_effect = provenance.Effect{ .family = .base_vector, .key = foreign_key, .source_index = 0, .value_digest = null, .value_bytes = 0 };
    proof.effects = (&foreign_effect)[0..1];
    const foreign_proof = try provenance.encodeAlloc(alloc, proof);
    defer alloc.free(foreign_proof);
    const foreign_value = try encodeValueAlloc(alloc, &.{1}, foreign_proof);
    defer alloc.free(foreign_value);
    try std.testing.expectError(error.SourceSnapshotCorrupt, decodeValue(alloc, proof.namespace, proof.publication_digest, foreign_value));
    try std.testing.expectError(error.SourceSnapshotCorrupt, Reader.init("\xff\xff\xff\x7f"));
}

test "ordered artifact inventory receiver inputs remap exact causal revisions without adopting donor authority" {
    const alloc = std.testing.allocator;
    const keys = @import("../internal_keys.zig");
    const Fake = struct {
        values: std.StringHashMap([]const u8),
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            return self.values.get(key) orelse error.NotFound;
        }
    };
    var receiver: Fake = .{ .values = std.StringHashMap([]const u8).init(alloc) };
    defer receiver.values.deinit();
    const row_key = try keys.documentKeyAlloc(alloc, "doc");
    defer alloc.free(row_key);
    const ttl_key = try keys.ttlKeyAlloc(alloc, "doc");
    defer alloc.free(ttl_key);
    const guard_key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "guard");
    defer alloc.free(guard_key);
    const output_key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "output");
    defer alloc.free(output_key);
    const tombstone_key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "removed");
    defer alloc.free(tombstone_key);
    const row = "{\"v\":1}";
    const guard_value = "causal input";
    const output_value = "derived output";
    var timestamp: [8]u8 = undefined;
    std.mem.writeInt(u64, &timestamp, 7, .little);
    try receiver.values.put(row_key, row);
    try receiver.values.put(ttl_key, &timestamp);
    try receiver.values.put(guard_key, guard_value);
    try receiver.values.put(output_key, output_value);
    const receiver_namespace: publication.Namespace = @splat(2);
    const index_prefix = "AIDX\x02\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00index\x00\x02\x00\x00\x00{}";
    const donor_catalog: inventory.Catalogs = .{ .indexes = index_prefix ++ "\x01\x00\x00\x00\x00\x00\x00\x00" };
    const catalogs: inventory.Catalogs = .{ .indexes = index_prefix ++ "\x09\x00\x00\x00\x00\x00\x00\x00" };
    const catalog_digest = donor_catalog.digest();
    const semantic_digest = try donor_catalog.semanticDigest(alloc);
    const donor_binding: inventory.Binding = .{ .epoch = 1, .digest = catalog_digest, .semantic_digest = semantic_digest, .effect_protocol = 15 };
    const receiver_binding: inventory.Binding = .{ .epoch = 2, .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(alloc), .effect_protocol = 15 };
    try std.testing.expect(donor_binding.compatible(receiver_binding));
    var plan = try ReceiverProducerPlan.init(alloc, donor_catalog, donor_binding, catalogs, receiver_binding);
    defer plan.deinit();
    const ordered: inventory.Ordered = .{ .command = .{ .namespace = receiver_namespace, .previous = donor_binding, .binding = receiver_binding, .catalogs = catalogs }, .applied_index = 4 };
    const ordered_bytes = try std.json.Stringify.valueAlloc(alloc, ordered, .{});
    defer alloc.free(ordered_bytes);
    try receiver.values.put(inventory.ordered_key, ordered_bytes);
    var authority_bytes: [100]u8 = undefined;
    @memcpy(authority_bytes[0..4], "APA1");
    @memcpy(authority_bytes[4..28], &receiver_namespace);
    std.mem.writeInt(u64, authority_bytes[28..36], receiver_binding.epoch, .little);
    @memcpy(authority_bytes[36..68], &receiver_binding.digest);
    std.crypto.hash.Blake3.hash(authority_bytes[0..68], authority_bytes[68..100], .{});
    try receiver.values.put(publication.authority_key, &authority_bytes);
    const row_revision_key = publication.inputRevisionKey(receiver_namespace, "doc");
    const guard_revision_key = publication.artifactRevisionKey(receiver_namespace, guard_key);
    const output_revision_key = publication.artifactRevisionKey(receiver_namespace, output_key);
    const row_position: publication.Position = .{ .raft = .{ .term = 9, .index = 10 } };
    const guard_position: publication.Position = .{ .raft = .{ .term = 9, .index = 11 } };
    const output_position: publication.Position = .{ .raft = .{ .term = 9, .index = 12 } };
    const row_position_bytes = try row_position.encode();
    const guard_position_bytes = try guard_position.encode();
    const output_position_bytes = try output_position.encode();
    try receiver.values.put(&row_revision_key, &row_position_bytes);
    try receiver.values.put(&guard_revision_key, &guard_position_bytes);
    try receiver.values.put(&output_revision_key, &output_position_bytes);
    var row_digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(row, &row_digest, .{});
    var guard_digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(guard_value, &guard_digest, .{});
    var output_digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(output_value, &output_digest, .{});
    const source = publication.Source{ .document_key = "doc", .content_digest = row_digest, .timestamp = 7, .input_position = .{ .raft = .{ .term = 1, .index = 4 } } };
    const guard = publication.ArtifactSource{ .key = guard_key, .content_digest = guard_digest, .input_position = .{ .raft = .{ .term = 1, .index = 5 } }, .source_index = 0 };
    // The donor produced `output_key` from absence; the receiver sees its
    // committed postimage. Historical CAS is authenticated provenance, not a
    // receiver-side read guard to replay against that postimage.
    const output_before = publication.ArtifactSource{ .key = output_key, .content_digest = null, .input_position = .{ .raft = .{ .term = 1, .index = 6 } }, .source_index = 0 };
    const effects = [_]provenance.Effect{
        .{ .family = .base_vector, .key = output_key, .source_index = 0, .value_digest = output_digest, .value_bytes = output_value.len },
        .{ .family = .base_vector, .key = tombstone_key, .source_index = 0, .value_digest = null, .value_bytes = 0 },
    };
    var proof: provenance.Proof = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = catalog_digest, .producer_kind = .index, .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "output", .publication_digest = @splat(4), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = (&guard)[0..1], .mutation_preconditions = (&output_before)[0..1], .effects = &effects };
    proof.input_digest = proof.inputCommand().inputDigest();
    const encoded = try provenance.encodeAlloc(alloc, proof);
    defer alloc.free(encoded);
    const value = try encodeValueAlloc(alloc, &.{1}, encoded);
    defer alloc.free(value);
    var decoded = try decodeValue(alloc, proof.namespace, proof.publication_digest, value);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(output_before, decoded.proof.proof.mutation_preconditions[0]);
    const donor_range: @import("../byte_range.zig").ByteRange = .{ .start = "doc", .end = "dop" };
    const source_pin: publication.Digest = @splat(9);
    const witness_key = witnessKey(proof.namespace, source_pin, proof.publication_digest);
    try receiver.values.put(&witness_key, &decoded.record_digest);
    var mapped = (try prepareReceiverCandidate(alloc, &receiver, receiver_namespace, donor_range, source_pin, &plan, decoded)) orelse return error.TestUnexpectedResult;
    defer mapped.deinit();
    try std.testing.expectEqualDeep(receiver_binding, mapped.receiver.binding);
    try std.testing.expectEqual(@as(?u64, 9), mapped.receiver_producer_generation);
    try std.testing.expectEqualDeep(source_pin, mapped.donor.source_pin);
    try std.testing.expectEqualDeep(donor_binding, mapped.donor.binding);
    try std.testing.expectEqualDeep(decoded.proof_checksum, mapped.donor.proof_checksum);
    try std.testing.expectEqualDeep(decoded.record_digest, mapped.donor.record_digest);
    try std.testing.expectEqualSlices(u8, decoded.bitmap, mapped.donor.selected_bitmap);
    try std.testing.expectEqualStrings("index", mapped.donor.producer_name);
    try std.testing.expectEqualDeep(row_position, mapped.sources[0].input_position.?);
    try std.testing.expectEqualDeep(guard_position, mapped.artifact_sources[0].input_position.?);
    try std.testing.expectEqual(@as(usize, 2), mapped.effects.len);
    try std.testing.expectEqualDeep(output_position, mapped.effects[0].input_position.?);
    try std.testing.expect(mapped.effects[1].input_position == null);
    try std.testing.expectEqualSlices(u8, &row_digest, &mapped.sources[0].content_digest);
    try std.testing.expect(try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    const wrong_witness: publication.Digest = @splat(0);
    try receiver.values.put(&witness_key, &wrong_witness);
    try std.testing.expect(!try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try std.testing.expect((try prepareReceiverCandidate(alloc, &receiver, receiver_namespace, donor_range, source_pin, &plan, decoded)) == null);
    try receiver.values.put(&witness_key, &decoded.record_digest);
    var adopted = try buildAdoptedProofAlloc(alloc, &mapped);
    defer adopted.deinit(alloc);
    try std.testing.expectEqualDeep(receiver_namespace, adopted.proof.namespace);
    try std.testing.expectEqualDeep(receiver_binding.digest, adopted.proof.catalog_digest);
    try std.testing.expectEqual(@as(u64, 9), adopted.proof.producer_generation);
    try std.testing.expectEqualDeep(row_position, adopted.proof.sources[0].input_position.?);
    try std.testing.expectEqualDeep(guard_position, adopted.proof.artifact_sources[0].input_position.?);
    try std.testing.expectEqual(@as(usize, 0), adopted.proof.mutation_preconditions.len);
    try std.testing.expectEqualDeep(source_pin, adopted.proof.origin.?.source_pin);
    try std.testing.expectEqualDeep(proof.publication_digest, adopted.proof.origin.?.publication_digest);
    try std.testing.expect(!std.mem.eql(u8, &proof.publication_digest, &adopted.proof.publication_digest));
    var roundtrip = try provenance.decodeAlloc(alloc, adopted.encoded);
    defer roundtrip.deinit();
    try std.testing.expectEqualDeep(adopted.proof.publication_digest, roundtrip.proof.publication_digest);
    const ProofAllocationCheck = struct {
        fn run(a: std.mem.Allocator, candidate: *const ReceiverCandidate) !void {
            var built = try buildAdoptedProofAlloc(a, candidate);
            defer built.deinit(a);
            try std.testing.expectEqual(candidate.receiver.namespace, built.proof.namespace);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, ProofAllocationCheck.run, .{&mapped});
    mapped.donor.producer_kind = .graph;
    try std.testing.expectError(error.ArtifactAdoptionUnsupported, buildAdoptedProofAlloc(alloc, &mapped));
    mapped.donor.producer_kind = .index;
    const changed_position: publication.Position = .{ .raft = .{ .term = 9, .index = 13 } };
    const changed_position_bytes = try changed_position.encode();
    try receiver.values.put(&output_revision_key, &changed_position_bytes);
    try std.testing.expect(!try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try receiver.values.put(&output_revision_key, &output_position_bytes);
    try receiver.values.put(&guard_revision_key, &changed_position_bytes);
    try std.testing.expect(!try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try receiver.values.put(&guard_revision_key, &guard_position_bytes);
    try receiver.values.put(&row_revision_key, &changed_position_bytes);
    try std.testing.expect(!try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try receiver.values.put(&row_revision_key, &row_position_bytes);
    try std.testing.expect(try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try std.testing.expect(receiver.values.remove(&output_revision_key));
    try std.testing.expect((try prepareReceiverCandidate(alloc, &receiver, receiver_namespace, donor_range, source_pin, &plan, decoded)) == null);
    try receiver.values.put(&output_revision_key, &output_position_bytes);
    std.mem.writeInt(u64, authority_bytes[28..36], receiver_binding.epoch + 1, .little);
    std.crypto.hash.Blake3.hash(authority_bytes[0..68], authority_bytes[68..100], .{});
    try std.testing.expect(!try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try std.testing.expect((try prepareReceiverCandidate(alloc, &receiver, receiver_namespace, donor_range, source_pin, &plan, decoded)) == null);
    std.mem.writeInt(u64, authority_bytes[28..36], receiver_binding.epoch, .little);
    std.crypto.hash.Blake3.hash(authority_bytes[0..68], authority_bytes[68..100], .{});
    try std.testing.expect(try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    var changed_binding = receiver_binding;
    changed_binding.epoch += 1;
    const changed_ordered: inventory.Ordered = .{ .command = .{ .namespace = receiver_namespace, .previous = receiver_binding, .binding = changed_binding, .catalogs = catalogs }, .applied_index = ordered.applied_index + 1 };
    const changed_ordered_bytes = try std.json.Stringify.valueAlloc(alloc, changed_ordered, .{});
    defer alloc.free(changed_ordered_bytes);
    try receiver.values.put(inventory.ordered_key, changed_ordered_bytes);
    try std.testing.expect(!try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try std.testing.expect((try prepareReceiverCandidate(alloc, &receiver, receiver_namespace, donor_range, source_pin, &plan, decoded)) == null);
    try receiver.values.put(inventory.ordered_key, ordered_bytes);
    try std.testing.expect(try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try std.testing.expectError(error.SourceSnapshotCorrupt, prepareReceiverCandidate(alloc, &receiver, receiver_namespace, .{ .start = "e", .end = "f" }, source_pin, &plan, decoded));
    try std.testing.expectError(error.SourceSnapshotCorrupt, prepareReceiverCandidate(alloc, &receiver, receiver_namespace, donor_range, @splat(0), &plan, decoded));
    var wrong_donor = donor_binding;
    wrong_donor.digest = @splat(3);
    try std.testing.expectError(error.SourceSnapshotCorrupt, ReceiverProducerPlan.init(alloc, donor_catalog, wrong_donor, catalogs, receiver_binding));
    const AllocationCheck = struct {
        fn run(a: std.mem.Allocator, txn: *Fake, donor_namespace: publication.Namespace, receiver_ns: publication.Namespace, range: @import("../byte_range.zig").ByteRange, pin: publication.Digest, producer_plan: *const ReceiverProducerPlan, digest: publication.Digest, encoded_value: []const u8, expected_position: publication.Position) !void {
            const transfer = try a.dupe(u8, encoded_value);
            var transfer_live = true;
            defer if (transfer_live) a.free(transfer);
            var candidate = try decodeValue(a, donor_namespace, digest, transfer);
            var candidate_live = true;
            defer if (candidate_live) candidate.deinit();
            var receiver_inputs = (try prepareReceiverCandidate(a, txn, receiver_ns, range, pin, producer_plan, candidate)) orelse return error.TestUnexpectedResult;
            candidate.deinit();
            candidate_live = false;
            @memset(transfer, 0);
            a.free(transfer);
            transfer_live = false;
            defer receiver_inputs.deinit();
            try std.testing.expectEqualDeep(expected_position, receiver_inputs.sources[0].input_position.?);
            try std.testing.expectEqualDeep(pin, receiver_inputs.donor.source_pin);
            try std.testing.expectEqualStrings("index", receiver_inputs.donor.producer_name);
            try std.testing.expectEqualSlices(u8, &.{1}, receiver_inputs.donor.selected_bitmap);
            try std.testing.expect(try revalidateReceiverCandidate(a, txn, receiver_ns, receiver_inputs));
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{ &receiver, proof.namespace, receiver_namespace, donor_range, source_pin, &plan, proof.publication_digest, value, row_position });
    try receiver.values.put(output_key, "changed output");
    try std.testing.expect(!try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try std.testing.expect((try prepareReceiverCandidate(alloc, &receiver, receiver_namespace, donor_range, source_pin, &plan, decoded)) == null);
    try receiver.values.put(output_key, output_value);
    try receiver.values.put(tombstone_key, "resurrected");
    try std.testing.expect(!try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try std.testing.expect((try prepareReceiverCandidate(alloc, &receiver, receiver_namespace, donor_range, source_pin, &plan, decoded)) == null);
    try std.testing.expect(receiver.values.remove(tombstone_key));
    try receiver.values.put(guard_key, "changed");
    try std.testing.expect(!try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try std.testing.expect((try prepareReceiverCandidate(alloc, &receiver, receiver_namespace, donor_range, source_pin, &plan, decoded)) == null);
    try receiver.values.put(guard_key, guard_value);
    try receiver.values.put(row_key, "{\"v\":2}");
    try std.testing.expect(!try revalidateReceiverCandidate(alloc, &receiver, receiver_namespace, mapped));
    try std.testing.expect((try prepareReceiverCandidate(alloc, &receiver, receiver_namespace, donor_range, source_pin, &plan, decoded)) == null);
}

test "ordered artifact inventory source proof descriptor resumes a certified large body" {
    const alloc = std.testing.allocator;
    const document = try alloc.alloc(u8, 2 * 1024 * 1024);
    defer alloc.free(document);
    @memset(document, 'd');
    const effect_key = try @import("../internal_keys.zig").embeddingArtifactKeyForDocumentAlloc(alloc, document, "index");
    defer alloc.free(effect_key);
    const source = publication.Source{ .document_key = document, .content_digest = @splat(3), .timestamp = 1, .input_position = null };
    const effect = provenance.Effect{ .family = .base_vector, .key = effect_key, .source_index = 0, .value_digest = null, .value_bytes = 0 };
    var proof: provenance.Proof = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_kind = .index, .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "asset", .publication_digest = @splat(4), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
    proof.input_digest = proof.inputCommand().inputDigest();
    const raw = try provenance.encodeAlloc(alloc, proof);
    defer alloc.free(raw);
    const value = try encodeValueAlloc(alloc, &.{1}, raw);
    defer alloc.free(value);
    const batch = try @import("../backup_codec.zig").encodeKeyValueBatch(alloc, &.{.{ .key = &proof.publication_digest, .value = value }});
    defer alloc.free(batch);
    const Mock = struct {
        bytes: []const u8,
        fn readAt(self: *@This(), object: u32, offset: u64, out: []u8) !usize {
            if (object != 7 or offset > self.bytes.len) return 0;
            const count = @min(out.len, self.bytes.len - @as(usize, @intCast(offset)));
            if (count == 0) return 0;
            @memcpy(out[0..count], self.bytes[@intCast(offset)..][0..count]);
            return count;
        }
    };
    var reader: Mock = .{ .bytes = batch };
    const first = (try descriptor(&reader, batch.len, .{ .object = 7 })).?;
    try std.testing.expectEqualSlices(u8, &proof.publication_digest, &first.digest);
    try std.testing.expectEqual(@as(u32, @intCast(value.len)), first.value_len);
    var sample: [17]u8 = undefined;
    const crossing_offset: usize = 1024 * 1024 - 8;
    try first.read(&reader, crossing_offset, &sample);
    try std.testing.expectEqualSlices(u8, value[crossing_offset..][0..sample.len], &sample);
    try std.testing.expect((try descriptor(&reader, batch.len, first.next_position)) == null);
    try std.testing.expectError(error.SourceSnapshotCorrupt, first.read(&reader, value.len - 1, sample[0..2]));
    try std.testing.expectError(error.SourceSnapshotCorrupt, descriptor(&reader, batch.len - 1, .{ .object = 7 }));
    try std.testing.expectError(error.SourceSnapshotCorrupt, descriptor(&reader, batch.len, .{ .object = 7, .offset = 0, .remaining = 1 }));
}

test "ordered artifact inventory merge proof payload is isolated and chunk-resumable" {
    const alloc = std.testing.allocator;
    const pages = @import("merge_page_contract.zig");
    const types = @import("types.zig");
    const identity: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 };
    var namespace: publication.Namespace = undefined;
    @import("doc_identity.zig").encodeNamespace(&namespace, identity);
    const document = try alloc.alloc(u8, 2 * pages.chunk_bytes);
    defer alloc.free(document);
    @memset(document, 'd');
    const effect_key = try @import("../internal_keys.zig").embeddingArtifactKeyForDocumentAlloc(alloc, document, "index");
    defer alloc.free(effect_key);
    const source = publication.Source{ .document_key = document, .content_digest = @splat(3), .timestamp = 1, .input_position = null };
    const effect = provenance.Effect{ .family = .base_vector, .key = effect_key, .source_index = 0, .value_digest = null, .value_bytes = 0 };
    var proof: provenance.Proof = .{ .namespace = namespace, .authority_epoch = 1, .catalog_digest = @splat(2), .producer_kind = .index, .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "asset", .publication_digest = @splat(4), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
    proof.input_digest = proof.inputCommand().inputDigest();
    const raw = try provenance.encodeAlloc(alloc, proof);
    defer alloc.free(raw);
    const value = try encodeValueAlloc(alloc, &.{1}, raw);
    defer alloc.free(value);
    const import_key = mergeKey(namespace, @splat(1), proof.publication_digest);
    var request: types.BatchRequest = .{
        .merge_replication = .{ .transition_id = 7, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } },
        .merge_page = .{ .source = .{ .namespace = identity, .pin_digest = @splat(1), .applied_index = 1, .retention = .{ .epoch = 1, .after_sequence = 0 }, .provenance_required = true }, .sequence = 1, .phase = .artifacts, .next = &import_key, .exhausted = false, .digest = @splat(0), .next_snapshot_position = .{ .object = 7, .offset = 64, .remaining = 0 }, .provenance_effects = &.{.{ .key = &import_key, .value = value }} },
    };
    request.merge_page.?.digest = pages.commandDigest(request);
    try pages.validateRequest(request);
    const chunks = try pages.RowChunks(types.BatchRequest).init(request);
    const first = try chunks.requestAt(0);
    const second = try chunks.requestAt(pages.chunk_bytes);
    const final = try chunks.requestAt(((value.len - 1) / pages.chunk_bytes) * pages.chunk_bytes);
    try std.testing.expectEqual(pages.ChunkPayload.provenance, first.merge_page.?.chunk.?.payload);
    try std.testing.expect(!first.merge_page.?.chunk.?.complete());
    try std.testing.expect(!second.merge_page.?.chunk.?.complete());
    try std.testing.expect(final.merge_page.?.chunk.?.complete());
    const encoded = try std.json.Stringify.valueAlloc(alloc, first.merge_page.?, .{});
    defer alloc.free(encoded);
    var decoded = try std.json.parseFromSlice(pages.Command, alloc, encoded, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(pages.ChunkPayload.provenance, decoded.value.chunk.?.payload);
    try std.testing.expectEqualSlices(u8, first.merge_page.?.chunk.?.data, decoded.value.chunk.?.data);
    const wrong_key = mergeKey(@splat(9), @splat(1), proof.publication_digest);
    request.merge_page.?.provenance_effects = &.{.{ .key = &wrong_key, .value = value }};
    request.merge_page.?.next = &wrong_key;
    request.merge_page.?.digest = pages.commandDigest(request);
    try std.testing.expectError(error.InvalidMergePage, pages.validateRequest(request));
}
