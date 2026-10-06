// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Accepted producer provenance contains the complete immutable read-set and
//! output digests, never a second copy of large artifact bodies. Native apply
//! constructs it from the validated command; transfer authenticates its bytes
//! together with source receipts and the immutable source snapshot/tail.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const inventory = @import("artifact_inventory.zig");
const binary = @import("relational_integrity_json.zig");
const internal_keys = @import("../internal_keys.zig");
pub const prefix = "\x00\x00__artifact_publication__:proof:";

pub const Effect = struct {
    family: publication.Family,
    key: []const u8,
    source_index: u32,
    value_digest: ?publication.Digest,
    value_bytes: u64,
};

/// Receiver-owned provenance of an adopted publication. Only the selected
/// output subset is installed locally. Encoding this origin does not certify
/// the donor cut: ordered adoption must verify the source proof first, then
/// install local receipts without copying donor receipt or authority keys.
pub const Origin = struct {
    source_pin: publication.Digest,
    namespace: publication.Namespace,
    binding: inventory.Binding,
    publication_digest: publication.Digest,
    input_digest: publication.Digest,
    proof_checksum: publication.Digest,
    selected_bitmap: []const u8,
};

fn hashNumber(hash: *std.crypto.hash.Blake3, value: u64) void {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, value, .little);
    hash.update(&encoded);
}

fn hashBytes(hash: *std.crypto.hash.Blake3, value: []const u8) void {
    hashNumber(hash, value.len);
    hash.update(value);
}

pub const Proof = struct {
    version: u8 = 3,
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
    /// Historical output compare-and-swap guards are not causal input guards.
    /// They remain part of the authenticated donor publication even though
    /// receiver adoption must compare the imported postimage instead.
    mutation_preconditions: []const publication.ArtifactSource = &.{},
    effects: []const Effect,
    origin: ?Origin = null,

    /// Adopted receipts use a receiver-local identity over the complete
    /// logical inputs, selected postimage and certified donor lineage. Normal
    /// producer publications retain their original Command.digest identity.
    pub fn adoptionDigest(self: Proof) publication.Digest {
        const origin = self.origin.?;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-artifact-adoption-v1\x00");
        hash.update(&self.namespace);
        hashNumber(&hash, self.authority_epoch);
        hash.update(&self.catalog_digest);
        hashNumber(&hash, @backingInt(self.producer_kind));
        hashBytes(&hash, self.producer_name);
        hashNumber(&hash, self.producer_generation);
        hashBytes(&hash, self.producer_artifact_name);
        hashBytes(&hash, self.producer_scope_key);
        hash.update(&self.input_digest);
        hashNumber(&hash, self.effects.len);
        for (self.effects) |effect| {
            hashNumber(&hash, @backingInt(effect.family));
            hashBytes(&hash, effect.key);
            hashNumber(&hash, effect.source_index);
            hash.update(&.{@intFromBool(effect.value_digest != null)});
            if (effect.value_digest) |digest| hash.update(&digest);
            hashNumber(&hash, effect.value_bytes);
        }
        hash.update(&origin.source_pin);
        hash.update(&origin.namespace);
        hashNumber(&hash, origin.binding.epoch);
        hash.update(&origin.binding.digest);
        hash.update(&origin.binding.semantic_digest);
        hashNumber(&hash, origin.binding.effect_protocol);
        hash.update(&origin.publication_digest);
        hash.update(&origin.input_digest);
        hash.update(&origin.proof_checksum);
        hashBytes(&hash, origin.selected_bitmap);
        var result: publication.Digest = undefined;
        hash.final(&result);
        return result;
    }

    pub fn jsonStringify(self: Proof, stream: anytype) @TypeOf(stream.*).Error!void {
        try binary.write(self, stream);
    }

    pub fn inputCommand(self: Proof) publication.Command {
        return .{ .namespace = self.namespace, .authority_epoch = self.authority_epoch, .catalog_digest = self.catalog_digest, .producer_kind = self.producer_kind, .producer_name = self.producer_name, .producer_generation = self.producer_generation, .producer_artifact_name = self.producer_artifact_name, .producer_scope_key = self.producer_scope_key, .sources = self.sources, .artifact_sources = self.artifact_sources, .mutation_preconditions = self.mutation_preconditions, .mutations = &.{}, .publication_digest = self.publication_digest };
    }

    pub fn validate(self: Proof) !void {
        if (self.version != 3 or self.authority_epoch == 0 or (self.producer_generation == 0 and self.producer_kind != .resolver) or
            self.producer_name.len == 0 or self.producer_artifact_name.len == 0 or
            self.sources.len == 0 or self.sources.len > publication.max_source_documents or
            self.artifact_sources.len > publication.max_source_documents or
            self.mutation_preconditions.len > publication.max_source_documents - self.artifact_sources.len or self.effects.len == 0 or
            self.effects.len > publication.max_mutations or !std.mem.eql(u8, &self.input_digest, &self.inputCommand().inputDigest())) return error.ArtifactCatalogCorrupt;
        for (self.effects) |effect| if (effect.source_index >= self.sources.len or
            (effect.value_digest == null and effect.value_bytes != 0)) return error.ArtifactCatalogCorrupt;
        if (self.origin) |origin| {
            if (!origin.binding.valid() or origin.binding.effect_protocol != 15 or
                std.mem.allEqual(u8, &origin.source_pin, 0) or std.mem.allEqual(u8, &origin.binding.digest, 0) or
                std.mem.allEqual(u8, &origin.proof_checksum, 0) or self.mutation_preconditions.len != 0 or
                origin.selected_bitmap.len != (self.sources.len + 7) / 8) return error.ArtifactCatalogCorrupt;
            var outputs = std.StaticBitSet(publication.max_source_documents).empty;
            for (self.effects) |effect| {
                if (origin.selected_bitmap[effect.source_index / 8] & (@as(u8, 1) << @intCast(effect.source_index % 8)) == 0)
                    return error.ArtifactCatalogCorrupt;
                outputs.set(effect.source_index);
            }
            for (origin.selected_bitmap, 0..) |bits, byte_index| for (0..8) |bit_index| {
                if (bits & (@as(u8, 1) << @intCast(bit_index)) == 0) continue;
                const ordinal = byte_index * 8 + bit_index;
                if (ordinal >= self.sources.len or !outputs.isSet(ordinal)) return error.ArtifactCatalogCorrupt;
            };
            if (!std.mem.eql(u8, &self.publication_digest, &self.adoptionDigest())) return error.ArtifactCatalogCorrupt;
        }
    }

    /// A transferred APF3 body is only candidate evidence. Before importing
    /// it, recheck the canonical owner/key shape that local publication
    /// admission checked, without loading any large output value. Receiver
    /// input revisions and output bytes are validated again during adoption.
    pub fn validatePortableShape(self: Proof, alloc: std.mem.Allocator) !void {
        try self.validate();
        if (std.mem.allEqual(u8, &self.namespace, 0) or std.mem.allEqual(u8, &self.catalog_digest, 0))
            return error.ArtifactCatalogCorrupt;
        publication.validateScopeGuard(alloc, self.producer_scope_key, self.artifact_sources) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.ArtifactCatalogCorrupt,
        };
        const identity = publication.namespaceFromBytes(self.namespace);
        for (self.sources, 0..) |source, index| {
            if (source.document_key.len == 0 or (index != 0 and
                std.mem.order(u8, self.sources[index - 1].document_key, source.document_key) != .lt))
                return error.ArtifactCatalogCorrupt;
            if (source.exists) {
                if (source.timestamp == 0) return error.ArtifactCatalogCorrupt;
            } else if (source.timestamp != 0 or !std.mem.allEqual(u8, &source.content_digest, 0) or source.input_position == null)
                return error.ArtifactCatalogCorrupt;
            if (source.input_position) |position| position.requireNamespace(identity) catch return error.ArtifactCatalogCorrupt;
        }
        for ([_][]const publication.ArtifactSource{ self.artifact_sources, self.mutation_preconditions }) |guards| {
            for (guards, 0..) |source, index| {
                if (source.source_index >= self.sources.len or !publication.guardedArtifactKey(source.key) or
                    (index != 0 and std.mem.order(u8, guards[index - 1].key, source.key) != .lt))
                    return error.ArtifactCatalogCorrupt;
                if (source.input_position) |position| position.requireNamespace(identity) catch return error.ArtifactCatalogCorrupt;
                const owner = @import("artifact_publication_owner.zig").documentAlloc(alloc, source.key) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => return error.ArtifactCatalogCorrupt,
                };
                defer alloc.free(owner);
                if (!std.mem.eql(u8, owner, self.sources[source.source_index].document_key)) return error.ArtifactCatalogCorrupt;
            }
        }
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        defer seen.deinit(alloc);
        for (self.effects) |effect| {
            if (!publication.validFamilyKey(effect.family, effect.key) or
                (try seen.getOrPut(alloc, effect.key)).found_existing) return error.ArtifactCatalogCorrupt;
            const owner = @import("artifact_publication_owner.zig").documentAlloc(alloc, effect.key) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.ArtifactCatalogCorrupt,
            };
            defer alloc.free(owner);
            if (!std.mem.eql(u8, owner, self.sources[effect.source_index].document_key)) return error.ArtifactCatalogCorrupt;
        }
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
    /// `decodeAlloc` owns all slices; `decodeBorrowed` retains a borrowed raw
    /// buffer which its caller must keep alive until deinit.
    proof: Proof,
    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

// Publication validation caps all variable key bytes at 64 MiB. A compact
// proof adds fixed per-source/effect fields, so one legal proof fits inside an
// AFB2 block without expanding binary keys into decimal JSON arrays. This is
// a PR-only storage format; there is no deployed APF1 compatibility contract.
pub const max_encoded_bytes = @import("artifact_publication_transport_codec.zig").max_encoded_bytes;
const proof_magic = "APF3";

fn proofAdd(size: *usize, amount: usize) !void {
    size.* = std.math.add(usize, size.*, amount) catch return error.TransactionTooLarge;
    if (size.* > max_encoded_bytes) return error.TransactionTooLarge;
}

fn proofBlobSize(size: *usize, value: []const u8) !void {
    if (value.len > std.math.maxInt(u32)) return error.TransactionTooLarge;
    try proofAdd(size, 4 + value.len);
}

fn proofPositionSize(size: *usize, value: ?publication.Position) !void {
    try proofAdd(size, 1 + @as(usize, if (value == null) 0 else publication.Position.encoded_len));
}

fn proofEncodedLength(proof: Proof) !usize {
    var size: usize = 4 + 1 + 1 + 24 + 8 + 32 + 8 + 32 + 32 + 4 * 4 + 1 + 32;
    try proofBlobSize(&size, proof.producer_name);
    try proofBlobSize(&size, proof.producer_artifact_name);
    try proofBlobSize(&size, proof.producer_scope_key);
    if (proof.origin) |origin| {
        try proofAdd(&size, 32 + 24 + 8 + 32 + 32 + 4 + 32 + 32 + 32);
        try proofBlobSize(&size, origin.selected_bitmap);
    }
    for (proof.sources) |source| {
        try proofBlobSize(&size, source.document_key);
        try proofAdd(&size, 1 + 32 + 8);
        try proofPositionSize(&size, source.input_position);
    }
    for ([_][]const publication.ArtifactSource{ proof.artifact_sources, proof.mutation_preconditions }) |guards| for (guards) |source| {
        try proofBlobSize(&size, source.key);
        try proofAdd(&size, 1 + @as(usize, if (source.content_digest == null) 0 else 32) + 4);
        try proofPositionSize(&size, source.input_position);
    };
    for (proof.effects) |effect| {
        try proofBlobSize(&size, effect.key);
        try proofAdd(&size, 1 + 4 + 1 + @as(usize, if (effect.value_digest == null) 0 else 32) + 8);
    }
    return size;
}

const ProofWriter = struct {
    bytes: []u8,
    pos: usize = 0,
    fn write(self: *@This(), value: []const u8) void {
        @memcpy(self.bytes[self.pos..][0..value.len], value);
        self.pos += value.len;
    }
    fn byte(self: *@This(), value: u8) void {
        self.bytes[self.pos] = value;
        self.pos += 1;
    }
    fn writeU32(self: *@This(), value: u32) void {
        std.mem.writeInt(u32, self.bytes[self.pos..][0..4], value, .little);
        self.pos += 4;
    }
    fn writeU64(self: *@This(), value: u64) void {
        std.mem.writeInt(u64, self.bytes[self.pos..][0..8], value, .little);
        self.pos += 8;
    }
    fn blob(self: *@This(), value: []const u8) void {
        self.writeU32(@intCast(value.len));
        self.write(value);
    }
    fn position(self: *@This(), value: ?publication.Position) !void {
        self.byte(@intFromBool(value != null));
        if (value) |position_value| self.write(&try position_value.encode());
    }
};

pub fn encodeAlloc(alloc: std.mem.Allocator, proof: Proof) ![]u8 {
    try proof.validate();
    const result = try alloc.alloc(u8, try proofEncodedLength(proof));
    errdefer alloc.free(result);
    var writer: ProofWriter = .{ .bytes = result };
    writer.write(proof_magic);
    writer.byte(proof.version);
    writer.byte(@backingInt(proof.producer_kind));
    writer.write(&proof.namespace);
    writer.writeU64(proof.authority_epoch);
    writer.write(&proof.catalog_digest);
    writer.writeU64(proof.producer_generation);
    writer.write(&proof.publication_digest);
    writer.write(&proof.input_digest);
    writer.blob(proof.producer_name);
    writer.blob(proof.producer_artifact_name);
    writer.blob(proof.producer_scope_key);
    writer.writeU32(@intCast(proof.sources.len));
    writer.writeU32(@intCast(proof.artifact_sources.len));
    writer.writeU32(@intCast(proof.mutation_preconditions.len));
    writer.writeU32(@intCast(proof.effects.len));
    writer.byte(@intFromBool(proof.origin != null));
    if (proof.origin) |origin| {
        writer.write(&origin.source_pin);
        writer.write(&origin.namespace);
        writer.writeU64(origin.binding.epoch);
        writer.write(&origin.binding.digest);
        writer.write(&origin.binding.semantic_digest);
        writer.writeU32(origin.binding.effect_protocol);
        writer.write(&origin.publication_digest);
        writer.write(&origin.input_digest);
        writer.write(&origin.proof_checksum);
        writer.blob(origin.selected_bitmap);
    }
    for (proof.sources) |source| {
        writer.blob(source.document_key);
        writer.byte(@intFromBool(source.exists));
        writer.write(&source.content_digest);
        writer.writeU64(source.timestamp);
        try writer.position(source.input_position);
    }
    for ([_][]const publication.ArtifactSource{ proof.artifact_sources, proof.mutation_preconditions }) |guards| for (guards) |source| {
        writer.blob(source.key);
        writer.byte(@intFromBool(source.content_digest != null));
        if (source.content_digest) |digest| writer.write(&digest);
        try writer.position(source.input_position);
        writer.writeU32(source.source_index);
    };
    for (proof.effects) |effect| {
        writer.byte(@backingInt(effect.family));
        writer.blob(effect.key);
        writer.writeU32(effect.source_index);
        writer.byte(@intFromBool(effect.value_digest != null));
        if (effect.value_digest) |digest| writer.write(&digest);
        writer.writeU64(effect.value_bytes);
    }
    if (writer.pos != result.len - 32) return error.ArtifactCatalogCorrupt;
    std.crypto.hash.sha2.Sha256.hash(result[0..writer.pos], result[writer.pos..][0..32], .{});
    return result;
}

const ProofCursor = struct {
    bytes: []const u8,
    pos: usize = 0,
    fn take(self: *@This(), length: usize) ![]const u8 {
        if (length > self.bytes.len -| self.pos) return error.ArtifactCatalogCorrupt;
        const result = self.bytes[self.pos..][0..length];
        self.pos += length;
        return result;
    }
    fn byte(self: *@This()) !u8 {
        return (try self.take(1))[0];
    }
    fn readU32(self: *@This()) !u32 {
        return std.mem.readInt(u32, (try self.take(4))[0..4], .little);
    }
    fn readU64(self: *@This()) !u64 {
        return std.mem.readInt(u64, (try self.take(8))[0..8], .little);
    }
    fn blob(self: *@This()) ![]const u8 {
        return self.take(try self.readU32());
    }
    fn flag(self: *@This()) !bool {
        return switch (try self.byte()) {
            0 => false,
            1 => true,
            else => error.ArtifactCatalogCorrupt,
        };
    }
    fn position(self: *@This()) !?publication.Position {
        if (!try self.flag()) return null;
        return publication.Position.decode(try self.take(publication.Position.encoded_len)) catch return error.ArtifactCatalogCorrupt;
    }
};

pub fn decodeAlloc(alloc: std.mem.Allocator, raw: []const u8) !Owned {
    return decode(alloc, raw, true);
}

/// Borrow the immutable proof bytes for the decoded value's lifetime. Source
/// export/import holds a pinned snapshot or certified AFB object throughout
/// validation, avoiding a second proof-sized allocation for a large block.
pub fn decodeBorrowed(alloc: std.mem.Allocator, raw: []const u8) !Owned {
    return decode(alloc, raw, false);
}

fn decode(alloc: std.mem.Allocator, raw: []const u8, copy_bytes: bool) !Owned {
    if (raw.len < 4 + 1 + 1 + 24 + 8 + 32 + 8 + 32 + 32 + 4 * 7 + 1 + 32 or raw.len > max_encoded_bytes)
        return error.ArtifactCatalogCorrupt;
    var digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned_raw = if (copy_bytes) try arena.allocator().dupe(u8, raw) else raw;
    var cursor: ProofCursor = .{ .bytes = owned_raw[0 .. owned_raw.len - 32] };
    if (!std.mem.eql(u8, try cursor.take(4), proof_magic)) return error.ArtifactCatalogCorrupt;
    const version = try cursor.byte();
    const producer_kind = std.enums.fromInt(@FieldType(publication.Command, "producer_kind"), try cursor.byte()) orelse return error.ArtifactCatalogCorrupt;
    const namespace: publication.Namespace = (try cursor.take(24))[0..24].*;
    const epoch = try cursor.readU64();
    const catalog_digest: publication.Digest = (try cursor.take(32))[0..32].*;
    const generation = try cursor.readU64();
    const publication_digest: publication.Digest = (try cursor.take(32))[0..32].*;
    const input_digest: publication.Digest = (try cursor.take(32))[0..32].*;
    const producer_name = try cursor.blob();
    const producer_artifact_name = try cursor.blob();
    const producer_scope_key = try cursor.blob();
    const source_count = try cursor.readU32();
    const artifact_count = try cursor.readU32();
    const precondition_count = try cursor.readU32();
    const effect_count = try cursor.readU32();
    const origin: ?Origin = if (try cursor.flag()) blk: {
        const source_pin: publication.Digest = (try cursor.take(32))[0..32].*;
        const donor_namespace: publication.Namespace = (try cursor.take(24))[0..24].*;
        const donor_epoch = try cursor.readU64();
        const donor_catalog: publication.Digest = (try cursor.take(32))[0..32].*;
        const donor_semantic: publication.Digest = (try cursor.take(32))[0..32].*;
        const protocol = try cursor.readU32();
        if (protocol > std.math.maxInt(u16)) return error.ArtifactCatalogCorrupt;
        const donor_publication: publication.Digest = (try cursor.take(32))[0..32].*;
        const donor_input: publication.Digest = (try cursor.take(32))[0..32].*;
        const donor_checksum: publication.Digest = (try cursor.take(32))[0..32].*;
        break :blk .{
            .source_pin = source_pin,
            .namespace = donor_namespace,
            .binding = .{ .epoch = donor_epoch, .digest = donor_catalog, .semantic_digest = donor_semantic, .effect_protocol = @intCast(protocol) },
            .publication_digest = donor_publication,
            .input_digest = donor_input,
            .proof_checksum = donor_checksum,
            .selected_bitmap = try cursor.blob(),
        };
    } else null;
    if (source_count == 0 or source_count > publication.max_source_documents or artifact_count > publication.max_source_documents or
        precondition_count > publication.max_source_documents - artifact_count or effect_count == 0 or effect_count > publication.max_mutations)
        return error.ArtifactCatalogCorrupt;
    const sources = try arena.allocator().alloc(publication.Source, source_count);
    const artifact_sources = try arena.allocator().alloc(publication.ArtifactSource, artifact_count);
    const mutation_preconditions = try arena.allocator().alloc(publication.ArtifactSource, precondition_count);
    const effects = try arena.allocator().alloc(Effect, effect_count);
    for (sources) |*source| {
        source.* = .{ .document_key = try cursor.blob(), .exists = try cursor.flag(), .content_digest = (try cursor.take(32))[0..32].*, .timestamp = try cursor.readU64(), .input_position = try cursor.position() };
    }
    for ([_][]publication.ArtifactSource{ artifact_sources, mutation_preconditions }) |guards| for (guards) |*source| {
        const key_bytes = try cursor.blob();
        const content_digest: ?publication.Digest = if (try cursor.flag()) (try cursor.take(32))[0..32].* else null;
        source.* = .{ .key = key_bytes, .content_digest = content_digest, .input_position = try cursor.position(), .source_index = try cursor.readU32() };
    };
    for (effects) |*effect| {
        const family = std.enums.fromInt(publication.Family, try cursor.byte()) orelse return error.ArtifactCatalogCorrupt;
        const key_bytes = try cursor.blob();
        const source_index = try cursor.readU32();
        const value_digest: ?publication.Digest = if (try cursor.flag()) (try cursor.take(32))[0..32].* else null;
        effect.* = .{ .family = family, .key = key_bytes, .source_index = source_index, .value_digest = value_digest, .value_bytes = try cursor.readU64() };
    }
    if (cursor.pos != cursor.bytes.len) return error.ArtifactCatalogCorrupt;
    const proof: Proof = .{ .version = version, .namespace = namespace, .authority_epoch = epoch, .catalog_digest = catalog_digest, .producer_kind = producer_kind, .producer_name = producer_name, .producer_generation = generation, .producer_artifact_name = producer_artifact_name, .producer_scope_key = producer_scope_key, .publication_digest = publication_digest, .input_digest = input_digest, .sources = sources, .artifact_sources = artifact_sources, .mutation_preconditions = mutation_preconditions, .effects = effects, .origin = origin };
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
    const mutation_preconditions = try owned.dupe(publication.ArtifactSource, command.mutation_preconditions);
    for (mutation_preconditions) |*source| source.key = try owned.dupe(u8, source.key);
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
        .mutation_preconditions = mutation_preconditions,
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
/// Range-seekable source-side evidence. The value is the accepted proof digest;
/// the source reference and its count remain the authority. Cold retirement
/// recovers the document from the bounded proof rather than duplicating it in
/// another hot-path record.
pub const document_reference_prefix = "\x00\x00__artifact_publication__:proof_doc:";

const DocumentReference = struct {
    source_index: usize,
    key: []const u8,
};

pub const PreparedDocumentReferences = struct {
    arena: std.heap.ArenaAllocator,
    entries: []const DocumentReference,

    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn documentReferenceKeyAlloc(alloc: std.mem.Allocator, namespace: publication.Namespace, epoch: u64, document: []const u8, receipt_hash: []const u8) ![]u8 {
    if (receipt_hash.len != 32) return error.ArtifactCatalogCorrupt;
    const prefix_len = document_reference_prefix.len + 24 + 8;
    const encoded_len = internal_keys.encodedComponentLen(document);
    const result = try alloc.alloc(u8, prefix_len + encoded_len + 32);
    @memcpy(result[0..document_reference_prefix.len], document_reference_prefix);
    @memcpy(result[document_reference_prefix.len..][0..24], &namespace);
    std.mem.writeInt(u64, result[document_reference_prefix.len + 24 ..][0..8], epoch, .big);
    _ = internal_keys.encodeComponent(result[prefix_len..][0..encoded_len], document);
    @memcpy(result[prefix_len + encoded_len ..], receipt_hash);
    return result;
}

/// Prepare variable-length document keys before serialized apply. Every output
/// source, including an absence publication, gets one range-seekable entry.
pub fn prepareDocumentReferences(alloc: std.mem.Allocator, command: publication.Command) !PreparedDocumentReferences {
    const owners = try command.outputSources();
    return prepareSelectedDocumentReferences(alloc, command, owners);
}

fn selectedOwners(proof: Proof) !std.StaticBitSet(publication.max_source_documents) {
    if (proof.origin == null) return error.ArtifactCatalogCorrupt;
    var owners = std.StaticBitSet(publication.max_source_documents).empty;
    for (proof.effects) |effect| {
        if (effect.source_index >= proof.sources.len) return error.ArtifactCatalogCorrupt;
        owners.set(effect.source_index);
    }
    return owners;
}

pub fn prepareAdoptedDocumentReferences(alloc: std.mem.Allocator, proof: Proof) !PreparedDocumentReferences {
    try proof.validate();
    return prepareSelectedDocumentReferences(alloc, proof.inputCommand(), try selectedOwners(proof));
}

fn prepareSelectedDocumentReferences(alloc: std.mem.Allocator, command: publication.Command, owners: std.StaticBitSet(publication.max_source_documents)) !PreparedDocumentReferences {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var entries: std.ArrayList(DocumentReference) = .empty;
    for (command.sources, 0..) |source, source_index| {
        if (!owners.isSet(source_index)) continue;
        const reference = referenceKey(command, source);
        try entries.append(owned, .{
            .source_index = source_index,
            .key = try documentReferenceKeyAlloc(owned, command.namespace, command.authority_epoch, source.document_key, reference[reference.len - 32 ..]),
        });
    }
    return .{ .arena = arena, .entries = entries.items };
}

/// A source-copy candidate, not an adoption certificate. The proof digest is
/// backed by a current source reference in the same pinned snapshot. The
/// exporter must still validate the proof's document association, causal
/// inputs and output scope before committing portable evidence.
pub const DocumentProofReference = struct {
    document_key: []const u8,
    index_key: []const u8,
    receipt_hash: publication.Digest,
    proof_digest: publication.Digest,
};

pub const DocumentProofPage = struct {
    arena: std.heap.ArenaAllocator,
    entries: []const DocumentProofReference,
    /// Exclusive physical resume key. Null only after the scoped range ends.
    next_cursor: ?[]const u8,

    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Enumerate one bounded, binary-key-safe source-evidence page from a pinned
/// snapshot. The caller must retain that snapshot across pages; these local
/// references are never portable authority. A single long key may exceed the
/// byte target, but the entry count and maximum key size remain bounded.
pub fn readDocumentProofPage(
    alloc: std.mem.Allocator,
    txn: anytype,
    expected: publication.Authority,
    range: @import("../byte_range.zig").ByteRange,
    after: ?[]const u8,
    max_entries: usize,
    max_bytes: usize,
) !DocumentProofPage {
    if (max_entries == 0 or max_entries > 128 or max_bytes == 0) return error.InvalidBatchRequest;
    const current = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(current, expected)) return error.ArtifactCatalogDrift;
    var scoped_storage: [document_reference_prefix.len + 24 + 8]u8 = undefined;
    const scoped = scoped_storage[0..];
    @memcpy(scoped[0..document_reference_prefix.len], document_reference_prefix);
    @memcpy(scoped[document_reference_prefix.len..][0..24], &expected.namespace);
    std.mem.writeInt(u64, scoped[document_reference_prefix.len + 24 ..][0..8], expected.epoch, .big);
    // Construct bounds with a temporary arena so both encoded components and
    // combined keys are released together, including allocation failures.
    var bound_arena = std.heap.ArenaAllocator.init(alloc);
    defer bound_arena.deinit();
    const bounds = bound_arena.allocator();
    const lower: []const u8 = if (range.start.len == 0) scoped else blk: {
        const component = try bounds.alloc(u8, internal_keys.encodedComponentLen(range.start));
        _ = internal_keys.encodeComponent(component, range.start);
        break :blk try std.mem.concat(bounds, u8, &.{ scoped, component });
    };
    const upper: ?[]const u8 = if (range.end.len == 0) null else blk: {
        const component = try bounds.alloc(u8, internal_keys.encodedComponentLen(range.end));
        _ = internal_keys.encodeComponent(component, range.end);
        break :blk try std.mem.concat(bounds, u8, &.{ scoped, component });
    };
    if (after) |cursor| {
        if (!std.mem.startsWith(u8, cursor, scoped) or std.mem.order(u8, cursor, lower) == .lt or
            (upper != null and std.mem.order(u8, cursor, upper.?) != .lt)) return error.InvalidBatchRequest;
    }
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var entries: std.ArrayList(DocumentProofReference) = .empty;
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    var item = try cursor.seekAtOrAfter(after orelse lower);
    if (after) |resume_key| {
        if (item != null and std.mem.eql(u8, item.?.key, resume_key)) item = try cursor.next();
    }
    var bytes: usize = 0;
    while (item) |entry| {
        if (!std.mem.startsWith(u8, entry.key, scoped) or (upper != null and std.mem.order(u8, entry.key, upper.?) != .lt)) break;
        if (entries.items.len != 0 and (entries.items.len >= max_entries or bytes >= max_bytes)) break;
        if (entry.key.len > scoped.len + 2 * @import("artifact_producer_obligations.zig").max_cursor_bytes + 32 or entry.value.len != 32)
            return error.ArtifactCatalogCorrupt;
        const end = internal_keys.findComponentTerminator(entry.key, scoped.len) orelse return error.ArtifactCatalogCorrupt;
        if (end + 2 + 32 != entry.key.len) return error.ArtifactCatalogCorrupt;
        const document = internal_keys.decodeBodyAlloc(owned, entry.key[scoped.len..end]) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.ArtifactCatalogCorrupt,
        };
        if (!range.contains(document)) return error.ArtifactCatalogCorrupt;
        const receipt_hash: publication.Digest = entry.key[entry.key.len - 32 ..][0..32].*;
        const proof_digest: publication.Digest = entry.value[0..32].*;
        var reference: [reference_prefix.len + 24 + 8 + 32]u8 = undefined;
        @memcpy(reference[0..reference_prefix.len], reference_prefix);
        @memcpy(reference[reference_prefix.len..][0..24], &expected.namespace);
        std.mem.writeInt(u64, reference[reference_prefix.len + 24 ..][0..8], expected.epoch, .big);
        @memcpy(reference[reference.len - 32 ..], &receipt_hash);
        const actual = txn.get(&reference) catch |err| switch (err) {
            error.NotFound => return error.ArtifactCatalogCorrupt,
            else => return err,
        };
        if (!std.mem.eql(u8, actual, &proof_digest)) return error.ArtifactCatalogCorrupt;
        const proof_bytes = txn.get(&key(expected.namespace, proof_digest)) catch |err| switch (err) {
            error.NotFound => return error.ArtifactCatalogCorrupt,
            else => return err,
        };
        if (proof_bytes.len < 40 or proof_bytes.len > max_encoded_bytes) return error.ArtifactCatalogCorrupt;
        try entries.append(owned, .{ .document_key = document, .index_key = try owned.dupe(u8, entry.key), .receipt_hash = receipt_hash, .proof_digest = proof_digest });
        bytes +|= entry.key.len + entry.value.len;
        item = try cursor.next();
    }
    const next_cursor = if (item == null or !std.mem.startsWith(u8, item.?.key, scoped) or
        (upper != null and std.mem.order(u8, item.?.key, upper.?) != .lt)) null else try owned.dupe(u8, entries.items[entries.items.len - 1].index_key);
    return .{ .arena = arena, .entries = entries.items, .next_cursor = next_cursor };
}

/// Select exactly those output sources whose receipt still names this proof
/// at the immutable source cut. The bitmap is deliberately portable evidence,
/// not a receiver-local reference: adoption must recheck logical inputs and
/// reconstruct positions under the receiver's own authority.
pub fn selectedSourceBitmapAlloc(
    alloc: std.mem.Allocator,
    txn: anytype,
    active: publication.Authority,
    range: @import("../byte_range.zig").ByteRange,
    proof: Proof,
) !?[]u8 {
    try proof.validate();
    if (!std.mem.eql(u8, &proof.namespace, &active.namespace) or proof.authority_epoch != active.epoch or
        !std.mem.eql(u8, &proof.catalog_digest, &active.catalog_digest)) return null;
    var owners = std.StaticBitSet(publication.max_source_documents).empty;
    for (proof.effects) |effect| owners.set(effect.source_index);
    var bitmap: [publication.max_source_documents / 8]u8 = @splat(0);
    var found = false;
    for (proof.sources, 0..) |source, source_index| {
        if (!owners.isSet(source_index) or !range.contains(source.document_key)) continue;
        const reference = referenceKey(proof.inputCommand(), source);
        const raw = txn.get(&reference) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        if (raw.len != 32) return error.ArtifactCatalogCorrupt;
        if (!std.mem.eql(u8, raw, &proof.publication_digest)) continue;
        const index = try documentReferenceKeyAlloc(alloc, active.namespace, active.epoch, source.document_key, reference[reference.len - 32 ..]);
        defer alloc.free(index);
        const indexed = txn.get(index) catch |err| switch (err) {
            error.NotFound => return error.ArtifactCatalogCorrupt,
            else => return err,
        };
        if (!std.mem.eql(u8, indexed, &proof.publication_digest)) return error.ArtifactCatalogCorrupt;
        bitmap[source_index / 8] |= @as(u8, 1) << @intCast(source_index % 8);
        found = true;
    }
    if (!found) return null;
    return try alloc.dupe(u8, bitmap[0 .. (proof.sources.len + 7) / 8]);
}

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

/// Capture the fixed receiver-local reference after the caller has validated
/// the current artifact proof and its inputs in the same pinned snapshot.
/// Final apply checks this CAS value without decoding the proof again.
pub fn captureCurrentArtifactCertificate(txn: anytype, authority: publication.Authority, artifact: []const u8, expected_digest: publication.Digest) !ArtifactCertificate {
    const reference = artifactReferenceKey(authority, artifact);
    const raw = txn.get(&reference) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
    if (raw.len != 32 + publication.Position.encoded_len) return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, raw[0..32], &expected_digest)) return error.EnrichmentSourceChanged;
    return .{ .reference = reference, .value = raw[0 .. 32 + publication.Position.encoded_len].* };
}

pub fn certifyInheritedArtifact(alloc: std.mem.Allocator, txn: anytype, artifact: []const u8, expected_value: ?[]const u8, consumer: publication.Command) !ArtifactCertificate {
    var accepted = (try readCurrentForArtifact(alloc, txn, artifact, expected_value)) orelse return error.ArtifactCoverageBaselinePending;
    defer accepted.deinit();
    try accepted.proof.requireInheritedBy(consumer);
    return captureCurrentArtifactCertificate(txn, .{ .namespace = consumer.namespace, .epoch = consumer.authority_epoch, .catalog_digest = consumer.catalog_digest }, artifact, accepted.proof.publication_digest);
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

    pub fn deinit(self: *Projection) void {
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
pub fn retireReference(alloc: std.mem.Allocator, txn: anytype, reference: []const u8, current: publication.Authority) !void {
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
    if (!artifact) {
        var owned = try decodeAlloc(alloc, try txn.get(&key(namespace, owned_digest)));
        defer owned.deinit();
        const source = for (owned.proof.sources) |candidate| {
            if (std.mem.eql(u8, reference, &referenceKey(owned.proof.inputCommand(), candidate))) break candidate;
        } else return error.ArtifactCatalogCorrupt;
        const document_key = try documentReferenceKeyAlloc(alloc, namespace, epoch, source.document_key, reference[reference.len - 32 ..]);
        defer alloc.free(document_key);
        const indexed_digest = txn.get(document_key) catch |err| switch (err) {
            error.NotFound => return error.ArtifactCatalogCorrupt,
            else => return err,
        };
        if (!std.mem.eql(u8, indexed_digest, &owned_digest)) return error.ArtifactCatalogCorrupt;
        try txn.delete(document_key);
    }
    try changeReferences(txn, namespace, owned_digest, false);
    try txn.delete(reference);
}

/// Retire at most one bounded page from each old-epoch reference family.
/// Current references remain addressable to pinned readers, while each old
/// document entry, reference count, and proof body retire atomically. There is
/// no history-sized cursor: removing the first page exposes the next one.
pub fn collectObsoletePage(alloc: std.mem.Allocator, store_handle: anytype) !bool {
    const sources_done = try collectObsoleteReferencePage(alloc, store_handle, reference_prefix);
    const artifacts_done = try collectObsoleteReferencePage(alloc, store_handle, artifact_prefix);
    return sources_done and artifacts_done;
}

fn collectObsoleteReferencePage(alloc: std.mem.Allocator, store_handle: anytype, comptime family: []const u8) !bool {
    const Retirement = struct { reference: []const u8, expected: []const u8, document_index: ?[]const u8, digest: publication.Digest };
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var retired: std.ArrayList(Retirement) = .empty;
    var at_end = false;
    const authority = blk: {
        var read = try store_handle.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        const current = (try publication.authority(&read)) orelse return true;
        const scoped = try scratch.alloc(u8, family.len + current.namespace.len);
        @memcpy(scoped[0..family.len], family);
        @memcpy(scoped[family.len..], &current.namespace);
        var cursor = try read.openPhysicalCursorAdapter();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(scoped);
        var bytes: usize = 0;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
        while (entry) |item| {
            if (!std.mem.startsWith(u8, item.key, scoped)) {
                entry = null;
                break;
            }
            if (item.key.len != scoped.len + 8 + 32) return error.ArtifactCatalogCorrupt;
            const epoch = std.mem.readInt(u64, item.key[scoped.len..][0..8], .big);
            if (epoch == 0 or epoch > current.epoch) return error.ArtifactCatalogCorrupt;
            if (epoch == current.epoch) {
                entry = null;
                break;
            }
            if (retired.items.len != 0 and (retired.items.len >= 128 or bytes >= 64 * 1024 or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
            const expected_len = 32 + @as(usize, if (std.mem.eql(u8, family, artifact_prefix)) publication.Position.encoded_len else 0);
            if (item.value.len != expected_len) return error.ArtifactCatalogCorrupt;
            const owned_key = try scratch.dupe(u8, item.key);
            const owned_value = try scratch.dupe(u8, item.value);
            const digest: publication.Digest = owned_value[0..32].*;
            var proof_bytes: usize = 0;
            const document_index: ?[]const u8 = if (std.mem.eql(u8, family, reference_prefix)) source: {
                const encoded = try read.get(&key(current.namespace, digest));
                proof_bytes = encoded.len;
                var proof = try decodeAlloc(scratch, encoded);
                defer proof.deinit();
                if (proof.proof.authority_epoch != epoch or !std.mem.eql(u8, &proof.proof.namespace, &current.namespace) or !std.mem.eql(u8, &proof.proof.publication_digest, &digest)) return error.ArtifactCatalogCorrupt;
                const source = for (proof.proof.sources) |candidate| {
                    if (std.mem.eql(u8, owned_key, &referenceKey(proof.proof.inputCommand(), candidate))) break candidate;
                } else return error.ArtifactCatalogCorrupt;
                const indexed = try documentReferenceKeyAlloc(scratch, current.namespace, epoch, source.document_key, owned_key[owned_key.len - 32 ..]);
                const indexed_digest = read.get(indexed) catch |err| switch (err) {
                    error.NotFound => return error.ArtifactCatalogCorrupt,
                    else => return err,
                };
                if (!std.mem.eql(u8, indexed_digest, &digest)) return error.ArtifactCatalogCorrupt;
                break :source indexed;
            } else null;
            try retired.append(scratch, .{ .reference = owned_key, .expected = owned_value, .document_index = document_index, .digest = digest });
            bytes += owned_key.len + owned_value.len + proof_bytes + (if (document_index) |value| value.len else 0);
            entry = try cursor.next();
        }
        at_end = entry == null;
        break :blk current;
    };
    if (retired.items.len == 0) return at_end;
    var writer = try store_handle.beginWriteTxn();
    errdefer writer.abort();
    const current = (try publication.authority(&writer)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(current, authority)) return error.ArtifactCatalogDrift;
    for (retired.items) |item| {
        const observed = writer.get(item.reference) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        if (!std.mem.eql(u8, observed, item.expected)) return error.ArtifactCatalogDrift;
        if (item.document_index) |indexed| {
            const value = writer.get(indexed) catch |err| switch (err) {
                error.NotFound => return error.ArtifactCatalogDrift,
                else => return err,
            };
            if (!std.mem.eql(u8, value, &item.digest)) return error.ArtifactCatalogDrift;
            try writer.delete(indexed);
        }
        try changeReferences(&writer, current.namespace, item.digest, false);
        try writer.delete(item.reference);
    }
    try writer.commit();
    return at_end;
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
    return stageWithDocumentReferences(txn, command, encoded_proof, position, null, null);
}

pub fn stageIndexed(txn: anytype, command: publication.Command, encoded_proof: []const u8, position: publication.Position, prepared: *const PreparedDocumentReferences) !void {
    return stageWithDocumentReferences(txn, command, encoded_proof, position, prepared, null);
}

/// Stage adopted proof references and selected receipts together. This is a
/// transaction participant, not a certificate: the caller must verify source
/// evidence, receiver inputs and postimages before invoking it and abort the
/// writer transaction on any error.
pub fn stageAdoptedIndexed(txn: anytype, proof: Proof, encoded_proof: []const u8, positions: []const ?publication.Position, adoption_position: publication.Position, prepared: *const PreparedDocumentReferences, sequence: u64) !void {
    try proof.validate();
    if (proof.origin == null or proof.producer_kind != .index) return error.ArtifactAdoptionUnsupported;
    if (positions.len != proof.effects.len) return error.ArtifactCatalogCorrupt;
    try adoption_position.requireNamespace(publication.namespaceFromBytes(proof.namespace));
    const authority: publication.Authority = .{ .namespace = proof.namespace, .epoch = proof.authority_epoch, .catalog_digest = proof.catalog_digest };
    const encoded_adoption_position = try adoption_position.encode();
    // Verify all effects before writing any tombstone revision. An imported
    // present output must already carry its own exact local revision; only an
    // absent postimage may acquire a new witness at adoption apply.
    for (proof.effects, positions) |effect, maybe_position| {
        if (effect.family == .graph or !publication.validFamilyKey(effect.family, effect.key)) return error.ArtifactAdoptionUnsupported;
        if (maybe_position) |position| try position.requireNamespace(publication.namespaceFromBytes(proof.namespace));
        const current_revision = try publication.artifactRevision(txn, proof.namespace, effect.key);
        if (maybe_position) |position| {
            if (!std.meta.eql(current_revision, @as(?publication.Position, position))) return error.EnrichmentSourceChanged;
        } else if (current_revision) |position| {
            if (!std.meta.eql(position, adoption_position)) return error.EnrichmentSourceChanged;
            const previous = txn.get(&artifactReferenceKey(authority, effect.key)) catch |err| switch (err) {
                error.NotFound => return error.EnrichmentSourceChanged,
                else => return err,
            };
            if (previous.len != 32 + publication.Position.encoded_len or
                !std.mem.eql(u8, previous[0..32], &proof.publication_digest) or
                !std.mem.eql(u8, previous[32..], &encoded_adoption_position)) return error.EnrichmentSourceChanged;
        }
        if (maybe_position == null) {
            if (effect.value_digest != null) return error.EnrichmentSourceChanged;
            _ = txn.get(effect.key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
            return error.EnrichmentSourceChanged;
        }
    }
    for (proof.effects, positions) |effect, maybe_position| {
        if (maybe_position == null and (try publication.artifactRevision(txn, proof.namespace, effect.key)) == null)
            try publication.stageArtifactTombstoneRevision(txn, proof.namespace, effect.key, adoption_position);
    }
    const owners = try selectedOwners(proof);
    const command = proof.inputCommand();
    try publication.stageSelectedReceipts(txn, command, proof.input_digest, owners, sequence);
    try stageWithDocumentReferences(txn, command, encoded_proof, adoption_position, prepared, .{ .proof = proof, .positions = positions });
}

const AdoptedEffects = struct { proof: Proof, positions: []const ?publication.Position };

fn stageWithDocumentReferences(txn: anytype, command: publication.Command, encoded_proof: []const u8, position: publication.Position, prepared: ?*const PreparedDocumentReferences, adopted: ?AdoptedEffects) !void {
    const owners = if (adopted) |selected| try selectedOwners(selected.proof) else try command.outputSources();
    const proof_key = key(command.namespace, command.publication_digest);
    const old = txn.get(&proof_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (old) |value| {
        if (!std.mem.eql(u8, value, encoded_proof)) return error.ArtifactCatalogCorrupt;
    } else try txn.put(&proof_key, encoded_proof);
    var next_document: usize = 0;
    for (command.sources, 0..) |source, source_index| {
        if (!owners.isSet(source_index)) continue;
        const reference = referenceKey(command, source);
        const previous = txn.get(&reference) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        var unchanged = false;
        if (previous) |digest| {
            if (digest.len != 32) return error.ArtifactCatalogCorrupt;
            unchanged = std.mem.eql(u8, digest, &command.publication_digest);
            // Copy before mutating the transaction: borrowed values may be
            // invalidated by the first subsequent write.
            if (!unchanged) {
                const old_digest: publication.Digest = digest[0..32].*;
                try changeReferences(txn, command.namespace, old_digest, false);
            }
        }
        if (!unchanged) {
            try changeReferences(txn, command.namespace, command.publication_digest, true);
            try txn.put(&reference, &command.publication_digest);
        }
        if (prepared) |index| {
            if (next_document == index.entries.len) return error.ArtifactCatalogCorrupt;
            const entry = index.entries[next_document];
            next_document += 1;
            if (entry.source_index != source_index) return error.ArtifactCatalogCorrupt;
            try txn.put(entry.key, &command.publication_digest);
        }
    }
    if (prepared) |index| if (next_document != index.entries.len) return error.ArtifactCatalogCorrupt;
    const authority: publication.Authority = .{ .namespace = command.namespace, .epoch = command.authority_epoch, .catalog_digest = command.catalog_digest };
    const encoded_position = try position.encode();
    var value: [32 + publication.Position.encoded_len]u8 = undefined;
    @memcpy(value[0..32], &command.publication_digest);
    const effect_count = if (adopted) |selected| selected.proof.effects.len else command.mutations.len;
    for (0..effect_count) |effect_index| {
        const effect_key = if (adopted) |selected| selected.proof.effects[effect_index].key else command.mutations[effect_index].key;
        const effect_position = if (adopted) |selected| try (selected.positions[effect_index] orelse position).encode() else encoded_position;
        @memcpy(value[32..], &effect_position);
        const reference = artifactReferenceKey(authority, effect_key);
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

fn testEncodedProofAlloc(alloc: std.mem.Allocator, command: publication.Command) ![]u8 {
    const effects = try alloc.alloc(Effect, command.mutations.len);
    defer alloc.free(effects);
    for (command.mutations, effects) |mutation, *effect| {
        var digest: ?publication.Digest = null;
        if (mutation.value) |value| {
            var hashed: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &hashed, .{});
            digest = hashed;
        }
        effect.* = .{ .family = mutation.family, .key = mutation.key, .source_index = mutation.source_index, .value_digest = digest, .value_bytes = if (mutation.value) |value| value.len else 0 };
    }
    return encodeAlloc(alloc, .{ .namespace = command.namespace, .authority_epoch = command.authority_epoch, .catalog_digest = command.catalog_digest, .producer_kind = command.producer_kind, .producer_name = command.producer_name, .producer_generation = command.producer_generation, .producer_artifact_name = command.producer_artifact_name, .producer_scope_key = command.producer_scope_key, .publication_digest = command.publication_digest, .input_digest = command.inputDigest(), .sources = command.sources, .artifact_sources = command.artifact_sources, .mutation_preconditions = command.mutation_preconditions, .effects = effects });
}

test "ordered artifact inventory adopted proof stages only selected receipts and references" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        values: std.StringHashMap([]u8),
        pub fn get(self: *@This(), name: []const u8) anyerror![]const u8 {
            return self.values.get(name) orelse error.NotFound;
        }
        pub fn put(self: *@This(), name: []const u8, value: []const u8) !void {
            const copied = try std.testing.allocator.dupe(u8, value);
            errdefer std.testing.allocator.free(copied);
            const entry = try self.values.getOrPut(name);
            if (entry.found_existing) std.testing.allocator.free(entry.value_ptr.*) else entry.key_ptr.* = try std.testing.allocator.dupe(u8, name);
            entry.value_ptr.* = copied;
        }
        pub fn delete(self: *@This(), name: []const u8) !void {
            const entry = self.values.fetchRemove(name) orelse return error.NotFound;
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        pub fn deinit(self: *@This()) void {
            var iter = self.values.iterator();
            while (iter.next()) |entry| {
                std.testing.allocator.free(entry.key_ptr.*);
                std.testing.allocator.free(entry.value_ptr.*);
            }
            self.values.deinit();
        }
    };
    var txn: Fake = .{ .values = std.StringHashMap([]u8).init(alloc) };
    defer txn.deinit();
    const sources = [_]publication.Source{
        .{ .document_key = "a", .content_digest = @splat(1), .timestamp = 1, .input_position = null },
        .{ .document_key = "b", .content_digest = @splat(2), .timestamp = 1, .input_position = null },
    };
    const output = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "a", "index");
    defer alloc.free(output);
    const removed = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "a", "removed");
    defer alloc.free(removed);
    const effects = [_]Effect{
        .{ .family = .base_vector, .key = output, .source_index = 0, .value_digest = null, .value_bytes = 0 },
        .{ .family = .base_vector, .key = removed, .source_index = 0, .value_digest = null, .value_bytes = 0 },
    };
    var proof: Proof = .{
        .namespace = @splat(1),
        .authority_epoch = 4,
        .catalog_digest = @splat(2),
        .producer_kind = .index,
        .producer_name = "index",
        .producer_generation = 9,
        .producer_artifact_name = "index",
        .publication_digest = @splat(0),
        .input_digest = undefined,
        .sources = &sources,
        .artifact_sources = &.{},
        .effects = &effects,
        .origin = .{ .source_pin = @splat(3), .namespace = @splat(4), .binding = .{ .epoch = 2, .digest = @splat(5), .semantic_digest = @splat(6), .effect_protocol = 15 }, .publication_digest = @splat(7), .input_digest = @splat(8), .proof_checksum = @splat(9), .selected_bitmap = &.{1} },
    };
    proof.input_digest = proof.inputCommand().inputDigest();
    proof.publication_digest = proof.adoptionDigest();
    try proof.validatePortableShape(alloc);
    const encoded = try encodeAlloc(alloc, proof);
    defer alloc.free(encoded);
    var prepared = try prepareAdoptedDocumentReferences(alloc, proof);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 1), prepared.entries.len);
    // The imported output was written before the later adoption command.
    const position: publication.Position = .{ .raft = .{ .term = 1, .index = 9 } };
    const adoption_position: publication.Position = .{ .raft = .{ .term = 1, .index = 11 } };
    const output_revision = try position.encode();
    try std.testing.expectError(error.EnrichmentSourceChanged, stageAdoptedIndexed(&txn, proof, encoded, &.{ position, null }, adoption_position, &prepared, 11));
    try txn.put(&publication.artifactRevisionKey(proof.namespace, output), &output_revision);
    try txn.put(removed, "unexpected");
    try std.testing.expectError(error.EnrichmentSourceChanged, stageAdoptedIndexed(&txn, proof, encoded, &.{ position, null }, adoption_position, &prepared, 11));
    try txn.delete(removed);
    try stageAdoptedIndexed(&txn, proof, encoded, &.{ position, null }, adoption_position, &prepared, 11);
    try stageAdoptedIndexed(&txn, proof, encoded, &.{ position, null }, adoption_position, &prepared, 11);
    try std.testing.expectEqualDeep(proof.publication_digest, (try publication.readReceipt(&txn, proof.inputCommand(), sources[0])).?.publication_digest);
    try std.testing.expect((try publication.readReceipt(&txn, proof.inputCommand(), sources[1])) == null);
    try std.testing.expectEqualSlices(u8, encoded, try txn.get(&key(proof.namespace, proof.publication_digest)));
    try std.testing.expectEqualSlices(u8, &proof.publication_digest, try txn.get(prepared.entries[0].key));
    try std.testing.expectEqual(@as(u64, 3), std.mem.readInt(u64, (try txn.get(&countKey(proof.namespace, proof.publication_digest)))[0..8], .little));
    const artifact_reference = artifactReferenceKey(.{ .namespace = proof.namespace, .epoch = proof.authority_epoch, .catalog_digest = proof.catalog_digest }, output);
    const stored_reference = try txn.get(&artifact_reference);
    try std.testing.expectEqualSlices(u8, &output_revision, stored_reference[32..]);
    const removed_reference = artifactReferenceKey(.{ .namespace = proof.namespace, .epoch = proof.authority_epoch, .catalog_digest = proof.catalog_digest }, removed);
    try std.testing.expectEqualSlices(u8, &try adoption_position.encode(), (try txn.get(&removed_reference))[32..]);
    try std.testing.expectEqualDeep(adoption_position, (try publication.artifactRevision(&txn, proof.namespace, removed)).?);
    const original_removed_reference = try alloc.dupe(u8, try txn.get(&removed_reference));
    defer alloc.free(original_removed_reference);
    var forged_reference = try alloc.dupe(u8, original_removed_reference);
    defer alloc.free(forged_reference);
    forged_reference[0] ^= 1;
    try txn.put(&removed_reference, forged_reference);
    try std.testing.expectError(error.EnrichmentSourceChanged, stageAdoptedIndexed(&txn, proof, encoded, &.{ position, null }, adoption_position, &prepared, 11));
    try txn.put(&removed_reference, original_removed_reference);
    try std.testing.expectError(error.NotFound, txn.get(output));
    const later: publication.Position = .{ .raft = .{ .term = 1, .index = 12 } };
    try txn.put(&publication.artifactRevisionKey(proof.namespace, output), &try later.encode());
    try std.testing.expectError(error.EnrichmentSourceChanged, stageAdoptedIndexed(&txn, proof, encoded, &.{ position, null }, adoption_position, &prepared, 11));
}

test "ordered artifact inventory document proof index tracks absence replacement and retirement" {
    const alloc = std.testing.allocator;
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
        pub fn deinit(self: *@This()) void {
            var it = self.values.iterator();
            while (it.next()) |entry| {
                std.testing.allocator.free(entry.key_ptr.*);
                std.testing.allocator.free(entry.value_ptr.*);
            }
            self.values.deinit();
        }
    };
    var txn: Fake = .{ .values = std.StringHashMap([]u8).init(alloc) };
    defer txn.deinit();
    const sources = [_]publication.Source{
        .{ .document_key = "a\x00b", .content_digest = @splat(1), .timestamp = 1, .input_position = null },
        .{ .document_key = "neighbor", .content_digest = @splat(2), .timestamp = 1, .input_position = null },
    };
    const effects = [_]publication.Mutation{.{ .family = .document_artifact, .key = "absent-output", .value = null, .source_index = 0 }};
    var command: publication.Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "asset", .sources = &sources, .mutations = &effects, .publication_digest = @splat(3) };
    const AllocationCheck = struct {
        fn run(a: std.mem.Allocator, selected: publication.Command) !void {
            var prepared = try prepareDocumentReferences(a, selected);
            defer prepared.deinit();
            try std.testing.expectEqual(@as(usize, 1), prepared.entries.len);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{command});
    var index = try prepareDocumentReferences(alloc, command);
    defer index.deinit();
    try std.testing.expectEqual(@as(usize, 1), index.entries.len);
    try std.testing.expectEqual(@as(usize, 0), index.entries[0].source_index);
    const reference = referenceKey(command, sources[0]);
    const position: publication.Position = .{ .raft = .{ .term = 1, .index = 1 } };
    const first_proof = try testEncodedProofAlloc(alloc, command);
    defer alloc.free(first_proof);
    try stageIndexed(&txn, command, first_proof, position, &index);
    try stageIndexed(&txn, command, first_proof, position, &index);
    try std.testing.expectEqualSlices(u8, &command.publication_digest, try txn.get(index.entries[0].key));
    command.publication_digest = @splat(4);
    const second_proof = try testEncodedProofAlloc(alloc, command);
    defer alloc.free(second_proof);
    try stageIndexed(&txn, command, second_proof, position, &index);
    try std.testing.expectEqualSlices(u8, &command.publication_digest, try txn.get(index.entries[0].key));
    const current: publication.Authority = .{ .namespace = command.namespace, .epoch = 2, .catalog_digest = command.catalog_digest };
    try retireReference(alloc, &txn, &reference, current);
    try std.testing.expectError(error.NotFound, txn.get(index.entries[0].key));
}

test "ordered artifact inventory obsolete proof GC bounds pages and preserves current and pinned evidence" {
    const alloc = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/proof-gc", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    var namespace: publication.Namespace = undefined;
    @import("doc_identity.zig").encodeNamespace(&namespace, db.core.identity_namespace);
    const active: publication.Authority = .{ .namespace = namespace, .epoch = 2, .catalog_digest = @splat(3) };
    const position: publication.Position = .{ .raft = .{ .term = 1, .index = 1 } };
    var first_reference: [reference_prefix.len + 24 + 8 + 32]u8 = undefined;
    var first_proof: publication.Digest = undefined;
    var first_document_index: ?[]u8 = null;
    defer if (first_document_index) |value| alloc.free(value);
    var current_reference: [reference_prefix.len + 24 + 8 + 32]u8 = undefined;
    {
        var writer = try db.core.store.beginWriteTxn();
        errdefer writer.abort();
        try publication.stageAuthority(&writer, .{ .mode = .activate, .namespace = namespace, .authority_epoch = active.epoch, .catalog_digest = active.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        for (0..129) |ordinal| {
            const document = try std.fmt.allocPrint(alloc, "doc{d:0>4}", .{ordinal});
            defer alloc.free(document);
            const output = try internal_keys.artifactNamedPrefixAlloc(alloc, document, "asset", "proof");
            defer alloc.free(output);
            const source = publication.Source{ .document_key = document, .content_digest = @splat(1), .timestamp = 1, .input_position = null };
            const effect = publication.Mutation{ .family = .document_artifact, .key = output, .value = null, .source_index = 0 };
            var digest: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(document, &digest, .{});
            const command: publication.Command = .{ .namespace = namespace, .authority_epoch = 1, .catalog_digest = active.catalog_digest, .producer_name = "proof", .producer_generation = 1, .producer_artifact_name = "proof", .sources = (&source)[0..1], .mutations = (&effect)[0..1], .publication_digest = digest };
            const encoded = try testEncodedProofAlloc(alloc, command);
            defer alloc.free(encoded);
            var indexed = try prepareDocumentReferences(alloc, command);
            defer indexed.deinit();
            try stageIndexed(&writer, command, encoded, position, &indexed);
            if (ordinal == 0) {
                first_reference = referenceKey(command, source);
                first_proof = digest;
                first_document_index = try alloc.dupe(u8, indexed.entries[0].key);
            }
        }
        const source = publication.Source{ .document_key = "current", .content_digest = @splat(1), .timestamp = 1, .input_position = null };
        const output = try internal_keys.artifactNamedPrefixAlloc(alloc, "current", "asset", "proof");
        defer alloc.free(output);
        const effect = publication.Mutation{ .family = .document_artifact, .key = output, .value = null, .source_index = 0 };
        const command: publication.Command = .{ .namespace = namespace, .authority_epoch = 2, .catalog_digest = active.catalog_digest, .producer_name = "proof", .producer_generation = 2, .producer_artifact_name = "proof", .sources = (&source)[0..1], .mutations = (&effect)[0..1], .publication_digest = @splat(9) };
        const encoded = try testEncodedProofAlloc(alloc, command);
        defer alloc.free(encoded);
        var indexed = try prepareDocumentReferences(alloc, command);
        defer indexed.deinit();
        try stageIndexed(&writer, command, encoded, position, &indexed);
        current_reference = referenceKey(command, source);
        try writer.commit();
    }
    var pinned = try db.core.store.beginReadTxn();
    defer pinned.abort();
    try std.testing.expect((try publication.authority(&pinned)) != null);
    _ = try pinned.get(&first_reference);
    var pages: usize = 0;
    while (pages < 512) {
        pages += 1;
        if (try collectObsoletePage(alloc, db.core.store)) break;
    }
    try std.testing.expect(pages > 1 and pages < 512);
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(&first_reference));
    try std.testing.expectError(error.NotFound, read.get(first_document_index.?));
    try std.testing.expectError(error.NotFound, read.get(&key(namespace, first_proof)));
    _ = try read.get(&current_reference);
}

test "ordered artifact inventory document proof pages seek binary ranges and reject drift" {
    const alloc = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/proof-page", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 4 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    var namespace: publication.Namespace = undefined;
    @import("doc_identity.zig").encodeNamespace(&namespace, db.core.identity_namespace);
    const active: publication.Authority = .{ .namespace = namespace, .epoch = 1, .catalog_digest = @splat(3) };
    const documents = [_][]const u8{ "a", "a\x00b", "b", "c", "d" };
    var tamper_key: ?[]u8 = null;
    defer if (tamper_key) |value| alloc.free(value);
    {
        var writer = try db.core.store.beginWriteTxn();
        errdefer writer.abort();
        try publication.stageAuthority(&writer, .{ .mode = .activate, .namespace = namespace, .authority_epoch = active.epoch, .catalog_digest = active.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        for (documents) |document| {
            const output = try internal_keys.artifactNamedPrefixAlloc(alloc, document, "asset", "proof");
            defer alloc.free(output);
            const source = publication.Source{ .document_key = document, .content_digest = @splat(1), .timestamp = 1, .input_position = null };
            const effect = publication.Mutation{ .family = .document_artifact, .key = output, .value = null, .source_index = 0 };
            var digest: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(document, &digest, .{});
            const command: publication.Command = .{ .namespace = namespace, .authority_epoch = 1, .catalog_digest = active.catalog_digest, .producer_name = "proof", .producer_generation = 1, .producer_artifact_name = "proof", .sources = (&source)[0..1], .mutations = (&effect)[0..1], .publication_digest = digest };
            const encoded = try testEncodedProofAlloc(alloc, command);
            defer alloc.free(encoded);
            var indexed = try prepareDocumentReferences(alloc, command);
            defer indexed.deinit();
            try stageIndexed(&writer, command, encoded, .{ .raft = .{ .term = 1, .index = 1 } }, &indexed);
            if (std.mem.eql(u8, document, "c")) tamper_key = try alloc.dupe(u8, indexed.entries[0].key);
        }
        try writer.commit();
    }
    var pinned = try db.core.store.beginReadTxn();
    defer pinned.abort();
    const range: @import("../byte_range.zig").ByteRange = .{ .start = "a\x00b", .end = "d" };
    var byte_limited = try readDocumentProofPage(alloc, &pinned, active, range, null, 2, 1);
    defer byte_limited.deinit();
    try std.testing.expectEqual(@as(usize, 1), byte_limited.entries.len);
    try std.testing.expect(byte_limited.next_cursor != null);
    var first = try readDocumentProofPage(alloc, &pinned, active, range, null, 2, 4096);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 2), first.entries.len);
    try std.testing.expectEqualStrings("a\x00b", first.entries[0].document_key);
    try std.testing.expectEqualStrings("b", first.entries[1].document_key);
    try std.testing.expect(first.next_cursor != null);
    var second = try readDocumentProofPage(alloc, &pinned, active, range, first.next_cursor, 2, 4096);
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.entries.len);
    try std.testing.expectEqualStrings("c", second.entries[0].document_key);
    try std.testing.expect(second.next_cursor == null);
    try std.testing.expectError(error.InvalidBatchRequest, readDocumentProofPage(alloc, &pinned, active, range, "outside", 2, 4096));
    const AllocationCheck = struct {
        fn run(a: std.mem.Allocator, read: *@import("../docstore.zig").DocStore.Txn, authority_value: publication.Authority, selected: @import("../byte_range.zig").ByteRange) !void {
            var page = try readDocumentProofPage(a, read, authority_value, selected, null, 2, 4096);
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 2), page.entries.len);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, AllocationCheck.run, .{ &pinned, active, range });
    {
        var writer = try db.core.store.beginWriteTxn();
        errdefer writer.abort();
        try writer.put(tamper_key.?, &(@as([32]u8, @splat(9))));
        try writer.commit();
    }
    var changed = try db.core.store.beginReadTxn();
    defer changed.abort();
    try std.testing.expectError(error.ArtifactCatalogCorrupt, readDocumentProofPage(alloc, &changed, active, .{ .start = "c", .end = "d" }, null, 2, 4096));
    // The pinned cut still observes its original, self-consistent evidence.
    var unchanged = try readDocumentProofPage(alloc, &pinned, active, .{ .start = "c", .end = "d" }, null, 2, 4096);
    defer unchanged.deinit();
    try std.testing.expectEqual(@as(usize, 1), unchanged.entries.len);
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
        pub fn deinit(self: *@This()) void {
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
    const first_proof = try testEncodedProofAlloc(std.testing.allocator, command);
    defer std.testing.allocator.free(first_proof);
    var first_index = try prepareDocumentReferences(std.testing.allocator, command);
    defer first_index.deinit();
    try stageIndexed(&txn, command, first_proof, position, &first_index);
    try stageIndexed(&txn, command, first_proof, position, &first_index);
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
    command.publication_digest = @splat(4);
    const second_proof = try testEncodedProofAlloc(std.testing.allocator, command);
    defer std.testing.allocator.free(second_proof);
    var second_index = try prepareDocumentReferences(std.testing.allocator, command);
    defer second_index.deinit();
    try stageIndexed(&txn, command, second_proof, position, &second_index);
    try std.testing.expectError(error.EnrichmentSourceChanged, certificate.requireCurrent(&txn));
    try std.testing.expectError(error.NotFound, txn.get(&original_key));
    try std.testing.expectEqual(@as(u64, 4), std.mem.readInt(u64, (try txn.get(&countKey(command.namespace, command.publication_digest)))[0..8], .little));
    try std.testing.expectError(error.ArtifactCatalogCorrupt, stage(&txn, command, "different-proof", position));
    const reference_b = referenceKey(command, sources[1]);
    const authority: publication.Authority = .{ .namespace = command.namespace, .epoch = 1, .catalog_digest = command.catalog_digest };
    try std.testing.expectError(error.ArtifactCatalogScopeChanged, retireReference(std.testing.allocator, &txn, &reference_b, authority));
    var next_authority = authority;
    next_authority.epoch = 2;
    try retireReference(std.testing.allocator, &txn, &reference_b, next_authority);
    try std.testing.expectEqualSlices(u8, second_proof, try txn.get(&key(command.namespace, command.publication_digest)));
    try retireReference(std.testing.allocator, &txn, &referenceKey(command, sources[0]), next_authority);
    for (mutations) |effect| try retireReference(std.testing.allocator, &txn, &artifactReferenceKey(authority, effect.key), next_authority);
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

test "ordered artifact inventory compact proof keeps binary keys bounded and rejects forged fields" {
    const alloc = std.testing.allocator;
    const document = @as([(32 * 1024)]u8, @splat(0));
    const source = publication.Source{ .document_key = &document, .content_digest = @splat(1), .timestamp = 1, .input_position = null };
    const effect = Effect{ .family = .document_artifact, .key = "effect", .source_index = 0, .value_digest = null, .value_bytes = 0 };
    var proof: Proof = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_kind = .index, .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "asset", .publication_digest = @splat(3), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
    proof.input_digest = proof.inputCommand().inputDigest();
    const encoded = try encodeAlloc(alloc, proof);
    defer alloc.free(encoded);
    try std.testing.expect(encoded.len < document.len + 512);
    try std.testing.expect(encoded.len < @import("../backup_codec.zig").max_block_payload_bytes);
    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(proof, decoded.proof);
    const forged = try alloc.dupe(u8, encoded);
    defer alloc.free(forged);
    forged[5] = 255; // unknown producer kind, with a valid physical checksum
    std.crypto.hash.sha2.Sha256.hash(forged[0 .. forged.len - 32], forged[forged.len - 32 ..][0..32], .{});
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeAlloc(alloc, forged));
    const AllocationCheck = struct {
        fn run(a: std.mem.Allocator, logical: Proof) !void {
            const raw = try encodeAlloc(a, logical);
            defer a.free(raw);
            var value = try decodeAlloc(a, raw);
            defer value.deinit();
            try std.testing.expectEqualDeep(logical, value.proof);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{proof});
}

test "ordered artifact inventory compact proof preserves output CAS history" {
    const alloc = std.testing.allocator;
    const artifact = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(artifact);
    const source = publication.Source{ .document_key = "doc", .content_digest = @splat(1), .timestamp = 7, .input_position = .{ .raft = .{ .term = 2, .index = 8 } } };
    const before = publication.ArtifactSource{ .key = artifact, .content_digest = null, .input_position = .{ .raft = .{ .term = 2, .index = 9 } }, .source_index = 0 };
    const after = publication.Mutation{ .family = .base_vector, .key = artifact, .value = null, .source_index = 0 };
    var command: publication.Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "model", .producer_generation = 1, .producer_artifact_name = "model", .sources = (&source)[0..1], .mutation_preconditions = (&before)[0..1], .mutations = (&after)[0..1], .publication_digest = @splat(0) };
    command.publication_digest = command.digest();
    var prepared = try fromCommand(alloc, command);
    defer prepared.deinit();
    try std.testing.expectEqualDeep(before, prepared.proof.mutation_preconditions[0]);
    const raw = try encodeAlloc(alloc, prepared.proof);
    defer alloc.free(raw);
    var recovered = try decodeAlloc(alloc, raw);
    defer recovered.deinit();
    try std.testing.expectEqualDeep(before, recovered.proof.mutation_preconditions[0]);
    try recovered.proof.validatePortableShape(alloc);
    try std.testing.expectEqualDeep(command.inputDigest(), recovered.proof.inputCommand().inputDigest());
}

test "ordered artifact inventory portable scoped proof requires its causal unit guard" {
    const alloc = std.testing.allocator;
    const scope = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", 0);
    defer alloc.free(scope);
    const output = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(output);
    const source = publication.Source{ .document_key = "doc", .content_digest = @splat(1), .timestamp = 1, .input_position = null };
    const guard = publication.ArtifactSource{ .key = scope, .content_digest = @splat(2), .input_position = null, .source_index = 0 };
    const effect = Effect{ .family = .base_vector, .key = output, .source_index = 0, .value_digest = null, .value_bytes = 0 };
    var proof: Proof = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_kind = .index, .producer_name = "model", .producer_generation = 1, .producer_artifact_name = "model", .producer_scope_key = scope, .publication_digest = @splat(3), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
    proof.input_digest = proof.inputCommand().inputDigest();
    try std.testing.expectError(error.ArtifactCatalogCorrupt, proof.validatePortableShape(alloc));
    proof.artifact_sources = (&guard)[0..1];
    proof.input_digest = proof.inputCommand().inputDigest();
    try proof.validatePortableShape(alloc);
}

test "ordered artifact inventory adopted proof binds certified origin and selected postimage" {
    const alloc = std.testing.allocator;
    const output = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(output);
    const source = publication.Source{ .document_key = "doc", .content_digest = @splat(1), .timestamp = 7, .input_position = .{ .raft = .{ .term = 2, .index = 3 } } };
    const effect = Effect{ .family = .base_vector, .key = output, .source_index = 0, .value_digest = @splat(4), .value_bytes = 8 };
    var proof: Proof = .{
        .namespace = @splat(9),
        .authority_epoch = 3,
        .catalog_digest = @splat(10),
        .producer_kind = .index,
        .producer_name = "model",
        .producer_generation = 3,
        .producer_artifact_name = "model",
        .publication_digest = @splat(0),
        .input_digest = undefined,
        .sources = (&source)[0..1],
        .artifact_sources = &.{},
        .effects = (&effect)[0..1],
        .origin = .{
            .source_pin = @splat(7),
            .namespace = @splat(8),
            .binding = .{ .epoch = 1, .digest = @splat(2), .semantic_digest = @splat(3), .effect_protocol = 15 },
            .publication_digest = @splat(4),
            .input_digest = @splat(5),
            .proof_checksum = @splat(6),
            .selected_bitmap = &.{1},
        },
    };
    proof.input_digest = proof.inputCommand().inputDigest();
    proof.publication_digest = proof.adoptionDigest();
    try proof.validatePortableShape(alloc);
    const raw = try encodeAlloc(alloc, proof);
    defer alloc.free(raw);
    var decoded = try decodeAlloc(alloc, raw);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(proof, decoded.proof);
    var borrowed = try decodeBorrowed(alloc, raw);
    defer borrowed.deinit();
    try std.testing.expectEqualDeep(proof, borrowed.proof);
    var forged = proof;
    forged.origin.?.source_pin = @splat(11);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, encodeAlloc(alloc, forged));
    forged = proof;
    forged.origin.?.selected_bitmap = &.{2};
    try std.testing.expectError(error.ArtifactCatalogCorrupt, encodeAlloc(alloc, forged));
    const old_format = try alloc.dupe(u8, raw);
    defer alloc.free(old_format);
    @memcpy(old_format[0..4], "APF2");
    std.crypto.hash.sha2.Sha256.hash(old_format[0 .. old_format.len - 32], old_format[old_format.len - 32 ..][0..32], .{});
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeAlloc(alloc, old_format));
    const AllocationCheck = struct {
        fn run(a: std.mem.Allocator, logical: Proof) !void {
            const encoded = try encodeAlloc(a, logical);
            defer a.free(encoded);
            var value = try decodeBorrowed(a, encoded);
            defer value.deinit();
            try std.testing.expectEqualDeep(logical, value.proof);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{proof});
}
