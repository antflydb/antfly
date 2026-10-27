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

//! Versioned scoped producer inputs. Field selectors are resolved from active
//! catalog definitions; requests carry identities and observations, never a
//! caller-defined narrower field list. Legacy whole-source guards stay valid.
const std = @import("std");
const revision = @import("producer_input_revision.zig");
const requests = @import("enrichment/enrichment_types.zig");
const logical = @import("../document_commitment.zig");
const payload = @import("../document_payload.zig");
const identity = @import("doc_identity.zig");
const keys = @import("../internal_keys.zig");
const inventory = @import("artifact_inventory.zig");
const manager = @import("catalog/index_manager.zig");
const enrichments = @import("catalog/enrichment_catalog.zig");
const A = std.mem.Allocator;
/// Persisted together with the ordered catalog before accepting APF4. Older
/// readers must refuse this root, rather than opening it and misreading causal
/// evidence. The released reader remains25 until cohort activation qualifies.
pub const minimum_reader_format: u32 = 27;
pub fn admitProtocol(a: A, txn: anytype) !void {
    const schema = @import("../schema.zig");
    const raw = try payload.rawGet(txn, schema.schema_key);
    const decoded = try schema.deserializeSchema(a, raw);
    defer schema.freeSchema(a, decoded);
    if (decoded.decoded_format_version >= minimum_reader_format) return;
    const encoded = try schema.serializeSchemaWithMinimumFormat(a, decoded, minimum_reader_format);
    defer a.free(encoded);
    try txn.put(schema.schema_key, encoded);
}
pub fn requireProtocol(a: A, txn: anytype) !void {
    const schema = @import("../schema.zig");
    const decoded = try schema.deserializeSchema(a, try payload.rawGet(txn, schema.schema_key));
    defer schema.freeSchema(a, decoded);
    if (decoded.decoded_format_version < minimum_reader_format) return error.UnsupportedVersion;
}
pub const Digest = [32]u8;
pub const maximum_terms = revision.maximum_terms;
pub const Term = struct {
    producer_name: []const u8,
    artifact_name: []const u8,
    definition: Digest,
    reprocess_generation: u64,
    value_identity: ?Digest,
    input_revision: Digest,
};
pub const Guard = struct {
    source_index: u32,
    canonical_document: u64,
    created_generation: u64,
    require_whole_source: bool = false,
    observed_root: Digest,
    terms: []const Term,
};
pub fn forSource(guards: []const Guard, index: usize) ?*const Guard {
    var low: usize = 0;
    var high = guards.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (guards[mid].source_index < index) low = mid + 1 else high = mid;
    }
    return if (low < guards.len and guards[low].source_index == index) &guards[low] else null;
}
pub fn validate(guards: []const Guard, source_count: usize) !usize {
    if (guards.len > source_count) return error.InvalidBatchRequest;
    var size: usize = 0;
    var total: usize = 0;
    for (guards, 0..) |guard, index| {
        if (guard.source_index >= source_count or guard.canonical_document == 0 or guard.created_generation == 0 or
            guard.terms.len == 0 or guard.terms.len > maximum_terms or (index != 0 and guards[index - 1].source_index >= guard.source_index)) return error.InvalidBatchRequest;
        total = try std.math.add(usize, total, guard.terms.len);
        if (total > 1024) return error.InvalidBatchRequest;
        size = try std.math.add(usize, size, 4 + 8 + 8 + 1 + 32 + 4);
        for (guard.terms, 0..) |term, ordinal| {
            if (term.producer_name.len == 0 or term.artifact_name.len == 0 or term.producer_name.len > 1024 * 1024 or term.artifact_name.len > 1024 * 1024 or
                std.mem.allEqual(u8, &term.definition, 0) or std.mem.allEqual(u8, &term.input_revision, 0) or
                (ordinal != 0 and std.mem.order(u8, &guard.terms[ordinal - 1].definition, &term.definition) != .lt)) return error.InvalidBatchRequest;
            size = try std.math.add(usize, size, 4 + term.producer_name.len + 4 + term.artifact_name.len + 32 + 8 + 1 + @as(usize, if (term.value_identity == null) 0 else 32) + 32);
        }
    }
    return size;
}
pub fn hashSemantic(hash: anytype, guard: Guard) void {
    hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u32, guard.source_index)));
    hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, guard.canonical_document)));
    hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, guard.created_generation)));
    hash.update(&.{@intFromBool(guard.require_whole_source)});
    hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u32, @intCast(guard.terms.len))));
    for (guard.terms) |term| {
        for ([_][]const u8{ term.producer_name, term.artifact_name }) |name| {
            hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, name.len)));
            hash.update(name);
        }
        hash.update(&term.definition);
        hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, term.reprocess_generation)));
        hash.update(&.{@intFromBool(term.value_identity != null)});
        if (term.value_identity) |value| hash.update(&value);
        hash.update(&term.input_revision);
    }
}
pub fn write(writer: anytype, guards: []const Guard) !void {
    writer.writeU32(@intCast(guards.len));
    for (guards) |guard| {
        writer.writeU32(guard.source_index);
        writer.writeU64(guard.canonical_document);
        writer.writeU64(guard.created_generation);
        writer.byte(@intFromBool(guard.require_whole_source));
        writer.write(&guard.observed_root);
        writer.writeU32(@intCast(guard.terms.len));
        for (guard.terms) |term| {
            writer.blob(term.producer_name);
            writer.blob(term.artifact_name);
            writer.write(&term.definition);
            writer.writeU64(term.reprocess_generation);
            writer.byte(@intFromBool(term.value_identity != null));
            if (term.value_identity) |value| writer.write(&value);
            writer.write(&term.input_revision);
        }
    }
}
pub fn read(a: A, cursor: anytype, source_count: usize) ![]Guard {
    const count = try cursor.readU32();
    if (count == 0 or count > source_count) return error.ArtifactCatalogCorrupt;
    const guards = try a.alloc(Guard, count);
    var initialized: usize = 0;
    errdefer {
        for (guards[0..initialized]) |guard| a.free(guard.terms);
        a.free(guards);
    }
    var total: usize = 0;
    for (guards) |*guard| {
        const source_index = try cursor.readU32();
        const canonical = try cursor.readU64();
        const created = try cursor.readU64();
        const whole = try cursor.flag();
        const root: Digest = (try cursor.take(32))[0..32].*;
        const terms_count = try cursor.readU32();
        total = try std.math.add(usize, total, terms_count);
        if (terms_count == 0 or terms_count > maximum_terms or total > 1024) return error.ArtifactCatalogCorrupt;
        const terms = try a.alloc(Term, terms_count);
        guard.* = .{ .source_index = source_index, .canonical_document = canonical, .created_generation = created, .require_whole_source = whole, .observed_root = root, .terms = terms };
        initialized += 1;
        for (terms) |*term| {
            const name = try cursor.blob();
            const artifact = try cursor.blob();
            const definition: Digest = (try cursor.take(32))[0..32].*;
            const generation = try cursor.readU64();
            const value: ?Digest = if (try cursor.flag()) (try cursor.take(32))[0..32].* else null;
            term.* = .{ .producer_name = name, .artifact_name = artifact, .definition = definition, .reprocess_generation = generation, .value_identity = value, .input_revision = (try cursor.take(32))[0..32].* };
        }
    }
    _ = validate(guards, source_count) catch return error.ArtifactCatalogCorrupt;
    return guards;
}
pub fn cloneIntoArena(a: A, guards: []const Guard) ![]Guard {
    const copy = try a.dupe(Guard, guards);
    for (copy) |*guard| {
        const terms = try a.dupe(Term, guard.terms);
        guard.terms = terms;
        for (terms) |*term| {
            term.producer_name = try a.dupe(u8, term.producer_name);
            term.artifact_name = try a.dupe(u8, term.artifact_name);
        }
    }
    return copy;
}
pub fn freeDecoded(a: A, guards: []const Guard) void {
    for (guards) |guard| a.free(guard.terms);
    a.free(guards);
}
pub fn requireIncluded(expected: Guard, actual: Guard) !void {
    if (expected.canonical_document != actual.canonical_document or expected.created_generation != actual.created_generation or
        (expected.require_whole_source and !actual.require_whole_source)) return error.EnrichmentSourceChanged;
    var index: usize = 0;
    for (expected.terms) |term| {
        while (index < actual.terms.len and std.mem.order(u8, &actual.terms[index].definition, &term.definition) == .lt) index += 1;
        if (index == actual.terms.len) return error.EnrichmentSourceChanged;
        const found = actual.terms[index];
        if (!std.mem.eql(u8, &found.definition, &term.definition) or !std.mem.eql(u8, found.producer_name, term.producer_name) or
            !std.mem.eql(u8, found.artifact_name, term.artifact_name) or found.reprocess_generation != term.reprocess_generation or
            !std.meta.eql(found.value_identity, term.value_identity) or !std.mem.eql(u8, &found.input_revision, &term.input_revision)) return error.EnrichmentSourceChanged;
    }
}

/// Linear merge of sorted definition terms. Arena ownership makes the result
/// independent of a decoded upstream proof's lifetime.
pub fn conjoinIntoArena(a: A, left: Guard, right: Guard) !Guard {
    if (left.canonical_document != right.canonical_document or left.created_generation != right.created_generation) return error.EnrichmentSourceChanged;
    var terms: std.ArrayList(Term) = .empty;
    var l: usize = 0;
    var r: usize = 0;
    while (l < left.terms.len or r < right.terms.len) {
        const order = if (l == left.terms.len) .gt else if (r == right.terms.len) .lt else std.mem.order(u8, &left.terms[l].definition, &right.terms[r].definition);
        var term = if (order == .gt) right.terms[r] else left.terms[l];
        if (order == .eq) {
            var expected = left;
            expected.terms = left.terms[l..][0..1];
            var actual = right;
            actual.terms = right.terms[r..][0..1];
            expected.require_whole_source = false;
            try requireIncluded(expected, actual);
        }
        if (terms.items.len == maximum_terms) return error.ResourceBudgetExceeded;
        term.producer_name = try a.dupe(u8, term.producer_name);
        term.artifact_name = try a.dupe(u8, term.artifact_name);
        try terms.append(a, term);
        if (order != .gt) l += 1;
        if (order != .lt) r += 1;
    }
    var result = left;
    result.require_whole_source = left.require_whole_source or right.require_whole_source;
    result.terms = terms.items;
    return result;
}

pub const Definition = struct {
    request: requests.GeneratedEnrichmentRequest,
    dependency: revision.Dependency,
    digest: Digest,
    generation: u64,
};
pub const Catalog = struct {
    arena: std.heap.ArenaAllocator,
    entries: []const Definition,
    pub fn deinit(self: *Catalog) void {
        self.arena.deinit();
    }
    pub fn find(self: Catalog, definition: Digest) ?*const Definition {
        var low: usize = 0;
        var high = self.entries.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (std.mem.order(u8, &self.entries[mid].digest, &definition) == .lt) low = mid + 1 else high = mid;
        }
        return if (low < self.entries.len and std.mem.eql(u8, &self.entries[low].digest, &definition)) &self.entries[low] else null;
    }
    pub fn producer(self: Catalog, name: []const u8, artifact: []const u8) ?*const Definition {
        for (self.entries) |*entry| if (std.mem.eql(u8, entry.request.index_name, name) and std.mem.eql(u8, requests.requestEmbeddingName(entry.request), artifact)) return entry;
        return null;
    }
    pub fn init(a: A, catalogs: inventory.Catalogs) !Catalog {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const indexes = if (catalogs.indexes.len == 0) &.{} else try manager.deserializeCatalog(owned, catalogs.indexes);
        const producers = if (catalogs.enrichments.len == 0) &.{} else try enrichments.deserializeCatalog(owned, catalogs.enrichments);
        if (indexes.len > 16384 or producers.len > 16384) return error.ResourceLimitExceeded;
        var entries: std.ArrayList(Definition) = .empty;
        for (indexes) |index| {
            if (index.kind != .dense_vector and index.kind != .sparse_vector) continue;
            // Legacy inline generator definitions retain their complete read set.
            const config = try std.json.parseFromSlice(std.json.Value, owned, index.config_json, .{});
            if (config.value != .object or config.value.object.contains("generator")) continue;
            if (!try manager.onlineBaseGeneratedVectorConfig(owned, index, producers)) continue;
            const names = try manager.baseVectorArtifactNamesAlloc(owned, index);
            for (names) |name| {
                const producer_config = for (producers) |candidate| {
                    if (std.mem.eql(u8, candidate.name, name)) break candidate;
                } else continue;
                if (producer_config.group_source_json.len != 0 or producer_config.prior_context_json.len != 0) continue;
                const kind: requests.GeneratedEnrichmentKind = if (index.kind == .dense_vector) .dense_embedding else .sparse_embedding;
                const dims = if (index.kind == .dense_vector) try manager.denseConfigDimensions(owned, index) else 0;
                const request = requests.directDocumentEmbeddingRequest(kind, try owned.dupe(u8, index.name), "", try owned.dupe(u8, name), dims, producer_config);
                const dependency = revision.compile(request) orelse continue;
                if (entries.items.len >= 16384) return error.ResourceLimitExceeded;
                try entries.append(owned, .{ .request = request, .dependency = dependency, .digest = @import("artifact_producer_input.zig").definitionDigest(request), .generation = keys.derivedCoverageGenerationForConfig(index.coverage_generation, index.config_json) });
            }
        }
        std.mem.sort(Definition, entries.items, {}, struct {
            fn less(_: void, l: Definition, r: Definition) bool {
                return std.mem.order(u8, &l.digest, &r.digest) == .lt;
            }
        }.less);
        for (entries.items, 0..) |entry, i| if (i != 0 and std.mem.eql(u8, &entries.items[i - 1].digest, &entry.digest)) return error.ArtifactCatalogCorrupt;
        return .{ .arena = arena, .entries = try entries.toOwnedSlice(owned) };
    }
};
pub const Current = struct {
    root: logical.Root,
    canonical_document: u64,
    created_generation: u64,
    pub fn deinit(self: Current, a: A) void {
        self.root.deinit(a);
    }
    pub fn value(self: Current, field: []const u8) ?Digest {
        for (self.root.members) |member| if (std.mem.eql(u8, member.name, field)) return member.digest;
        return null;
    }
};
/// All calls share the caller's one pinned authority/source view. Decoded
/// commitments own their members before identity lookups can retire raw bytes.
pub fn current(a: A, txn: anytype, document: []const u8) !Current {
    const physical = try keys.documentKeyAlloc(a, document);
    defer a.free(physical);
    const raw = payload.rawGet(txn, physical) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
    var root: logical.Root = undefined;
    if (payload.isManifest(raw)) {
        const manifest = try payload.Manifest.decode(a, raw);
        defer a.free(manifest.parts);
        if (manifest.logical_record.len == 0) return error.OnlineMergeArtifactTailsUnsupported;
        root = try logical.decodeMembers(a, manifest.logical_record, manifest.logical_digest);
    } else root = try logical.build(a, raw);
    errdefer root.deinit(a);
    const ordinal = (try identity.lookupOrdinalTxn(a, txn, document)) orelse return error.EnrichmentSourceChanged;
    const state = (try identity.lookupStateTxn(txn, ordinal)) orelse return error.EnrichmentSourceChanged;
    if (!state.isLive() or state.created_generation == 0) return error.EnrichmentSourceChanged;
    return .{ .root = root, .canonical_document = state.canonical_doc_id, .created_generation = state.created_generation };
}
pub fn inputRevision(definition: Definition, namespace: [24]u8, document: []const u8, source: Current, generation: u64) !Digest {
    return revision.revision(definition.dependency, .{ .namespace = namespace, .document_key = document, .canonical_document = source.canonical_document, .created_generation = source.created_generation, .definition = definition.digest, .reprocess_generation = generation }, source.root.members);
}
pub const Captured = struct {
    arena: std.heap.ArenaAllocator,
    guards: []const Guard,
    pub fn deinit(self: *Captured, a: A) void {
        self.arena.deinit();
        a.destroy(self);
    }
};
/// Called only after binding the executable request to the pinned write plan.
/// Unsupported catalog forms keep legacy guards; no caller field list is used.
pub fn capture(a: A, txn: anytype, catalogs: inventory.Catalogs, namespace: [24]u8, request: requests.GeneratedEnrichmentRequest) !?*Captured {
    var definitions = try Catalog.init(a, catalogs);
    defer definitions.deinit();
    const definition = definitions.find(@import("artifact_producer_input.zig").definitionDigest(request)) orelse return null;
    if (!std.mem.eql(u8, definition.request.index_name, request.index_name) or !std.mem.eql(u8, requests.requestEmbeddingName(definition.request), requests.requestEmbeddingName(request))) return null;
    const source = current(a, txn, request.doc_key) catch |err| switch (err) {
        error.EnrichmentSourceChanged, error.OnlineMergeArtifactTailsUnsupported => return null,
        else => return err,
    };
    defer source.deinit(a);
    try @import("artifact_reprocess_intent.zig").requireEmbeddingGeneration(a, txn, request.doc_key, requests.requestEmbeddingName(request), definition.request.producer_json, request.reprocess_generation);
    const result = try a.create(Captured);
    errdefer a.destroy(result);
    result.* = .{ .arena = .init(a), .guards = undefined };
    errdefer result.arena.deinit();
    const owned = result.arena.allocator();
    const terms = try owned.alloc(Term, 1);
    terms[0] = .{ .producer_name = try owned.dupe(u8, request.index_name), .artifact_name = try owned.dupe(u8, requests.requestEmbeddingName(request)), .definition = definition.digest, .reprocess_generation = request.reprocess_generation, .value_identity = source.value(definition.dependency.field), .input_revision = try inputRevision(definition.*, namespace, request.doc_key, source, request.reprocess_generation) };
    const guards = try owned.alloc(Guard, 1);
    guards[0] = .{ .source_index = 0, .canonical_document = source.canonical_document, .created_generation = source.created_generation, .observed_root = source.root.digest, .terms = terms };
    result.guards = guards;
    return result;
}

pub fn validateCurrent(a: A, txn: anytype, catalog: Catalog, namespace: [24]u8, document: []const u8, guard: Guard) !void {
    const source = try current(a, txn, document);
    defer source.deinit(a);
    if (source.canonical_document != guard.canonical_document or source.created_generation != guard.created_generation) return error.EnrichmentSourceChanged;
    for (guard.terms) |term| {
        const definition = catalog.find(term.definition) orelse return error.EnrichmentSourceChanged;
        if (!std.mem.eql(u8, term.producer_name, definition.request.index_name) or !std.mem.eql(u8, term.artifact_name, requests.requestEmbeddingName(definition.request))) return error.EnrichmentSourceChanged;
        try @import("artifact_reprocess_intent.zig").requireEmbeddingGeneration(a, txn, document, term.artifact_name, definition.request.producer_json, term.reprocess_generation);
        if (!std.meta.eql(source.value(definition.dependency.field), term.value_identity) or !std.mem.eql(u8, &try inputRevision(definition.*, namespace, document, source, term.reprocess_generation), &term.input_revision)) return error.EnrichmentSourceChanged;
    }
}

/// Rebind authenticated donor inputs to the receiver's actual live incarnation.
/// The caller first certifies donor/receiver catalog semantic equivalence. Each
/// term is checked against both definitions and the receiver field identity;
/// no donor physical observation becomes receiver completion evidence.
pub fn rebaseIntoArena(a: A, txn: anytype, donor_catalog: Catalog, receiver_catalog: Catalog, donor_namespace: [24]u8, receiver_namespace: [24]u8, document: []const u8, guard: Guard) !Guard {
    const source = try current(a, txn, document);
    defer source.deinit(a);
    var donor_source = source;
    donor_source.canonical_document = guard.canonical_document;
    donor_source.created_generation = guard.created_generation;
    const terms = try a.alloc(Term, guard.terms.len);
    for (guard.terms, terms) |term, *out| {
        const donor = donor_catalog.find(term.definition) orelse return error.EnrichmentSourceChanged;
        if (!std.mem.eql(u8, term.producer_name, donor.request.index_name) or !std.mem.eql(u8, term.artifact_name, requests.requestEmbeddingName(donor.request))) return error.EnrichmentSourceChanged;
        const receiver = receiver_catalog.producer(term.producer_name, term.artifact_name) orelse return error.EnrichmentSourceChanged;
        if (!std.mem.eql(u8, donor.dependency.field, receiver.dependency.field) or
            !std.meta.eql(source.value(receiver.dependency.field), term.value_identity) or
            !std.mem.eql(u8, &try inputRevision(donor.*, donor_namespace, document, donor_source, term.reprocess_generation), &term.input_revision)) return error.EnrichmentSourceChanged;
        try @import("artifact_reprocess_intent.zig").requireEmbeddingGeneration(a, txn, document, term.artifact_name, receiver.request.producer_json, term.reprocess_generation);
        out.* = term;
        out.producer_name = try a.dupe(u8, term.producer_name);
        out.artifact_name = try a.dupe(u8, term.artifact_name);
        out.definition = receiver.digest;
        out.input_revision = try inputRevision(receiver.*, receiver_namespace, document, source, term.reprocess_generation);
    }
    std.mem.sort(Term, terms, {}, struct {
        fn less(_: void, l: Term, r: Term) bool {
            return std.mem.order(u8, &l.definition, &r.definition) == .lt;
        }
    }.less);
    var result = guard;
    result.canonical_document = source.canonical_document;
    result.created_generation = source.created_generation;
    result.observed_root = source.root.digest;
    result.terms = terms;
    return result;
}

test {
    _ = @import("scoped_producer_inputs_test.zig");
}
