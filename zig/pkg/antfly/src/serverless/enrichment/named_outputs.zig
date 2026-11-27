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

//! Outputs of a serverless table's named enrichments (#1791).
//!
//! Serverless keeps every derived output in the document body. Named outputs
//! live there in exactly the public seed write shape, `_artifacts.<name>`
//! (`{"value": ...}`) and `_chunks.<name>` (a chunk array), so reads, the
//! seed view and migrations between tables all use one shape. Each output's
//! acceptance record is `_enrichment.named.<name>`:
//! `{"origin": "seeded"|"generated"|"external", "input": <hex digest>}`, or
//! `{"pending": true, "input": ...}` while an `on_missing: pending` mapping
//! waits for its seed.
//!
//! `input` binds an output to what it was produced from: the declaration
//! (kind, producer, chunker, source) and the source value (field, template
//! document or upstream output). An output whose input no longer matches is
//! dropped on write so the producer regenerates it; externally maintained
//! assets bind to nothing and are always kept.
const std = @import("std");
const Allocator = std.mem.Allocator;
const local = @import("antfly_local_sources");
const artifact_seeds = local.storage_db_artifact_seeds;
const named_enrichments = @import("../catalog/named_enrichments.zig");
const EnrichmentConfig = named_enrichments.EnrichmentConfig;

pub const marker_key = "named";

pub const Accepted = struct {
    body: []u8,
    /// Per-output outcomes of the seeds this write carried (owned).
    outcomes: []artifact_seeds.OutcomeEntry = &.{},

    pub fn deinit(self: *Accepted, alloc: Allocator) void {
        alloc.free(self.body);
        freeOutcomes(alloc, self.outcomes);
        self.* = undefined;
    }
};

pub fn freeOutcomes(alloc: Allocator, outcomes: []artifact_seeds.OutcomeEntry) void {
    for (outcomes) |entry| {
        alloc.free(entry.key);
        alloc.free(entry.name);
    }
    if (outcomes.len != 0) alloc.free(outcomes);
}

/// Accepts the write of `body` for `doc_key`, whose stored body was
/// `prior_body`: explicit seeds (`_artifacts`/`_chunks` values that differ
/// from the stored outputs), field-mapping seeds, carried-over outputs whose
/// input is unchanged, and pending markers. `policy` is the request's
/// `seed_conflict`; a body's own `_seed_conflict` wins.
pub fn acceptAlloc(
    alloc: Allocator,
    declarations: named_enrichments.Declarations,
    doc_key: []const u8,
    prior_body: ?[]const u8,
    body: []const u8,
    policy: artifact_seeds.ConflictPolicy,
) !Accepted {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var document = std.json.parseFromSliceLeaky(std.json.Value, a, body, .{}) catch return .{ .body = try alloc.dupe(u8, body) };
    if (document != .object) {
        // A plain-text body has no room for outputs; seeds cannot be in it.
        return .{ .body = try alloc.dupe(u8, body) };
    }
    const prior: ?std.json.Value = if (prior_body) |bytes| prior: {
        const parsed = std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{}) catch break :prior null;
        break :prior if (parsed == .object) parsed else null;
    } else null;

    var row_policy = policy;
    if (document.object.fetchSwapRemove("_seed_conflict")) |entry| {
        if (entry.value != .string) return error.InvalidArtifactSeed;
        row_policy = artifact_seeds.ConflictPolicy.parse(entry.value.string) orelse return error.InvalidArtifactSeed;
    }
    const incoming_artifacts = try reservedObject(&document, "_artifacts");
    const incoming_chunks = try reservedObject(&document, "_chunks");
    // Acceptance records are private: a client cannot assert them.
    var state = document.object.get("_enrichment") orelse std.json.Value{ .object = .empty };
    if (state != .object) return error.InvalidDocumentBody;
    _ = state.object.swapRemove(marker_key);

    const prior_artifacts = objectMember(prior, "_artifacts");
    const prior_chunks = objectMember(prior, "_chunks");
    const prior_markers = if (objectMember(prior, "_enrichment")) |prior_state| if (prior_state.get(marker_key)) |named| if (named == .object) named.object else null else null else null;

    // Seeds of undeclared names fail visibly, as in embedded writes.
    for ([_]?std.json.ObjectMap{ incoming_artifacts, incoming_chunks }, [_]bool{ false, true }) |incoming, chunk_field| if (incoming) |map| {
        for (map.keys()) |name| {
            const cfg = declarations.find(name) orelse return error.UnknownArtifactSeed;
            if ((cfg.kind == .chunk) != chunk_field) return error.UnknownArtifactSeed;
        }
    };

    var out_artifacts = std.json.ObjectMap.empty;
    var out_chunks = std.json.ObjectMap.empty;
    var markers = std.json.ObjectMap.empty;
    var outcomes = std.ArrayListUnmanaged(artifact_seeds.OutcomeEntry).empty;
    errdefer {
        for (outcomes.items) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.name);
        }
        outcomes.deinit(alloc);
    }

    const ordered = try a.dupe(EnrichmentConfig, declarations.configs);
    try local.api_local_indexes.sortArtifactEnrichmentsByDependency(a, ordered);
    for (ordered) |cfg| {
        const chunk_kind = cfg.kind == .chunk;
        const external = named_enrichments.externallyMaintained(cfg);
        const outputs = if (chunk_kind) &out_chunks else &out_artifacts;
        const input = try inputDigestHexAlloc(a, cfg, document, out_artifacts, out_chunks);
        const prior_map = if (chunk_kind) prior_chunks else prior_artifacts;
        const prior_value: ?std.json.Value = if (prior_map) |map| map.get(cfg.name) else null;
        const prior_marker: ?std.json.ObjectMap = if (prior_markers) |map| if (map.get(cfg.name)) |marker| if (marker == .object) marker.object else null else null else null;
        const prior_origin: ?artifact_seeds.Origin = if (prior_marker) |marker| originOf(marker) else null;
        const prior_valid = prior_value != null and (external or (prior_marker != null and prior_origin != null and markerInputMatches(prior_marker.?, input)));

        // The seed: an explicit value that is not just the stored output
        // echoed back, else the field mapping.
        var via: artifact_seeds.Via = .explicit;
        const Seed = union(enum) { value: std.json.Value, delete, pending };
        const seed: ?Seed = seed: {
            const incoming = if (chunk_kind) incoming_chunks else incoming_artifacts;
            if (incoming) |map| if (map.get(cfg.name)) |value| {
                if (value == .null) break :seed .delete;
                if (prior_value) |stored| if (try jsonEqual(a, stored, value)) break :seed null;
                break :seed .{ .value = value };
            };
            if (cfg.seed_json.len == 0) break :seed null;
            const mapping = try artifact_seeds.parseMapping(a, cfg.seed_json);
            via = .mapping;
            const entry = (try artifact_seeds.mappedEntry(mapping, document, chunk_kind)) orelse switch (mapping.on_missing) {
                .fail => return error.ArtifactSeedMissing,
                .pending => break :seed if (prior_valid) null else .pending,
                .generate => break :seed null,
            };
            break :seed switch (entry) {
                .delete => .delete,
                .chunks => |items| chunks: {
                    var array = std.json.Array.init(a);
                    try array.appendSlice(items);
                    break :chunks .{ .value = .{ .array = array } };
                },
                .artifact => |envelope| .{ .value = try envelopeValue(a, envelope.value) },
            };
        };

        const name = cfg.name;
        if (seed) |found| switch (found) {
            .delete => {
                if (prior_value != null) try appendOutcome(alloc, &outcomes, doc_key, name, .{ .outcome = .deleted, .existing_origin = prior_origin }, via);
            },
            .pending => {
                var marker = std.json.ObjectMap.empty;
                try marker.put(a, "pending", .{ .bool = true });
                if (input) |digest| try marker.put(a, "input", .{ .string = digest });
                try markers.put(a, name, .{ .object = marker });
                try appendOutcome(alloc, &outcomes, doc_key, name, .{ .outcome = .pending }, via);
            },
            .value => |value| {
                try validateSeed(a, cfg, value);
                const origin: artifact_seeds.Origin = if (external) .external else .seeded;
                const result: artifact_seeds.Result = if (!prior_valid)
                    .{ .outcome = .accepted }
                else if (try jsonEqual(a, prior_value.?, value))
                    .{ .outcome = .unchanged, .existing_origin = prior_origin }
                else if (external or row_policy == .replace)
                    .{ .outcome = .replaced, .existing_origin = prior_origin }
                else
                    .{ .outcome = .conflict, .existing_origin = prior_origin };
                if (result.outcome == .conflict or result.outcome == .unchanged) {
                    try outputs.put(a, name, prior_value.?);
                    try markers.put(a, name, .{ .object = prior_marker orelse try markerObject(a, origin, input) });
                } else {
                    try outputs.put(a, name, value);
                    try markers.put(a, name, .{ .object = try markerObject(a, origin, input) });
                }
                try appendOutcome(alloc, &outcomes, doc_key, name, result, via);
            },
        } else if (prior_valid) {
            // Unchanged input: the accepted output stays.
            try outputs.put(a, name, prior_value.?);
            if (prior_marker) |marker| try markers.put(a, name, .{ .object = marker }) else try markers.put(a, name, .{ .object = try markerObject(a, .external, input) });
        } else if (!external and (incoming_artifacts != null or incoming_chunks != null)) {
            try appendOutcome(alloc, &outcomes, doc_key, name, .{ .outcome = .missing }, null);
        }
    }

    if (out_artifacts.count() != 0) try document.object.put(a, "_artifacts", .{ .object = out_artifacts });
    if (out_chunks.count() != 0) try document.object.put(a, "_chunks", .{ .object = out_chunks });
    if (markers.count() != 0) try state.object.put(a, marker_key, .{ .object = markers });
    if (state.object.count() != 0) try document.object.put(a, "_enrichment", state) else _ = document.object.swapRemove("_enrichment");
    return .{
        .body = try std.json.Stringify.valueAlloc(alloc, document, .{}),
        .outcomes = try outcomes.toOwnedSlice(alloc),
    };
}

/// Whether `body` has a declared output the worker must still generate: no
/// acceptance record for its current input (and none waiting on a seed),
/// with its source present. Externally maintained assets never generate.
pub fn needsGeneration(alloc: Allocator, declarations: named_enrichments.Declarations, body: []const u8) !bool {
    if (declarations.configs.len == 0) return false;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const document = std.json.parseFromSliceLeaky(std.json.Value, a, body, .{}) catch return false;
    if (document != .object) return false;
    const artifacts = objectMember(document, "_artifacts") orelse std.json.ObjectMap.empty;
    const chunks = objectMember(document, "_chunks") orelse std.json.ObjectMap.empty;
    const markers = if (objectMember(document, "_enrichment")) |state| if (state.get(marker_key)) |named| if (named == .object) named.object else null else null else null;
    for (declarations.configs) |cfg| {
        if (named_enrichments.externallyMaintained(cfg)) continue;
        // Upstream outputs are produced first; their consumers follow.
        const input = (try inputDigestHexAlloc(a, cfg, document, artifacts, chunks)) orelse continue;
        const marker = if (markers) |map| if (map.get(cfg.name)) |found| if (found == .object) found.object else null else null else null;
        if (marker) |record| {
            if (markerInputMatches(record, input)) continue;
        }
        return true;
    }
    return false;
}

fn reservedObject(document: *std.json.Value, name: []const u8) !?std.json.ObjectMap {
    const entry = document.object.fetchSwapRemove(name) orelse return null;
    return switch (entry.value) {
        .object => |map| map,
        .null => null,
        else => error.InvalidArtifactSeed,
    };
}

fn objectMember(value: ?std.json.Value, name: []const u8) ?std.json.ObjectMap {
    const object = value orelse return null;
    const member = object.object.get(name) orelse return null;
    return if (member == .object) member.object else null;
}

fn originOf(marker: std.json.ObjectMap) ?artifact_seeds.Origin {
    const origin = marker.get("origin") orelse return null;
    if (origin != .string) return null;
    return std.meta.stringToEnum(artifact_seeds.Origin, origin.string);
}

fn markerInputMatches(marker: std.json.ObjectMap, input: ?[]const u8) bool {
    const recorded = marker.get("input");
    const expected = input orelse return recorded == null;
    const found = recorded orelse return false;
    return found == .string and std.mem.eql(u8, found.string, expected);
}

fn markerObject(a: Allocator, origin: artifact_seeds.Origin, input: ?[]const u8) !std.json.ObjectMap {
    var marker = std.json.ObjectMap.empty;
    try marker.put(a, "origin", .{ .string = @tagName(origin) });
    if (input) |digest| try marker.put(a, "input", .{ .string = digest });
    return marker;
}

/// The public `_artifacts` envelope keeps only `value` and the optional
/// caller provenance; a mapping supplies a bare value.
fn envelopeValue(a: Allocator, value: std.json.Value) !std.json.Value {
    var envelope = std.json.ObjectMap.empty;
    try envelope.put(a, "value", value);
    return .{ .object = envelope };
}

fn validateSeed(a: Allocator, cfg: EnrichmentConfig, value: std.json.Value) !void {
    if (cfg.kind == .chunk) return artifact_seeds.validateChunkCollection(value);
    try artifact_seeds.validateArtifactEnvelope(value);
    if (cfg.kind != .embedding) return;
    const inner = value.object.get("value") orelse return error.InvalidArtifactSeed;
    switch (inner) {
        // A document vector, or one vector per chunk.
        .array => |items| if (items.items.len != 0 and items.items[0] == .object) {
            const vectors = try artifact_seeds.decodeChunkVectorsAlloc(a, inner, cfg.expected_dims);
            artifact_seeds.freeChunkVectors(a, vectors);
        } else {
            a.free(try artifact_seeds.decodeDenseVectorAlloc(a, inner, cfg.expected_dims));
        },
        // A sparse vector.
        .object => {
            var sparse = try artifact_seeds.decodeSparseVectorAlloc(a, inner);
            sparse.deinit(a);
        },
        else => return error.InvalidArtifactSeed,
    }
}

fn jsonEqual(a: Allocator, left: std.json.Value, right: std.json.Value) !bool {
    const canonical = local.storage_db_document_content_hash.canonicalJsonValueAlloc;
    return std.mem.eql(u8, try canonical(a, left), try canonical(a, right));
}

/// Hex SHA-256 of the declaration and its source value, or null for an
/// externally maintained asset (bound to nothing) or an absent source.
fn inputDigestHexAlloc(a: Allocator, cfg: EnrichmentConfig, document: std.json.Value, artifacts: std.json.ObjectMap, chunks: std.json.ObjectMap) !?[]const u8 {
    if (named_enrichments.externallyMaintained(cfg)) return null;
    const canonical = local.storage_db_document_content_hash.canonicalJsonValueAlloc;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    const definition = try std.fmt.allocPrint(a, "{s}\x00{s}\x00{s}\x00{s}\x00{s}\x00{s}\x00{d}\x00{d}\x00{d}", .{ @tagName(cfg.kind), cfg.producer_json, cfg.chunker_json, cfg.field, cfg.template, cfg.source_artifact_name, cfg.chunk_size, cfg.chunk_overlap, cfg.expected_dims });
    hashPart(&hash, "antfly-serverless-named-v1");
    hashPart(&hash, definition);
    const source: ?std.json.Value = source: {
        if (cfg.source_artifact_name.len != 0) {
            break :source artifacts.get(cfg.source_artifact_name) orelse chunks.get(cfg.source_artifact_name);
        }
        if (cfg.template.len != 0) {
            // The template reads the whole record; outputs are not input.
            var record = std.json.ObjectMap.empty;
            var it = document.object.iterator();
            while (it.next()) |entry| {
                if (isOutputField(entry.key_ptr.*)) continue;
                try record.put(a, entry.key_ptr.*, entry.value_ptr.*);
            }
            break :source .{ .object = record };
        }
        var current = document;
        var parts = std.mem.splitScalar(u8, cfg.field, '.');
        while (parts.next()) |part| {
            if (current != .object) break :source null;
            current = current.object.get(part) orelse break :source null;
        }
        break :source current;
    };
    const value = source orelse return null;
    hashPart(&hash, try canonical(a, value));
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return try a.dupe(u8, &hex);
}

fn hashPart(hash: *std.crypto.hash.sha2.Sha256, part: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, part.len, .little);
    hash.update(&length);
    hash.update(part);
}

/// Body members that hold outputs or private state, never producer input.
pub fn isOutputField(name: []const u8) bool {
    for ([_][]const u8{ "_artifacts", "_chunks", "_enrichment", "_embeddings", "embedding", "sparse_embedding", "chunk_preview", "chunk_embeddings", "rerank_terms", "graph_edges", "media_outputs", "transcript", "transcript_spans" }) |reserved| {
        if (std.mem.eql(u8, name, reserved)) return true;
    }
    return false;
}

fn appendOutcome(alloc: Allocator, outcomes: *std.ArrayListUnmanaged(artifact_seeds.OutcomeEntry), doc_key: []const u8, name: []const u8, result: artifact_seeds.Result, via: ?artifact_seeds.Via) !void {
    const key = try alloc.dupe(u8, doc_key);
    errdefer alloc.free(key);
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    try outcomes.append(alloc, .{ .key = key, .name = owned_name, .outcome = result.outcome, .existing_origin = result.existing_origin, .via = via });
}

const test_indexes =
    \\{"enrichments":[
    \\  {"name":"summary_v1","kind":"asset","field":"body","content_type":"text/plain","producer":{"type":"generator","config":{"provider":"antfly","model":"g"}},"seed":{"field":"legacy.summary","format":"text","on_missing":"pending"}},
    \\  {"name":"passages","kind":"chunk","field":"body","chunk_size":64},
    \\  {"name":"notes","kind":"asset","content_type":"text/plain"}
    \\]}
;

fn expectOutcome(accepted: Accepted, name: []const u8, outcome: artifact_seeds.Outcome) !void {
    for (accepted.outcomes) |entry| if (std.mem.eql(u8, entry.name, name)) {
        try std.testing.expectEqual(outcome, entry.outcome);
        return;
    };
    return error.TestUnexpectedResult;
}

test "serverless named outputs accept seeds and keep them while their input stands" {
    const alloc = std.testing.allocator;
    var declarations = try named_enrichments.parseAlloc(alloc, test_indexes);
    defer declarations.deinit(alloc);

    // Explicit seeds, a mapping seed and an external value.
    var first = try acceptAlloc(alloc, declarations, "doc:a", null,
        \\{"body":"alpha beta","legacy":{"summary":"short"},"_artifacts":{"notes":{"value":"n"}},"_chunks":{"passages":[{"_chunk_id":0,"_content":"alpha beta"}]}}
    , .keep);
    defer first.deinit(alloc);
    try expectOutcome(first, "summary_v1", .accepted);
    try expectOutcome(first, "passages", .accepted);
    try expectOutcome(first, "notes", .accepted);
    try std.testing.expect(std.mem.indexOf(u8, first.body, "\"summary_v1\":{\"value\":\"short\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, first.body, "\"origin\":\"seeded\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, first.body, "\"origin\":\"external\"") != null);

    // An unrelated edit keeps every output; a changed input drops the
    // summary so its producer regenerates it.
    var edited = try acceptAlloc(alloc, declarations, "doc:a", first.body, "{\"body\":\"alpha beta\",\"legacy\":{\"summary\":\"short\"},\"title\":\"t\"}", .keep);
    defer edited.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, edited.body, "\"passages\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, edited.body, "\"notes\":{\"value\":\"n\"}") != null);
    var changed = try acceptAlloc(alloc, declarations, "doc:a", edited.body, "{\"body\":\"other\"}", .keep);
    defer changed.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, changed.body, "\"passages\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, changed.body, "\"notes\":{\"value\":\"n\"}") != null);
    // The mapping field is gone and the summary waits for its seed.
    try expectOutcome(changed, "summary_v1", .pending);
    try std.testing.expect(std.mem.indexOf(u8, changed.body, "\"pending\":true") != null);

    // A differing seed for accepted output conflicts under keep.
    var conflicting = try acceptAlloc(alloc, declarations, "doc:a", first.body, "{\"body\":\"alpha beta\",\"legacy\":{\"summary\":\"short\"},\"_chunks\":{\"passages\":[{\"_chunk_id\":0,\"_content\":\"alpha\"}]}}", .keep);
    defer conflicting.deinit(alloc);
    try expectOutcome(conflicting, "passages", .conflict);
    try std.testing.expect(std.mem.indexOf(u8, conflicting.body, "\"_content\":\"alpha beta\"") != null);

    // Clients cannot assert acceptance records, and undeclared names fail.
    try std.testing.expectError(error.UnknownArtifactSeed, acceptAlloc(alloc, declarations, "doc:b", null, "{\"_artifacts\":{\"missing\":{\"value\":1}}}", .keep));
    try std.testing.expectError(error.UnknownArtifactSeed, acceptAlloc(alloc, declarations, "doc:b", null, "{\"_artifacts\":{\"passages\":{\"value\":1}}}", .keep));
    var forged = try acceptAlloc(alloc, declarations, "doc:b", null, "{\"body\":\"x\",\"_enrichment\":{\"named\":{\"summary_v1\":{\"origin\":\"seeded\"}}}}", .keep);
    defer forged.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, forged.body, "\"origin\"") == null);
}
