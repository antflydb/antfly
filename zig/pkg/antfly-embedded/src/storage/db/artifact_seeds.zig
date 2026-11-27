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

//! Artifact seeds: caller-supplied results for named enrichments.
//!
//! A write may carry `_artifacts.<name>` (assets and embeddings) and
//! `_chunks.<name>` (chunk collections) beside its source fields. The document
//! mapper strips both reserved fields from the primary row and keeps one
//! canonical, envelope-validated JSON copy with the extracted write. Producer
//! preparation then accepts each seed in place of the producer invocation, so
//! the seeded value is published through exactly the same artifact lifecycle
//! (input identity, coverage, consumer indexing, replication) as generated
//! output. See `docs/design/artifact-seeding.md`.
//!
//! This leaf owns only the public write shapes. It has no storage, catalog or
//! producer capability: envelope checks run at extraction, typed decoding runs
//! at acceptance once the target enrichment's kind and contract are known.
//! Writes accept exactly the documented shapes; descriptive read fields
//! (`artifact_id`, `status`, `dims`, ...) and internal routing fields
//! (`_parent_doc_key`, `_artifact_range_id`, `_chunk`, ...) fail visibly.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const artifacts_field = "_artifacts";
pub const chunks_field = "_chunks";
/// Per-document conflict policy for the seeds it carries. Request-level
/// `seed_conflict` lowers to this member so the policy travels with the
/// seeds through Raft, 2PC intents and relational sidecars.
pub const conflict_field = "_seed_conflict";

/// What happens when a seed differs from output already accepted for the
/// same input: `keep` rejects the write (it is never dropped silently),
/// `replace` overwrites it. Identical values are always unchanged no-ops.
pub const ConflictPolicy = enum {
    keep,
    replace,

    pub fn parse(text: []const u8) ?ConflictPolicy {
        return std.meta.stringToEnum(ConflictPolicy, text);
    }
};

/// Per-seed acceptance result, as reported by seed planning.
pub const Outcome = enum {
    /// No accepted output existed for this input; the seed becomes it.
    accepted,
    /// Accepted output for this input already equals the seed.
    unchanged,
    /// Accepted output for this input differs and the policy keeps it.
    conflict,
    /// Accepted output for this input differs and the seed replaces it.
    replaced,
    /// `null`: accepted output is dropped so the producer regenerates it.
    deleted,
    /// An output of the seeded document without a seed: its producer
    /// generates it as usual.
    missing,
    /// No seed, and its field mapping says `on_missing: pending`: nothing
    /// generates it; it stays absent until a seed supplies it.
    pending,
};

pub const Origin = enum { seeded, generated, external };

/// The acceptance decision for one seeded output.
pub const Result = struct {
    outcome: Outcome,
    existing_origin: ?Origin = null,

    /// Combines per-row results (chunk windows) into the output's result:
    /// any conflict dominates, then replacement, then acceptance.
    pub fn merge(self: Result, other: Result) Result {
        const rank = struct {
            fn of(outcome: Outcome) u8 {
                return switch (outcome) {
                    .missing, .pending => 0,
                    .unchanged => 0,
                    .accepted => 1,
                    .deleted => 2,
                    .replaced => 3,
                    .conflict => 4,
                };
            }
        }.of;
        return if (rank(other.outcome) > rank(self.outcome)) other else self;
    }
};

pub const OutcomeEntry = struct {
    key: []const u8,
    name: []const u8,
    outcome: Outcome,
    /// Origin of the output that existed before this write, when any did.
    existing_origin: ?Origin = null,
    /// Where the seed came from: an explicit `_artifacts`/`_chunks` seed in
    /// the write, or the enrichment's field mapping. Absent for `missing`.
    via: ?Via = null,
};

pub const Via = enum { explicit, mapping };

/// Upper bound on one seeded chunk collection published inside the seeding
/// write itself (one atomic mutation, with its manifest).
pub const max_seed_chunks: usize = 4095;
/// Upper bound on one seeded chunk collection. Above `max_seed_chunks` the
/// write records the accepted set and the chunk producer publishes it on
/// replay, through staged publication where the ordered authority applies
/// (`docs/design/media-pass.md`).
pub const max_staged_seed_chunks: usize = 65536;

/// Whether a seeded collection of `count` chunks publishes on replay.
pub fn stagedChunkCollection(count: usize) bool {
    return count > max_seed_chunks;
}
/// Upper bound on seeded artifact names per document and kind.
pub const max_seed_names: usize = 64;
pub const max_name_bytes: usize = 256;
pub const max_vector_dims: usize = 65536;

pub const Error = error{
    /// The seed does not match the documented write shape.
    InvalidArtifactSeed,
    /// The seed names an output that no enrichment declares, or targets it
    /// through the wrong reserved field (`_artifacts` versus `_chunks`).
    UnknownArtifactSeed,
    /// The target enrichment exists but this release cannot accept a seed for
    /// it (for example a context-dependent producer).
    ArtifactSeedNotAccepted,
    /// A seed-only transform names a document that does not exist. There are
    /// no pending seeds and no seed-only rows.
    ArtifactSeedDocumentNotFound,
    /// A seed-only transform must name the source version (`expected_version`)
    /// its seeds were computed from.
    ArtifactSeedSourceBindingRequired,
    /// Under `seed_conflict: keep`, a seed differs from output already
    /// accepted for the same input. Plan the write to see which items.
    ArtifactSeedConflict,
    /// An enrichment field mapping with `on_missing: fail` found no value.
    ArtifactSeedMissing,
    /// `source.source_hash` names a different input than the one the seed
    /// would be bound to: the seed was computed from another source version
    /// or producer revision.
    ArtifactSeedSourceMismatch,
};

/// Checks a seed's `source.source_hash` against the input identity it would
/// be bound to. Without a declared hash there is nothing to check.
pub fn requireSourceHash(source: ?Source, bound: ?u64) !void {
    const declared = (source orelse return).source_hash orelse return;
    if (bound == null or bound.? != declared) return error.ArtifactSeedSourceMismatch;
}

/// Outputs whose identity is not one `source_hash` (chunk-derived vectors,
/// assets) cannot honor a declared hash; refuse rather than ignore it.
pub fn rejectSourceHash(source: ?Source) !void {
    if (source) |declared| if (declared.source_hash != null) return error.ArtifactSeedNotAccepted;
}

pub fn isSeedField(name: []const u8) bool {
    return std.mem.eql(u8, name, artifacts_field) or std.mem.eql(u8, name, chunks_field) or std.mem.eql(u8, name, conflict_field);
}

/// Returns whether a JSON object contains either reserved seed field.
pub fn hasSeedFields(object: std.json.ObjectMap) bool {
    return object.contains(artifacts_field) or object.contains(chunks_field);
}

/// Validates both reserved fields of `object` and returns their canonical
/// JSON, `{"_artifacts":{...},"_chunks":{...}}` with only present members, or
/// null when neither is present. The result is owned by `alloc`.
pub fn extractCanonicalAlloc(alloc: Allocator, object: std.json.ObjectMap) !?[]u8 {
    const artifacts = object.get(artifacts_field);
    const chunks = object.get(chunks_field);
    const conflict = object.get(conflict_field);
    if (artifacts == null and chunks == null) {
        // A policy without seeds would be silently meaningless.
        if (conflict != null) return error.InvalidArtifactSeed;
        return null;
    }
    if (conflict) |value| {
        if (value != .string or ConflictPolicy.parse(value.string) == null) return error.InvalidArtifactSeed;
    }
    if (artifacts) |value| try validateArtifactsField(value);
    if (chunks) |value| try validateChunksField(value);
    if (artifacts != null and chunks != null) {
        // One write path per kind: a name may not be seeded through both.
        var it = artifacts.?.object.iterator();
        while (it.next()) |entry| {
            if (chunks.?.object.contains(entry.key_ptr.*)) return error.InvalidArtifactSeed;
        }
    }
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    var writer: std.json.Stringify = .{ .writer = &out.writer };
    writer.beginObject() catch return error.OutOfMemory;
    if (artifacts) |value| {
        writer.objectField(artifacts_field) catch return error.OutOfMemory;
        writer.write(value) catch return error.OutOfMemory;
    }
    if (chunks) |value| {
        writer.objectField(chunks_field) catch return error.OutOfMemory;
        writer.write(value) catch return error.OutOfMemory;
    }
    if (conflict) |value| {
        writer.objectField(conflict_field) catch return error.OutOfMemory;
        writer.write(value) catch return error.OutOfMemory;
    }
    writer.endObject() catch return error.OutOfMemory;
    return try out.toOwnedSlice();
}

fn validateName(name: []const u8) !void {
    if (name.len == 0 or name.len > max_name_bytes) return error.InvalidArtifactSeed;
    // Paths address `_artifacts.<name>` with exactly two segments.
    if (std.mem.indexOfScalar(u8, name, '.') != null) return error.InvalidArtifactSeed;
    if (!std.unicode.utf8ValidateSlice(name)) return error.InvalidArtifactSeed;
}

fn validateArtifactsField(value: std.json.Value) !void {
    if (value != .object) return error.InvalidArtifactSeed;
    if (value.object.count() > max_seed_names) return error.InvalidArtifactSeed;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        try validateName(entry.key_ptr.*);
        try validateArtifactEnvelope(entry.value_ptr.*);
    }
}

/// `null` deletes accepted output; otherwise `{value, source?, provenance?}`.
pub fn validateArtifactEnvelope(value: std.json.Value) !void {
    switch (value) {
        .null => return,
        .object => {},
        else => return error.InvalidArtifactSeed,
    }
    var has_value = false;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.mem.eql(u8, key, "value")) {
            has_value = true;
        } else if (std.mem.eql(u8, key, "source")) {
            try validateSource(entry.value_ptr.*);
        } else if (std.mem.eql(u8, key, "provenance")) {
            try validateProvenance(entry.value_ptr.*);
        } else return error.InvalidArtifactSeed;
    }
    if (!has_value) return error.InvalidArtifactSeed;
}

fn validateSource(value: std.json.Value) !void {
    if (value != .object or value.object.count() == 0) return error.InvalidArtifactSeed;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const item = entry.value_ptr.*;
        if (item != .string or item.string.len == 0 or item.string.len > 512) return error.InvalidArtifactSeed;
        if (std.mem.eql(u8, key, "generation")) continue;
        if (std.mem.eql(u8, key, "sha256")) {
            if (item.string.len != 64) return error.InvalidArtifactSeed;
            for (item.string) |c| if (!std.ascii.isHex(c)) return error.InvalidArtifactSeed;
            continue;
        }
        if (std.mem.eql(u8, key, "source_hash")) {
            _ = parseSourceHash(item.string) orelse return error.InvalidArtifactSeed;
            continue;
        }
        return error.InvalidArtifactSeed;
    }
}

/// Parses the `xxh64:<hex>` form emitted by artifact reads.
pub fn parseSourceHash(text: []const u8) ?u64 {
    const prefix = "xxh64:";
    if (!std.mem.startsWith(u8, text, prefix)) return null;
    const digits = text[prefix.len..];
    if (digits.len == 0 or digits.len > 16) return null;
    return std.fmt.parseInt(u64, digits, 16) catch null;
}

fn validateProvenance(value: std.json.Value) !void {
    if (value != .object) return error.InvalidArtifactSeed;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (!std.mem.eql(u8, key, "producer") and !std.mem.eql(u8, key, "model") and !std.mem.eql(u8, key, "revision"))
            return error.InvalidArtifactSeed;
        const item = entry.value_ptr.*;
        if (item != .string or item.string.len > 512) return error.InvalidArtifactSeed;
    }
}

fn validateChunksField(value: std.json.Value) !void {
    if (value != .object) return error.InvalidArtifactSeed;
    if (value.object.count() > max_seed_names) return error.InvalidArtifactSeed;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        try validateName(entry.key_ptr.*);
        try validateChunkCollection(entry.value_ptr.*);
    }
}

/// `null` deletes accepted output; otherwise the plain `_chunks` read array.
pub fn validateChunkCollection(value: std.json.Value) !void {
    switch (value) {
        .null => return,
        .array => {},
        else => return error.InvalidArtifactSeed,
    }
    if (value.array.items.len > max_staged_seed_chunks) return error.InvalidArtifactSeed;
    var previous: ?ChunkFields = null;
    var scoped: ?bool = null;
    for (value.array.items) |item| {
        const parsed = try parseChunkObject(item);
        const is_scoped = parsed.parent_unit_id != null;
        if (scoped) |known| if (known != is_scoped) return error.InvalidArtifactSeed;
        scoped = is_scoped;
        if (previous) |prior| {
            // Unit-scoped collections list each unit's chunks contiguously.
            const same_unit = !is_scoped or std.mem.eql(u8, prior.parent_unit_id.?, parsed.parent_unit_id.?);
            if (same_unit and parsed.chunk_id <= prior.chunk_id) return error.InvalidArtifactSeed;
        }
        previous = parsed;
    }
}

pub const ChunkFields = struct {
    chunk_id: u32,
    content: []const u8,
    start_char: ?u32 = null,
    end_char: ?u32 = null,
    start_time_ms: ?f32 = null,
    end_time_ms: ?f32 = null,
    /// Unit-scoped chunk of a document extraction (`_parent_unit_id`).
    parent_unit_id: ?[]const u8 = null,
    /// Diarized speaker index (`_speaker`: `SPEAKER_NN` or an index).
    speaker: ?u8 = null,
};

pub const max_speakers: u16 = 255;

/// A speaker as chunk reads label it (`SPEAKER_00`, ...) or as a bare index.
pub fn parseSpeaker(value: std.json.Value) !u8 {
    const index: u64 = switch (value) {
        .integer => |i| std.math.cast(u64, i) orelse return error.InvalidArtifactSeed,
        .number_string => |text| std.fmt.parseInt(u64, text, 10) catch return error.InvalidArtifactSeed,
        .string => |text| blk: {
            const prefix = "SPEAKER_";
            if (!std.mem.startsWith(u8, text, prefix) or text.len == prefix.len or text.len > prefix.len + 3) return error.InvalidArtifactSeed;
            for (text[prefix.len..]) |c| if (!std.ascii.isDigit(c)) return error.InvalidArtifactSeed;
            break :blk std.fmt.parseInt(u64, text[prefix.len..], 10) catch return error.InvalidArtifactSeed;
        },
        else => return error.InvalidArtifactSeed,
    };
    if (index >= max_speakers) return error.InvalidArtifactSeed;
    return @intCast(index);
}

fn jsonU32(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |i| std.math.cast(u32, i) orelse error.InvalidArtifactSeed,
        .number_string => |s| std.fmt.parseInt(u32, s, 10) catch error.InvalidArtifactSeed,
        else => error.InvalidArtifactSeed,
    };
}

fn jsonF64(value: std.json.Value) !f64 {
    const number: f64 = switch (value) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        .number_string => |s| std.fmt.parseFloat(f64, s) catch return error.InvalidArtifactSeed,
        else => return error.InvalidArtifactSeed,
    };
    if (!std.math.isFinite(number)) return error.InvalidArtifactSeed;
    return number;
}

fn jsonF32(value: std.json.Value) !f32 {
    const wide = try jsonF64(value);
    const narrow: f32 = @floatCast(wide);
    if (!std.math.isFinite(narrow)) return error.InvalidArtifactSeed;
    return narrow;
}

fn jsonTimeMs(value: std.json.Value) !f32 {
    const ms = try jsonF32(value);
    if (ms < 0) return error.InvalidArtifactSeed;
    return ms;
}

pub fn parseChunkObject(value: std.json.Value) !ChunkFields {
    if (value != .object) return error.InvalidArtifactSeed;
    var chunk_id: ?u32 = null;
    var alias_id: ?u32 = null;
    var content: ?[]const u8 = null;
    var out: ChunkFields = .{ .chunk_id = 0, .content = "" };
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const item = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "_chunk_id")) {
            chunk_id = try jsonU32(item);
        } else if (std.mem.eql(u8, key, "_id")) {
            alias_id = try jsonU32(item);
        } else if (std.mem.eql(u8, key, "_content")) {
            if (item != .string) return error.InvalidArtifactSeed;
            content = item.string;
        } else if (std.mem.eql(u8, key, "_start_char")) {
            out.start_char = try jsonU32(item);
        } else if (std.mem.eql(u8, key, "_end_char")) {
            out.end_char = try jsonU32(item);
        } else if (std.mem.eql(u8, key, "_start_time_ms")) {
            out.start_time_ms = try jsonTimeMs(item);
        } else if (std.mem.eql(u8, key, "_end_time_ms")) {
            out.end_time_ms = try jsonTimeMs(item);
        } else if (std.mem.eql(u8, key, "_parent_unit_id")) {
            // Unit-scoped chunks bind to one unit of a document extraction.
            if (item != .string or item.string.len == 0 or item.string.len > max_name_bytes) return error.InvalidArtifactSeed;
            out.parent_unit_id = item.string;
        } else if (std.mem.eql(u8, key, "_speaker")) {
            out.speaker = try parseSpeaker(item);
        } else return error.InvalidArtifactSeed;
    }
    if (chunk_id == null and alias_id == null) return error.InvalidArtifactSeed;
    if (chunk_id != null and alias_id != null and chunk_id.? != alias_id.?) return error.InvalidArtifactSeed;
    out.chunk_id = chunk_id orelse alias_id.?;
    out.content = content orelse return error.InvalidArtifactSeed;
    if ((out.start_char == null) != (out.end_char == null)) return error.InvalidArtifactSeed;
    if (out.start_char) |start| if (start > out.end_char.?) return error.InvalidArtifactSeed;
    if ((out.start_time_ms == null) != (out.end_time_ms == null)) return error.InvalidArtifactSeed;
    if (out.start_time_ms) |start| if (start > out.end_time_ms.?) return error.InvalidArtifactSeed;
    return out;
}

/// One reserved-field entry. `delete` removes accepted output so the producer
/// regenerates it; omission leaves accepted output untouched.
pub const Entry = union(enum) {
    delete,
    /// `_artifacts.<name>` envelope.
    artifact: Envelope,
    /// `_chunks.<name>` array.
    chunks: []const std.json.Value,
};

pub const Envelope = struct {
    value: std.json.Value,
    source: ?Source = null,
};

pub const Source = struct {
    generation: ?[]const u8 = null,
    sha256: ?[]const u8 = null,
    source_hash: ?u64 = null,
};

/// Canonical seeds of one write, parsed for acceptance.
pub const Seeds = struct {
    parsed: std.json.Parsed(std.json.Value),

    pub fn parse(alloc: Allocator, canonical: []const u8) !Seeds {
        const parsed = std.json.parseFromSlice(std.json.Value, alloc, canonical, .{ .parse_numbers = false }) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidArtifactSeed,
        };
        errdefer parsed.deinit();
        if (parsed.value != .object) return error.InvalidArtifactSeed;
        // Canonical bytes may arrive from a durable intent or a peer; repeat
        // the envelope checks rather than trusting their producer.
        if (parsed.value.object.get(artifacts_field)) |value| try validateArtifactsField(value);
        if (parsed.value.object.get(chunks_field)) |value| try validateChunksField(value);
        if (parsed.value.object.get(conflict_field)) |value| {
            if (value != .string or ConflictPolicy.parse(value.string) == null) return error.InvalidArtifactSeed;
        }
        var it = parsed.value.object.iterator();
        while (it.next()) |entry| if (!isSeedField(entry.key_ptr.*)) return error.InvalidArtifactSeed;
        return .{ .parsed = parsed };
    }

    pub fn conflictPolicy(self: *const Seeds) ConflictPolicy {
        const value = self.parsed.value.object.get(conflict_field) orelse return .keep;
        return ConflictPolicy.parse(value.string) orelse .keep;
    }

    pub fn deinit(self: *Seeds) void {
        self.parsed.deinit();
        self.* = undefined;
    }

    pub fn artifact(self: *const Seeds, name: []const u8) ?Entry {
        const field = self.parsed.value.object.get(artifacts_field) orelse return null;
        const value = field.object.get(name) orelse return null;
        if (value == .null) return .delete;
        var envelope: Envelope = .{ .value = value.object.get("value").? };
        if (value.object.get("source")) |source| {
            var out: Source = .{};
            if (source.object.get("generation")) |item| out.generation = item.string;
            if (source.object.get("sha256")) |item| out.sha256 = item.string;
            if (source.object.get("source_hash")) |item| out.source_hash = parseSourceHash(item.string);
            envelope.source = out;
        }
        return .{ .artifact = envelope };
    }

    pub fn chunks(self: *const Seeds, name: []const u8) ?Entry {
        const field = self.parsed.value.object.get(chunks_field) orelse return null;
        const value = field.object.get(name) orelse return null;
        if (value == .null) return .delete;
        return .{ .chunks = value.array.items };
    }

    /// Either field, for callers that accept the output under its owning
    /// reserved field and must reject the other as a kind mismatch.
    pub fn any(self: *const Seeds, name: []const u8) ?Entry {
        return self.artifact(name) orelse self.chunks(name);
    }

    pub const Name = struct { name: []const u8, field: enum { artifacts, chunks } };

    /// Every seeded name, for the "names that match no enrichment" check.
    pub fn names(self: *const Seeds, alloc: Allocator) ![]Name {
        var out = std.ArrayListUnmanaged(Name).empty;
        errdefer out.deinit(alloc);
        if (self.parsed.value.object.get(artifacts_field)) |field| {
            var it = field.object.iterator();
            while (it.next()) |entry| try out.append(alloc, .{ .name = entry.key_ptr.*, .field = .artifacts });
        }
        if (self.parsed.value.object.get(chunks_field)) |field| {
            var it = field.object.iterator();
            while (it.next()) |entry| try out.append(alloc, .{ .name = entry.key_ptr.*, .field = .chunks });
        }
        return try out.toOwnedSlice(alloc);
    }
};

/// Enrichment field mapping (`seed: {field, format, on_missing}`): an
/// existing record field supplies the enrichment's seed. Explicit
/// `_artifacts`/`_chunks` seeds in the same write win over the mapping.
pub const Mapping = struct {
    /// Dotted path of the record field holding the existing result.
    field: []const u8,
    format: Format = .auto,
    on_missing: OnMissing = .generate,

    pub const Format = enum {
        /// Follow the enrichment kind: text or JSON per the asset content
        /// type, a vector for document embeddings, chunk vectors for
        /// chunk-derived embeddings, and chunk arrays for chunk collections.
        auto,
        /// A string, accepted as a text asset value.
        text,
        /// Any JSON value, accepted as a JSON asset value.
        json,
        /// `[number, ...]`.
        vector,
        /// `[{chunk_id, window_index?, vector}]`.
        chunk_vectors,
        /// `[{_chunk_id, _content, ...}]`.
        chunks,
    };

    pub const OnMissing = enum {
        /// The producer generates the output as usual.
        generate,
        /// The write is rejected (`ArtifactSeedMissing`).
        fail,
        /// No producer runs; the output stays absent until a later write
        /// supplies a seed (explicitly or through this mapping).
        pending,
    };
};

/// Validates a public `seed` mapping object and returns its canonical JSON.
/// A transform whose every operation writes an artifact seed has no source
/// fields of its own: unless all it seeds is externally maintained (bound to
/// no input), it must name the source version its seeds bind to with
/// `expected_version` (a version predicate on its key).
/// Documents of this request written only by seed-only transforms: their
/// primary row and version stay as stored (#1791). A key also inserted,
/// deleted or transformed otherwise in the request is not seed-only.
pub fn seedOnlyDocumentsAlloc(alloc: Allocator, req: anytype) ![]const []const u8 {
    const transform_mod = @import("transform.zig");
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer out.deinit(alloc);
    outer: for (req.transforms) |transform| {
        for (req.writes) |write| if (std.mem.eql(u8, write.key, transform.key)) continue :outer;
        for (req.deletes) |key| if (std.mem.eql(u8, key, transform.key)) continue :outer;
        for (req.transforms) |other| {
            if (!std.mem.eql(u8, other.key, transform.key)) continue;
            if (!try transformOnlySeeds(transform_mod, other)) continue :outer;
        }
        for (out.items) |known| if (std.mem.eql(u8, known, transform.key)) continue :outer;
        try out.append(alloc, transform.key);
    }
    return try out.toOwnedSlice(alloc);
}

fn transformOnlySeeds(transform_mod: anytype, transform: anytype) !bool {
    var seeds = false;
    for (transform.operations) |operation| {
        const reserved = (transform_mod.reservedProjectionPath(operation.path) catch return false) orelse return false;
        switch (reserved.field) {
            .artifacts, .chunks => seeds = true,
            .seed_conflict => {},
            .embeddings => return false,
        }
    }
    return seeds;
}

pub fn requireSeedOnlyTransformBindings(manager: anytype, transforms: anytype, predicates: anytype) !void {
    const transform_mod = @import("transform.zig");
    const enrichment_catalog = @import("catalog/enrichment_catalog.zig");
    for (transforms) |transform| {
        if (transform.operations.len == 0) continue;
        var seeds = false;
        var bound_required = false;
        for (transform.operations) |operation| {
            const reserved = (try transform_mod.reservedProjectionPath(operation.path)) orelse break;
            switch (reserved.field) {
                .artifacts, .chunks => {
                    seeds = true;
                    const external = if (reserved.field == .artifacts) if (manager.getEnrichment(.asset, reserved.name)) |entry| enrichment_catalog.externallyMaintained(entry.*) else false else false;
                    if (!external) bound_required = true;
                },
                .seed_conflict => {},
                .embeddings => break,
            }
        } else {
            if (!seeds or !bound_required) continue;
            for (predicates) |predicate| {
                if (std.mem.eql(u8, predicate.key, transform.key)) break;
            } else return error.ArtifactSeedSourceBindingRequired;
        }
    }
}

pub fn canonicalMappingAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    if (value != .object) return error.InvalidArtifactSeed;
    var mapping: Mapping = .{ .field = "" };
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const item = entry.value_ptr.*;
        if (item != .string) return error.InvalidArtifactSeed;
        if (std.mem.eql(u8, key, "field")) {
            mapping.field = item.string;
        } else if (std.mem.eql(u8, key, "format")) {
            mapping.format = std.meta.stringToEnum(Mapping.Format, item.string) orelse return error.InvalidArtifactSeed;
        } else if (std.mem.eql(u8, key, "on_missing")) {
            mapping.on_missing = std.meta.stringToEnum(Mapping.OnMissing, item.string) orelse return error.InvalidArtifactSeed;
        } else return error.InvalidArtifactSeed;
    }
    if (mapping.field.len == 0 or mapping.field.len > max_name_bytes or mapping.field[0] == '_') return error.InvalidArtifactSeed;
    var parts = std.mem.splitScalar(u8, mapping.field, '.');
    while (parts.next()) |part| if (part.len == 0) return error.InvalidArtifactSeed;
    return try std.json.Stringify.valueAlloc(alloc, mapping, .{});
}

/// Whether a canonical mapping waits for its seed (`on_missing: pending`)
/// and `document_json` lacks its field: no producer may generate the output.
pub fn pendingMappingUnsatisfied(alloc: Allocator, canonical: []const u8, document_json: []const u8) !bool {
    if (canonical.len == 0) return false;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const mapping = try parseMapping(a, canonical);
    if (mapping.on_missing != .pending) return false;
    const document = std.json.parseFromSliceLeaky(std.json.Value, a, document_json, .{ .parse_numbers = false }) catch return false;
    return !mappedFieldPresent(mapping, document);
}

pub fn mappingWaitsForSeed(a: Allocator, canonical: []const u8) !bool {
    if (canonical.len == 0) return false;
    return (try parseMapping(a, canonical)).on_missing == .pending;
}

fn mappedFieldPresent(mapping: Mapping, document: std.json.Value) bool {
    var current = document;
    var parts = std.mem.splitScalar(u8, mapping.field, '.');
    while (parts.next()) |part| {
        if (current != .object) return false;
        current = current.object.get(part) orelse return false;
    }
    return current != .null;
}

pub fn parseMapping(a: Allocator, canonical: []const u8) !Mapping {
    return std.json.parseFromSliceLeaky(Mapping, a, canonical, .{ .allocate = .alloc_always }) catch error.InvalidArtifactSeed;
}

/// The seed a field mapping derives from `document`, or null when the field
/// is absent or null. `chunk_collection` selects the `_chunks` shape. The
/// entry borrows `document`'s values.
pub fn mappedEntry(mapping: Mapping, document: std.json.Value, chunk_collection: bool) !?Entry {
    var current = document;
    var parts = std.mem.splitScalar(u8, mapping.field, '.');
    while (parts.next()) |part| {
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    if (current == .null) return null;
    if (chunk_collection) {
        if (mapping.format != .auto and mapping.format != .chunks) return error.InvalidArtifactSeed;
        try validateChunkCollection(current);
        return .{ .chunks = current.array.items };
    }
    switch (mapping.format) {
        .chunks => return error.InvalidArtifactSeed,
        .text => if (current != .string) return error.InvalidArtifactSeed,
        .vector, .chunk_vectors => if (current != .array) return error.InvalidArtifactSeed,
        .auto, .json => {},
    }
    return .{ .artifact = .{ .value = current } };
}

test "enrichment field mappings derive typed seeds from record fields" {
    const alloc = std.testing.allocator;
    var spec = try parseForTest(alloc,
        \\{"field":"legacy.ocr","on_missing":"fail"}
    );
    defer spec.deinit();
    const canonical = try canonicalMappingAlloc(alloc, spec.value);
    defer alloc.free(canonical);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const mapping = try parseMapping(arena.allocator(), canonical);
    try std.testing.expectEqual(Mapping.OnMissing.fail, mapping.on_missing);

    var doc = try parseForTest(alloc,
        \\{"legacy":{"ocr":"scanned text"},"chunks":[{"_chunk_id":0,"_content":"a"}]}
    );
    defer doc.deinit();
    const entry = (try mappedEntry(mapping, doc.value, false)).?;
    try std.testing.expectEqualStrings("scanned text", entry.artifact.value.string);
    const chunk_mapping: Mapping = .{ .field = "chunks" };
    try std.testing.expectEqual(@as(usize, 1), (try mappedEntry(chunk_mapping, doc.value, true)).?.chunks.len);
    try std.testing.expect((try mappedEntry(.{ .field = "absent" }, doc.value, false)) == null);
    try std.testing.expectError(error.InvalidArtifactSeed, mappedEntry(.{ .field = "legacy", .format = .text }, doc.value, false));

    for ([_][]const u8{
        \\{"field":""}
        ,
        \\{"field":"_artifacts"}
        ,
        \\{"field":"a","format":"pdf"}
        ,
        \\{"field":"a","on_missing":"later"}
        ,
        \\{"field":"a","template":"{{x}}"}
    }) |json| {
        var bad = try parseForTest(alloc, json);
        defer bad.deinit();
        try std.testing.expectError(error.InvalidArtifactSeed, canonicalMappingAlloc(alloc, bad.value));
    }
}

/// Dense vectors are arrays of finite numbers, as in `_embeddings`.
pub fn decodeDenseVectorAlloc(alloc: Allocator, value: std.json.Value, expected_dims: u32) ![]f32 {
    if (value != .array or value.array.items.len == 0 or value.array.items.len > max_vector_dims) return error.InvalidArtifactSeed;
    if (expected_dims != 0 and value.array.items.len != expected_dims) return error.InvalidArtifactSeed;
    const vector = try alloc.alloc(f32, value.array.items.len);
    errdefer alloc.free(vector);
    for (value.array.items, vector) |item, *slot| slot.* = try jsonF32(item);
    return vector;
}

pub const SparseVector = struct {
    indices: []u32,
    values: []f32,

    pub fn deinit(self: SparseVector, alloc: Allocator) void {
        alloc.free(self.indices);
        alloc.free(self.values);
    }
};

/// Sparse vectors are token-weight maps (`{"7": 1.5}`), as in `_embeddings`,
/// or `{indices, values}`. Indices are returned ascending and unique.
pub fn decodeSparseVectorAlloc(alloc: Allocator, value: std.json.Value) !SparseVector {
    if (value != .object) return error.InvalidArtifactSeed;
    var pairs = std.ArrayListUnmanaged(struct { index: u32, weight: f32 }).empty;
    defer pairs.deinit(alloc);
    if (value.object.get("indices")) |indices| {
        const weights = value.object.get("values") orelse return error.InvalidArtifactSeed;
        if (value.object.count() != 2 or indices != .array or weights != .array or indices.array.items.len != weights.array.items.len) return error.InvalidArtifactSeed;
        for (indices.array.items, weights.array.items) |index, weight| try pairs.append(alloc, .{ .index = try jsonU32(index), .weight = try jsonF32(weight) });
    } else {
        var it = value.object.iterator();
        while (it.next()) |entry| {
            const index = std.fmt.parseUnsigned(u32, entry.key_ptr.*, 10) catch return error.InvalidArtifactSeed;
            try pairs.append(alloc, .{ .index = index, .weight = try jsonF32(entry.value_ptr.*) });
        }
    }
    if (pairs.items.len > max_vector_dims) return error.InvalidArtifactSeed;
    std.mem.sort(@TypeOf(pairs.items[0]), pairs.items, {}, struct {
        fn less(_: void, a: @TypeOf(pairs.items[0]), b: @TypeOf(pairs.items[0])) bool {
            return a.index < b.index;
        }
    }.less);
    const indices = try alloc.alloc(u32, pairs.items.len);
    errdefer alloc.free(indices);
    const values = try alloc.alloc(f32, pairs.items.len);
    errdefer alloc.free(values);
    for (pairs.items, indices, values, 0..) |pair, *index, *weight, i| {
        if (i > 0 and pairs.items[i - 1].index == pair.index) return error.InvalidArtifactSeed;
        index.* = pair.index;
        weight.* = pair.weight;
    }
    return .{ .indices = indices, .values = values };
}

test "artifact seed sparse vectors accept token-weight maps and index arrays" {
    const alloc = std.testing.allocator;
    var parsed = try parseForTest(alloc,
        \\{"map":{"42":2.0,"7":1.5},"arrays":{"indices":[3,1],"values":[0.5,0.25]},"dup":{"indices":[1,1],"values":[1,2]},"bad":{"x":1}}
    );
    defer parsed.deinit();
    const map = try decodeSparseVectorAlloc(alloc, parsed.value.object.get("map").?);
    defer map.deinit(alloc);
    try std.testing.expectEqualSlices(u32, &.{ 7, 42 }, map.indices);
    try std.testing.expectEqualSlices(f32, &.{ 1.5, 2.0 }, map.values);
    const arrays = try decodeSparseVectorAlloc(alloc, parsed.value.object.get("arrays").?);
    defer arrays.deinit(alloc);
    try std.testing.expectEqualSlices(u32, &.{ 1, 3 }, arrays.indices);
    try std.testing.expectError(error.InvalidArtifactSeed, decodeSparseVectorAlloc(alloc, parsed.value.object.get("dup").?));
    try std.testing.expectError(error.InvalidArtifactSeed, decodeSparseVectorAlloc(alloc, parsed.value.object.get("bad").?));
}

pub const ChunkVector = struct {
    chunk_id: u32,
    /// Null means one vector for the whole chunk, stored unsplit (window 0).
    window_index: ?u32,
    vector: []f32,
};

/// One seeded vector of a document extraction unit chunk. Unit chunks are
/// embedded whole, so there are no windows.
pub const UnitChunkVector = struct {
    parent_unit_id: []const u8,
    chunk_id: u32,
    vector: []f32,
};

pub fn freeUnitChunkVectors(alloc: Allocator, vectors: []UnitChunkVector) void {
    for (vectors) |entry| alloc.free(entry.vector);
    if (vectors.len > 0) alloc.free(vectors);
}

/// A chunk vector names its extraction unit as `unit_id` (the internal
/// identity `(chunk_id, window_index, unit_id)`) or `parent_unit_id` (the
/// chunk row's `_parent_unit_id`). Both may appear only when they agree.
fn isUnitIdKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "unit_id") or std.mem.eql(u8, key, "parent_unit_id");
}

fn unitIdField(field: std.json.Value, prior: ?[]const u8) ![]const u8 {
    if (field != .string or field.string.len == 0 or field.string.len > max_name_bytes) return error.InvalidArtifactSeed;
    if (prior) |seen| if (!std.mem.eql(u8, seen, field.string)) return error.InvalidArtifactSeed;
    return field.string;
}

/// `[{parent_unit_id, chunk_id, vector}]`, each (unit, chunk) at most once.
/// Unit ids borrow from `value`.
pub fn decodeUnitChunkVectorsAlloc(alloc: Allocator, value: std.json.Value, expected_dims: u32) ![]UnitChunkVector {
    if (value != .array or value.array.items.len == 0 or value.array.items.len > max_seed_chunks * 64) return error.InvalidArtifactSeed;
    var out = std.ArrayListUnmanaged(UnitChunkVector).empty;
    errdefer {
        for (out.items) |entry| alloc.free(entry.vector);
        out.deinit(alloc);
    }
    for (value.array.items) |item| {
        if (item != .object) return error.InvalidArtifactSeed;
        var unit_id: ?[]const u8 = null;
        var chunk_id: ?u32 = null;
        var vector_value: ?std.json.Value = null;
        var it = item.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            const field = entry.value_ptr.*;
            if (isUnitIdKey(key)) {
                unit_id = try unitIdField(field, unit_id);
            } else if (std.mem.eql(u8, key, "chunk_id")) {
                chunk_id = try jsonU32(field);
            } else if (std.mem.eql(u8, key, "vector")) {
                vector_value = field;
            } else if (std.mem.eql(u8, key, "window_index")) {
                return error.ArtifactSeedNotAccepted;
            } else return error.InvalidArtifactSeed;
        }
        const vector = try decodeDenseVectorAlloc(alloc, vector_value orelse return error.InvalidArtifactSeed, expected_dims);
        errdefer alloc.free(vector);
        if (out.items.len != 0 and out.items[0].vector.len != vector.len) return error.InvalidArtifactSeed;
        try out.append(alloc, .{
            .parent_unit_id = unit_id orelse return error.InvalidArtifactSeed,
            .chunk_id = chunk_id orelse return error.InvalidArtifactSeed,
            .vector = vector,
        });
    }
    for (out.items, 0..) |entry, i| {
        for (out.items[i + 1 ..]) |other| {
            if (entry.chunk_id == other.chunk_id and std.mem.eql(u8, entry.parent_unit_id, other.parent_unit_id)) return error.InvalidArtifactSeed;
        }
    }
    return try out.toOwnedSlice(alloc);
}

test "artifact seed unit chunk vectors name their unit and never window" {
    const alloc = std.testing.allocator;
    var ok = try parseForTest(alloc,
        \\[{"parent_unit_id":"page:000001","chunk_id":0,"vector":[1,2]},{"parent_unit_id":"page:000002","chunk_id":0,"vector":[3,4]}]
    );
    defer ok.deinit();
    const vectors = try decodeUnitChunkVectorsAlloc(alloc, ok.value, 2);
    defer freeUnitChunkVectors(alloc, vectors);
    try std.testing.expectEqualStrings("page:000002", vectors[1].parent_unit_id);
    var aliased = try parseForTest(alloc, "[{\"unit_id\":\"page:000003\",\"parent_unit_id\":\"page:000003\",\"chunk_id\":0,\"vector\":[1,2]}]");
    defer aliased.deinit();
    const aliased_vectors = try decodeUnitChunkVectorsAlloc(alloc, aliased.value, 2);
    defer freeUnitChunkVectors(alloc, aliased_vectors);
    try std.testing.expectEqualStrings("page:000003", aliased_vectors[0].parent_unit_id);
    var disagreeing = try parseForTest(alloc, "[{\"unit_id\":\"a\",\"parent_unit_id\":\"b\",\"chunk_id\":0,\"vector\":[1,2]}]");
    defer disagreeing.deinit();
    try std.testing.expectError(error.InvalidArtifactSeed, decodeUnitChunkVectorsAlloc(alloc, disagreeing.value, 2));
    try std.testing.expectError(error.ArtifactSeedNotAccepted, decodeChunkVectorsAlloc(alloc, ok.value, 2));
    for ([_][]const u8{
        "[{\"chunk_id\":0,\"vector\":[1,2]}]",
        "[{\"parent_unit_id\":\"p\",\"chunk_id\":0,\"vector\":[1,2]},{\"parent_unit_id\":\"p\",\"chunk_id\":0,\"vector\":[1,2]}]",
        "[{\"parent_unit_id\":\"p\",\"chunk_id\":0,\"vector\":[1,2,3]}]",
        "[]",
    }) |bad| {
        var doc = try parseForTest(alloc, bad);
        defer doc.deinit();
        try std.testing.expectError(error.InvalidArtifactSeed, decodeUnitChunkVectorsAlloc(alloc, doc.value, 2));
    }
    var windowed = try parseForTest(alloc, "[{\"parent_unit_id\":\"p\",\"chunk_id\":0,\"window_index\":0,\"vector\":[1,2]}]");
    defer windowed.deinit();
    try std.testing.expectError(error.ArtifactSeedNotAccepted, decodeUnitChunkVectorsAlloc(alloc, windowed.value, 2));
}

/// One seeded sparse vector of a chunk: of a plain chunk collection, or of
/// a document extraction's unit chunk (`parent_unit_id`). Sparse chunk
/// vectors are never windowed.
pub const SparseChunkVector = struct {
    parent_unit_id: ?[]const u8 = null,
    chunk_id: u32,
    sparse: SparseVector,
};

pub fn freeSparseChunkVectors(alloc: Allocator, vectors: []SparseChunkVector) void {
    for (vectors) |entry| entry.sparse.deinit(alloc);
    if (vectors.len > 0) alloc.free(vectors);
}

pub const ChunkScope = enum { plain, unit };

/// `[{chunk_id, parent_unit_id?, indices, values}]`, each chunk at most once.
/// `parent_unit_id` is required for unit chunks and refused otherwise.
/// Unit ids borrow from `value`.
pub fn decodeSparseChunkVectorsAlloc(alloc: Allocator, value: std.json.Value, scope: ChunkScope) ![]SparseChunkVector {
    if (value != .array or value.array.items.len == 0 or value.array.items.len > max_seed_chunks * 64) return error.InvalidArtifactSeed;
    var out = std.ArrayListUnmanaged(SparseChunkVector).empty;
    errdefer {
        for (out.items) |entry| entry.sparse.deinit(alloc);
        out.deinit(alloc);
    }
    for (value.array.items) |item| {
        if (item != .object) return error.InvalidArtifactSeed;
        var unit_id: ?[]const u8 = null;
        var chunk_id: ?u32 = null;
        var weights = std.json.ObjectMap.empty;
        defer weights.deinit(alloc);
        var it = item.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            const field = entry.value_ptr.*;
            if (std.mem.eql(u8, key, "chunk_id")) {
                chunk_id = try jsonU32(field);
            } else if (isUnitIdKey(key)) {
                if (scope == .plain) return error.ArtifactSeedNotAccepted;
                unit_id = try unitIdField(field, unit_id);
            } else if (std.mem.eql(u8, key, "indices") or std.mem.eql(u8, key, "values")) {
                try weights.put(alloc, key, field);
            } else if (std.mem.eql(u8, key, "window_index")) {
                return error.ArtifactSeedNotAccepted;
            } else return error.InvalidArtifactSeed;
        }
        if (scope == .unit and unit_id == null) return error.InvalidArtifactSeed;
        const sparse = try decodeSparseVectorAlloc(alloc, .{ .object = weights });
        errdefer sparse.deinit(alloc);
        try out.append(alloc, .{ .parent_unit_id = unit_id, .chunk_id = chunk_id orelse return error.InvalidArtifactSeed, .sparse = sparse });
    }
    for (out.items, 0..) |entry, i| {
        for (out.items[i + 1 ..]) |other| {
            if (entry.chunk_id != other.chunk_id) continue;
            if (entry.parent_unit_id == null or std.mem.eql(u8, entry.parent_unit_id.?, other.parent_unit_id.?)) return error.InvalidArtifactSeed;
        }
    }
    return try out.toOwnedSlice(alloc);
}

test "artifact seed sparse chunk vectors bind per chunk and unit" {
    const alloc = std.testing.allocator;
    var plain = try parseForTest(alloc,
        \\[{"chunk_id":0,"indices":[1,4],"values":[0.5,0.25]},{"chunk_id":1,"indices":[2],"values":[1]}]
    );
    defer plain.deinit();
    const vectors = try decodeSparseChunkVectorsAlloc(alloc, plain.value, .plain);
    defer freeSparseChunkVectors(alloc, vectors);
    try std.testing.expectEqualSlices(u32, &.{ 1, 4 }, vectors[0].sparse.indices);
    try std.testing.expectError(error.InvalidArtifactSeed, decodeSparseChunkVectorsAlloc(alloc, plain.value, .unit));

    var unit = try parseForTest(alloc,
        \\[{"parent_unit_id":"page:000001","chunk_id":0,"indices":[3],"values":[1]},{"parent_unit_id":"page:000002","chunk_id":0,"indices":[3],"values":[2]}]
    );
    defer unit.deinit();
    const unit_vectors = try decodeSparseChunkVectorsAlloc(alloc, unit.value, .unit);
    defer freeSparseChunkVectors(alloc, unit_vectors);
    try std.testing.expectEqualStrings("page:000002", unit_vectors[1].parent_unit_id.?);
    try std.testing.expectError(error.ArtifactSeedNotAccepted, decodeSparseChunkVectorsAlloc(alloc, unit.value, .plain));

    for ([_][]const u8{
        "[{\"chunk_id\":0,\"indices\":[1],\"values\":[1]},{\"chunk_id\":0,\"indices\":[2],\"values\":[1]}]",
        "[{\"chunk_id\":0,\"indices\":[1,1],\"values\":[1,1]}]",
        "[{\"indices\":[1],\"values\":[1]}]",
        "[{\"chunk_id\":0,\"vector\":[1]}]",
    }) |bad| {
        var doc = try parseForTest(alloc, bad);
        defer doc.deinit();
        try std.testing.expectError(error.InvalidArtifactSeed, decodeSparseChunkVectorsAlloc(alloc, doc.value, .plain));
    }
}

pub fn freeChunkVectors(alloc: Allocator, vectors: []ChunkVector) void {
    for (vectors) |entry| alloc.free(entry.vector);
    alloc.free(vectors);
}

/// `[{chunk_id, window_index?, vector}]`, sorted by `(chunk_id, window)`.
/// A chunk's windows must be complete and contiguous from zero; an entry
/// without `window_index` must be that chunk's only vector.
pub fn decodeChunkVectorsAlloc(alloc: Allocator, value: std.json.Value, expected_dims: u32) ![]ChunkVector {
    if (value != .array or value.array.items.len > max_seed_chunks * 64) return error.InvalidArtifactSeed;
    var out = std.ArrayListUnmanaged(ChunkVector).empty;
    errdefer {
        for (out.items) |entry| alloc.free(entry.vector);
        out.deinit(alloc);
    }
    for (value.array.items) |item| {
        if (item != .object) return error.InvalidArtifactSeed;
        var chunk_id: ?u32 = null;
        var window_index: ?u32 = null;
        var vector_value: ?std.json.Value = null;
        var it = item.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            if (std.mem.eql(u8, key, "chunk_id")) {
                chunk_id = try jsonU32(entry.value_ptr.*);
            } else if (std.mem.eql(u8, key, "window_index")) {
                window_index = try jsonU32(entry.value_ptr.*);
            } else if (std.mem.eql(u8, key, "vector")) {
                vector_value = entry.value_ptr.*;
            } else if (std.mem.eql(u8, key, "unit_id") or std.mem.eql(u8, key, "parent_unit_id")) {
                // Vectors of extraction unit chunks are accepted by that
                // extraction, never by a plain chunk-vector producer.
                return error.ArtifactSeedNotAccepted;
            } else return error.InvalidArtifactSeed;
        }
        const vector = try decodeDenseVectorAlloc(alloc, vector_value orelse return error.InvalidArtifactSeed, expected_dims);
        errdefer alloc.free(vector);
        if (out.items.len != 0) {
            // Consistent dimensions even when the contract leaves them open.
            if (out.items[0].vector.len != vector.len) return error.InvalidArtifactSeed;
        }
        try out.append(alloc, .{ .chunk_id = chunk_id orelse return error.InvalidArtifactSeed, .window_index = window_index, .vector = vector });
    }
    std.mem.sort(ChunkVector, out.items, {}, struct {
        fn less(_: void, a: ChunkVector, b: ChunkVector) bool {
            if (a.chunk_id != b.chunk_id) return a.chunk_id < b.chunk_id;
            return (a.window_index orelse 0) < (b.window_index orelse 0);
        }
    }.less);
    var i: usize = 0;
    while (i < out.items.len) {
        var j = i;
        while (j < out.items.len and out.items[j].chunk_id == out.items[i].chunk_id) : (j += 1) {}
        const group = out.items[i..j];
        if (group.len == 1 and group[0].window_index == null) {
            i = j;
            continue;
        }
        for (group, 0..) |entry, expected| {
            const window = entry.window_index orelse return error.InvalidArtifactSeed;
            if (window != expected) return error.InvalidArtifactSeed;
        }
        i = j;
    }
    return try out.toOwnedSlice(alloc);
}

/// Seeded chunks, borrowing their content from the parsed seeds. Callers map
/// them onto producer chunks. `source_len` bounds character spans when the
/// bound source text is known.
pub fn decodeChunksAlloc(alloc: Allocator, items: []const std.json.Value, source_len: ?usize) ![]ChunkFields {
    const out = try alloc.alloc(ChunkFields, items.len);
    errdefer alloc.free(out);
    var previous: ?u32 = null;
    for (items, out) |item, *slot| {
        const fields = try parseChunkObject(item);
        // Unit-scoped and diarized chunks belong to a document extraction
        // consumer; a plain chunk producer cannot accept them.
        if (fields.parent_unit_id != null or fields.speaker != null) return error.ArtifactSeedNotAccepted;
        if (previous) |prior| if (fields.chunk_id <= prior) return error.InvalidArtifactSeed;
        previous = fields.chunk_id;
        if (source_len) |len| if (fields.end_char) |end| if (end > len) return error.InvalidArtifactSeed;
        slot.* = fields;
    }
    return out;
}

/// The seeded chunks of one extraction unit, in order. Every item of a
/// unit-scoped collection names its unit; spans lie within the unit text.
pub fn decodeUnitChunksAlloc(alloc: Allocator, items: []const std.json.Value, unit_id: []const u8, unit_text_len: usize) ![]ChunkFields {
    var out = std.ArrayListUnmanaged(ChunkFields).empty;
    errdefer out.deinit(alloc);
    var previous: ?u32 = null;
    for (items) |item| {
        const fields = try parseChunkObject(item);
        const parent = fields.parent_unit_id orelse return error.InvalidArtifactSeed;
        if (!std.mem.eql(u8, parent, unit_id)) continue;
        if (previous) |prior| if (fields.chunk_id <= prior) return error.InvalidArtifactSeed;
        previous = fields.chunk_id;
        if (fields.end_char) |end| if (end > unit_text_len) return error.InvalidArtifactSeed;
        try out.append(alloc, fields);
    }
    return try out.toOwnedSlice(alloc);
}

/// One phrase of a seeded transcript unit, in unit text coordinates.
pub const TranscriptSpan = struct {
    char_start: u32,
    char_end: u32,
    start_ms: u64,
    end_ms: u64,
    speaker: ?u8 = null,
};

/// One unit of a rich document extraction seed (`{"units": [...]}`): a page,
/// a transcript or any other unit with the layout data the caller has.
/// Nothing absent is fabricated.
pub const ExtractionUnit = struct {
    text: []const u8,
    unit_type: []const u8,
    page_number: ?u32 = null,
    page_label: ?[]const u8 = null,
    page_bbox: ?[4]f64 = null,
    page_rotation: ?i32 = null,
    spans: []TranscriptSpan = &.{},
    /// Grounded text regions (citations, highlights).
    text_regions: []TextBox = &.{},
    /// Reading-order text blocks, listed in reading order.
    text_blocks: []TextBox = &.{},
};

/// Geometry of a span of unit text on its page, in the page's coordinate
/// space (`page_bbox`, source page points): `[x0, y0, x1, y1]`.
pub const TextBox = struct {
    /// UTF-8 byte offsets into the unit text.
    span: [2]u32,
    bbox: [4]f64,
    font_size: ?f64 = null,
    bold: ?bool = null,
    italic: ?bool = null,
};

/// Coordinates within this distance of the page box count as inside it.
const page_box_tolerance: f64 = 0.5;

fn decodeTextBoxesAlloc(alloc: Allocator, value: std.json.Value, text: []const u8, page_bbox: ?[4]f64, blocks: bool) ![]TextBox {
    if (value != .array or value.array.items.len > max_transcript_spans) return error.InvalidArtifactSeed;
    if (value.array.items.len == 0) return &.{};
    // A box means nothing without the page coordinate space it lies in.
    const page = page_bbox orelse return error.InvalidArtifactSeed;
    const out = try alloc.alloc(TextBox, value.array.items.len);
    errdefer alloc.free(out);
    var previous_start: u32 = 0;
    for (value.array.items, out, 0..) |item, *box, index| {
        if (item != .object) return error.InvalidArtifactSeed;
        var span: ?[2]u32 = null;
        var bbox: ?[4]f64 = null;
        box.* = .{ .span = undefined, .bbox = undefined };
        var it = item.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            const field = entry.value_ptr.*;
            if (std.mem.eql(u8, key, "span")) {
                if (field != .array or field.array.items.len != 2) return error.InvalidArtifactSeed;
                span = .{ try jsonU32(field.array.items[0]), try jsonU32(field.array.items[1]) };
            } else if (std.mem.eql(u8, key, "bbox")) {
                if (field != .array or field.array.items.len != 4) return error.InvalidArtifactSeed;
                var coordinates: [4]f64 = undefined;
                for (field.array.items, &coordinates) |coordinate, *slot| slot.* = try jsonF64(coordinate);
                bbox = coordinates;
            } else if (std.mem.eql(u8, key, "font_size")) {
                if (field == .null) continue;
                const size = try jsonF64(field);
                if (size <= 0) return error.InvalidArtifactSeed;
                box.font_size = size;
            } else if (std.mem.eql(u8, key, "bold") or std.mem.eql(u8, key, "italic")) {
                if (field == .null) continue;
                if (field != .bool) return error.InvalidArtifactSeed;
                if (key[0] == 'b') box.bold = field.bool else box.italic = field.bool;
            } else if (blocks and std.mem.eql(u8, key, "reading_order")) {
                // Blocks are listed in reading order; an explicit order agrees.
                if (try jsonU32(field) != index) return error.InvalidArtifactSeed;
            } else return error.InvalidArtifactSeed;
        }
        box.span = span orelse return error.InvalidArtifactSeed;
        box.bbox = bbox orelse return error.InvalidArtifactSeed;
        const start = box.span[0];
        const end = box.span[1];
        if (start > end or end > text.len) return error.InvalidArtifactSeed;
        if (!utf8Boundary(text, start) or !utf8Boundary(text, end)) return error.InvalidArtifactSeed;
        // Ordered by position in the text, as the native extractor emits them.
        if (start < previous_start) return error.InvalidArtifactSeed;
        previous_start = start;
        const b = box.bbox;
        if (b[0] > b[2] or b[1] > b[3]) return error.InvalidArtifactSeed;
        const min_x = @min(page[0], page[2]) - page_box_tolerance;
        const max_x = @max(page[0], page[2]) + page_box_tolerance;
        const min_y = @min(page[1], page[3]) - page_box_tolerance;
        const max_y = @max(page[1], page[3]) + page_box_tolerance;
        if (b[0] < min_x or b[2] > max_x or b[1] < min_y or b[3] > max_y) return error.InvalidArtifactSeed;
    }
    return out;
}

fn utf8Boundary(text: []const u8, offset: u32) bool {
    if (offset == 0 or offset == text.len) return true;
    return (text[offset] & 0xC0) != 0x80;
}

pub const max_extraction_units: usize = 65536;
pub const max_transcript_spans: usize = 1 << 20;

pub fn freeExtractionUnits(alloc: Allocator, units: []ExtractionUnit) void {
    for (units) |unit| freeExtractionUnitParts(alloc, unit);
    if (units.len > 0) alloc.free(units);
}

fn freeExtractionUnitParts(alloc: Allocator, unit: ExtractionUnit) void {
    if (unit.spans.len > 0) alloc.free(unit.spans);
    if (unit.text_regions.len > 0) alloc.free(unit.text_regions);
    if (unit.text_blocks.len > 0) alloc.free(unit.text_blocks);
}

fn validUnitType(text: []const u8) bool {
    if (text.len == 0 or text.len > 64) return false;
    for (text) |c| if (!(std.ascii.isLower(c) or std.ascii.isDigit(c) or c == '_')) return false;
    return true;
}

/// A partial extraction seed (`{"units": [...], "partial": true}`) carries
/// some pages; the producer extracts the rest.
pub fn extractionSeedPartial(value: std.json.Value) bool {
    if (value != .object) return false;
    const flag = value.object.get("partial") orelse return false;
    return flag == .bool and flag.bool;
}

/// Units of a document extraction seed. `{"text": ...}` (or a string) is the
/// plain-text form and returns no units; `{"units": [...]}` is the rich form.
/// A partial seed's units must all be pages, each page at most once.
/// Strings borrow from `value`.
pub fn decodeExtractionUnitsAlloc(alloc: Allocator, value: std.json.Value) ![]ExtractionUnit {
    if (value == .string) return &.{};
    if (value != .object) return error.InvalidArtifactSeed;
    if (value.object.get("text") != null) {
        _ = try assetText(value);
        return &.{};
    }
    const partial = extractionSeedPartial(value);
    if (value.object.count() != @as(usize, if (value.object.get("partial") != null) 2 else 1)) return error.InvalidArtifactSeed;
    if (value.object.get("partial")) |flag| if (flag != .bool) return error.InvalidArtifactSeed;
    const units_value = value.object.get("units") orelse return error.InvalidArtifactSeed;
    if (units_value != .array or units_value.array.items.len == 0 or units_value.array.items.len > max_extraction_units) return error.InvalidArtifactSeed;
    const out = try alloc.alloc(ExtractionUnit, units_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |unit| freeExtractionUnitParts(alloc, unit);
        alloc.free(out);
    }
    for (units_value.array.items, out) |item, *unit| {
        if (item != .object) return error.InvalidArtifactSeed;
        var text: ?[]const u8 = null;
        var unit_type: ?[]const u8 = null;
        var parsed: ExtractionUnit = .{ .text = "", .unit_type = "" };
        var spans_value: ?std.json.Value = null;
        var regions_value: ?std.json.Value = null;
        var blocks_value: ?std.json.Value = null;
        var it = item.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            const field = entry.value_ptr.*;
            if (std.mem.eql(u8, key, "text")) {
                if (field != .string) return error.InvalidArtifactSeed;
                text = field.string;
            } else if (std.mem.eql(u8, key, "unit_type")) {
                if (field != .string or !validUnitType(field.string)) return error.InvalidArtifactSeed;
                unit_type = field.string;
            } else if (std.mem.eql(u8, key, "page_number")) {
                const page = try jsonU32(field);
                if (page == 0) return error.InvalidArtifactSeed;
                parsed.page_number = page;
            } else if (std.mem.eql(u8, key, "page_label")) {
                if (field != .string or field.string.len == 0 or field.string.len > max_name_bytes) return error.InvalidArtifactSeed;
                parsed.page_label = field.string;
            } else if (std.mem.eql(u8, key, "page_bbox")) {
                if (field != .array or field.array.items.len != 4) return error.InvalidArtifactSeed;
                var bbox: [4]f64 = undefined;
                for (field.array.items, &bbox) |coordinate, *slot| slot.* = try jsonF64(coordinate);
                parsed.page_bbox = bbox;
            } else if (std.mem.eql(u8, key, "page_rotation")) {
                const rotation: i64 = switch (field) {
                    .integer => |i| i,
                    .number_string => |raw| std.fmt.parseInt(i64, raw, 10) catch return error.InvalidArtifactSeed,
                    else => return error.InvalidArtifactSeed,
                };
                if (rotation != 0 and rotation != 90 and rotation != 180 and rotation != 270) return error.InvalidArtifactSeed;
                parsed.page_rotation = @intCast(rotation);
            } else if (std.mem.eql(u8, key, "spans")) {
                spans_value = field;
            } else if (std.mem.eql(u8, key, "text_regions")) {
                regions_value = field;
            } else if (std.mem.eql(u8, key, "text_blocks")) {
                blocks_value = field;
            } else return error.InvalidArtifactSeed;
        }
        parsed.text = text orelse return error.InvalidArtifactSeed;
        if (spans_value) |spans| {
            if (spans != .array or spans.array.items.len > max_transcript_spans) return error.InvalidArtifactSeed;
            const decoded = try alloc.alloc(TranscriptSpan, spans.array.items.len);
            errdefer alloc.free(decoded);
            var covered: u32 = 0;
            for (spans.array.items, decoded) |raw, *span| {
                if (raw != .object) return error.InvalidArtifactSeed;
                var char_start: ?u32 = null;
                var char_end: ?u32 = null;
                var start_ms: ?u64 = null;
                var end_ms: ?u64 = null;
                var speaker: ?u8 = null;
                var span_it = raw.object.iterator();
                while (span_it.next()) |entry| {
                    const key = entry.key_ptr.*;
                    const field = entry.value_ptr.*;
                    if (std.mem.eql(u8, key, "char_start")) {
                        char_start = try jsonU32(field);
                    } else if (std.mem.eql(u8, key, "char_end")) {
                        char_end = try jsonU32(field);
                    } else if (std.mem.eql(u8, key, "start_ms")) {
                        start_ms = try jsonU64(field);
                    } else if (std.mem.eql(u8, key, "end_ms")) {
                        end_ms = try jsonU64(field);
                    } else if (std.mem.eql(u8, key, "speaker")) {
                        speaker = try parseSpeaker(field);
                    } else return error.InvalidArtifactSeed;
                }
                span.* = .{
                    .char_start = char_start orelse return error.InvalidArtifactSeed,
                    .char_end = char_end orelse return error.InvalidArtifactSeed,
                    .start_ms = start_ms orelse return error.InvalidArtifactSeed,
                    .end_ms = end_ms orelse return error.InvalidArtifactSeed,
                    .speaker = speaker,
                };
                // Disjoint, ordered phrases within the unit text.
                if (span.char_start > span.char_end or span.char_end > parsed.text.len or span.char_start < covered) return error.InvalidArtifactSeed;
                if (span.start_ms > span.end_ms) return error.InvalidArtifactSeed;
                covered = span.char_end;
            }
            parsed.spans = decoded;
        }
        errdefer if (parsed.spans.len > 0) alloc.free(parsed.spans);
        if (regions_value) |regions| parsed.text_regions = try decodeTextBoxesAlloc(alloc, regions, parsed.text, parsed.page_bbox, false);
        errdefer if (parsed.text_regions.len > 0) alloc.free(parsed.text_regions);
        if (blocks_value) |blocks| parsed.text_blocks = try decodeTextBoxesAlloc(alloc, blocks, parsed.text, parsed.page_bbox, true);
        // `document` is the plain text route's unit type, so a single plain
        // unit is the same output as the `{"text": ...}` form.
        parsed.unit_type = unit_type orelse if (parsed.page_number != null) "page" else if (parsed.spans.len > 0) "transcript" else "document";
        unit.* = parsed;
        initialized += 1;
    }
    if (partial) {
        // Pages stand in for the producer's own pages by number.
        for (out, 0..) |unit, i| {
            const page = unit.page_number orelse return error.InvalidArtifactSeed;
            if (page == 0 or !std.mem.eql(u8, unit.unit_type, "page")) return error.InvalidArtifactSeed;
            for (out[0..i]) |other| if (other.page_number.? == page) return error.InvalidArtifactSeed;
        }
    }
    return out;
}

fn jsonU64(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |i| std.math.cast(u64, i) orelse error.InvalidArtifactSeed,
        .number_string => |text| std.fmt.parseInt(u64, text, 10) catch error.InvalidArtifactSeed,
        else => error.InvalidArtifactSeed,
    };
}

/// Asset seed bytes as stored for the enrichment's content type: a JSON
/// string for text assets, any JSON value for JSON assets. Text extraction
/// seeds may also use `{ "text": ... }`.
pub fn assetBytesAlloc(alloc: Allocator, value: std.json.Value, json_content: bool) ![]u8 {
    if (json_content) return try std.json.Stringify.valueAlloc(alloc, value, .{});
    return try alloc.dupe(u8, try assetText(value));
}

pub fn assetText(value: std.json.Value) ![]const u8 {
    switch (value) {
        .string => |text| return text,
        .object => |object| {
            if (object.count() != 1) return error.InvalidArtifactSeed;
            const text = object.get("text") orelse return error.InvalidArtifactSeed;
            if (text != .string) return error.InvalidArtifactSeed;
            return text.string;
        },
        else => return error.InvalidArtifactSeed,
    }
}

fn parseForTest(alloc: Allocator, json: []const u8) !std.json.Parsed(std.json.Value) {
    return std.json.parseFromSlice(std.json.Value, alloc, json, .{ .parse_numbers = false });
}

test "artifact seeds canonicalize both reserved fields and round-trip" {
    const alloc = std.testing.allocator;
    var doc = try parseForTest(alloc,
        \\{"url":"gs://a","_artifacts":{"extracted_text":{"value":{"text":"hi"},"source":{"generation":"17"},"provenance":{"producer":"legacy-ocr"}},"doc_vec":{"value":[0.5,1]},"gone":null},
        \\ "_chunks":{"body_chunks":[{"_chunk_id":0,"_content":"h","_start_char":0,"_end_char":1},{"_id":1,"_content":"i"}]}}
    );
    defer doc.deinit();
    const canonical = (try extractCanonicalAlloc(alloc, doc.value.object)).?;
    defer alloc.free(canonical);
    var seeds = try Seeds.parse(alloc, canonical);
    defer seeds.deinit();

    const text = seeds.artifact("extracted_text").?.artifact;
    try std.testing.expectEqualStrings("hi", try assetText(text.value));
    try std.testing.expectEqualStrings("17", text.source.?.generation.?);
    try std.testing.expect(seeds.artifact("gone").? == .delete);
    try std.testing.expect(seeds.artifact("missing") == null);
    try std.testing.expect(seeds.artifact("body_chunks") == null);

    const vector = try decodeDenseVectorAlloc(alloc, seeds.artifact("doc_vec").?.artifact.value, 2);
    defer alloc.free(vector);
    try std.testing.expectEqualSlices(f32, &.{ 0.5, 1 }, vector);
    try std.testing.expectError(error.InvalidArtifactSeed, decodeDenseVectorAlloc(alloc, seeds.artifact("doc_vec").?.artifact.value, 3));

    const chunks = try decodeChunksAlloc(alloc, seeds.chunks("body_chunks").?.chunks, 2);
    defer alloc.free(chunks);
    try std.testing.expectEqual(@as(usize, 2), chunks.len);
    try std.testing.expectEqual(@as(u32, 1), chunks[1].chunk_id);
    try std.testing.expectEqualStrings("i", chunks[1].content);
    try std.testing.expectError(error.InvalidArtifactSeed, decodeChunksAlloc(alloc, seeds.chunks("body_chunks").?.chunks, 0));

    const names = try seeds.names(alloc);
    defer alloc.free(names);
    try std.testing.expectEqual(@as(usize, 4), names.len);
}

test "artifact seeds reject descriptive and internal read fields" {
    const alloc = std.testing.allocator;
    const rejected = [_][]const u8{
        \\{"_artifacts":{"a":{"value":"x","status":"ready"}}}
        ,
        \\{"_artifacts":{"a":{"artifact_id":"af1:asset:x","value":"x"}}}
        ,
        \\{"_artifacts":{"a":{"kind":"embedding_set","items":[]}}}
        ,
        \\{"_artifacts":{"a":{"source":{"generation":"1"}}}}
        ,
        \\{"_artifacts":{"a":"bare"}}
        ,
        \\{"_artifacts":{"a.b":{"value":"x"}}}
        ,
        \\{"_artifacts":{"a":{"value":"x","source":{"etag":"1"}}}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_content":"x","_parent_doc_key":"d"}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_content":"x","_chunk":{"plan_id":"p"}}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":1,"_content":"x"},{"_chunk_id":1,"_content":"y"}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_id":1,"_content":"x"}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_content":"x","_start_char":4,"_end_char":2}]}}
        ,
        \\{"_chunks":{"c":{"_chunk_id":0}}}
        ,
        \\{"_artifacts":{"c":{"value":"x"}},"_chunks":{"c":[]}}
    };
    for (rejected) |json| {
        var doc = try parseForTest(alloc, json);
        defer doc.deinit();
        try std.testing.expectError(error.InvalidArtifactSeed, extractCanonicalAlloc(alloc, doc.value.object));
    }
    // Unit-scoped collections validate per unit and never mix with plain
    // chunks; plain chunk producers refuse them when decoding.
    var unit = try parseForTest(alloc,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_content":"x","_parent_unit_id":"page:000001","_speaker":"SPEAKER_01"},{"_chunk_id":0,"_content":"y","_parent_unit_id":"page:000002"}]}}
    );
    defer unit.deinit();
    const unit_canonical = (try extractCanonicalAlloc(alloc, unit.value.object)).?;
    alloc.free(unit_canonical);
    const unit_items = unit.value.object.get("_chunks").?.object.get("c").?.array.items;
    try std.testing.expectError(error.ArtifactSeedNotAccepted, decodeChunksAlloc(alloc, unit_items, null));
    const second = try decodeUnitChunksAlloc(alloc, unit_items, "page:000002", 1);
    defer alloc.free(second);
    try std.testing.expectEqual(@as(usize, 1), second.len);
    try std.testing.expectEqualStrings("y", second[0].content);
    const first = try decodeUnitChunksAlloc(alloc, unit_items, "page:000001", 1);
    defer alloc.free(first);
    try std.testing.expectEqual(@as(?u8, 1), first[0].speaker);
    for ([_][]const u8{
        \\{"_chunks":{"c":[{"_chunk_id":0,"_content":"x","_parent_unit_id":"page:000001"},{"_chunk_id":1,"_content":"y"}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":1,"_content":"x","_parent_unit_id":"p"},{"_chunk_id":0,"_content":"y","_parent_unit_id":"p"}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_content":"x","_speaker":"SPEAKER_X"}]}}
    }) |bad| {
        var doc = try parseForTest(alloc, bad);
        defer doc.deinit();
        try std.testing.expectError(error.InvalidArtifactSeed, extractCanonicalAlloc(alloc, doc.value.object));
    }

    var plain = try parseForTest(alloc,
        \\{"title":"no seeds"}
    );
    defer plain.deinit();
    try std.testing.expect((try extractCanonicalAlloc(alloc, plain.value.object)) == null);
}

test "artifact seeds validate chunk vector windows" {
    const alloc = std.testing.allocator;
    var ok = try parseForTest(alloc,
        \\[{"chunk_id":1,"window_index":1,"vector":[3,4]},{"chunk_id":0,"vector":[1,2]},{"chunk_id":1,"window_index":0,"vector":[5,6]}]
    );
    defer ok.deinit();
    const vectors = try decodeChunkVectorsAlloc(alloc, ok.value, 2);
    defer freeChunkVectors(alloc, vectors);
    try std.testing.expectEqual(@as(usize, 3), vectors.len);
    try std.testing.expectEqual(@as(u32, 0), vectors[0].chunk_id);
    try std.testing.expectEqual(@as(?u32, 0), vectors[1].window_index);
    try std.testing.expectEqual(@as(f32, 5), vectors[1].vector[0]);

    const invalid = [_][]const u8{
        // Partial window set.
        \\[{"chunk_id":1,"window_index":1,"vector":[3,4]}]
        ,
        // Unsplit and windowed vectors for one chunk.
        \\[{"chunk_id":1,"vector":[3,4]},{"chunk_id":1,"window_index":1,"vector":[3,4]}]
        ,
        // Duplicate unsplit vectors.
        \\[{"chunk_id":1,"vector":[3,4]},{"chunk_id":1,"vector":[3,4]}]
        ,
        // Mixed dimensions.
        \\[{"chunk_id":0,"vector":[3,4]},{"chunk_id":1,"vector":[3]}]
        ,
        \\[{"chunk_id":0,"vector":[]}]
        ,
        \\[{"chunk_id":0,"vector":[1,2],"dims":2}]
    };
    for (invalid) |json| {
        var parsed = try parseForTest(alloc, json);
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidArtifactSeed, decodeChunkVectorsAlloc(alloc, parsed.value, 0));
    }
}

test "artifact seed asset values follow the content type" {
    const alloc = std.testing.allocator;
    var parsed = try parseForTest(alloc,
        \\{"text":"","json":{"label":"x","score":1},"wrapped":{"text":"t"}}
    );
    defer parsed.deinit();
    // Empty is a complete result, distinct from missing.
    const empty = try assetBytesAlloc(alloc, parsed.value.object.get("text").?, false);
    defer alloc.free(empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
    const json = try assetBytesAlloc(alloc, parsed.value.object.get("json").?, true);
    defer alloc.free(json);
    try std.testing.expectEqualStrings("{\"label\":\"x\",\"score\":1}", json);
    try std.testing.expectEqualStrings("t", try assetText(parsed.value.object.get("wrapped").?));
    try std.testing.expectError(error.InvalidArtifactSeed, assetBytesAlloc(alloc, parsed.value.object.get("json").?, false));
    try std.testing.expectEqual(@as(?u64, 0xabc), parseSourceHash("xxh64:abc"));
    try std.testing.expectEqual(@as(?u64, null), parseSourceHash("sha:abc"));
}

test "artifact seed conflict policy travels with the seeds" {
    const alloc = std.testing.allocator;
    var doc = try parseForTest(alloc,
        \\{"b":1,"_artifacts":{"a":{"value":"x"}},"_seed_conflict":"replace"}
    );
    defer doc.deinit();
    const canonical = (try extractCanonicalAlloc(alloc, doc.value.object)).?;
    defer alloc.free(canonical);
    var seeds = try Seeds.parse(alloc, canonical);
    defer seeds.deinit();
    try std.testing.expectEqual(ConflictPolicy.replace, seeds.conflictPolicy());
    const names = try seeds.names(alloc);
    defer alloc.free(names);
    try std.testing.expectEqual(@as(usize, 1), names.len);

    for ([_][]const u8{
        \\{"b":1,"_seed_conflict":"replace"}
        ,
        \\{"b":1,"_artifacts":{"a":{"value":"x"}},"_seed_conflict":"merge"}
    }) |json| {
        var bad = try parseForTest(alloc, json);
        defer bad.deinit();
        try std.testing.expectError(error.InvalidArtifactSeed, extractCanonicalAlloc(alloc, bad.value.object));
    }
}

test "artifact seed extraction units carry pages and transcript phrases" {
    const alloc = std.testing.allocator;
    var text_form = try parseForTest(alloc, "{\"text\":\"plain\"}");
    defer text_form.deinit();
    try std.testing.expectEqual(@as(usize, 0), (try decodeExtractionUnitsAlloc(alloc, text_form.value)).len);

    var rich = try parseForTest(alloc,
        \\{"units":[{"text":"Page one.","page_number":1,"page_label":"i","page_bbox":[0,0,612,792],"page_rotation":90},
        \\ {"text":"Hello there. Hi.","spans":[{"char_start":0,"char_end":12,"start_ms":0,"end_ms":900,"speaker":"SPEAKER_00"},{"char_start":13,"char_end":16,"start_ms":1000,"end_ms":1400,"speaker":1}]},
        \\ {"text":"Notes","unit_type":"slide"}]}
    );
    defer rich.deinit();
    const units = try decodeExtractionUnitsAlloc(alloc, rich.value);
    defer freeExtractionUnits(alloc, units);
    try std.testing.expectEqual(@as(usize, 3), units.len);
    try std.testing.expectEqualStrings("page", units[0].unit_type);
    try std.testing.expectEqual(@as(?u32, 1), units[0].page_number);
    try std.testing.expectEqual(@as(?i32, 90), units[0].page_rotation);
    try std.testing.expectEqualStrings("transcript", units[1].unit_type);
    try std.testing.expectEqual(@as(?u8, 1), units[1].spans[1].speaker);
    try std.testing.expectEqualStrings("slide", units[2].unit_type);

    var layout = try parseForTest(alloc,
        \\{"units":[{"text":"Héllo world","page_number":1,"page_bbox":[0,0,612,792],
        \\  "text_regions":[{"span":[0,6],"bbox":[72,700,110,712],"font_size":11,"bold":null},{"span":[7,12],"bbox":[112,700,150,712]}],
        \\  "text_blocks":[{"span":[0,12],"bbox":[72,700,150,712],"reading_order":0,"italic":false}]}]}
    );
    defer layout.deinit();
    const laid_out = try decodeExtractionUnitsAlloc(alloc, layout.value);
    defer freeExtractionUnits(alloc, laid_out);
    try std.testing.expectEqual(@as(usize, 2), laid_out[0].text_regions.len);
    try std.testing.expectEqual(@as(?f64, 11), laid_out[0].text_regions[0].font_size);
    try std.testing.expectEqual(@as(?bool, false), laid_out[0].text_blocks[0].italic);

    for ([_][]const u8{
        "{\"units\":[]}",
        "{\"units\":[{\"page_number\":1}]}",
        "{\"units\":[{\"text\":\"x\",\"ocr_bbox\":[0,0,1,1]}]}",
        "{\"units\":[{\"text\":\"x\",\"page_number\":0}]}",
        "{\"units\":[{\"text\":\"x\",\"unit_type\":\"Page!\"}]}",
        "{\"units\":[{\"text\":\"ab\",\"spans\":[{\"char_start\":0,\"char_end\":3,\"start_ms\":0,\"end_ms\":1}]}]}",
        "{\"units\":[{\"text\":\"abcd\",\"spans\":[{\"char_start\":0,\"char_end\":2,\"start_ms\":0,\"end_ms\":1},{\"char_start\":1,\"char_end\":3,\"start_ms\":1,\"end_ms\":2}]}]}",
        "{\"units\":[{\"text\":\"x\"}],\"extra\":1}",
        // Layout needs the page box, lies inside it, and spans whole
        // characters of the unit text in text order; blocks list in reading
        // order, and only blocks have one.
        "{\"units\":[{\"text\":\"ab\",\"text_regions\":[{\"span\":[0,1],\"bbox\":[0,0,1,1]}]}]}",
        "{\"units\":[{\"text\":\"ab\",\"page_bbox\":[0,0,10,10],\"text_regions\":[{\"span\":[0,1],\"bbox\":[0,0,11,1]}]}]}",
        "{\"units\":[{\"text\":\"ab\",\"page_bbox\":[0,0,10,10],\"text_regions\":[{\"span\":[0,3],\"bbox\":[0,0,1,1]}]}]}",
        "{\"units\":[{\"text\":\"é\",\"page_bbox\":[0,0,10,10],\"text_regions\":[{\"span\":[0,1],\"bbox\":[0,0,1,1]}]}]}",
        "{\"units\":[{\"text\":\"ab\",\"page_bbox\":[0,0,10,10],\"text_regions\":[{\"span\":[1,2],\"bbox\":[0,0,1,1]},{\"span\":[0,1],\"bbox\":[0,0,1,1]}]}]}",
        "{\"units\":[{\"text\":\"ab\",\"page_bbox\":[0,0,10,10],\"text_regions\":[{\"span\":[0,1],\"bbox\":[2,0,1,1]}]}]}",
        "{\"units\":[{\"text\":\"ab\",\"page_bbox\":[0,0,10,10],\"text_blocks\":[{\"span\":[0,1],\"bbox\":[0,0,1,1],\"reading_order\":1}]}]}",
        "{\"units\":[{\"text\":\"ab\",\"page_bbox\":[0,0,10,10],\"text_regions\":[{\"span\":[0,1],\"bbox\":[0,0,1,1],\"reading_order\":0}]}]}",
        "{\"units\":[{\"text\":\"ab\",\"page_bbox\":[0,0,10,10],\"text_regions\":[{\"span\":[0,1],\"bbox\":[0,0,1,1],\"font_size\":0}]}]}",
    }) |bad| {
        var doc = try parseForTest(alloc, bad);
        defer doc.deinit();
        try std.testing.expectError(error.InvalidArtifactSeed, decodeExtractionUnitsAlloc(alloc, doc.value));
    }
    var speakers = try parseForTest(alloc, "[\"SPEAKER_07\",3,\"SPEAKER_255\",255]");
    defer speakers.deinit();
    try std.testing.expectEqual(@as(u8, 7), try parseSpeaker(speakers.value.array.items[0]));
    try std.testing.expectEqual(@as(u8, 3), try parseSpeaker(speakers.value.array.items[1]));
    try std.testing.expectError(error.InvalidArtifactSeed, parseSpeaker(speakers.value.array.items[2]));
    try std.testing.expectError(error.InvalidArtifactSeed, parseSpeaker(speakers.value.array.items[3]));
}
