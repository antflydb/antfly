// Copyright 2026 Antfly, Inc.
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

//! Deterministic entity resolver core (see zig/RESOLUTION.md).
//!
//! Turns extracted entity mentions into a durable resolution artifact mapping
//! each local id to a canonical `DocRef`. Two paths:
//!
//!   * deterministic: render a canonical key from the mention via `key_template`
//!     ("mint a new entity"); this is pure and needs no global state, so the
//!     graph works even before the entity document exists.
//!   * scored: when candidates are supplied (the blocking/candidate-fetch step
//!     lives in the DB integration layer), score the mention against each with
//!     the `matcher` scorer and link to the best MATCH; otherwise fall back to
//!     minting.
//!
//! The decision is recorded in the artifact and is what replay re-applies --
//! resolution is never silently recomputed against a moved-on entity table
//! (the replay-stability invariant in RESOLUTION.md).

const std = @import("std");
const matcher = @import("antfly_matcher");

/// A reference to a document in some table. Phase 1 only hydrates same-table,
/// but threading `DocRef` now keeps cross-table entity graphs from being a
/// later redesign.
pub const DocRef = struct {
    table: []const u8,
    key: []const u8,
};

/// One extracted mention to resolve. `text` is the surface form; `local_id` is
/// the extraction-local id (e.g. "e0").
pub const ExtractedEntity = struct {
    local_id: []const u8,
    label: []const u8,
    text: []const u8,
    /// Optional query embedding (e.g. a name embedding) for cosine/ANN scoring.
    embedding: ?[]const f32 = null,
};

/// A resolution candidate fetched by blocking. `record` is scored against the
/// mention; `label` gates `type_must_match`.
pub const Candidate = struct {
    doc_ref: DocRef,
    label: []const u8,
    record: matcher.Record,
};

pub const Decision = enum {
    /// Linked to an existing candidate entity.
    match,
    /// Scored into the review band; phase 1 records it but mints rather than
    /// linking (the human review workflow is phase 2).
    review,
    /// No confident match; a new canonical entity key was minted.
    new,
};

pub const ResolvedEntity = struct {
    local_id: []const u8,
    doc_ref: DocRef,
    confidence: f64,
    decision: Decision,
    /// Entity label / type carried from the mention so the promoter can build
    /// the canonical entity document without re-reading the extraction artifact.
    label: []const u8 = "",
    /// Mention surface form; the promoter uses it as the canonical name (on a
    /// freshly minted entity) and as an alias to union into an existing one.
    canonical_name: []const u8 = "",
};

/// The resolution artifact: the durable record of identity decisions for one
/// source document. Owns its strings in an arena.
pub const Resolution = struct {
    arena: std.heap.ArenaAllocator,
    config_generation: u64,
    entities: []const ResolvedEntity,

    pub fn deinit(self: *Resolution) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Serialize to the JSON schema documented in RESOLUTION.md. Caller owns the
    /// returned bytes.
    pub fn toJson(self: Resolution, allocator: std.mem.Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(allocator);

        try out.appendSlice(allocator, "{\"config_generation\":");
        try appendInt(allocator, &out, self.config_generation);
        try out.appendSlice(allocator, ",\"entities\":[");
        for (self.entities, 0..) |e, i| {
            if (i > 0) try out.append(allocator, ',');
            try out.appendSlice(allocator, "{\"local_id\":");
            try writeJsonString(allocator, &out, e.local_id);
            try out.appendSlice(allocator, ",\"doc_ref\":{\"table\":");
            try writeJsonString(allocator, &out, e.doc_ref.table);
            try out.appendSlice(allocator, ",\"key\":");
            try writeJsonString(allocator, &out, e.doc_ref.key);
            try out.appendSlice(allocator, "},\"confidence\":");
            try appendFloat(allocator, &out, e.confidence);
            try out.appendSlice(allocator, ",\"decision\":");
            try writeJsonString(allocator, &out, @tagName(e.decision));
            if (e.label.len > 0) {
                try out.appendSlice(allocator, ",\"label\":");
                try writeJsonString(allocator, &out, e.label);
            }
            if (e.canonical_name.len > 0) {
                try out.appendSlice(allocator, ",\"canonical_name\":");
                try writeJsonString(allocator, &out, e.canonical_name);
            }
            try out.append(allocator, '}');
        }
        try out.appendSlice(allocator, "]}");
        return out.toOwnedSlice(allocator);
    }
};

/// A parsed, reusable resolver config. Owns the entity table name, key template,
/// and optional scorer.
pub const Resolver = struct {
    arena: std.heap.ArenaAllocator,
    table: []const u8,
    key_template: []const u8,
    type_must_match: bool,
    scorer: ?matcher.Scorer,

    pub fn parse(gpa: std.mem.Allocator, json_bytes: []const u8) !Resolver {
        var parsed = try std.json.parseFromSlice(std.json.Value, gpa, json_bytes, .{});
        defer parsed.deinit();
        return parseValue(gpa, parsed.value);
    }

    pub fn parseValue(gpa: std.mem.Allocator, root: std.json.Value) !Resolver {
        if (root != .object) return error.InvalidConfig;
        const obj = root.object;

        var arena = std.heap.ArenaAllocator.init(gpa);
        errdefer arena.deinit();
        const a = arena.allocator();

        const table = try a.dupe(u8, jsonString(obj.get("table") orelse return error.MissingTable) orelse
            return error.MissingTable);
        const key_template = try a.dupe(u8, jsonString(obj.get("key_template") orelse return error.MissingKeyTemplate) orelse
            return error.MissingKeyTemplate);

        var type_must_match = true;
        if (obj.get("type_must_match")) |v| {
            if (v == .bool) type_must_match = v.bool;
        }

        var scorer: ?matcher.Scorer = null;
        errdefer if (scorer) |*s| s.deinit();
        if (obj.get("scorer")) |sv| {
            scorer = try matcher.Scorer.parseValue(gpa, sv);
        }

        return .{
            .arena = arena,
            .table = table,
            .key_template = key_template,
            .type_must_match = type_must_match,
            .scorer = scorer,
        };
    }

    /// Build a resolver directly from its parts (the durable catalog config),
    /// parsing `scorer_json` if present. An empty `scorer_json` means a purely
    /// deterministic resolver that mints canonical keys from `key_template`.
    pub fn initFromParts(
        gpa: std.mem.Allocator,
        table: []const u8,
        key_template: []const u8,
        type_must_match: bool,
        scorer_json: []const u8,
    ) !Resolver {
        var arena = std.heap.ArenaAllocator.init(gpa);
        errdefer arena.deinit();
        const a = arena.allocator();
        const owned_table = try a.dupe(u8, table);
        const owned_template = try a.dupe(u8, key_template);

        var scorer: ?matcher.Scorer = null;
        errdefer if (scorer) |*s| s.deinit();
        if (scorer_json.len > 0) {
            scorer = try matcher.Scorer.parse(gpa, scorer_json);
        }

        return .{
            .arena = arena,
            .table = owned_table,
            .key_template = owned_template,
            .type_must_match = type_must_match,
            .scorer = scorer,
        };
    }

    pub fn deinit(self: *Resolver) void {
        if (self.scorer) |*s| s.deinit();
        self.arena.deinit();
        self.* = undefined;
    }

    /// Render the canonical entity key this resolver would mint for a mention.
    /// Used by candidate blocking to look up an existing entity by key.
    pub fn renderKeyAlloc(self: *const Resolver, gpa: std.mem.Allocator, entity: ExtractedEntity) ![]const u8 {
        return renderKey(gpa, self.key_template, entity);
    }

    /// Resolve every mention. `candidates[i]` holds candidates for `entities[i]`
    /// (an empty top-level slice means deterministic minting only). The returned
    /// `Resolution` owns its memory; `gpa` is also used as transient scoring
    /// scratch.
    pub fn resolve(
        self: *const Resolver,
        gpa: std.mem.Allocator,
        config_generation: u64,
        entities: []const ExtractedEntity,
        candidates: []const []const Candidate,
    ) !Resolution {
        var arena = std.heap.ArenaAllocator.init(gpa);
        errdefer arena.deinit();
        const a = arena.allocator();

        const resolved = try a.alloc(ResolvedEntity, entities.len);
        for (entities, 0..) |entity, i| {
            const cands: []const Candidate = if (i < candidates.len) candidates[i] else &.{};
            resolved[i] = try self.resolveOne(a, gpa, entity, cands);
        }

        return .{ .arena = arena, .config_generation = config_generation, .entities = resolved };
    }

    fn resolveOne(
        self: *const Resolver,
        a: std.mem.Allocator,
        scratch: std.mem.Allocator,
        entity: ExtractedEntity,
        candidates: []const Candidate,
    ) !ResolvedEntity {
        const local_id = try a.dupe(u8, entity.local_id);

        if (self.scorer) |*scorer| {
            if (candidates.len > 0) {
                var mention_fields = [_]matcher.Field{
                    .{ .name = "label", .value = .{ .text = entity.label } },
                    .{ .name = "text", .value = .{ .text = entity.text } },
                    .{ .name = "canonical_text", .value = .{ .text = entity.text } },
                    .{ .name = "name_embedding", .value = if (entity.embedding) |emb| .{ .vector = emb } else .none },
                };
                const mention = matcher.Record{ .fields = mention_fields[0 .. if (entity.embedding != null) @as(usize, 4) else 3] };

                var best_prob: f64 = -1;
                var best: ?usize = null;
                var best_outcome: matcher.Outcome = .no_match;
                for (candidates, 0..) |cand, ci| {
                    if (self.type_must_match and !std.mem.eql(u8, cand.label, entity.label)) continue;
                    const r = scorer.score(scratch, mention, cand.record);
                    if (r.probability > best_prob) {
                        best_prob = r.probability;
                        best = ci;
                        best_outcome = r.outcome;
                    }
                }

                if (best) |bi| {
                    if (best_outcome == .match) {
                        return .{
                            .local_id = local_id,
                            .doc_ref = .{
                                .table = try a.dupe(u8, candidates[bi].doc_ref.table),
                                .key = try a.dupe(u8, candidates[bi].doc_ref.key),
                            },
                            .confidence = best_prob,
                            .decision = .match,
                            .label = try a.dupe(u8, entity.label),
                            .canonical_name = try a.dupe(u8, entity.text),
                        };
                    }
                    return .{
                        .local_id = local_id,
                        .doc_ref = try self.mintRef(a, entity),
                        .confidence = best_prob,
                        .decision = if (best_outcome == .review) .review else .new,
                        .label = try a.dupe(u8, entity.label),
                        .canonical_name = try a.dupe(u8, entity.text),
                    };
                }
            }
        }

        return .{
            .local_id = local_id,
            .doc_ref = try self.mintRef(a, entity),
            .confidence = 1.0,
            .decision = .new,
            .label = try a.dupe(u8, entity.label),
            .canonical_name = try a.dupe(u8, entity.text),
        };
    }

    fn mintRef(self: *const Resolver, a: std.mem.Allocator, entity: ExtractedEntity) !DocRef {
        return .{
            .table = try a.dupe(u8, self.table),
            .key = try renderKey(a, self.key_template, entity),
        };
    }
};

fn jsonString(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |s| s,
        else => null,
    };
}

/// Parse a JSON number array into an owned f32 slice, or null (absent / not an
/// array / empty / non-numeric). Used for entity query embeddings.
fn parseEmbedding(a: std.mem.Allocator, value: ?std.json.Value) !?[]const f32 {
    const v = value orelse return null;
    if (v != .array or v.array.items.len == 0) return null;
    const out = try a.alloc(f32, v.array.items.len);
    for (v.array.items, 0..) |item, i| {
        out[i] = switch (item) {
            .float => |f| @floatCast(f),
            .integer => |n| @floatFromInt(n),
            .number_string => |s| std.fmt.parseFloat(f32, s) catch return null,
            else => return null,
        };
    }
    return out;
}

// --- Key template -----------------------------------------------------------

/// Minimal `{{ helper var }}` / `{{ var }}` renderer for canonical keys.
/// Variables: `_entity.label`, `_entity.text` (alias `_entity.canonical_text`),
/// `_entity.local_id` (alias `_entity.id`). Helpers: `lower`, `upper`, `trim`,
/// `slug`. Unknown variables/helpers fail closed. This is deliberately small;
/// it should later align with Antfly's shared template engine.
fn renderKey(a: std.mem.Allocator, template: []const u8, entity: ExtractedEntity) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(a);

    var i: usize = 0;
    while (i < template.len) {
        if (i + 1 < template.len and template[i] == '{' and template[i + 1] == '{') {
            const end = std.mem.indexOfPos(u8, template, i + 2, "}}") orelse return error.InvalidTemplate;
            try renderExpr(a, &out, std.mem.trim(u8, template[i + 2 .. end], " \t"), entity);
            i = end + 2;
        } else {
            try out.append(a, template[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice(a);
}

fn renderExpr(
    a: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    expr: []const u8,
    entity: ExtractedEntity,
) !void {
    var it = std.mem.tokenizeAny(u8, expr, " \t");
    const first = it.next() orelse return error.InvalidTemplate;
    const second = it.next();
    if (it.next() != null) return error.InvalidTemplate;

    if (second) |var_name| {
        try applyHelper(a, out, first, try entityVar(entity, var_name));
    } else {
        try out.appendSlice(a, try entityVar(entity, first));
    }
}

fn entityVar(entity: ExtractedEntity, name: []const u8) ![]const u8 {
    if (std.mem.eql(u8, name, "_entity.label")) return entity.label;
    if (std.mem.eql(u8, name, "_entity.text")) return entity.text;
    if (std.mem.eql(u8, name, "_entity.canonical_text")) return entity.text;
    if (std.mem.eql(u8, name, "_entity.local_id")) return entity.local_id;
    if (std.mem.eql(u8, name, "_entity.id")) return entity.local_id;
    return error.InvalidTemplate;
}

fn applyHelper(
    a: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    helper: []const u8,
    value: []const u8,
) !void {
    if (std.mem.eql(u8, helper, "lower")) {
        for (value) |c| try out.append(a, std.ascii.toLower(c));
        return;
    }
    if (std.mem.eql(u8, helper, "upper")) {
        for (value) |c| try out.append(a, std.ascii.toUpper(c));
        return;
    }
    if (std.mem.eql(u8, helper, "trim")) {
        try out.appendSlice(a, std.mem.trim(u8, value, " \t\r\n"));
        return;
    }
    if (std.mem.eql(u8, helper, "slug")) {
        try appendSlug(a, out, value);
        return;
    }
    return error.InvalidTemplate;
}

/// lowercased, alphanumeric runs separated by single '_', no leading/trailing
/// separators. "A. Lovelace" -> "a_lovelace".
fn appendSlug(a: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    var wrote = false;
    var pending_sep = false;
    for (value) |c| {
        const lc = std.ascii.toLower(c);
        if (std.ascii.isAlphanumeric(lc)) {
            if (pending_sep and wrote) try out.append(a, '_');
            try out.append(a, lc);
            wrote = true;
            pending_sep = false;
        } else if (wrote) {
            pending_sep = true;
        }
    }
}

// --- JSON writing -----------------------------------------------------------

fn appendInt(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), n: u64) !void {
    var buf: [20]u8 = undefined;
    try out.appendSlice(allocator, std.fmt.bufPrint(&buf, "{d}", .{n}) catch unreachable);
}

fn appendFloat(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), f: f64) !void {
    var buf: [64]u8 = undefined;
    try out.appendSlice(allocator, std.fmt.bufPrint(&buf, "{d}", .{f}) catch unreachable);
}

fn writeJsonString(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), s: []const u8) !void {
    try out.append(allocator, '"');
    for (s) |c| {
        switch (c) {
            '"' => try out.appendSlice(allocator, "\\\""),
            '\\' => try out.appendSlice(allocator, "\\\\"),
            '\n' => try out.appendSlice(allocator, "\\n"),
            '\r' => try out.appendSlice(allocator, "\\r"),
            '\t' => try out.appendSlice(allocator, "\\t"),
            else => {
                if (c < 0x20) {
                    var buf: [8]u8 = undefined;
                    try out.appendSlice(allocator, std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable);
                } else {
                    try out.append(allocator, c);
                }
            },
        }
    }
    try out.append(allocator, '"');
}

// --- Extraction parsing -----------------------------------------------------

/// Parsed entities from an extraction artifact, owning their backing memory.
pub const ParsedEntities = struct {
    arena: std.heap.ArenaAllocator,
    /// Mutable so the resolution stage can backfill name embeddings in place.
    entities: []ExtractedEntity,

    pub fn deinit(self: *ParsedEntities) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Computes a name embedding for a mention on demand, so ANN/cosine blocking has
/// a query vector even when the extraction artifact carries none. A function-
/// pointer seam keeps the resolver library decoupled from the storage embedder;
/// the returned vector must be owned by `alloc` (the parse arena) so it lives as
/// long as the mention. Returns null to leave the mention un-embedded.
pub const MentionEmbedder = struct {
    ptr: *anyopaque,
    embed_fn: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, text: []const u8) anyerror!?[]const f32,

    pub fn embed(self: MentionEmbedder, alloc: std.mem.Allocator, text: []const u8) anyerror!?[]const f32 {
        return self.embed_fn(self.ptr, alloc, text);
    }
};

/// Owns the parsed entities of a resolution artifact in an arena.
pub const ParsedResolution = struct {
    arena: std.heap.ArenaAllocator,
    config_generation: u64,
    entities: []const ResolvedEntity,

    pub fn deinit(self: *ParsedResolution) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn decisionFromTag(tag: []const u8) ?Decision {
    if (std.mem.eql(u8, tag, "match")) return .match;
    if (std.mem.eql(u8, tag, "review")) return .review;
    if (std.mem.eql(u8, tag, "new")) return .new;
    return null;
}

/// Parse a resolution artifact (the shape produced by `Resolution.toJson`) back
/// into resolved entities. The promoter reads this to upsert canonical entity
/// documents; carrying `label`/`canonical_name` keeps it self-contained.
pub fn parseResolution(gpa: std.mem.Allocator, json_bytes: []const u8) !ParsedResolution {
    var parsed = try std.json.parseFromSlice(std.json.Value, gpa, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidResolution;
    const obj = parsed.value.object;

    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();
    const a = arena.allocator();

    var config_generation: u64 = 0;
    if (obj.get("config_generation")) |v| {
        if (v == .integer and v.integer >= 0) config_generation = @intCast(v.integer);
    }

    const entities_v = obj.get("entities") orelse return error.InvalidResolution;
    if (entities_v != .array) return error.InvalidResolution;

    const out = try a.alloc(ResolvedEntity, entities_v.array.items.len);
    for (entities_v.array.items, 0..) |ev, i| {
        if (ev != .object) return error.InvalidResolution;
        const o = ev.object;
        const ref = o.get("doc_ref") orelse return error.InvalidResolution;
        if (ref != .object) return error.InvalidResolution;
        const decision_tag = jsonString(o.get("decision") orelse return error.InvalidResolution) orelse return error.InvalidResolution;
        out[i] = .{
            .local_id = try a.dupe(u8, jsonString(o.get("local_id") orelse return error.InvalidResolution) orelse return error.InvalidResolution),
            .doc_ref = .{
                .table = try a.dupe(u8, jsonString(ref.object.get("table") orelse return error.InvalidResolution) orelse return error.InvalidResolution),
                .key = try a.dupe(u8, jsonString(ref.object.get("key") orelse return error.InvalidResolution) orelse return error.InvalidResolution),
            },
            .confidence = switch (o.get("confidence") orelse std.json.Value{ .float = 0 }) {
                .float => |f| f,
                .integer => |n| @floatFromInt(n),
                else => 0,
            },
            .decision = decisionFromTag(decision_tag) orelse return error.InvalidResolution,
            .label = if (o.get("label")) |v| try a.dupe(u8, jsonString(v) orelse "") else "",
            .canonical_name = if (o.get("canonical_name")) |v| try a.dupe(u8, jsonString(v) orelse "") else "",
        };
    }
    return .{ .arena = arena, .config_generation = config_generation, .entities = out };
}

/// Parse the `entities` array of an extraction artifact (the shape produced by
/// the extractor and documented in RESOLUTION.md / GRAPH.md). Relations are not
/// needed here -- they are consumed by the graph materializer, which reads the
/// extraction artifact plus this resolver's resolution artifact.
pub fn parseExtractionEntities(gpa: std.mem.Allocator, json_bytes: []const u8) !ParsedEntities {
    var parsed = try std.json.parseFromSlice(std.json.Value, gpa, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidExtraction;

    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();
    const a = arena.allocator();

    const entities_v = parsed.value.object.get("entities") orelse return error.InvalidExtraction;
    if (entities_v != .array) return error.InvalidExtraction;

    const out = try a.alloc(ExtractedEntity, entities_v.array.items.len);
    for (entities_v.array.items, 0..) |ev, i| {
        if (ev != .object) return error.InvalidExtraction;
        const o = ev.object;
        out[i] = .{
            .local_id = try a.dupe(u8, jsonString(o.get("id") orelse return error.InvalidExtraction) orelse return error.InvalidExtraction),
            .label = try a.dupe(u8, jsonString(o.get("label") orelse return error.InvalidExtraction) orelse return error.InvalidExtraction),
            .text = try a.dupe(u8, jsonString(o.get("text") orelse return error.InvalidExtraction) orelse return error.InvalidExtraction),
            .embedding = try parseEmbedding(a, o.get("embedding")),
        };
    }
    return .{ .arena = arena, .entities = out };
}

// --- Resolution replay stage ------------------------------------------------

/// Storage seam for the resolution stage. The DB adapter implements this over
/// the shard's primary store (artifact get/put/delete); tests use an in-memory
/// map. Kept as a vtable so the stage logic stays pure and unit-testable.
pub const ArtifactStore = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Returns owned bytes (caller frees with the passed allocator) or null.
        get: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]u8,
        put: *const fn (ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void,
        delete: *const fn (ptr: *anyopaque, key: []const u8) anyerror!void,
        /// Optional range scan over `[lower, upper)` for prefix candidate
        /// blocking; `consume` borrows the key/value for the call. Null means
        /// the store does not support scanning (blocking yields no candidates).
        scan_prefix: ?*const fn (
            ptr: *anyopaque,
            lower: []const u8,
            upper: []const u8,
            ctx: *anyopaque,
            consume: *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!void,
        ) anyerror!void = null,
    };

    pub fn get(self: ArtifactStore, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]u8 {
        return self.vtable.get(self.ptr, allocator, key);
    }
    pub fn put(self: ArtifactStore, key: []const u8, value: []const u8) anyerror!void {
        return self.vtable.put(self.ptr, key, value);
    }
    pub fn delete(self: ArtifactStore, key: []const u8) anyerror!void {
        return self.vtable.delete(self.ptr, key);
    }
    pub fn scanPrefix(
        self: ArtifactStore,
        lower: []const u8,
        upper: []const u8,
        ctx: *anyopaque,
        consume: *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!void,
    ) anyerror!void {
        const f = self.vtable.scan_prefix orelse return error.ScanUnsupported;
        return f(self.ptr, lower, upper, ctx, consume);
    }
};

/// Blocking seam: fetch the ~k candidate entities to score a mention against.
/// The DB adapter implements this over the entity table's indexes
/// (ann/exact/prefix); a null provider means deterministic minting only.
pub const CandidateProvider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Append candidates for `entity` into `out`, allocating any candidate
        /// memory with `allocator` (valid until the stage finishes one mention).
        candidates_for: *const fn (
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            entity: ExtractedEntity,
            out: *std.ArrayListUnmanaged(Candidate),
        ) anyerror!void,
    };

    pub fn candidatesFor(
        self: CandidateProvider,
        allocator: std.mem.Allocator,
        entity: ExtractedEntity,
        out: *std.ArrayListUnmanaged(Candidate),
    ) anyerror!void {
        return self.vtable.candidates_for(self.ptr, allocator, entity, out);
    }
};

pub const RunResult = enum {
    /// Resolution artifact was (re)written.
    written,
    /// Recomputed bytes matched the stored artifact; nothing written (the
    /// enrichment-style skip that keeps replay idempotent and cheap).
    unchanged,
    /// Source extraction artifact is gone; the stale resolution artifact was
    /// deleted.
    cleared,
    /// Source extraction artifact is gone and there was nothing to clear.
    source_missing,
};

/// One resolution replay stage: read a changed extraction artifact, resolve its
/// mentions, and persist the resolution artifact idempotently. This is the body
/// the managed worker runs per changed extraction artifact key.
///
/// In the DB, the adapter wraps `run` in a `background_runtime.Job`
/// (class `.maintenance`, keyed by the shard's owner id) and submits it on the
/// shard's `BackendRuntime.durable_jobs` lane; crash recovery is the normal
/// replay path (the change journal re-emits the extraction artifact key).
pub const ResolutionStage = struct {
    resolver: *const Resolver,
    config_generation: u64,
    /// Optional name-embedding backfill: when set, each mention lacking an
    /// `embedding` gets one from its text before blocking/scoring, so `ann`
    /// candidate search and cosine comparisons have a query vector.
    embedder: ?MentionEmbedder = null,

    /// `extraction_key` / `resolution_key` are the primary-store artifact keys
    /// (encoded by the DB adapter). Returns what happened, for status/metrics.
    pub fn run(
        self: ResolutionStage,
        gpa: std.mem.Allocator,
        store: ArtifactStore,
        provider: ?CandidateProvider,
        extraction_key: []const u8,
        resolution_key: []const u8,
    ) !RunResult {
        const extraction = try store.get(gpa, extraction_key);
        defer if (extraction) |e| gpa.free(e);

        if (extraction == null) {
            const existing = try store.get(gpa, resolution_key);
            defer if (existing) |e| gpa.free(e);
            if (existing != null) {
                try store.delete(resolution_key);
                return .cleared;
            }
            return .source_missing;
        }

        var parsed = try parseExtractionEntities(gpa, extraction.?);
        defer parsed.deinit();

        // Backfill name embeddings for mentions that arrived without one, using
        // the parse arena so the vectors outlive scoring. Embedding failures are
        // non-fatal: the mention simply scores without a vector.
        if (self.embedder) |embedder| {
            const arena = parsed.arena.allocator();
            for (parsed.entities) |*entity| {
                if (entity.embedding != null or entity.text.len == 0) continue;
                entity.embedding = embedder.embed(arena, entity.text) catch null;
            }
        }

        var scratch = std.heap.ArenaAllocator.init(gpa);
        defer scratch.deinit();
        const a = scratch.allocator();

        const lists = try a.alloc([]const Candidate, parsed.entities.len);
        if (provider) |p| {
            for (parsed.entities, 0..) |entity, i| {
                var candidates = std.ArrayListUnmanaged(Candidate).empty;
                try p.candidatesFor(a, entity, &candidates);
                lists[i] = candidates.items;
            }
        } else {
            for (lists) |*l| l.* = &.{};
        }

        var resolution = try self.resolver.resolve(gpa, self.config_generation, parsed.entities, lists);
        defer resolution.deinit();

        const bytes = try resolution.toJson(gpa);
        defer gpa.free(bytes);

        const existing = try store.get(gpa, resolution_key);
        defer if (existing) |e| gpa.free(e);
        if (existing) |e| {
            if (std.mem.eql(u8, e, bytes)) return .unchanged;
        }
        try store.put(resolution_key, bytes);
        return .written;
    }
};

// --- Tests ------------------------------------------------------------------

const testing = std.testing;

test "initFromParts builds a deterministic resolver and one with a scorer" {
    var deterministic = try Resolver.initFromParts(testing.allocator, "entities", "{{ slug _entity.text }}", true, "");
    defer deterministic.deinit();
    try testing.expectEqualStrings("entities", deterministic.table);
    try testing.expect(deterministic.scorer == null);

    var scored = try Resolver.initFromParts(
        testing.allocator,
        "entities",
        "{{ slug _entity.text }}",
        false,
        \\{ "comparisons": [ { "name": "n", "left": "canonical_text", "right": "canonical_name",
        \\  "levels": [ { "when": "exact", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9 } }
    );
    defer scored.deinit();
    try testing.expect(!scored.type_must_match);
    try testing.expect(scored.scorer != null);

    const entities = [_]ExtractedEntity{.{ .local_id = "e0", .label = "person", .text = "Ada Lovelace" }};
    var res = try scored.resolve(testing.allocator, 1, &entities, &[_][]const Candidate{});
    defer res.deinit();
    try testing.expectEqualStrings("ada_lovelace", res.entities[0].doc_ref.key);
}

test "deterministic resolver mints a canonical key for each entity" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ lower _entity.label }}/{{ slug _entity.canonical_text }}" }
    );
    defer resolver.deinit();

    const entities = [_]ExtractedEntity{
        .{ .local_id = "e0", .label = "Person", .text = "Ada Lovelace" },
        .{ .local_id = "e1", .label = "Org", .text = "Antfly, Inc." },
    };
    var res = try resolver.resolve(testing.allocator, 7, &entities, &[_][]const Candidate{});
    defer res.deinit();

    try testing.expectEqual(@as(usize, 2), res.entities.len);
    try testing.expectEqualStrings("entities", res.entities[0].doc_ref.table);
    try testing.expectEqualStrings("person/ada_lovelace", res.entities[0].doc_ref.key);
    try testing.expectEqual(Decision.new, res.entities[0].decision);
    try testing.expectEqualStrings("org/antfly_inc", res.entities[1].doc_ref.key);
}

test "resolver links a mention to a matching candidate" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ slug _entity.text }}",
        \\  "scorer": {
        \\    "comparisons": [
        \\      { "name": "name", "left": "canonical_text", "right": "canonical_name",
        \\        "levels": [ { "when": "exact", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] }
        \\    ],
        \\    "combine": { "bias": -3.0 }, "decision": { "match": 0.9 }
        \\  } }
    );
    defer resolver.deinit();

    var cand_fields = [_]matcher.Field{.{ .name = "canonical_name", .value = .{ .text = "Ada Lovelace" } }};
    const cands = [_]Candidate{.{
        .doc_ref = .{ .table = "entities", .key = "person/ada_lovelace" },
        .label = "Person",
        .record = .{ .fields = &cand_fields },
    }};
    const cand_lists = [_][]const Candidate{&cands};
    const entities = [_]ExtractedEntity{.{ .local_id = "e0", .label = "Person", .text = "Ada Lovelace" }};

    var res = try resolver.resolve(testing.allocator, 1, &entities, &cand_lists);
    defer res.deinit();

    try testing.expectEqual(Decision.match, res.entities[0].decision);
    try testing.expectEqualStrings("person/ada_lovelace", res.entities[0].doc_ref.key);
    try testing.expect(res.entities[0].confidence > 0.9);
}

test "type_must_match prevents a cross-type link and mints a new entity" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ slug _entity.text }}",
        \\  "scorer": {
        \\    "comparisons": [
        \\      { "name": "name", "left": "canonical_text", "right": "canonical_name",
        \\        "levels": [ { "when": "exact", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] }
        \\    ],
        \\    "combine": { "bias": -3.0 }, "decision": { "match": 0.9 }
        \\  } }
    );
    defer resolver.deinit();

    var cand_fields = [_]matcher.Field{.{ .name = "canonical_name", .value = .{ .text = "Ada Lovelace" } }};
    const cands = [_]Candidate{.{
        .doc_ref = .{ .table = "entities", .key = "org/ada_lovelace" },
        .label = "Org", // different type than the mention
        .record = .{ .fields = &cand_fields },
    }};
    const cand_lists = [_][]const Candidate{&cands};
    const entities = [_]ExtractedEntity{.{ .local_id = "e0", .label = "Person", .text = "Ada Lovelace" }};

    var res = try resolver.resolve(testing.allocator, 1, &entities, &cand_lists);
    defer res.deinit();

    try testing.expectEqual(Decision.new, res.entities[0].decision);
    try testing.expectEqualStrings("ada_lovelace", res.entities[0].doc_ref.key);
}

test "resolution artifact serializes to the documented schema" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ slug _entity.text }}" }
    );
    defer resolver.deinit();

    const entities = [_]ExtractedEntity{.{ .local_id = "e0", .label = "Person", .text = "Ada Lovelace" }};
    var res = try resolver.resolve(testing.allocator, 42, &entities, &[_][]const Candidate{});
    defer res.deinit();

    const json = try res.toJson(testing.allocator);
    defer testing.allocator.free(json);

    // Round-trip to confirm it is valid and well-shaped.
    var parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, json, .{});
    defer parsed.deinit();
    const obj = parsed.value.object;
    try testing.expectEqual(@as(i64, 42), obj.get("config_generation").?.integer);
    const ents = obj.get("entities").?.array.items;
    try testing.expectEqual(@as(usize, 1), ents.len);
    try testing.expectEqualStrings("e0", ents[0].object.get("local_id").?.string);
    try testing.expectEqualStrings("new", ents[0].object.get("decision").?.string);
    try testing.expectEqualStrings("ada_lovelace", ents[0].object.get("doc_ref").?.object.get("key").?.string);
}

test "invalid resolver configs are rejected" {
    try testing.expectError(error.MissingTable, Resolver.parse(testing.allocator,
        \\{ "key_template": "{{ slug _entity.text }}" }
    ));
    try testing.expectError(error.MissingKeyTemplate, Resolver.parse(testing.allocator,
        \\{ "table": "entities" }
    ));
}

test "unknown template variables and helpers fail closed" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ bogus _entity.text }}" }
    );
    defer resolver.deinit();
    const entities = [_]ExtractedEntity{.{ .local_id = "e0", .label = "Person", .text = "Ada" }};
    try testing.expectError(error.InvalidTemplate, resolver.resolve(testing.allocator, 1, &entities, &[_][]const Candidate{}));
}

const extraction_json =
    \\{ "entities": [
    \\    { "id": "e0", "label": "person", "text": "Ada Lovelace", "spans": [{ "start": 0, "end": 12 }] },
    \\    { "id": "e1", "label": "org", "text": "Antfly" }
    \\  ],
    \\  "relations": [ { "type": "works_at", "source": { "entity_id": "e0" }, "target": { "entity_id": "e1" } } ]
    \\}
;

test "parseExtractionEntities reads the documented extraction shape" {
    var parsed = try parseExtractionEntities(testing.allocator, extraction_json);
    defer parsed.deinit();
    try testing.expectEqual(@as(usize, 2), parsed.entities.len);
    try testing.expectEqualStrings("e0", parsed.entities[0].local_id);
    try testing.expectEqualStrings("person", parsed.entities[0].label);
    try testing.expectEqualStrings("Ada Lovelace", parsed.entities[0].text);
    try testing.expectEqualStrings("Antfly", parsed.entities[1].text);
}

/// Minimal in-memory ArtifactStore for tests.
const MapStore = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMapUnmanaged([]u8) = .empty,

    fn deinit(self: *MapStore) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.alloc.free(e.value_ptr.*);
        }
        self.map.deinit(self.alloc);
    }

    fn store(self: *MapStore) ArtifactStore {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = ArtifactStore.VTable{ .get = get, .put = put, .delete = delete };

    fn get(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]u8 {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        const v = self.map.get(key) orelse return null;
        return try allocator.dupe(u8, v);
    }

    fn put(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        const owned_value = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned_value);
        const gop = try self.map.getOrPut(self.alloc, key);
        if (gop.found_existing) {
            self.alloc.free(gop.value_ptr.*);
        } else {
            gop.key_ptr.* = try self.alloc.dupe(u8, key);
        }
        gop.value_ptr.* = owned_value;
    }

    fn delete(ptr: *anyopaque, key: []const u8) anyerror!void {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        if (self.map.fetchRemove(key)) |kv| {
            self.alloc.free(kv.key);
            self.alloc.free(kv.value);
        }
    }
};

test "resolution stage writes, then skips when unchanged" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ lower _entity.label }}/{{ slug _entity.text }}" }
    );
    defer resolver.deinit();

    var map = MapStore{ .alloc = testing.allocator };
    defer map.deinit();
    try map.store().put("ext:doc1", extraction_json);

    const stage = ResolutionStage{ .resolver = &resolver, .config_generation = 3 };

    try testing.expectEqual(RunResult.written, try stage.run(testing.allocator, map.store(), null, "ext:doc1", "res:doc1"));
    // Idempotent replay: same input -> no rewrite.
    try testing.expectEqual(RunResult.unchanged, try stage.run(testing.allocator, map.store(), null, "ext:doc1", "res:doc1"));

    const stored = (try map.store().get(testing.allocator, "res:doc1")).?;
    defer testing.allocator.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, stored, .{});
    defer parsed.deinit();
    const ents = parsed.value.object.get("entities").?.array.items;
    try testing.expectEqual(@as(usize, 2), ents.len);
    try testing.expectEqualStrings("person/ada_lovelace", ents[0].object.get("doc_ref").?.object.get("key").?.string);
    try testing.expectEqualStrings("new", ents[0].object.get("decision").?.string);
}

test "resolution stage clears the artifact when the source is gone" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ slug _entity.text }}" }
    );
    defer resolver.deinit();

    var map = MapStore{ .alloc = testing.allocator };
    defer map.deinit();
    try map.store().put("ext:doc1", extraction_json);

    const stage = ResolutionStage{ .resolver = &resolver, .config_generation = 1 };
    _ = try stage.run(testing.allocator, map.store(), null, "ext:doc1", "res:doc1");
    try testing.expect(map.map.contains("res:doc1"));

    // Source deleted -> resolution cleared.
    try map.store().delete("ext:doc1");
    try testing.expectEqual(RunResult.cleared, try stage.run(testing.allocator, map.store(), null, "ext:doc1", "res:doc1"));
    try testing.expect(!map.map.contains("res:doc1"));
    // Nothing left to clear.
    try testing.expectEqual(RunResult.source_missing, try stage.run(testing.allocator, map.store(), null, "ext:doc1", "res:doc1"));
}

/// Test candidate provider that offers one fixed entity for a given label.
const FixedCandidate = struct {
    doc_ref: DocRef,
    label: []const u8,
    name: []const u8,

    fn provider(self: *FixedCandidate) CandidateProvider {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = CandidateProvider.VTable{ .candidates_for = candidatesFor };

    fn candidatesFor(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        entity: ExtractedEntity,
        out: *std.ArrayListUnmanaged(Candidate),
    ) anyerror!void {
        const self: *FixedCandidate = @ptrCast(@alignCast(ptr));
        _ = entity;
        const fields = try allocator.alloc(matcher.Field, 1);
        fields[0] = .{ .name = "canonical_name", .value = .{ .text = try allocator.dupe(u8, self.name) } };
        try out.append(allocator, .{ .doc_ref = self.doc_ref, .label = self.label, .record = .{ .fields = fields } });
    }
};

test "resolution stage links to a candidate supplied by the provider" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ slug _entity.text }}",
        \\  "scorer": {
        \\    "comparisons": [
        \\      { "name": "name", "left": "canonical_text", "right": "canonical_name",
        \\        "levels": [ { "when": "exact", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] }
        \\    ],
        \\    "combine": { "bias": -3.0 }, "decision": { "match": 0.9 }
        \\  } }
    );
    defer resolver.deinit();

    var map = MapStore{ .alloc = testing.allocator };
    defer map.deinit();
    try map.store().put("ext:doc1",
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovelace" } ] }
    );

    var candidate = FixedCandidate{
        .doc_ref = .{ .table = "entities", .key = "person/ada_lovelace" },
        .label = "person",
        .name = "Ada Lovelace",
    };
    const stage = ResolutionStage{ .resolver = &resolver, .config_generation = 1 };
    try testing.expectEqual(RunResult.written, try stage.run(testing.allocator, map.store(), candidate.provider(), "ext:doc1", "res:doc1"));

    const stored = (try map.store().get(testing.allocator, "res:doc1")).?;
    defer testing.allocator.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
}

const FixedVectorCandidate = struct {
    doc_ref: DocRef,
    label: []const u8,
    vector: []const f32,

    fn provider(self: *FixedVectorCandidate) CandidateProvider {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = CandidateProvider.VTable{ .candidates_for = candidatesFor };
    fn candidatesFor(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        entity: ExtractedEntity,
        out: *std.ArrayListUnmanaged(Candidate),
    ) anyerror!void {
        const self: *FixedVectorCandidate = @ptrCast(@alignCast(ptr));
        _ = entity;
        const fields = try allocator.alloc(matcher.Field, 1);
        fields[0] = .{ .name = "name_embedding", .value = .{ .vector = try allocator.dupe(f32, self.vector) } };
        try out.append(allocator, .{ .doc_ref = self.doc_ref, .label = self.label, .record = .{ .fields = fields } });
    }
};

const FakeMentionEmbedder = struct {
    vector: []const f32,
    calls: usize = 0,

    fn mentionEmbedder(self: *FakeMentionEmbedder) MentionEmbedder {
        return .{ .ptr = self, .embed_fn = embed };
    }
    fn embed(ptr: *anyopaque, alloc: std.mem.Allocator, text: []const u8) anyerror!?[]const f32 {
        _ = text;
        const self: *FakeMentionEmbedder = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        return try alloc.dupe(f32, self.vector);
    }
};

test "resolution stage backfills a mention embedding so cosine blocking links" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ lower _entity.label }}/{{ slug _entity.text }}",
        \\  "scorer": {
        \\    "comparisons": [
        \\      { "name": "emb", "left": "name_embedding", "right": "name_embedding",
        \\        "levels": [ { "when": "cosine > 0.9", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] }
        \\    ],
        \\    "combine": { "bias": -3.0 }, "decision": { "match": 0.9 }
        \\  } }
    );
    defer resolver.deinit();

    var map = MapStore{ .alloc = testing.allocator };
    defer map.deinit();
    // Extraction carries no embedding; the stage must backfill it.
    try map.store().put("ext:doc1",
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovelace" } ] }
    );

    const vec = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    var candidate = FixedVectorCandidate{
        .doc_ref = .{ .table = "entities", .key = "person/ada_lovelace" },
        .label = "person",
        .vector = vec[0..],
    };

    // Without an embedder, the mention has no vector: cosine cannot match, so a
    // new key is minted instead of linking.
    {
        const stage = ResolutionStage{ .resolver = &resolver, .config_generation = 1 };
        try testing.expectEqual(RunResult.written, try stage.run(testing.allocator, map.store(), candidate.provider(), "ext:doc1", "res:none"));
        const stored = (try map.store().get(testing.allocator, "res:none")).?;
        defer testing.allocator.free(stored);
        var parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, stored, .{});
        defer parsed.deinit();
        try testing.expectEqualStrings("new", parsed.value.object.get("entities").?.array.items[0].object.get("decision").?.string);
    }

    // With the embedder backfilling the mention vector, cosine matches the
    // candidate and the mention links to the existing entity.
    {
        var embedder = FakeMentionEmbedder{ .vector = vec[0..] };
        const stage = ResolutionStage{ .resolver = &resolver, .config_generation = 1, .embedder = embedder.mentionEmbedder() };
        try testing.expectEqual(RunResult.written, try stage.run(testing.allocator, map.store(), candidate.provider(), "ext:doc1", "res:emb"));
        try testing.expect(embedder.calls >= 1);
        const stored = (try map.store().get(testing.allocator, "res:emb")).?;
        defer testing.allocator.free(stored);
        var parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, stored, .{});
        defer parsed.deinit();
        const ent = parsed.value.object.get("entities").?.array.items[0].object;
        try testing.expectEqualStrings("match", ent.get("decision").?.string);
        try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
    }
}
