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

//! Deterministic entity resolver core (see zig/RESOLUTION.md).
//!
//! Turns extracted entity mentions into a durable resolution artifact mapping
//! each local id to a canonical `DocRef`. Two paths:
//!
//!   * deterministic: render a canonical key from the mention via `key_template`
//!     ("mint a new entity"); this is pure and needs no global state, so the
//!     resolution artifact can be replayed consistently before the entity
//!     document exists.
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
    /// Trusted execution binding persisted for deferred promotion. Logical
    /// table remains the public endpoint and curation identity.
    storage_table: ?[]const u8 = null,
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
    /// Extractor's asserted confidence in this mention, in [0, 1]. Defaults to
    /// 1.0 when the extractor omits it (legacy mentions are fully trusted). Fed
    /// into provenance-edge confidence fusion.
    confidence: f64 = 1.0,
    /// Optional normalized predicate (main verb lemma) the extractor asserted
    /// for an event mention. Feeds `event_identity`.
    predicate: []const u8 = "",
    /// Compositional event identity computed by `parseExtractionEntities`
    /// from the artifact's relations: the sorted, comma-joined slugs of the
    /// NON-event mentions related to this mention, then `|`, then the
    /// slugged predicate (or the raw text when no predicate was asserted).
    /// Two differently worded sentences about the same participants and
    /// action converge on the same identity; with no related participants it
    /// degrades to the mention text, i.e. exactly the per-sentence identity
    /// a plain `{{ hash _entity.text }}` template produces. Empty when the
    /// mention was constructed without relation context — `_entity.event_identity`
    /// then renders the mention text.
    event_identity: []const u8 = "",
};

/// A resolution candidate fetched by blocking. `record` is scored against the
/// mention; `label` gates `type_must_match`. When blocking finds a merged-away
/// duplicate, `resolved_doc_ref` / `resolved_record` carry the canonical survivor
/// to link/promote after scoring against the duplicate.
/// Cache-only vector locator. Generated vectors reference their dense id;
/// application vectors reference the primary document field instead.
pub const VectorReference = struct {
    field: []const u8,
    table: []const u8,
    index_name: []const u8,
    key: []const u8,
    vector_id: ?u64 = null,
    index_revision: [32]u8,
    dimensions: usize,
};

pub const EmbeddingCache = struct {
    ptr: *anyopaque,
    get: *const fn (*anyopaque, std.mem.Allocator, [32]u8) anyerror!?[]const f32,
    put: *const fn (*anyopaque, [32]u8, []const f32) anyerror!void,
};

pub const VectorLoader = struct {
    ptr: *anyopaque,
    load: *const fn (*anyopaque, std.mem.Allocator, VectorReference) anyerror!?[]const f32,
};

pub const Candidate = struct {
    doc_ref: DocRef,
    label: []const u8,
    record: matcher.Record,
    resolved_doc_ref: ?DocRef = null,
    resolved_record: ?matcher.Record = null,
    /// Validated raw comparison features for this mention/candidate pair.
    features: ?[]const matcher.Feature = null,
    vector_refs: []const VectorReference = &.{},
};

fn hasCrossTableRedirect(candidates: []const Candidate) bool {
    for (candidates) |candidate| {
        if (candidate.resolved_doc_ref) |survivor| {
            if (!std.mem.eql(u8, candidate.doc_ref.table, survivor.table)) return true;
        }
    }
    return false;
}

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
    /// Canonical entity name to persist on the entity document. For matches this
    /// comes from the matched candidate; for new/review decisions it is the
    /// mention text that minted the entity.
    canonical_name: []const u8 = "",
    /// Original mention text. The promoter unions this as an alias so mention
    /// variants do not clobber an existing entity's canonical name.
    surface_form: []const u8 = "",
};

/// The resolution artifact: the durable record of identity decisions for one
/// source document. Owns its strings in an arena.
pub const Resolution = struct {
    arena: std.heap.ArenaAllocator,
    config_generation: u64,
    entities: []const ResolvedEntity,
    skipped_mentions: usize = 0,
    work: ?WorkCache = null,
    max_work_bytes: usize = 1024 * 1024,

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
        try out.appendSlice(allocator, ",\"skipped_mentions\":");
        try appendInt(allocator, &out, self.skipped_mentions);
        try out.appendSlice(allocator, ",\"entities\":[");
        for (self.entities, 0..) |e, i| {
            if (i > 0) try out.append(allocator, ',');
            try out.appendSlice(allocator, "{\"local_id\":");
            try writeJsonString(allocator, &out, e.local_id);
            try out.appendSlice(allocator, ",\"doc_ref\":{\"table\":");
            try writeJsonString(allocator, &out, e.doc_ref.table);
            try out.appendSlice(allocator, ",\"key\":");
            try writeJsonString(allocator, &out, e.doc_ref.key);
            if (e.doc_ref.storage_table) |physical| {
                try out.appendSlice(allocator, ",\"storage_table\":");
                try writeJsonString(allocator, &out, physical);
            }
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
            if (e.surface_form.len > 0) {
                try out.appendSlice(allocator, ",\"surface_form\":");
                try writeJsonString(allocator, &out, e.surface_form);
            }
            try out.append(allocator, '}');
        }
        try out.append(allocator, ']');
        if (self.work) |work| {
            const encoded = try work.toJson(allocator);
            defer allocator.free(encoded);
            if (encoded.len <= self.max_work_bytes) {
                try out.appendSlice(allocator, ",\"_resolution_work\":");
                try out.appendSlice(allocator, encoded);
            }
        }
        try out.append(allocator, '}');
        return out.toOwnedSlice(allocator);
    }
};

/// A parsed, reusable resolver config. Owns the entity table name, key template,
/// and optional scorer.
/// Explicit redirects between fully rendered keys in the resolver's table.
pub const KeyAlias = struct { key: []const u8, target: []const u8 };

pub fn canonicalAliasKey(aliases: []const KeyAlias, key: []const u8) ![]const u8 {
    var current = key;
    for (0..aliases.len + 1) |_| {
        var next: ?[]const u8 = null;
        for (aliases) |alias| {
            if (std.mem.eql(u8, alias.key, current)) {
                next = alias.target;
                break;
            }
        }
        current = next orelse return current;
    }
    return error.InvalidResolverAliasCycle;
}

pub fn validateAliases(aliases: []const KeyAlias) !void {
    for (aliases, 0..) |alias, i| {
        if (alias.key.len == 0 or alias.target.len == 0) return error.InvalidResolverAlias;
        for (aliases[0..i]) |prior| if (std.mem.eql(u8, prior.key, alias.key)) return error.InvalidResolverAlias;
        _ = try canonicalAliasKey(aliases, alias.key);
    }
}

pub const Resolver = struct {
    arena: std.heap.ArenaAllocator,
    table: []const u8,
    key_template: []const u8,
    aliases: []const KeyAlias = &.{},
    /// Mention labels this resolver consumes; empty is a catch-all. Lets
    /// several resolvers partition one extraction artifact by label (e.g.
    /// `event` mentions to an events table, everything else to entities).
    labels: []const []const u8,
    /// Mention labels this resolver must NOT consume. For a catch-all resolver
    /// sharing a source artifact with labeled siblings, the runtime derives
    /// this from the siblings' claimed labels so extraction labels stay
    /// open-vocabulary (unlisted labels fall through to the catch-all instead
    /// of being dropped).
    exclude_labels: []const []const u8,
    type_must_match: bool,
    scorer: ?matcher.Scorer,
    /// Mention admission floor; see LabelRouting.min_confidence.
    min_confidence: f64 = 0,

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

        var labels: []const []const u8 = &.{};
        if (obj.get("labels")) |lv| {
            if (lv != .array) return error.InvalidConfig;
            const owned = try a.alloc([]const u8, lv.array.items.len);
            for (lv.array.items, 0..) |item, i| {
                owned[i] = try a.dupe(u8, jsonString(item) orelse return error.InvalidConfig);
            }
            labels = owned;
        }
        var exclude_labels: []const []const u8 = &.{};
        if (obj.get("exclude_labels")) |lv| {
            if (lv != .array) return error.InvalidConfig;
            const owned = try a.alloc([]const u8, lv.array.items.len);
            for (lv.array.items, 0..) |item, i| {
                owned[i] = try a.dupe(u8, jsonString(item) orelse return error.InvalidConfig);
            }
            exclude_labels = owned;
        }

        var min_confidence: f64 = 0;
        if (obj.get("min_confidence")) |mv| {
            min_confidence = switch (mv) {
                .float => |f| f,
                .integer => |n| @floatFromInt(n),
                else => return error.InvalidConfig,
            };
            if (!(min_confidence >= 0 and min_confidence <= 1)) return error.InvalidConfig;
        }

        var scorer: ?matcher.Scorer = null;
        errdefer if (scorer) |*s| s.deinit();
        if (obj.get("scorer")) |sv| {
            scorer = try matcher.Scorer.parseValue(gpa, sv);
        }

        const aliases = if (obj.get("aliases")) |value|
            (try std.json.parseFromValue([]KeyAlias, a, value, .{ .allocate = .alloc_always })).value
        else
            &.{};
        try validateAliases(aliases);

        return .{
            .arena = arena,
            .table = table,
            .key_template = key_template,
            .aliases = aliases,
            .labels = labels,
            .exclude_labels = exclude_labels,
            .type_must_match = type_must_match,
            .scorer = scorer,
            .min_confidence = min_confidence,
        };
    }

    /// Build a resolver directly from its parts (the durable catalog config),
    /// parsing `scorer_json` if present. An empty `scorer_json` means a purely
    /// deterministic resolver that mints canonical keys from `key_template`.
    /// Label routing for `initFromParts`: which mention labels this resolver
    /// consumes (`labels` allowlist, empty = catch-all) and which it must skip
    /// (`exclude_labels`, typically the labels claimed by sibling resolvers on
    /// the same source artifact).
    pub const LabelRouting = struct {
        labels: []const []const u8 = &.{},
        exclude_labels: []const []const u8 = &.{},
        /// Mention admission floor: mentions whose extractor-asserted
        /// confidence is below this never resolve — no canonical key, no
        /// mention edge, and (through the canonical-only endpoint rule) no
        /// relation edge endpoint. The cheap post-extraction junk filter for
        /// score-carrying extractors (GLiNER); 0 admits everything.
        min_confidence: f64 = 0,
    };

    pub fn initFromParts(
        gpa: std.mem.Allocator,
        table: []const u8,
        key_template: []const u8,
        routing: LabelRouting,
        type_must_match: bool,
        scorer_json: []const u8,
    ) !Resolver {
        var arena = std.heap.ArenaAllocator.init(gpa);
        errdefer arena.deinit();
        const a = arena.allocator();
        const owned_table = try a.dupe(u8, table);
        const owned_template = try a.dupe(u8, key_template);
        var owned_labels: []const []const u8 = &.{};
        if (routing.labels.len > 0) {
            const out = try a.alloc([]const u8, routing.labels.len);
            for (routing.labels, 0..) |l, i| out[i] = try a.dupe(u8, l);
            owned_labels = out;
        }
        var owned_excludes: []const []const u8 = &.{};
        if (routing.exclude_labels.len > 0) {
            const out = try a.alloc([]const u8, routing.exclude_labels.len);
            for (routing.exclude_labels, 0..) |l, i| out[i] = try a.dupe(u8, l);
            owned_excludes = out;
        }

        var scorer: ?matcher.Scorer = null;
        errdefer if (scorer) |*s| s.deinit();
        if (scorer_json.len > 0) {
            scorer = try matcher.Scorer.parse(gpa, scorer_json);
        }

        return .{
            .arena = arena,
            .table = owned_table,
            .key_template = owned_template,
            .labels = owned_labels,
            .exclude_labels = owned_excludes,
            .type_must_match = type_must_match,
            .scorer = scorer,
            .min_confidence = routing.min_confidence,
        };
    }

    /// Whether this resolver consumes mentions with the given label; exclusions
    /// win, then an empty `labels` set consumes everything else.
    pub fn consumesLabel(self: *const Resolver, label: []const u8) bool {
        for (self.exclude_labels) |l| if (std.mem.eql(u8, l, label)) return false;
        if (self.labels.len == 0) return true;
        for (self.labels) |l| if (std.mem.eql(u8, l, label)) return true;
        return false;
    }

    /// Candidate fields compared to the mention's generated name vector.
    pub fn candidateVectorFields(self: *const Resolver, a: std.mem.Allocator) ![]const []const u8 {
        var fields: std.ArrayListUnmanaged([]const u8) = .empty;
        if (self.scorer) |scorer| for (scorer.comparisons) |comparison| {
            if (!std.mem.eql(u8, comparison.left, "name_embedding")) continue;
            var duplicate = false;
            for (fields.items) |field| if (std.mem.eql(u8, field, comparison.right)) {
                duplicate = true;
                break;
            };
            if (!duplicate) try fields.append(a, comparison.right);
        };
        return fields.items;
    }

    /// In-place compaction of `entities` down to the mentions this resolver
    /// consumes. Returns the kept prefix. Used before candidate blocking so
    /// filtered mentions never pay for embedding backfill or candidate search.
    pub fn filterEntitiesByLabel(self: *const Resolver, entities: []ExtractedEntity) []ExtractedEntity {
        if (self.labels.len == 0 and self.exclude_labels.len == 0 and self.min_confidence <= 0) return entities;
        var kept: usize = 0;
        for (entities) |entity| {
            if (!self.consumesLabel(entity.label)) continue;
            // Below-floor mentions never resolve: no canonical key, no
            // mention edge, and relation endpoints referencing them stay
            // unresolvable (the canonical-only rule drops those edges).
            if (entity.confidence < self.min_confidence) continue;
            entities[kept] = entity;
            kept += 1;
        }
        return entities[0..kept];
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

        var resolved: std.ArrayListUnmanaged(ResolvedEntity) = .empty;
        for (entities, 0..) |entity, i| {
            const cands: []const Candidate = if (i < candidates.len) candidates[i] else &.{};
            var value = self.resolveOne(a, gpa, entity, cands) catch |err| switch (err) {
                error.EmptyEntityKey => {
                    std.log.warn("resolver skipped mention {s}: key template produced an empty slug", .{entity.local_id});
                    continue;
                },
                else => return err,
            };
            if (std.mem.eql(u8, value.doc_ref.table, self.table)) {
                const canonical = try canonicalAliasKey(self.aliases, value.doc_ref.key);
                value.doc_ref.key = try a.dupe(u8, canonical);
            }
            try resolved.append(a, value);
        }

        return .{ .arena = arena, .config_generation = config_generation, .entities = resolved.items, .skipped_mentions = entities.len - resolved.items.len };
    }

    fn resolveOne(
        self: *const Resolver,
        a: std.mem.Allocator,
        scratch: std.mem.Allocator,
        entity: ExtractedEntity,
        candidates: []const Candidate,
    ) !ResolvedEntity {
        const local_id = try a.dupe(u8, entity.local_id);

        // An exact minted-key redirect is an explicit administrator alias,
        // independent of probabilistic scoring. Never infer this from a fuzzy
        // candidate, or link a dangling redirect to an unavailable survivor.
        const minted_key = try renderKey(a, self.key_template, entity);
        const canonical_key = try canonicalAliasKey(self.aliases, minted_key);
        if (!std.mem.eql(u8, minted_key, canonical_key)) return .{
            .local_id = local_id,
            .doc_ref = .{ .table = try a.dupe(u8, self.table), .key = try a.dupe(u8, canonical_key) },
            .confidence = 1,
            .decision = .match,
            .label = try a.dupe(u8, entity.label),
            .canonical_name = try a.dupe(u8, entity.text),
            .surface_form = try a.dupe(u8, entity.text),
        };
        for (candidates) |candidate| {
            if (!std.mem.eql(u8, candidate.doc_ref.table, self.table) or
                !std.mem.eql(u8, candidate.doc_ref.key, minted_key)) continue;
            if (self.type_must_match and !std.mem.eql(u8, candidate.label, entity.label)) continue;
            if (recordTextField(candidate.record, "merged_into") == null) continue;
            const survivor = candidate.resolved_doc_ref orelse continue;
            const record = candidate.resolved_record orelse continue;
            return .{
                .local_id = local_id,
                .doc_ref = .{ .table = try a.dupe(u8, survivor.table), .key = try a.dupe(u8, survivor.key) },
                .confidence = 1,
                .decision = .match,
                .label = try a.dupe(u8, entity.label),
                .canonical_name = try a.dupe(u8, recordTextField(record, "canonical_name") orelse entity.text),
                .surface_form = try a.dupe(u8, entity.text),
            };
        }

        if (self.scorer) |*scorer| {
            if (candidates.len > 0) {
                var mention_fields = [_]matcher.Field{
                    .{ .name = "label", .value = .{ .text = entity.label } },
                    .{ .name = "text", .value = .{ .text = entity.text } },
                    .{ .name = "canonical_text", .value = .{ .text = entity.text } },
                    .{ .name = "name_embedding", .value = if (entity.embedding) |emb| .{ .vector = emb } else .none },
                };
                const mention = matcher.Record{ .fields = mention_fields[0..if (entity.embedding != null) @as(usize, 4) else 3] };

                var best_prob: f64 = -1;
                var best: ?usize = null;
                var best_outcome: matcher.Outcome = .no_match;
                for (candidates, 0..) |cand, ci| {
                    if (self.type_must_match and !std.mem.eql(u8, cand.label, entity.label)) continue;
                    const r = if (cand.features) |features| scorer.scoreFeatures(features) else scorer.score(scratch, mention, cand.record);
                    if (r.probability > best_prob) {
                        best_prob = r.probability;
                        best = ci;
                        best_outcome = r.outcome;
                    }
                }

                if (best) |bi| {
                    if (best_outcome == .match) {
                        const matched = candidates[bi];
                        if (recordTextField(matched.record, "merged_into") != null and matched.resolved_record == null) {
                            // The duplicate points at a survivor, but blocking
                            // could not read that survivor. Do not promote a
                            // survivor key with duplicate scalar fields; surface
                            // the mention for review/retry instead.
                            return .{
                                .local_id = local_id,
                                .doc_ref = try self.mintRef(a, entity),
                                .confidence = best_prob,
                                .decision = .review,
                                .label = try a.dupe(u8, entity.label),
                                .canonical_name = try a.dupe(u8, entity.text),
                                .surface_form = try a.dupe(u8, entity.text),
                            };
                        }
                        const target_ref = matched.resolved_doc_ref orelse matched.doc_ref;
                        const canonical_record = matched.resolved_record orelse matched.record;
                        const matched_name = recordTextField(canonical_record, "canonical_name") orelse entity.text;
                        return .{
                            .local_id = local_id,
                            .doc_ref = .{
                                .table = try a.dupe(u8, target_ref.table),
                                .key = try a.dupe(u8, target_ref.key),
                            },
                            .confidence = best_prob,
                            .decision = .match,
                            .label = try a.dupe(u8, entity.label),
                            .canonical_name = try a.dupe(u8, matched_name),
                            .surface_form = try a.dupe(u8, entity.text),
                        };
                    }
                    return .{
                        .local_id = local_id,
                        .doc_ref = try self.mintRef(a, entity),
                        .confidence = best_prob,
                        .decision = if (best_outcome == .review) .review else .new,
                        .label = try a.dupe(u8, entity.label),
                        .canonical_name = try a.dupe(u8, entity.text),
                        .surface_form = try a.dupe(u8, entity.text),
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
            .surface_form = try a.dupe(u8, entity.text),
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
    if (out.items.len == 0) return error.EmptyEntityKey;
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
    if (std.mem.eql(u8, name, "_entity.predicate")) return entity.predicate;
    // Compositional event identity; degrades to the mention text when no
    // relation context was available (see ExtractedEntity.event_identity),
    // so `event/{{ hash _entity.event_identity }}` is never weaker than the
    // per-sentence `event/{{ hash _entity.text }}` it replaces.
    if (std.mem.eql(u8, name, "_entity.event_identity"))
        return if (entity.event_identity.len > 0) entity.event_identity else entity.text;
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
    if (std.mem.eql(u8, helper, "uslug")) {
        try @import("unicode_slug.zig").append(a, out, value);
        return;
    }
    if (std.mem.eql(u8, helper, "slug")) {
        const start = out.items.len;
        try appendSlug(a, out, value);
        if (out.items.len == start) return error.EmptyEntityKey;
        return;
    }
    if (std.mem.eql(u8, helper, "hash")) {
        try appendStableHash(a, out, value);
        return;
    }
    return error.InvalidTemplate;
}

/// 16 lowercase hex chars of xxhash64 (fixed seed 0) over the raw value.
/// Deterministic across replays and platforms so re-extraction of the same
/// normalized text (e.g. an event sentence) mints the same canonical key.
fn appendStableHash(a: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const digest = std.hash.XxHash64.hash(0, value);
    var buf: [16]u8 = undefined;
    const hex = std.fmt.bufPrint(&buf, "{x:0>16}", .{digest}) catch unreachable;
    try out.appendSlice(a, hex);
}

/// lowercased, alphanumeric runs separated by single '_', no leading/trailing
/// separators. "A. Lovelace" -> "a_lovelace". English possessives are
/// stripped ("Epstein's island" -> "epstein_island", not "epstein_s_island"),
/// for both ASCII apostrophe and U+2019, so possessive and bare mentions of
/// the same name converge on one canonical key.
fn appendSlug(a: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    var wrote = false;
    var pending_sep = false;
    var i: usize = 0;
    while (i < value.len) : (i += 1) {
        const c = value[i];
        const apostrophe_len: usize = if (c == '\'')
            1
        else if (c == 0xE2 and i + 2 < value.len and value[i + 1] == 0x80 and value[i + 2] == 0x99)
            3
        else
            0;
        if (apostrophe_len > 0 and wrote and !pending_sep) {
            const s_index = i + apostrophe_len;
            const after_s = s_index + 1;
            const s_is_possessive = s_index < value.len and
                (value[s_index] == 's' or value[s_index] == 'S') and
                (after_s >= value.len or !std.ascii.isAlphanumeric(value[after_s]));
            if (s_is_possessive) {
                i = s_index; // consume the apostrophe and the trailing s
                continue;
            }
        }
        const lc = std.ascii.toLower(c);
        if (std.ascii.isAlphanumeric(lc)) {
            if (pending_sep and wrote) try out.append(a, '_');
            try out.append(a, lc);
            wrote = true;
            pending_sep = false;
        } else if (wrote) {
            pending_sep = true;
        }
        if (apostrophe_len == 3) i += 2;
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
/// Reads a text field's value from a matcher record, or null. Used to follow a
/// candidate entity's `merged_into` redirect and canonical fields.
fn recordTextField(record: matcher.Record, name: []const u8) ?[]const u8 {
    for (record.fields) |field| {
        if (!std.mem.eql(u8, field.name, name)) continue;
        return switch (field.value) {
            .text => |t| if (t.len > 0) t else null,
            else => null,
        };
    }
    return null;
}

pub const ParsedEntities = struct {
    arena: std.heap.ArenaAllocator,
    /// Mutable so the resolution stage can backfill name embeddings in place.
    entities: []ExtractedEntity,

    pub fn deinit(self: *ParsedEntities) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// A human curation override for one mention: forces its decision and canonical
/// endpoint regardless of what the scorer would pick. The strings are borrowed
/// for the duration of the stage run (the stage copies them into the resolution
/// arena).
pub const Override = struct {
    decision: Decision,
    table: []const u8,
    key: []const u8,
};

/// Supplies curation overrides keyed by a mention's `local_id` within the
/// document being resolved. A function-pointer seam keeps the resolver library
/// decoupled from the override store; the storage layer constructs one per
/// document from recorded review decisions.
pub const OverrideProvider = struct {
    ptr: *anyopaque,
    override_for: *const fn (ptr: *anyopaque, local_id: []const u8) ?Override,

    pub fn overrideFor(self: OverrideProvider, local_id: []const u8) ?Override {
        return self.override_for(self.ptr, local_id);
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
/// documents; carrying `label`/`canonical_name`/`surface_form` keeps it
/// self-contained.
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
        const canonical_name = if (o.get("canonical_name")) |v| try a.dupe(u8, jsonString(v) orelse "") else "";
        const surface_form = if (o.get("surface_form")) |v|
            try a.dupe(u8, jsonString(v) orelse "")
        else if (canonical_name.len > 0)
            try a.dupe(u8, canonical_name)
        else
            "";
        out[i] = .{
            .local_id = try a.dupe(u8, jsonString(o.get("local_id") orelse return error.InvalidResolution) orelse return error.InvalidResolution),
            .doc_ref = .{
                .table = try a.dupe(u8, jsonString(ref.object.get("table") orelse return error.InvalidResolution) orelse return error.InvalidResolution),
                .key = try a.dupe(u8, jsonString(ref.object.get("key") orelse return error.InvalidResolution) orelse return error.InvalidResolution),
                .storage_table = if (ref.object.get("storage_table")) |v| try a.dupe(u8, jsonString(v) orelse return error.InvalidResolution) else null,
            },
            .confidence = switch (o.get("confidence") orelse std.json.Value{ .float = 0 }) {
                .float => |f| f,
                .integer => |n| @floatFromInt(n),
                else => 0,
            },
            .decision = decisionFromTag(decision_tag) orelse return error.InvalidResolution,
            .label = if (o.get("label")) |v| try a.dupe(u8, jsonString(v) orelse "") else "",
            .canonical_name = canonical_name,
            .surface_form = surface_form,
        };
    }
    return .{ .arena = arena, .config_generation = config_generation, .entities = out };
}

/// Parse the `entities` array of an extraction artifact (the shape produced by
/// the extractor and documented in RESOLUTION.md / GRAPH.md). Relations are not
/// needed here -- they are consumed by the graph materializer, which reads the
/// extraction artifact plus this resolver's resolution artifact.
pub fn parseExtractionEntities(gpa: std.mem.Allocator, json_bytes: []const u8) !ParsedEntities {
    return try parseExtractionEntitiesWithResolutions(gpa, json_bytes, null);
}

/// `sibling_resolutions_json`, when provided, is a map of mention local id to
/// `{"key": <canonical doc key>, "table": ...}` — the same shape the graph
/// materializer injects into an artifact as `_entities`. Event identity then
/// composes from the participants' CANONICAL keys instead of their raw
/// mention text, so a matcher-scorer merge ("A. Lovelace" into
/// entity/ada_lovelace) re-keys the events it participates in rather than
/// leaving them pinned to the stale surface-form slug.
pub fn parseExtractionEntitiesWithResolutions(
    gpa: std.mem.Allocator,
    json_bytes: []const u8,
    sibling_resolutions_json: ?[]const u8,
) !ParsedEntities {
    var parsed = try std.json.parseFromSlice(std.json.Value, gpa, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidExtraction;

    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();
    const a = arena.allocator();

    if (isDedicatedMentionArtifact(parsed.value.object)) {
        const out = try a.alloc(ExtractedEntity, 1);
        out[0] = try parseExtractedEntityObject(a, parsed.value.object, null);
        return .{ .arena = arena, .entities = out };
    }

    const entities_v = parsed.value.object.get("entities") orelse return error.InvalidExtraction;
    if (entities_v != .array) return error.InvalidExtraction;

    const out = try a.alloc(ExtractedEntity, entities_v.array.items.len);
    for (entities_v.array.items, 0..) |ev, i| {
        if (ev != .object) return error.InvalidExtraction;
        out[i] = try parseExtractedEntityObject(a, ev.object, i);
    }
    // An explicit sibling map wins; an artifact that already carries an
    // injected `_entities` map (the materializer's convention) composes from
    // it the same way. Malformed resolutions degrade to raw mention text.
    const resolutions: ?std.json.Value = blk: {
        if (sibling_resolutions_json) |raw| {
            const value = std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{}) catch break :blk null;
            if (value == .object) break :blk value;
            break :blk null;
        }
        break :blk parsed.value.object.get("_entities");
    };
    try computeEventIdentities(a, parsed.value.object, resolutions, out);
    return .{ .arena = arena, .entities = out };
}

/// Compositional event identity (see ExtractedEntity.event_identity): for
/// every mention, gather the OTHER mentions it is related to through the
/// artifact's `relations`, keep the non-"event" ones (an event related to
/// other events would otherwise fold their unstable sentence texts into its
/// own identity), and join their sorted slugs with the mention's asserted
/// predicate (or its text when no predicate was asserted). Two differently
/// worded event sentences with the same participants and predicate then mint
/// the same canonical key, which is what lets `participates_in` mass
/// accumulate on shared event nodes across documents. A participant covered
/// by `resolutions` (the `_entities` map shape) contributes its canonical
/// key's final path segment instead of its raw text, so entity merges re-key
/// the events they touch (see canonicalParticipantSegment). Endpoints are
/// matched the way the graph materializer matches them: local-id strings,
/// `{entity_id|id|local_id}` objects, or positional `{entity_index}` objects.
fn computeEventIdentities(a: std.mem.Allocator, root: std.json.ObjectMap, resolutions: ?std.json.Value, out: []ExtractedEntity) !void {
    if (out.len == 0) return;
    const relations_v = root.get("relations") orelse return;
    if (relations_v != .array) return;

    var related = try a.alloc(std.ArrayListUnmanaged(usize), out.len);
    for (related) |*list| list.* = .empty;

    for (relations_v.array.items) |rv| {
        if (rv != .object) continue;
        const source = resolveRelationEndpointIndex(rv.object.get("source"), out) orelse continue;
        const target = resolveRelationEndpointIndex(rv.object.get("target"), out) orelse continue;
        if (source == target) continue;
        try related[source].append(a, target);
        try related[target].append(a, source);
    }

    for (out, 0..) |*entity, i| {
        var slugs = std.ArrayListUnmanaged([]const u8).empty;
        for (related[i].items) |other| {
            if (std.ascii.eqlIgnoreCase(out[other].label, "event")) continue;
            var slug = std.ArrayListUnmanaged(u8).empty;
            // Prefer the participant's canonical key from the resolution
            // map: its final path segment IS the slug the entity key
            // template minted (`entity/{{ slug _entity.text }}` and
            // friends), so pre- and post-resolution identities agree until
            // a merge actually moves the mention — and then the event
            // re-keys with the survivor instead of staying pinned to the
            // stale surface-form slug.
            if (canonicalParticipantSegment(resolutions, out[other].local_id)) |segment| {
                try appendSlug(a, &slug, segment);
            } else {
                try appendSlug(a, &slug, out[other].text);
            }
            if (slug.items.len == 0) continue;
            try slugs.append(a, try slug.toOwnedSlice(a));
        }
        std.mem.sort([]const u8, slugs.items, {}, struct {
            fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
                return std.mem.order(u8, lhs, rhs) == .lt;
            }
        }.lessThan);

        // The composite identity requires at least one non-event
        // participant. A predicate alone is NOT identifying — "|meet" would
        // merge every participant-less meeting in the corpus into one node
        // (the event-event pass, whose related mentions are all events,
        // would produce exactly that). Without participants the identity
        // stays empty and `_entity.event_identity` degrades to the raw
        // sentence text — uniformly, whether the artifact carried an empty
        // relations array or none at all.
        if (slugs.items.len == 0) continue;

        var identity = std.ArrayListUnmanaged(u8).empty;
        var previous: ?[]const u8 = null;
        for (slugs.items) |slug| {
            if (previous) |seen| if (std.mem.eql(u8, seen, slug)) continue;
            if (identity.items.len > 0) try identity.append(a, ',');
            try identity.appendSlice(a, slug);
            previous = slug;
        }
        try identity.append(a, '|');
        if (entity.predicate.len > 0) {
            try appendSlug(a, &identity, entity.predicate);
        } else {
            try identity.appendSlice(a, entity.text);
        }
        entity.event_identity = try identity.toOwnedSlice(a);
    }
}

/// The canonical-key path segment an event identity composes for a resolved
/// participant: the substring after the key's last '/'. Null when the
/// resolution map is absent, does not cover the mention, or carries no key —
/// the caller then falls back to the raw mention text.
fn canonicalParticipantSegment(resolutions: ?std.json.Value, local_id: []const u8) ?[]const u8 {
    const res = resolutions orelse return null;
    if (res != .object) return null;
    const entry = res.object.get(local_id) orelse return null;
    if (entry != .object) return null;
    const key = entry.object.get("key") orelse return null;
    if (key != .string or key.string.len == 0) return null;
    const segment = if (std.mem.lastIndexOfScalar(u8, key.string, '/')) |idx|
        key.string[idx + 1 ..]
    else
        key.string;
    return if (segment.len > 0) segment else null;
}

fn resolveRelationEndpointIndex(value: ?std.json.Value, out: []ExtractedEntity) ?usize {
    const endpoint = value orelse return null;
    switch (endpoint) {
        .string => |id| return mentionIndexForLocalId(out, id),
        .object => |o| {
            if (o.get("entity_index")) |index_value| {
                if (index_value == .integer and index_value.integer >= 0 and index_value.integer < out.len)
                    return @intCast(index_value.integer);
                return null;
            }
            const id = jsonString(o.get("entity_id") orelse o.get("id") orelse o.get("local_id") orelse return null) orelse return null;
            return mentionIndexForLocalId(out, id);
        },
        else => return null,
    }
}

fn mentionIndexForLocalId(out: []ExtractedEntity, id: []const u8) ?usize {
    for (out, 0..) |entity, i| {
        if (std.mem.eql(u8, entity.local_id, id)) return i;
    }
    return null;
}

fn isDedicatedMentionArtifact(obj: std.json.ObjectMap) bool {
    const schema = jsonString(obj.get("_schema") orelse return false) orelse return false;
    return std.mem.eql(u8, schema, "antfly.entity_mention.v1");
}

fn parseExtractedEntityObject(a: std.mem.Allocator, o: std.json.ObjectMap, array_index: ?usize) !ExtractedEntity {
    // Extractor payloads whose entities carry no local id (GLiNER2.5's
    // boundary responses reference entities positionally, via each relation's
    // `entity_index`) resolve under their decimal array position, the same
    // identity the graph materializer derives for `entity_index` endpoints.
    const local_id = if (o.get("id") orelse o.get("local_id")) |id_value|
        try a.dupe(u8, jsonString(id_value) orelse return error.InvalidExtraction)
    else if (array_index) |index|
        try std.fmt.allocPrint(a, "{d}", .{index})
    else
        return error.InvalidExtraction;
    return .{
        .local_id = local_id,
        .label = try a.dupe(u8, jsonString(o.get("label") orelse return error.InvalidExtraction) orelse return error.InvalidExtraction),
        .text = try a.dupe(u8, jsonString(o.get("text") orelse return error.InvalidExtraction) orelse return error.InvalidExtraction),
        .embedding = try parseEmbedding(a, o.get("embedding")),
        // "score" is the GLiNER extractor's spelling of the same measure.
        .confidence = switch (o.get("confidence") orelse o.get("score") orelse std.json.Value{ .float = 1.0 }) {
            .float => |f| f,
            .integer => |n| @floatFromInt(n),
            else => 1.0,
        },
        .predicate = if (o.get("predicate")) |p| try a.dupe(u8, jsonString(p) orelse "") else "",
    };
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
        /// Optional physical-row decoder supplied by the storage integration.
        /// Keeping this at the seam lets candidate scans pin the row's exact
        /// immutable schema epoch without teaching the resolver about formats.
        materialize_row: ?*const fn (
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            key: []const u8,
            value: []const u8,
        ) anyerror![]u8 = null,
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
    pub fn materializeRow(
        self: ArtifactStore,
        allocator: std.mem.Allocator,
        key: []const u8,
        value: []const u8,
    ) anyerror![]u8 {
        const f = self.vtable.materialize_row orelse return allocator.dupe(u8, value);
        return f(self.ptr, allocator, key, value);
    }
};

/// Blocking seam: fetch the ~k candidate entities to score a mention against.
/// The DB adapter implements this over the entity table's indexes
/// (ann/exact/prefix); a null provider means deterministic minting only.
pub const CandidateProvider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Optional work-unit bulk path. Candidate lists borrow the caller's
        /// scratch allocator and retain one list per input mention.
        candidates_for_batch: ?*const fn (*anyopaque, std.mem.Allocator, []const ExtractedEntity, [][]const Candidate) anyerror!void = null,
        /// Append candidates for `entity` into `out`, allocating any candidate
        /// memory with `allocator` (valid until the stage finishes one mention).
        candidates_for: *const fn (
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            entity: ExtractedEntity,
            out: *std.ArrayListUnmanaged(Candidate),
        ) anyerror!void,
    };

    pub fn candidatesForBatch(self: CandidateProvider, alloc: std.mem.Allocator, entities: []const ExtractedEntity, lists: [][]const Candidate) !void {
        if (self.vtable.candidates_for_batch) |f| return f(self.ptr, alloc, entities, lists);
        for (entities, lists) |entity, *list| {
            var candidates = std.ArrayListUnmanaged(Candidate).empty;
            try self.candidatesFor(alloc, entity, &candidates);
            list.* = candidates.items;
        }
    }

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

/// Dependency records live inside the resolution artifact, so the existing
/// atomic artifact/replay commit, deletion and portable backup own them too.
/// Unknown versions and damaged cache data are discarded, never decisions.
pub const WorkRetention = enum { none, embeddings, full };

pub const WorkCache = struct {
    version: u32 = 1,
    promotion_enabled: ?bool = null,
    mentions: []const MentionWork,

    const Use = struct { candidate: usize, features: ?[]const matcher.Feature = null };
    const Mention = struct {
        local_id: []const u8,
        embedding_input: [32]u8,
        embedding_dependency: ?[32]u8 = null,
        embedding: ?[]const f32 = null,
        embedding_key: ?[32]u8 = null,
        candidate_dependency: ?[32]u8 = null,
        candidates: []const Use,
    };
    const Wire = struct {
        version: u32,
        promotion_enabled: ?bool = null,
        mentions: []const Mention,
        candidates: []const Candidate = &.{},
    };

    fn toJson(self: WorkCache, gpa: std.mem.Allocator) ![]u8 {
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();
        const a = arena.allocator();
        var pool: std.ArrayListUnmanaged(Candidate) = .empty;
        var indices: std.AutoHashMapUnmanaged([32]u8, usize) = .empty;
        const RecordIdentity = struct { fields: usize, len: usize, survivor_fields: usize, survivor_len: usize, endpoint: [32]u8 };
        var digests: std.AutoHashMapUnmanaged(RecordIdentity, [32]u8) = .empty;
        const mentions = try a.alloc(Mention, self.mentions.len);
        for (self.mentions, mentions) |entry, *mention| {
            const uses = try a.alloc(Use, entry.candidates.len);
            for (entry.candidates, uses) |candidate, *use| {
                var row = candidate;
                row.features = null;
                const fields = try a.dupe(matcher.Field, row.record.fields);
                for (fields) |*field| {
                    if (field.value != .vector) continue;
                    var referenced = entry.candidate_dependency == null;
                    for (row.vector_refs) |ref| if (std.mem.eql(u8, ref.field, field.name)) {
                        referenced = true;
                        break;
                    };
                    if (referenced) field.value = .none;
                }
                row.record = .{ .fields = fields };
                if (row.resolved_record) |record| {
                    const survivor_fields = try a.dupe(matcher.Field, record.fields);
                    for (survivor_fields) |*field| if (field.value == .vector) {
                        field.value = .none;
                    };
                    row.resolved_record = .{ .fields = survivor_fields };
                }
                const identity: RecordIdentity = .{
                    .fields = @intFromPtr(candidate.record.fields.ptr),
                    .len = candidate.record.fields.len,
                    .survivor_fields = if (row.resolved_record) |record| @intFromPtr(record.fields.ptr) else 0,
                    .survivor_len = if (row.resolved_record) |record| record.fields.len else 0,
                    .endpoint = try fingerprint(a, .{ row.doc_ref, row.label, row.resolved_doc_ref }),
                };
                const memo = try digests.getOrPut(a, identity);
                if (!memo.found_existing) memo.value_ptr.* = try fingerprint(a, row);
                const slot = try indices.getOrPut(a, memo.value_ptr.*);
                if (!slot.found_existing) {
                    slot.value_ptr.* = pool.items.len;
                    try pool.append(a, row);
                }
                use.* = .{ .candidate = slot.value_ptr.*, .features = candidate.features };
            }
            mention.* = .{ .local_id = entry.local_id, .embedding_input = entry.embedding_input, .embedding_dependency = entry.embedding_dependency, .embedding = if (entry.embedding_key == null) entry.embedding else null, .embedding_key = entry.embedding_key, .candidate_dependency = entry.candidate_dependency, .candidates = uses };
        }
        // Large canonical vectors are stored once per work unit, not once per
        // mention/candidate pair. Each pair retains just its comparison features.
        const raw = try std.json.Stringify.valueAlloc(a, Wire{ .version = self.version, .promotion_enabled = self.promotion_enabled, .mentions = mentions, .candidates = pool.items }, .{});
        var compact = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
        try transformVectors(a, &compact, true);
        return try std.json.Stringify.valueAlloc(gpa, compact, .{});
    }

    fn transformVectors(a: std.mem.Allocator, value: *std.json.Value, pack: bool) !void {
        switch (value.*) {
            .object => |*object| {
                var it = object.iterator();
                while (it.next()) |entry| {
                    const is_vector = std.mem.eql(u8, entry.key_ptr.*, "vector") or std.mem.eql(u8, entry.key_ptr.*, "embedding");
                    const v = entry.value_ptr;
                    if (pack and is_vector and v.* == .array) {
                        const bytes = try a.alloc(u8, try std.math.mul(usize, v.array.items.len, 4));
                        for (v.array.items, 0..) |item, i| {
                            const n: f32 = switch (item) {
                                .float => |f| @floatCast(f),
                                .integer => |n| @floatFromInt(n),
                                else => return error.InvalidResolutionCache,
                            };
                            std.mem.writeInt(u32, bytes[i * 4 ..][0..4], @bitCast(n), .little);
                        }
                        const codec = std.base64.standard.Encoder;
                        const encoded = try a.alloc(u8, 6 + codec.calcSize(bytes.len));
                        @memcpy(encoded[0..6], "f32le:");
                        _ = codec.encode(encoded[6..], bytes);
                        v.* = .{ .string = encoded };
                    } else if (!pack and is_vector and v.* == .string) {
                        if (!std.mem.startsWith(u8, v.string, "f32le:")) return error.InvalidResolutionCache;
                        const codec = std.base64.standard.Decoder;
                        const bytes = try a.alloc(u8, try codec.calcSizeForSlice(v.string[6..]));
                        try codec.decode(bytes, v.string[6..]);
                        if (bytes.len % 4 != 0) return error.InvalidResolutionCache;
                        var items: std.array_list.Managed(std.json.Value) = .init(a);
                        for (0..bytes.len / 4) |i| {
                            const n: f32 = @bitCast(std.mem.readInt(u32, bytes[i * 4 ..][0..4], .little));
                            if (!std.math.isFinite(n)) return error.InvalidResolutionCache;
                            try items.append(.{ .float = n });
                        }
                        v.* = .{ .array = items };
                    } else try transformVectors(a, v, pack);
                }
            },
            .array => |*items| for (items.items) |*item| {
                try transformVectors(a, item, pack);
            },
            else => {},
        }
    }

    fn parseValue(a: std.mem.Allocator, value: std.json.Value) !?WorkCache {
        var expanded = value;
        try transformVectors(a, &expanded, false);
        const wire = try std.json.parseFromValueLeaky(Wire, a, expanded, .{ .ignore_unknown_fields = true });
        if (wire.version != 1) return null;
        const mentions = try a.alloc(MentionWork, wire.mentions.len);
        for (wire.mentions, mentions) |entry, *mention| {
            const candidates = try a.alloc(Candidate, entry.candidates.len);
            for (entry.candidates, candidates) |use, *candidate| {
                if (use.candidate >= wire.candidates.len) return error.InvalidResolutionCache;
                candidate.* = wire.candidates[use.candidate];
                candidate.features = use.features;
            }
            mention.* = .{ .local_id = entry.local_id, .embedding_input = entry.embedding_input, .embedding_dependency = entry.embedding_dependency, .embedding = entry.embedding, .embedding_key = entry.embedding_key, .candidate_dependency = entry.candidate_dependency, .candidates = candidates };
        }
        return .{ .version = wire.version, .promotion_enabled = wire.promotion_enabled, .mentions = mentions };
    }
};

pub const MentionWork = struct {
    local_id: []const u8,
    embedding_input: [32]u8,
    embedding_dependency: ?[32]u8 = null,
    embedding: ?[]const f32 = null,
    embedding_key: ?[32]u8 = null,
    candidate_dependency: ?[32]u8 = null,
    candidates: []const Candidate = &.{},
};

pub const WorkStats = struct {
    embeddings: usize = 0,
    reused_embeddings: usize = 0,
    candidate_mentions: usize = 0,
    reused_candidate_mentions: usize = 0,
    features: matcher.FeatureStats = .{},
};

pub fn fingerprint(alloc: std.mem.Allocator, value: anytype) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}

fn sameDependency(left: ?[32]u8, right: ?[32]u8) bool {
    return left != null and right != null and std.mem.eql(u8, &left.?, &right.?);
}

fn mentionRecord(entity: ExtractedEntity, fields: *[4]matcher.Field) matcher.Record {
    fields.* = .{
        .{ .name = "label", .value = .{ .text = entity.label } },
        .{ .name = "text", .value = .{ .text = entity.text } },
        .{ .name = "canonical_text", .value = .{ .text = entity.text } },
        .{ .name = "name_embedding", .value = if (entity.embedding) |v| .{ .vector = v } else .none },
    };
    return .{ .fields = fields[0..if (entity.embedding != null) @as(usize, 4) else 3] };
}

/// One resolution replay stage: read a changed extraction artifact, resolve its
/// mentions, and persist the resolution artifact idempotently. This is the body
/// the managed worker runs per changed extraction artifact key.
///
/// In the DB, the adapter wraps `run` in a `background_runtime.Job`
/// (class `.maintenance`, keyed by the shard's owner id) and submits it on the
/// shard's `BackendRuntime.durable_jobs` lane; crash recovery is the normal
/// replay path (the change journal re-emits the extraction artifact key).
pub const ResolutionStage = struct {
    doc_ref_binding: ?struct { ptr: *anyopaque, bind: *const fn (*anyopaque, []const u8) anyerror!?[]const u8 } = null,
    resolver: *const Resolver,
    config_generation: u64,
    /// A destination/model revision and blocking/index revision supplied by the
    /// runtime. Null forbids reuse for that stage (remote sources may lack a
    /// snapshot-wide revision). Neither is the resolver generation.
    embedding_dependency: ?[32]u8 = null,
    candidate_dependency: ?[32]u8 = null,
    stats: ?*WorkStats = null,
    promotion_enabled: bool = true,
    retain_work: bool = true,
    work_retention: WorkRetention = .full,
    max_work_bytes: usize = 1024 * 1024,
    /// Optional name-embedding backfill: when set, each mention lacking an
    /// `embedding` gets one from its text before blocking/scoring, so `ann`
    /// candidate search and cosine comparisons have a query vector.
    embedder: ?MentionEmbedder = null,
    embedding_cache: ?EmbeddingCache = null,
    vector_loader: ?VectorLoader = null,
    /// Optional human-curation overrides: when set, a mention with a recorded
    /// review decision takes that decision/endpoint instead of the scorer's, so
    /// a curated link survives re-resolution (replay-stable curation).
    overrides: ?OverrideProvider = null,
    /// Optional sibling resolutions (local id -> {"key", "table"}, the
    /// `_entities` shape) from OTHER resolvers over the same extraction
    /// artifact. Compositional event identity then composes participants'
    /// canonical keys, so entity merges re-key the events they touch. The
    /// runtime re-drives this stage when a sibling resolution artifact
    /// changes; recomputing with an unchanged map is byte-stable.
    sibling_resolutions_json: ?[]const u8 = null,

    /// Allocate the canonical resolution bytes for `extraction_key` without
    /// mutating the store. The caller owns a non-null result and must free it
    /// with `gpa`; null means the source extraction no longer exists. Keeping
    /// computation separate lets the DB commit the artifact mutation and its
    /// downstream replay record in one atomic storage transaction.
    pub fn computeAlloc(
        self: ResolutionStage,
        gpa: std.mem.Allocator,
        store: ArtifactStore,
        provider: ?CandidateProvider,
        extraction_key: []const u8,
    ) !?[]u8 {
        return self.computeCachedAlloc(gpa, store, provider, extraction_key, null);
    }

    pub fn computeCachedAlloc(
        self: ResolutionStage,
        gpa: std.mem.Allocator,
        store: ArtifactStore,
        provider: ?CandidateProvider,
        extraction_key: []const u8,
        existing: ?[]const u8,
    ) !?[]u8 {
        const extraction = try store.get(gpa, extraction_key);
        defer if (extraction) |e| gpa.free(e);
        if (extraction == null) return null;

        var parsed = try parseExtractionEntitiesWithResolutions(gpa, extraction.?, self.sibling_resolutions_json);
        defer parsed.deinit();

        // Label routing: drop mentions this resolver does not consume before
        // any per-mention work (embedding backfill, candidate blocking). The
        // sibling resolver that owns those labels resolves them from the same
        // extraction artifact into its own resolution artifact.
        parsed.entities = self.resolver.filterEntitiesByLabel(parsed.entities);

        // Reject only the unkeyable mention before blocking; keep its siblings.
        // Invalid templates and resource failures still fail the operation.
        const admitted_count = parsed.entities.len;
        const alias_mask = try parsed.arena.allocator().alloc(bool, admitted_count);
        var kept: usize = 0;
        for (parsed.entities) |entity| {
            const key = self.resolver.renderKeyAlloc(parsed.arena.allocator(), entity) catch |err| switch (err) {
                error.EmptyEntityKey => {
                    std.log.warn("resolver skipped mention {s}: key template produced an empty slug", .{entity.local_id});
                    continue;
                },
                else => return err,
            };
            const canonical = try canonicalAliasKey(self.resolver.aliases, key);
            alias_mask[kept] = !std.mem.eql(u8, key, canonical);
            parsed.entities[kept] = entity;
            kept += 1;
        }
        parsed.entities = parsed.entities[0..kept];

        const arena = parsed.arena.allocator();
        const previous: ?WorkCache = cache: {
            const raw = existing orelse break :cache null;
            const root = std.json.parseFromSliceLeaky(std.json.Value, arena, raw, .{}) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => break :cache null,
            };
            if (root != .object) break :cache null;
            const work = root.object.get("_resolution_work") orelse break :cache null;
            const decoded = WorkCache.parseValue(arena, work) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => {
                    std.log.warn("resolver discarded malformed incremental work: {s}", .{@errorName(err)});
                    break :cache null;
                },
            };
            break :cache decoded;
        };
        var previous_mentions: std.StringHashMapUnmanaged(?MentionWork) = .empty;
        if (previous) |cache| for (cache.mentions) |entry| {
            const slot = try previous_mentions.getOrPut(arena, entry.local_id);
            // Duplicate ids cannot establish an unambiguous dependency.
            slot.value_ptr.* = if (slot.found_existing) null else entry;
        };
        const work = try arena.alloc(MentionWork, kept);
        const prior = try arena.alloc(?MentionWork, kept);
        for (parsed.entities, 0..) |entity, i| {
            prior[i] = previous_mentions.get(entity.local_id) orelse null;
            work[i] = .{ .local_id = entity.local_id, .embedding_input = try fingerprint(arena, entity.text), .embedding_dependency = self.embedding_dependency };
        }

        // Backfill name embeddings for mentions that arrived without one, using
        // the parse arena so the vectors outlive scoring. Embedding failures are
        // non-fatal: the mention simply scores without a vector.
        if (if (self.resolver.scorer != null) self.embedder else null) |embedder| {
            for (parsed.entities, 0..) |*entity, i| {
                if (alias_mask[i] or entity.embedding != null or entity.text.len == 0) continue;
                const cache_key: ?[32]u8 = if (self.embedding_dependency) |dependency| try fingerprint(arena, .{ dependency, work[i].embedding_input }) else null;
                if (self.embedding_cache) |cache| if (cache_key) |key| {
                    if (cache.get(cache.ptr, arena, key) catch null) |vector| {
                        entity.embedding = vector;
                        work[i].embedding_key = key;
                        if (self.stats) |stats| stats.reused_embeddings += 1;
                        continue;
                    }
                };
                if (prior[i]) |entry| {
                    if (sameDependency(entry.embedding_dependency, self.embedding_dependency) and std.mem.eql(u8, &entry.embedding_input, &work[i].embedding_input) and entry.embedding != null) {
                        entity.embedding = entry.embedding;
                        if (self.stats) |stats| stats.reused_embeddings += 1;
                        continue;
                    }
                }
                if (self.stats) |stats| stats.embeddings += 1;
                entity.embedding = embedder.embed(arena, entity.text) catch null;
                if (self.retain_work and self.work_retention != .none) if (self.embedding_cache) |cache| if (cache_key) |key| if (entity.embedding) |vector| {
                    cache.put(cache.ptr, key, vector) catch {};
                    work[i].embedding_key = key;
                };
            }
        }

        var scratch = std.heap.ArenaAllocator.init(gpa);
        defer scratch.deinit();
        const a = scratch.allocator();

        const lists = try a.alloc([]const Candidate, parsed.entities.len);
        for (lists) |*list| list.* = &.{};
        if (provider) |p| {
            // Explicit aliases require neither entity records nor ANN/embedding IO.
            var pending: std.ArrayListUnmanaged(ExtractedEntity) = .empty;
            var indices: std.ArrayListUnmanaged(usize) = .empty;
            for (parsed.entities, 0..) |entity, i| {
                work[i].embedding = entity.embedding;
                if (alias_mask[i]) continue;
                if (self.candidate_dependency) |dep| work[i].candidate_dependency = try fingerprint(a, .{ dep, entity });
                if (prior[i]) |entry| {
                    if (sameDependency(entry.candidate_dependency, work[i].candidate_dependency) and !hasCrossTableRedirect(entry.candidates)) {
                        if (try self.hydrateCachedCandidates(a, entry.candidates)) |hydrated| {
                            lists[i] = hydrated;
                            if (self.stats) |stats| stats.reused_candidate_mentions += 1;
                            continue;
                        }
                    }
                }
                if (self.stats) |stats| stats.candidate_mentions += 1;
                try pending.append(a, entity);
                try indices.append(a, i);
            }
            if (pending.items.len > 0) {
                const blocked = try a.alloc([]const Candidate, pending.items.len);
                try p.candidatesForBatch(a, pending.items, blocked);
                for (indices.items, blocked) |i, candidates| lists[i] = candidates;
            }
        }

        // Refetched candidates can still reuse features: only their consumed
        // input fields participate in a feature digest. New/deleted candidates
        // and changed vectors cannot borrow another candidate's work.
        for (parsed.entities, 0..) |entity, i| {
            const candidates = try a.dupe(Candidate, lists[i]);
            if (self.resolver.scorer) |*scorer| {
                var fields: [4]matcher.Field = undefined;
                const mention = mentionRecord(entity, &fields);
                for (candidates) |*candidate| {
                    var old_features: []const matcher.Feature = &.{};
                    if (prior[i]) |entry| for (entry.candidates) |old| {
                        if (std.mem.eql(u8, old.doc_ref.table, candidate.doc_ref.table) and std.mem.eql(u8, old.doc_ref.key, candidate.doc_ref.key)) {
                            old_features = old.features orelse &.{};
                            break;
                        }
                    };
                    const inputs_unchanged = if (prior[i]) |entry| sameDependency(entry.candidate_dependency, work[i].candidate_dependency) and !hasCrossTableRedirect(entry.candidates) else false;
                    candidate.features = try scorer.featuresAllocWithInputReuse(a, mention, candidate.record, old_features, inputs_unchanged, if (self.stats) |stats| &stats.features else null);
                }
            }
            lists[i] = candidates;
            work[i].embedding = entity.embedding;
            work[i].candidates = candidates;
            // The source-table revision cannot validate a survivor in another
            // table. Refetch until the provider supplies per-survivor revisions.
            if (hasCrossTableRedirect(candidates)) work[i].candidate_dependency = null;
        }

        var resolution = try self.resolver.resolve(gpa, self.config_generation, parsed.entities, lists);
        defer resolution.deinit();
        resolution.skipped_mentions += admitted_count - kept;

        // Apply human-curation overrides: a reviewed mention takes the curator's
        // decision and endpoint (strings copied into the resolution arena so they
        // outlive the override provider).
        if (self.overrides) |ov| {
            const result_arena = resolution.arena.allocator();
            const ents = @constCast(resolution.entities);
            for (ents) |*e| {
                const o = ov.overrideFor(e.local_id) orelse continue;
                e.decision = o.decision;
                e.doc_ref = .{
                    .table = try result_arena.dupe(u8, o.table),
                    .key = try result_arena.dupe(u8, o.key),
                };
            }
        }

        if (self.doc_ref_binding) |binding| {
            for (@constCast(resolution.entities)) |*entity| {
                if (try binding.bind(binding.ptr, entity.doc_ref.table)) |physical| {
                    entity.doc_ref.storage_table = try resolution.arena.allocator().dupe(u8, physical);
                }
            }
        }
        resolution.max_work_bytes = self.max_work_bytes;
        if (self.retain_work and self.work_retention != .none) {
            if (self.work_retention == .embeddings) for (work) |*mention| {
                mention.candidates = &.{};
                mention.candidate_dependency = null;
            };
            resolution.work = .{ .mentions = work, .promotion_enabled = self.promotion_enabled };
        }
        return try resolution.toJson(gpa);
    }

    fn hydrateCachedCandidates(self: ResolutionStage, a: std.mem.Allocator, cached: []const Candidate) !?[]const Candidate {
        const candidates = try a.dupe(Candidate, cached);
        for (candidates) |*candidate| {
            if (candidate.vector_refs.len == 0) continue;
            const loader = self.vector_loader orelse return null;
            const fields = try a.dupe(matcher.Field, candidate.record.fields);
            for (candidate.vector_refs) |ref| {
                const vector = loader.load(loader.ptr, a, ref) catch return null;
                const values = vector orelse return null;
                if (values.len != ref.dimensions) return null;
                var found = false;
                for (fields) |*field| if (std.mem.eql(u8, field.name, ref.field)) {
                    field.value = .{ .vector = values };
                    found = true;
                    break;
                };
                if (!found) return null;
            }
            candidate.record = .{ .fields = fields };
        }
        return candidates;
    }

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
        const existing = try store.get(gpa, resolution_key);
        defer if (existing) |bytes| gpa.free(bytes);
        const computed = try self.computeCachedAlloc(gpa, store, provider, extraction_key, existing);
        defer if (computed) |bytes| gpa.free(bytes);

        if (computed == null) {
            if (existing != null) {
                try store.delete(resolution_key);
                return .cleared;
            }
            return .source_missing;
        }

        if (existing) |e| {
            if (std.mem.eql(u8, e, computed.?)) return .unchanged;
        }
        try store.put(resolution_key, computed.?);
        return .written;
    }
};

// --- Tests ------------------------------------------------------------------

const testing = std.testing;

test "initFromParts builds a deterministic resolver and one with a scorer" {
    var deterministic = try Resolver.initFromParts(testing.allocator, "entities", "{{ slug _entity.text }}", .{}, true, "");
    defer deterministic.deinit();
    try testing.expectEqualStrings("entities", deterministic.table);
    try testing.expect(deterministic.scorer == null);

    var scored = try Resolver.initFromParts(testing.allocator, "entities", "{{ slug _entity.text }}", .{}, false,
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

test "min_confidence floors mention admission before resolution" {
    var resolver = try Resolver.initFromParts(
        testing.allocator,
        "entities",
        "entity/{{ slug _entity.text }}",
        .{ .min_confidence = 0.5 },
        true,
        "",
    );
    defer resolver.deinit();

    var entities = [_]ExtractedEntity{
        .{ .local_id = "0", .label = "person", .text = "Ada Lovelace", .confidence = 0.9 },
        .{ .local_id = "1", .label = "date", .text = "b2a6-4f", .confidence = 0.2 },
        .{ .local_id = "2", .label = "org", .text = "Antfly", .confidence = 0.5 },
    };
    const kept = resolver.filterEntitiesByLabel(&entities);
    try testing.expectEqual(@as(usize, 2), kept.len);
    try testing.expectEqualStrings("Ada Lovelace", kept[0].text);
    try testing.expectEqualStrings("Antfly", kept[1].text);
}

test "slug strips English possessives so mention variants converge" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "entity/{{ slug _entity.text }}" }
    );
    defer resolver.deinit();

    const entities = [_]ExtractedEntity{
        .{ .local_id = "e0", .label = "person", .text = "Epstein's island" },
        .{ .local_id = "e1", .label = "person", .text = "Epstein\u{2019}s Island" },
        .{ .local_id = "e2", .label = "org", .text = "O'Brien & Sons" },
    };
    var res = try resolver.resolve(testing.allocator, 1, &entities, &[_][]const Candidate{});
    defer res.deinit();
    try testing.expectEqualStrings("entity/epstein_island", res.entities[0].doc_ref.key);
    try testing.expectEqualStrings("entity/epstein_island", res.entities[1].doc_ref.key);
    // A non-possessive apostrophe (contraction/name) keeps its letters.
    try testing.expectEqualStrings("entity/o_brien_sons", res.entities[2].doc_ref.key);
}

test "hash helper mints a stable event key from normalized text" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "events", "key_template": "event/{{ hash _entity.text }}" }
    );
    defer resolver.deinit();

    const entities = [_]ExtractedEntity{
        .{ .local_id = "v0", .label = "event", .text = "Ada Lovelace writes the first program" },
        .{ .local_id = "v1", .label = "event", .text = "Ada Lovelace writes the first program" },
        .{ .local_id = "v2", .label = "event", .text = "Babbage designs the Analytical Engine" },
    };
    var res = try resolver.resolve(testing.allocator, 1, &entities, &[_][]const Candidate{});
    defer res.deinit();

    try testing.expectEqual(@as(usize, 3), res.entities.len);
    // "event/" + 16 hex chars, identical text -> identical key, replay-stable.
    try testing.expectEqual(@as(usize, "event/".len + 16), res.entities[0].doc_ref.key.len);
    try testing.expect(std.mem.startsWith(u8, res.entities[0].doc_ref.key, "event/"));
    try testing.expectEqualStrings(res.entities[0].doc_ref.key, res.entities[1].doc_ref.key);
    try testing.expect(!std.mem.eql(u8, res.entities[0].doc_ref.key, res.entities[2].doc_ref.key));
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
    try testing.expectEqualStrings("Ada Lovelace", ents[0].object.get("canonical_name").?.string);
    try testing.expectEqualStrings("Ada Lovelace", ents[0].object.get("surface_form").?.string);
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

test "parseExtractionEntities assigns positional local ids to id-less extractor entities" {
    // GLiNER2.5 boundary responses carry no per-entity ids; relations
    // reference entities positionally via `entity_index`, and "score" is the
    // extractor's confidence spelling.
    var parsed = try parseExtractionEntities(testing.allocator,
        \\{
        \\  "entities": [
        \\    {"label": "component", "text": "metadata server", "score": 0.83, "start": 4, "end": 19},
        \\    {"label": "test", "text": "VOPR", "score": 0.98, "start": 45, "end": 49}
        \\  ],
        \\  "relations": [
        \\    {"type": "tested_by", "source": {"entity_index": 0}, "target": {"entity_index": 1}, "score": 0.9}
        \\  ]
        \\}
    );
    defer parsed.deinit();
    try testing.expectEqual(@as(usize, 2), parsed.entities.len);
    try testing.expectEqualStrings("0", parsed.entities[0].local_id);
    try testing.expectEqualStrings("metadata server", parsed.entities[0].text);
    try testing.expectEqual(@as(f64, 0.83), parsed.entities[0].confidence);
    try testing.expectEqualStrings("1", parsed.entities[1].local_id);
    try testing.expectEqualStrings("VOPR", parsed.entities[1].text);
}

test "event identity composes participants and predicate across wordings" {
    // Two artifacts describe the same event with different sentence wording.
    // The per-sentence hash diverges; the compositional identity converges.
    const artifact_a =
        \\{
        \\  "entities": [
        \\    {"id": "e0", "label": "person", "text": "Ada Lovelace"},
        \\    {"id": "e1", "label": "person", "text": "Charles Babbage"},
        \\    {"id": "v0", "label": "event", "text": "Ada Lovelace met Charles Babbage at a salon.", "predicate": "meet"}
        \\  ],
        \\  "relations": [
        \\    {"type": "participates_in", "source": "e0", "target": "v0"},
        \\    {"type": "participates_in", "source": "e1", "target": "v0"}
        \\  ]
        \\}
    ;
    const artifact_b =
        \\{
        \\  "entities": [
        \\    {"id": "e0", "label": "person", "text": "Charles Babbage"},
        \\    {"id": "e1", "label": "person", "text": "Ada Lovelace"},
        \\    {"id": "v9", "label": "event", "text": "Babbage and Lovelace were introduced.", "predicate": "meet"}
        \\  ],
        \\  "relations": [
        \\    {"type": "participates_in", "source": "e0", "target": "v9"},
        \\    {"type": "participates_in", "source": "e1", "target": "v9"}
        \\  ]
        \\}
    ;
    var parsed_a = try parseExtractionEntities(testing.allocator, artifact_a);
    defer parsed_a.deinit();
    var parsed_b = try parseExtractionEntities(testing.allocator, artifact_b);
    defer parsed_b.deinit();

    try testing.expectEqualStrings("ada_lovelace,charles_babbage|meet", parsed_a.entities[2].event_identity);
    try testing.expectEqualStrings(parsed_a.entities[2].event_identity, parsed_b.entities[2].event_identity);

    // Same participants, different action: distinct identities.
    const artifact_c =
        \\{
        \\  "entities": [
        \\    {"id": "e0", "label": "person", "text": "Ada Lovelace"},
        \\    {"id": "e1", "label": "person", "text": "Charles Babbage"},
        \\    {"id": "v0", "label": "event", "text": "Ada Lovelace hired Charles Babbage.", "predicate": "hire"}
        \\  ],
        \\  "relations": [
        \\    {"type": "participates_in", "source": "e0", "target": "v0"},
        \\    {"type": "participates_in", "source": "e1", "target": "v0"}
        \\  ]
        \\}
    ;
    var parsed_c = try parseExtractionEntities(testing.allocator, artifact_c);
    defer parsed_c.deinit();
    try testing.expect(!std.mem.eql(u8, parsed_a.entities[2].event_identity, parsed_c.entities[2].event_identity));

    // No participants keeps the identity empty — the template variable then
    // degrades to the mention text (the legacy per-sentence identity),
    // identically with or without a relations array, and EVEN WITH a
    // predicate: "|meet" alone would merge every participant-less meeting
    // in the corpus into one node.
    const artifact_d =
        \\{"entities": [{"id": "v0", "label": "event", "text": "Something happened.", "predicate": "happen"}]}
    ;
    var parsed_d = try parseExtractionEntities(testing.allocator, artifact_d);
    defer parsed_d.deinit();
    try testing.expectEqualStrings("", parsed_d.entities[0].event_identity);
    const artifact_e =
        \\{"entities": [{"id": "v0", "label": "event", "text": "Something happened.", "predicate": "happen"}], "relations": []}
    ;
    var parsed_e = try parseExtractionEntities(testing.allocator, artifact_e);
    defer parsed_e.deinit();
    try testing.expectEqualStrings("", parsed_e.entities[0].event_identity);
    // Event-event relations (all mentions labeled event) contribute no
    // participants, so verb-sharing events keep distinct sentence identities.
    const artifact_f =
        \\{
        \\  "entities": [
        \\    {"id": "v0", "label": "event", "text": "Ada met Babbage.", "predicate": "meet"},
        \\    {"id": "v1", "label": "event", "text": "Curie met Langevin.", "predicate": "meet"}
        \\  ],
        \\  "relations": [{"type": "before", "source": "v0", "target": "v1"}]
        \\}
    ;
    var parsed_f = try parseExtractionEntities(testing.allocator, artifact_f);
    defer parsed_f.deinit();
    try testing.expectEqualStrings("", parsed_f.entities[0].event_identity);
    try testing.expectEqualStrings("", parsed_f.entities[1].event_identity);
}

test "event identity composes canonical participant keys from a resolution map" {
    // "A. Lovelace" resolved (merged) into entity/ada_lovelace: identity must
    // follow the survivor key, not the stale surface-form slug.
    const artifact =
        \\{
        \\  "entities": [
        \\    {"id": "e0", "label": "person", "text": "A. Lovelace"},
        \\    {"id": "e1", "label": "person", "text": "Charles Babbage"},
        \\    {"id": "v0", "label": "event", "text": "A. Lovelace met Charles Babbage.", "predicate": "meet"}
        \\  ],
        \\  "relations": [
        \\    {"type": "participates_in", "source": "e0", "target": "v0"},
        \\    {"type": "participates_in", "source": "e1", "target": "v0"}
        \\  ]
        \\}
    ;
    const resolutions =
        \\{
        \\  "e0": {"key": "entity/ada_lovelace", "table": "entities"},
        \\  "e1": {"key": "entity/charles_babbage", "table": "entities"}
        \\}
    ;
    var resolved = try parseExtractionEntitiesWithResolutions(testing.allocator, artifact, resolutions);
    defer resolved.deinit();
    try testing.expectEqualStrings("ada_lovelace,charles_babbage|meet", resolved.entities[2].event_identity);

    // Without the map the identity uses the raw mention slug: the two agree
    // exactly when the entity key template minted the same slug (the
    // no-merge fast path re-resolves to identical bytes), and diverge to the
    // survivor only when a merge actually moved the mention.
    var unresolved = try parseExtractionEntities(testing.allocator, artifact);
    defer unresolved.deinit();
    try testing.expectEqualStrings("a_lovelace,charles_babbage|meet", unresolved.entities[2].event_identity);

    // A partial map falls back per participant, and an artifact carrying the
    // materializer-injected `_entities` map composes the same way without an
    // explicit sibling map.
    const partial =
        \\{"e1": {"key": "entity/charles_babbage", "table": "entities"}}
    ;
    var partially = try parseExtractionEntitiesWithResolutions(testing.allocator, artifact, partial);
    defer partially.deinit();
    try testing.expectEqualStrings("a_lovelace,charles_babbage|meet", partially.entities[2].event_identity);

    const injected =
        \\{
        \\  "entities": [
        \\    {"id": "e0", "label": "person", "text": "A. Lovelace"},
        \\    {"id": "v0", "label": "event", "text": "A. Lovelace spoke.", "predicate": "speak"}
        \\  ],
        \\  "relations": [{"type": "participates_in", "source": "e0", "target": "v0"}],
        \\  "_entities": {"e0": {"key": "entity/ada_lovelace", "table": "entities"}}
        \\}
    ;
    var embedded = try parseExtractionEntities(testing.allocator, injected);
    defer embedded.deinit();
    try testing.expectEqualStrings("ada_lovelace|speak", embedded.entities[1].event_identity);
}

test "event identity template variable renders through the hash helper" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "events", "key_template": "event/{{ hash _entity.event_identity }}", "labels": ["event"] }
    );
    defer resolver.deinit();

    const with_identity = [_]ExtractedEntity{.{
        .local_id = "v0",
        .label = "event",
        .text = "Ada met Babbage.",
        .event_identity = "ada_lovelace,charles_babbage|meet",
    }};
    var res_a = try resolver.resolve(testing.allocator, 1, &with_identity, &[_][]const Candidate{});
    defer res_a.deinit();

    const reworded = [_]ExtractedEntity{.{
        .local_id = "v3",
        .label = "event",
        .text = "Babbage and Lovelace were introduced.",
        .event_identity = "ada_lovelace,charles_babbage|meet",
    }};
    var res_b = try resolver.resolve(testing.allocator, 1, &reworded, &[_][]const Candidate{});
    defer res_b.deinit();
    try testing.expectEqualStrings(res_a.entities[0].doc_ref.key, res_b.entities[0].doc_ref.key);

    // Without relation context the variable degrades to the mention text.
    const bare = [_]ExtractedEntity{.{ .local_id = "v0", .label = "event", .text = "Something happened." }};
    var res_c = try resolver.resolve(testing.allocator, 1, &bare, &[_][]const Candidate{});
    defer res_c.deinit();
    try testing.expect(std.mem.startsWith(u8, res_c.entities[0].doc_ref.key, "event/"));
}

test "parseExtractionEntities reads a dedicated mention artifact" {
    var parsed = try parseExtractionEntities(testing.allocator,
        \\{
        \\  "_schema": "antfly.entity_mention.v1",
        \\  "local_id": "m0",
        \\  "label": "person",
        \\  "text": "Ada Lovelace",
        \\  "confidence": 0.82
        \\}
    );
    defer parsed.deinit();
    try testing.expectEqual(@as(usize, 1), parsed.entities.len);
    try testing.expectEqualStrings("m0", parsed.entities[0].local_id);
    try testing.expectEqualStrings("person", parsed.entities[0].label);
    try testing.expectEqualStrings("Ada Lovelace", parsed.entities[0].text);
    try testing.expectEqual(@as(f64, 0.82), parsed.entities[0].confidence);
}

/// Minimal in-memory ArtifactStore for tests.
const MapStore = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMapUnmanaged([]u8) = .empty,

    pub fn deinit(self: *MapStore) void {
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

test "label-routed resolvers partition one extraction artifact" {
    // The AutoSchemaKG event/entity split: one artifact carries both event
    // mentions and entity mentions; an `event`-labeled resolver and a
    // catch-all... the catch-all consumes everything, so the entity resolver
    // must not double-resolve events. Partition = event resolver takes
    // label "event"; the entity resolver lists the entity labels explicitly.
    const kg_extraction =
        \\{ "entities": [
        \\  { "id": "e0", "label": "person", "text": "Ada Lovelace" },
        \\  { "id": "v0", "label": "event", "text": "Ada Lovelace writes the first program" }
        \\] }
    ;

    var event_resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "events", "key_template": "event/{{ hash _entity.text }}", "labels": ["event"] }
    );
    defer event_resolver.deinit();
    var entity_resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ lower _entity.label }}/{{ slug _entity.text }}", "labels": ["person", "org"] }
    );
    defer entity_resolver.deinit();

    var map = MapStore{ .alloc = testing.allocator };
    defer map.deinit();
    try map.store().put("ext:doc1", kg_extraction);

    const event_stage = ResolutionStage{ .resolver = &event_resolver, .config_generation = 1 };
    const entity_stage = ResolutionStage{ .resolver = &entity_resolver, .config_generation = 1 };
    try testing.expectEqual(RunResult.written, try event_stage.run(testing.allocator, map.store(), null, "ext:doc1", "res:events"));
    try testing.expectEqual(RunResult.written, try entity_stage.run(testing.allocator, map.store(), null, "ext:doc1", "res:entities"));

    const events_res = (try map.store().get(testing.allocator, "res:events")).?;
    defer testing.allocator.free(events_res);
    var events_parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, events_res, .{});
    defer events_parsed.deinit();
    const event_ents = events_parsed.value.object.get("entities").?.array.items;
    try testing.expectEqual(@as(usize, 1), event_ents.len);
    try testing.expectEqualStrings("v0", event_ents[0].object.get("local_id").?.string);
    try testing.expectEqualStrings("events", event_ents[0].object.get("doc_ref").?.object.get("table").?.string);

    const entities_res = (try map.store().get(testing.allocator, "res:entities")).?;
    defer testing.allocator.free(entities_res);
    var entities_parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, entities_res, .{});
    defer entities_parsed.deinit();
    const entity_ents = entities_parsed.value.object.get("entities").?.array.items;
    try testing.expectEqual(@as(usize, 1), entity_ents.len);
    try testing.expectEqualStrings("e0", entity_ents[0].object.get("local_id").?.string);
    try testing.expectEqualStrings("person/ada_lovelace", entity_ents[0].object.get("doc_ref").?.object.get("key").?.string);
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
    try testing.expectEqualStrings("Ada Lovelace", ent.get("canonical_name").?.string);
    try testing.expectEqualStrings("Ada Lovelace", ent.get("surface_form").?.string);
}

const FixedOverride = struct {
    local_id: []const u8,
    override: Override,

    fn provider(self: *FixedOverride) OverrideProvider {
        return .{ .ptr = self, .override_for = overrideFor };
    }
    fn overrideFor(ptr: *anyopaque, local_id: []const u8) ?Override {
        const self: *FixedOverride = @ptrCast(@alignCast(ptr));
        return if (std.mem.eql(u8, local_id, self.local_id)) self.override else null;
    }
};

test "a human-curation override replaces the resolver's decision for a mention" {
    var resolver = try Resolver.initFromParts(testing.allocator, "entities", "{{ slug _entity.text }}", .{}, true, "");
    defer resolver.deinit();

    var map = MapStore{ .alloc = testing.allocator };
    defer map.deinit();
    try map.store().put("ext:doc1",
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovelace" } ] }
    );

    // Deterministic resolver would mint "ada_lovelace" (decision new); the
    // curator links it to an existing canonical entity instead.
    var override = FixedOverride{
        .local_id = "e0",
        .override = .{ .decision = .match, .table = "entities", .key = "person/ada_lovelace" },
    };
    const stage = ResolutionStage{ .resolver = &resolver, .config_generation = 1, .overrides = override.provider() };
    try testing.expectEqual(RunResult.written, try stage.run(testing.allocator, map.store(), null, "ext:doc1", "res:doc1"));

    const stored = (try map.store().get(testing.allocator, "res:doc1")).?;
    defer testing.allocator.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
}

const MergedCandidate = struct {
    doc_ref: DocRef,
    label: []const u8,
    name: []const u8,
    merged_into: []const u8,
    survivor_name: []const u8,

    fn provider(self: *MergedCandidate) CandidateProvider {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = CandidateProvider.VTable{ .candidates_for = candidatesFor };
    fn candidatesFor(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        entity: ExtractedEntity,
        out: *std.ArrayListUnmanaged(Candidate),
    ) anyerror!void {
        const self: *MergedCandidate = @ptrCast(@alignCast(ptr));
        _ = entity;
        const fields = try allocator.alloc(matcher.Field, 2);
        fields[0] = .{ .name = "canonical_name", .value = .{ .text = try allocator.dupe(u8, self.name) } };
        fields[1] = .{ .name = "merged_into", .value = .{ .text = try allocator.dupe(u8, self.merged_into) } };
        const survivor_fields = try allocator.alloc(matcher.Field, 1);
        survivor_fields[0] = .{ .name = "canonical_name", .value = .{ .text = try allocator.dupe(u8, self.survivor_name) } };
        try out.append(allocator, .{
            .doc_ref = self.doc_ref,
            .label = self.label,
            .record = .{ .fields = fields },
            .resolved_doc_ref = .{ .table = self.doc_ref.table, .key = self.merged_into },
            .resolved_record = .{ .fields = survivor_fields },
        });
    }
};

test "resolution follows a candidate's merged_into redirect to the survivor" {
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

    // The blocked candidate is a merged-away duplicate pointing at the survivor.
    var candidate = MergedCandidate{
        .doc_ref = .{ .table = "entities", .key = "person/ada_dup" },
        .label = "person",
        .name = "Ada Lovelace",
        .merged_into = "person/ada_lovelace",
        .survivor_name = "Augusta Ada King",
    };
    const stage = ResolutionStage{ .resolver = &resolver, .config_generation = 1 };
    try testing.expectEqual(RunResult.written, try stage.run(testing.allocator, map.store(), candidate.provider(), "ext:doc1", "res:doc1"));

    const stored = (try map.store().get(testing.allocator, "res:doc1")).?;
    defer testing.allocator.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    // Resolved to the survivor, not the merged-away duplicate.
    try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
    try testing.expectEqualStrings("Augusta Ada King", ent.get("canonical_name").?.string);
    try testing.expectEqualStrings("Ada Lovelace", ent.get("surface_form").?.string);
}

test "resolution reviews a merged_into redirect when the survivor record is unavailable" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ lower _entity.label }}/{{ slug _entity.text }}",
        \\  "scorer": {
        \\    "comparisons": [
        \\      { "name": "name", "left": "canonical_text", "right": "canonical_name",
        \\        "levels": [ { "when": "exact", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] }
        \\    ],
        \\    "combine": { "bias": -3.0 }, "decision": { "match": 0.9 }
        \\  } }
    );
    defer resolver.deinit();

    const entities = [_]ExtractedEntity{.{ .local_id = "e0", .label = "person", .text = "Ada Lovelace" }};
    var fields = [_]matcher.Field{
        .{ .name = "canonical_name", .value = .{ .text = "Ada Lovelace" } },
        .{ .name = "merged_into", .value = .{ .text = "person/ada_canonical" } },
    };
    const cands = [_]Candidate{.{
        .doc_ref = .{ .table = "entities", .key = "person/ada_dup" },
        .label = "person",
        .record = .{ .fields = &fields },
        .resolved_doc_ref = .{ .table = "entities", .key = "person/ada_canonical" },
    }};
    const cand_lists = [_][]const Candidate{&cands};

    var res = try resolver.resolve(testing.allocator, 1, &entities, &cand_lists);
    defer res.deinit();

    try testing.expectEqual(Decision.review, res.entities[0].decision);
    try testing.expectEqualStrings("person/ada_lovelace", res.entities[0].doc_ref.key);
    try testing.expectEqualStrings("Ada Lovelace", res.entities[0].canonical_name);
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

test "resolution durable doc ref binding round trips without changing its logical table" {
    const alloc = testing.allocator;
    var parsed = try parseResolution(alloc, "{\"config_generation\":1,\"entities\":[{\"local_id\":\"e1\",\"doc_ref\":{\"table\":\"entities\",\"key\":\"person/ada\",\"storage_table\":\"table:original\"},\"confidence\":1,\"decision\":\"new\",\"label\":\"person\",\"canonical_name\":\"Ada\",\"surface_form\":\"Ada\"}]}");
    defer parsed.deinit();
    try testing.expectEqualStrings("entities", parsed.entities[0].doc_ref.table);
    try testing.expectEqualStrings("table:original", parsed.entities[0].doc_ref.storage_table.?);
    const value: Resolution = .{ .arena = undefined, .config_generation = parsed.config_generation, .entities = parsed.entities };
    const encoded = try value.toJson(alloc);
    defer alloc.free(encoded);
    var replay = try parseResolution(alloc, encoded);
    defer replay.deinit();
    try testing.expectEqualStrings("table:original", replay.entities[0].doc_ref.storage_table.?);
}

test "deterministic resolution consults aliases without embedding IO and retains destination binding" {
    const alloc = testing.allocator;
    const Ports = struct {
        reads: usize = 0,
        embeddings: usize = 0,
        bindings: usize = 0,
        fn candidates(ptr: *anyopaque, _: std.mem.Allocator, _: ExtractedEntity, _: *std.ArrayListUnmanaged(Candidate)) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            return;
        }
        fn embed(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8) anyerror!?[]const f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.embeddings += 1;
            return error.TestUnexpectedEmbedding;
        }
        fn bind(ptr: *anyopaque, _: []const u8) anyerror!?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.bindings += 1;
            return "table:original";
        }
    };
    var ports: Ports = .{};
    var resolver = try Resolver.initFromParts(alloc, "entities", "{{ slug _entity.text }}", .{}, false, "");
    defer resolver.deinit();
    var store = MapStore{ .alloc = alloc };
    defer store.deinit();
    try store.store().put("extraction", "{\"entities\":[{\"id\":\"one\",\"label\":\"person\",\"text\":\"Ada\"}]}");
    const stage = ResolutionStage{
        .resolver = &resolver,
        .config_generation = 1,
        .embedder = .{ .ptr = &ports, .embed_fn = Ports.embed },
        .doc_ref_binding = .{ .ptr = &ports, .bind = Ports.bind },
    };
    const result = (try stage.computeAlloc(alloc, store.store(), .{ .ptr = &ports, .vtable = &.{ .candidates_for = Ports.candidates } }, "extraction")).?;
    defer alloc.free(result);
    try testing.expectEqual(@as(usize, 1), ports.reads);
    try testing.expectEqual(@as(usize, 0), ports.embeddings);
    try testing.expectEqual(@as(usize, 1), ports.bindings);
    var parsed = try parseResolution(alloc, result);
    defer parsed.deinit();
    try testing.expectEqualStrings("table:original", parsed.entities[0].doc_ref.storage_table.?);
}

test "resolution exact minted key aliases require no scorer" {
    var resolver = try Resolver.parse(testing.allocator,
        \\{ "table": "entities", "key_template": "{{ slug _entity.text }}" }
    );
    defer resolver.deinit();

    var map = MapStore{ .alloc = testing.allocator };
    defer map.deinit();
    try map.store().put("ext:doc1",
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovelace" } ] }
    );

    // The blocked candidate is a merged-away duplicate pointing at the survivor.
    var candidate = MergedCandidate{
        .doc_ref = .{ .table = "entities", .key = "ada_lovelace" },
        .label = "person",
        .name = "Ada Lovelace",
        .merged_into = "person/ada_lovelace",
        .survivor_name = "Augusta Ada King",
    };
    const stage = ResolutionStage{ .resolver = &resolver, .config_generation = 1 };
    try testing.expectEqual(RunResult.written, try stage.run(testing.allocator, map.store(), candidate.provider(), "ext:doc1", "res:doc1"));

    const stored = (try map.store().get(testing.allocator, "res:doc1")).?;
    defer testing.allocator.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    // Resolved to the survivor, not the merged-away duplicate.
    try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
    try testing.expectEqualStrings("Augusta Ada King", ent.get("canonical_name").?.string);
    try testing.expectEqualStrings("Ada Lovelace", ent.get("surface_form").?.string);
}

test "uslug normalizes Unicode identities without changing legacy ASCII keys" {
    const a = std.testing.allocator;
    const cases = [_][2][]const u8{
        .{ "Qwen3", "qwen3" },        .{ "qwen3", "qwen3" },
        .{ "Ｑｗｅｎ３", "qwen3" },
        .{ "Straße", "strasse" },
        .{ "Σίσυφος", "σίσυφοσ" },
        .{ "東京 大学", "東京_大学" },
        .{ "CAFÉ", "café" },
        .{ "Cafe\u{301}", "café" },
        .{ "한글", "한글" },
        .{ "हिन्दी", "हिन्दी" },
        .{ "العَرَبِيَّة", "العَرَبِيَّة" },
        .{ "ภาษาไทย น้ำ", "ภาษาไทย_น้ํา" },
        .{ "\u{301}Hindi", "hindi" },
        .{ "ﬃ ①", "ffi_1" },
    };
    for (cases) |case| {
        const key = try renderKey(a, "entity/{{ uslug _entity.text }}", .{ .local_id = "e0", .label = "concept", .text = case[0] });
        defer a.free(key);
        const expected = try std.fmt.allocPrint(a, "entity/{s}", .{case[1]});
        defer a.free(expected);
        try std.testing.expectEqualStrings(expected, key);
    }
    const legacy = try renderKey(a, "entity/{{ slug _entity.text }}", .{ .local_id = "e0", .label = "concept", .text = "Epstein's island" });
    defer a.free(legacy);
    try std.testing.expectEqualStrings("entity/epstein_island", legacy);
    for ([_][]const u8{ "entity/{{ slug _entity.text }}", "entity/{{ uslug _entity.text }}" }) |template|
        try std.testing.expectError(error.EmptyEntityKey, renderKey(a, template, .{ .local_id = "e0", .label = "concept", .text = "!!!" }));
    try std.testing.expectError(error.EmptyEntityKey, renderKey(a, "entity/{{ slug _entity.text }}", .{ .local_id = "e0", .label = "concept", .text = "東京" }));
    try std.testing.expectError(error.InvalidUtf8, renderKey(a, "{{ uslug _entity.text }}", .{ .local_id = "e0", .label = "concept", .text = "\xff" }));
}

test "empty mention slug is isolated before candidate blocking" {
    const a = std.testing.allocator;
    var resolver = try Resolver.parse(a, "{\"table\":\"entities\",\"key_template\":\"entity/{{ uslug _entity.text }}\"}");
    defer resolver.deinit();
    const entities = [_]ExtractedEntity{
        .{ .local_id = "bad", .label = "concept", .text = "!!!" },
        .{ .local_id = "good", .label = "concept", .text = "हिन्दी" },
    };
    var resolution = try resolver.resolve(a, 1, &entities, &.{});
    defer resolution.deinit();
    try std.testing.expectEqual(@as(usize, 1), resolution.entities.len);
    try std.testing.expectEqual(@as(usize, 1), resolution.skipped_mentions);
    try std.testing.expectEqualStrings("entity/हिन्दी", resolution.entities[0].doc_ref.key);
}

test "resolver key aliases work without promoted documents and reject cycles" {
    const aliases = [_]KeyAlias{ .{ .key = "concept/qwen3", .target = "concept/qwen3_embedding" }, .{ .key = "concept/qwen3_embedding", .target = "concept/qwen" } };
    try validateAliases(&aliases);
    try testing.expectError(error.InvalidResolverAliasCycle, validateAliases(&.{ .{ .key = "a", .target = "b" }, .{ .key = "b", .target = "a" } }));
    var resolver = try Resolver.parse(testing.allocator, "{\"table\":\"default\",\"key_template\":\"concept/{{ uslug _entity.text }}\",\"aliases\":[{\"key\":\"concept/qwen3\",\"target\":\"concept/qwen3_embedding\"},{\"key\":\"concept/qwen3_embedding\",\"target\":\"concept/qwen\"}]}");
    defer resolver.deinit();
    var result = try resolver.resolve(testing.allocator, 3, &.{.{ .local_id = "q", .label = "concept", .text = "Qwen3" }}, &.{});
    defer result.deinit();
    try testing.expectEqualStrings("concept/qwen", result.entities[0].doc_ref.key);
    try testing.expectEqual(Decision.match, result.entities[0].decision);
}

test "resolver explicit aliases bypass unavailable candidate sources" {
    var resolver = try Resolver.initFromParts(testing.allocator, "default", "concept/{{ uslug _entity.text }}", .{}, true, "");
    defer resolver.deinit();
    resolver.aliases = &.{.{ .key = "concept/qwen3", .target = "concept/qwen" }};
    var store = MapStore{ .alloc = testing.allocator };
    defer store.deinit();
    try store.store().put("extraction", "{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Qwen3\"}]}");
    const Unavailable = struct {
        fn candidates(_: *anyopaque, _: std.mem.Allocator, _: ExtractedEntity, _: *std.ArrayListUnmanaged(Candidate)) anyerror!void {
            return error.Unavailable;
        }
    };
    var token: u8 = 0;
    const stage = ResolutionStage{ .resolver = &resolver, .config_generation = 1 };
    const bytes = (try stage.computeAlloc(testing.allocator, store.store(), .{ .ptr = &token, .vtable = &.{ .candidates_for = Unavailable.candidates } }, "extraction")).?;
    defer testing.allocator.free(bytes);
    var result = try parseResolution(testing.allocator, bytes);
    defer result.deinit();
    try testing.expectEqualStrings("concept/qwen", result.entities[0].doc_ref.key);
}

test "incremental resolution reuses stages and invalidates changed dependencies" {
    const a = testing.allocator;
    const Ports = struct {
        name: []const u8 = "Ada",
        vector: [2]f32 = .{ 1, 0 },
        calls: usize = 0,
        fn embed(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8) anyerror!?[]const f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try alloc.dupe(f32, &self.vector);
        }
        fn candidates(ptr: *anyopaque, alloc: std.mem.Allocator, _: ExtractedEntity, out: *std.ArrayListUnmanaged(Candidate)) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const fields = try alloc.alloc(matcher.Field, 2);
            fields[0] = .{ .name = "canonical_name", .value = .{ .text = self.name } };
            fields[1] = .{ .name = "vector", .value = .{ .vector = &.{ 1, 0 } } };
            try out.append(alloc, .{ .doc_ref = .{ .table = "entities", .key = "canonical" }, .label = "person", .record = .{ .fields = fields } });
        }
    };
    var ports: Ports = .{};
    var resolver = try Resolver.initFromParts(a, "entities", "{{ uslug _entity.text }}", .{}, true,
        \\{"comparisons":[{"name":"name","left":"text","right":"canonical_name","levels":[{"when":"exact","weight":2},{"else":true,"weight":-2}]},{"name":"vector","left":"name_embedding","right":"vector","levels":[{"when":"cosine > 0.9","weight":2},{"else":true,"weight":-2}]}],"combine":{"bias":0},"decision":{"match":0.9}}
    );
    defer resolver.deinit();
    var memory: MapStore = .{ .alloc = a };
    defer memory.deinit();
    try memory.store().put("ext", "{\"entities\":[{\"id\":\"a\",\"text\":\"Ada\",\"label\":\"person\"}]}");
    const provider: CandidateProvider = .{ .ptr = &ports, .vtable = &.{ .candidates_for = Ports.candidates } };
    var stats: WorkStats = .{};
    var stage: ResolutionStage = .{ .resolver = &resolver, .config_generation = 1, .embedder = .{ .ptr = &ports, .embed_fn = Ports.embed }, .embedding_dependency = @as([32]u8, @splat(1)), .candidate_dependency = @as([32]u8, @splat(2)), .stats = &stats };
    try testing.expectEqual(RunResult.written, try stage.run(a, memory.store(), provider, "ext", "res"));
    try testing.expectEqual(@as(usize, 1), stats.embeddings);
    try testing.expectEqual(@as(usize, 1), stats.candidate_mentions);
    try testing.expectEqual(@as(usize, 2), stats.features.computed);
    stats = .{};
    try testing.expectEqual(RunResult.unchanged, try stage.run(a, memory.store(), provider, "ext", "res"));
    try testing.expectEqual(@as(usize, 0), stats.embeddings);
    try testing.expectEqual(@as(usize, 0), stats.candidate_mentions);
    try testing.expectEqual(@as(usize, 0), stats.features.computed);
    try testing.expectEqual(@as(usize, 2), stats.features.reused);

    // Decisions are recomputed from features; thresholds and weights are not
    // dependencies of embeddings, candidate sets, or raw comparisons.
    resolver.scorer.?.match_threshold = 0.999;
    stage.config_generation += 1;
    stats = .{};
    _ = try stage.run(a, memory.store(), provider, "ext", "res");
    const changed = (try memory.store().get(a, "res")).?;
    defer a.free(changed);
    var parsed = try parseResolution(a, changed);
    defer parsed.deinit();
    try testing.expectEqual(Decision.review, parsed.entities[0].decision);
    try testing.expectEqual(@as(usize, 0), stats.embeddings + stats.candidate_mentions + stats.features.computed);
    resolver.scorer.?.bias = 10;
    stats = .{};
    _ = try stage.run(a, memory.store(), provider, "ext", "res");
    try testing.expectEqual(@as(usize, 0), stats.embeddings + stats.candidate_mentions + stats.features.computed);

    // Changing just one comparator computes just that feature.
    @constCast(@constCast(resolver.scorer.?.comparisons)[0].levels)[0].condition.threshold.comparator = .jaro_winkler;
    stats = .{};
    _ = try stage.run(a, memory.store(), provider, "ext", "res");
    try testing.expectEqual(@as(usize, 1), stats.features.computed);
    try testing.expectEqual(@as(usize, 1), stats.features.reused);
    try testing.expectEqual(@as(usize, 0), stats.embeddings + stats.candidate_mentions);

    // A canonical write/index-visibility revision refetches candidates. Only
    // consumed fields that changed invalidate their comparator values.
    ports.name = "Augusta";
    stage.candidate_dependency = @as([32]u8, @splat(3));
    stats = .{};
    _ = try stage.run(a, memory.store(), provider, "ext", "res");
    try testing.expectEqual(@as(usize, 1), stats.candidate_mentions);
    try testing.expectEqual(@as(usize, 0), stats.embeddings);
    try testing.expectEqual(@as(usize, 1), stats.features.computed);
    try testing.expectEqual(@as(usize, 1), stats.features.reused);

    // Model/dimension changes regenerate embeddings and dependent ANN work.
    stage.embedding_dependency = @as([32]u8, @splat(4));
    ports.vector = .{ 0, 1 };
    stats = .{};
    _ = try stage.run(a, memory.store(), provider, "ext", "res");
    try testing.expectEqual(@as(usize, 1), stats.embeddings);
    try testing.expectEqual(@as(usize, 1), stats.candidate_mentions);
    try testing.expectEqual(@as(usize, 1), stats.features.computed);

    // Adding/reordering mentions preserves the other mention's independent work.
    try memory.store().put("ext", "{\"entities\":[{\"id\":\"b\",\"text\":\"Grace\",\"label\":\"person\"},{\"id\":\"a\",\"text\":\"Ada\",\"label\":\"person\"}]}");
    stats = .{};
    _ = try stage.run(a, memory.store(), provider, "ext", "res");
    try testing.expectEqual(@as(usize, 1), stats.embeddings);
    try testing.expectEqual(@as(usize, 1), stats.reused_embeddings);
    try testing.expectEqual(@as(usize, 1), stats.candidate_mentions);
    try testing.expectEqual(@as(usize, 1), stats.reused_candidate_mentions);
    const warm = (try memory.store().get(a, "res")).?;
    defer a.free(warm);
    const cold = (try stage.computeAlloc(a, memory.store(), provider, "ext")).?;
    defer a.free(cold);
    // Independently recomputed and warm results, including cache, are identical.
    try testing.expectEqualStrings(cold, warm);
    var cache_json = try std.json.parseFromSlice(std.json.Value, a, warm, .{});
    defer cache_json.deinit();
    try testing.expectEqual(@as(usize, 1), cache_json.value.object.get("_resolution_work").?.object.get("candidates").?.array.items.len);

    // Both mention and candidate vectors use lossless compact storage.
    const packed_mention = cache_json.value.object.get("_resolution_work").?.object.get("mentions").?.array.items[0];
    try testing.expect(std.mem.startsWith(u8, packed_mention.object.get("embedding").?.string, "f32le:"));
    stage.work_retention = .embeddings;
    const embeddings_only = (try stage.computeAlloc(a, memory.store(), provider, "ext")).?;
    defer a.free(embeddings_only);
    var only_json = try std.json.parseFromSlice(std.json.Value, a, embeddings_only, .{});
    defer only_json.deinit();
    try testing.expectEqual(@as(usize, 0), only_json.value.object.get("_resolution_work").?.object.get("candidates").?.array.items.len);
    stage.work_retention = .none;
    const no_work = (try stage.computeAlloc(a, memory.store(), provider, "ext")).?;
    defer a.free(no_work);
    try testing.expect(std.mem.indexOf(u8, no_work, "_resolution_work") == null);
    stage.work_retention = .full;
    stage.max_work_bytes = 1;
    const bounded = (try stage.computeAlloc(a, memory.store(), provider, "ext")).?;
    defer a.free(bounded);
    try testing.expect(std.mem.indexOf(u8, bounded, "_resolution_work") == null);
    stage.max_work_bytes = 1024 * 1024;
    // Newly supplied curation must win over a cached candidate match.
    const previous_aliases = resolver.aliases;
    resolver.aliases = try a.alloc(KeyAlias, 1);
    @constCast(resolver.aliases)[0] = .{ .key = try a.dupe(u8, "ada"), .target = try a.dupe(u8, "curated") };
    _ = try stage.run(a, memory.store(), provider, "ext", "res");
    const curated_raw = (try memory.store().get(a, "res")).?;
    defer a.free(curated_raw);
    var curated = try parseResolution(a, curated_raw);
    defer curated.deinit();
    try testing.expectEqualStrings("curated", curated.entities[1].doc_ref.key);

    for (resolver.aliases) |alias| {
        a.free(alias.key);
        a.free(alias.target);
    }
    a.free(resolver.aliases);
    resolver.aliases = previous_aliases;

    // Unknown/corrupt cache versions are expendable and need no migration.
    try memory.store().put("res", "{\"_resolution_work\":{\"version\":99,\"mentions\":[]}}");
    stats = .{};
    _ = try stage.run(a, memory.store(), provider, "ext", "res");
    try testing.expectEqual(@as(usize, 2), stats.embeddings);
    try memory.store().put("res", "{\"_resolution_work\":{\"version\":1,\"mentions\":false}}");
    stats = .{};
    _ = try stage.run(a, memory.store(), provider, "ext", "res");
    try testing.expectEqual(@as(usize, 2), stats.candidate_mentions);
    // No candidate revision means no reuse, including previously empty results.
    stage.candidate_dependency = null;
    stats = .{};
    _ = try stage.run(a, memory.store(), provider, "ext", "res");
    try testing.expectEqual(@as(usize, 2), stats.candidate_mentions);
    try memory.store().delete("ext");
    try testing.expectEqual(RunResult.cleared, try stage.run(a, memory.store(), provider, "ext", "res"));
    try testing.expect((try memory.store().get(a, "res")) == null);
}
