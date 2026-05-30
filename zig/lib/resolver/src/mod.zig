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

    pub fn deinit(self: *Resolver) void {
        if (self.scorer) |*s| s.deinit();
        self.arena.deinit();
        self.* = undefined;
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
                };
                const mention = matcher.Record{ .fields = &mention_fields };

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
                        };
                    }
                    return .{
                        .local_id = local_id,
                        .doc_ref = try self.mintRef(a, entity),
                        .confidence = best_prob,
                        .decision = if (best_outcome == .review) .review else .new,
                    };
                }
            }
        }

        return .{
            .local_id = local_id,
            .doc_ref = try self.mintRef(a, entity),
            .confidence = 1.0,
            .decision = .new,
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

// --- Tests ------------------------------------------------------------------

const testing = std.testing;

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
