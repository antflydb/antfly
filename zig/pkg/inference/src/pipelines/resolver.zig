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

const std = @import("std");
const ner_mod = @import("ner.zig");
const gliner_mod = @import("gliner.zig");

pub const Entity = ner_mod.Entity;
pub const Relation = gliner_mod.Relation;

pub const ResolverConfig = struct {
    similarity_threshold: f64 = 0.85,
    type_must_match: bool = true,
    min_entity_confidence: f32 = 0.0,
    min_relation_confidence: f32 = 0.0,
    deduplicate_relations: bool = true,
    track_provenance: bool = true,
};

pub const ResolvedEntity = struct {
    id: []const u8,
    canonical_name: []const u8,
    label: []const u8,
    score: f32,
    mentions: ?[]const []const u8 = null,
    text_indices: ?[]const usize = null,

    pub fn deinit(self: *ResolvedEntity, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.canonical_name);
        allocator.free(self.label);
        if (self.mentions) |mentions| {
            for (mentions) |mention| allocator.free(mention);
            allocator.free(mentions);
        }
        if (self.text_indices) |indices| allocator.free(indices);
    }
};

pub const ResolvedRelation = struct {
    head_id: []const u8,
    tail_id: []const u8,
    label: []const u8,
    score: f32,
    text_indices: ?[]const usize = null,

    pub fn deinit(self: *ResolvedRelation, allocator: std.mem.Allocator) void {
        allocator.free(self.head_id);
        allocator.free(self.tail_id);
        allocator.free(self.label);
        if (self.text_indices) |indices| allocator.free(indices);
    }
};

pub const KnowledgeGraph = struct {
    entities: []ResolvedEntity,
    relations: []ResolvedRelation,

    pub fn deinit(self: *KnowledgeGraph, allocator: std.mem.Allocator) void {
        for (self.entities) |*entity| entity.deinit(allocator);
        allocator.free(self.entities);
        for (self.relations) |*relation| relation.deinit(allocator);
        allocator.free(self.relations);
    }
};

const MentionRef = struct {
    normalized_text: []const u8,
    label: []const u8,
    entity_id: []const u8,
    text_index: usize,

    fn deinit(self: *MentionRef, allocator: std.mem.Allocator) void {
        allocator.free(self.normalized_text);
    }
};

const IndexedEntity = struct {
    entity: Entity,
    text_index: usize,
};

const IndexedRelation = struct {
    relation: Relation,
    text_index: usize,
};

const Cluster = struct {
    indices: std.ArrayListUnmanaged(usize) = .empty,

    fn deinit(self: *Cluster, allocator: std.mem.Allocator) void {
        self.indices.deinit(allocator);
    }
};

pub fn buildKnowledgeGraph(
    allocator: std.mem.Allocator,
    entities_by_text: []const []const Entity,
    relations_by_text: ?[]const []const Relation,
    cfg: ResolverConfig,
) !KnowledgeGraph {
    var all_entities = std.ArrayListUnmanaged(IndexedEntity).empty;
    defer all_entities.deinit(allocator);
    for (entities_by_text, 0..) |entities, text_index| {
        for (entities) |entity| {
            if (entity.score >= cfg.min_entity_confidence) {
                try all_entities.append(allocator, .{
                    .entity = entity,
                    .text_index = text_index,
                });
            }
        }
    }

    var all_relations = std.ArrayListUnmanaged(IndexedRelation).empty;
    defer all_relations.deinit(allocator);
    if (relations_by_text) |relations_batches| {
        for (relations_batches, 0..) |relations, text_index| {
            for (relations) |relation| {
                if (relation.score >= cfg.min_relation_confidence) {
                    try all_relations.append(allocator, .{
                        .relation = relation,
                        .text_index = text_index,
                    });
                }
            }
        }
    }

    var mention_refs = std.ArrayListUnmanaged(MentionRef).empty;
    defer {
        for (mention_refs.items) |*mention| mention.deinit(allocator);
        mention_refs.deinit(allocator);
    }

    const resolved_entities = try resolveEntities(allocator, all_entities.items, cfg, &mention_refs);
    errdefer {
        for (resolved_entities) |*entity| entity.deinit(allocator);
        allocator.free(resolved_entities);
    }

    var resolved_relations = try allocator.alloc(ResolvedRelation, 0);
    errdefer {
        for (resolved_relations) |*relation| relation.deinit(allocator);
        allocator.free(resolved_relations);
    }

    if (all_relations.items.len > 0) {
        var relations = std.ArrayListUnmanaged(ResolvedRelation).empty;
        errdefer {
            for (relations.items) |*relation| relation.deinit(allocator);
            relations.deinit(allocator);
        }

        for (all_relations.items) |indexed_relation| {
            const relation = indexed_relation.relation;
            const head_id = try lookupEntityID(allocator, mention_refs.items, relation.head, cfg);
            defer if (head_id.len > 0) allocator.free(head_id);
            if (head_id.len == 0) continue;

            const tail_id = try lookupEntityID(allocator, mention_refs.items, relation.tail, cfg);
            defer if (tail_id.len > 0) allocator.free(tail_id);
            if (tail_id.len == 0) continue;

            const text_indices = try allocator.alloc(usize, 1);
            errdefer allocator.free(text_indices);
            text_indices[0] = indexed_relation.text_index;

            try relations.append(allocator, .{
                .head_id = try allocator.dupe(u8, head_id),
                .tail_id = try allocator.dupe(u8, tail_id),
                .label = try allocator.dupe(u8, relation.label),
                .score = relation.score,
                .text_indices = text_indices,
            });
        }

        if (cfg.deduplicate_relations) {
            resolved_relations = try deduplicateRelations(allocator, relations.items);
            for (relations.items) |*relation| relation.deinit(allocator);
            relations.deinit(allocator);
        } else {
            resolved_relations = try relations.toOwnedSlice(allocator);
        }
    }

    return .{
        .entities = resolved_entities,
        .relations = resolved_relations,
    };
}

fn resolveEntities(
    allocator: std.mem.Allocator,
    entities: []const IndexedEntity,
    cfg: ResolverConfig,
    mention_refs: *std.ArrayListUnmanaged(MentionRef),
) ![]ResolvedEntity {
    if (entities.len == 0) return try allocator.alloc(ResolvedEntity, 0);

    var clusters = std.ArrayListUnmanaged(Cluster).empty;
    defer {
        for (clusters.items) |*cluster| cluster.deinit(allocator);
        clusters.deinit(allocator);
    }

    const assignments = try allocator.alloc(i32, entities.len);
    defer allocator.free(assignments);
    @memset(assignments, -1);

    var normalized_cache = try allocator.alloc([]const u8, entities.len);
    defer {
        for (normalized_cache) |text| allocator.free(text);
        allocator.free(normalized_cache);
    }
    for (entities, 0..) |indexed_entity, i| {
        normalized_cache[i] = try normalizeText(allocator, indexed_entity.entity.text);
    }

    for (entities, 0..) |indexed_entity, i| {
        const entity = indexed_entity.entity;
        if (assignments[i] >= 0) continue;

        const cluster_index = clusters.items.len;
        try clusters.append(allocator, .{});
        try clusters.items[cluster_index].indices.append(allocator, i);
        assignments[i] = @intCast(cluster_index);

        var j = i + 1;
        while (j < entities.len) : (j += 1) {
            if (assignments[j] >= 0) continue;
            if (cfg.type_must_match and !std.mem.eql(u8, entity.label, entities[j].entity.label)) continue;

            const similarity = entitySimilarity(normalized_cache[i], normalized_cache[j]);
            if (similarity >= cfg.similarity_threshold) {
                try clusters.items[cluster_index].indices.append(allocator, j);
                assignments[j] = @intCast(cluster_index);
            }
        }
    }

    const resolved = try allocator.alloc(ResolvedEntity, clusters.items.len);
    errdefer {
        for (resolved) |*entity| entity.deinit(allocator);
        allocator.free(resolved);
    }

    for (clusters.items, 0..) |cluster, i| {
        resolved[i] = try buildResolvedEntity(allocator, i, cluster.indices.items, entities, normalized_cache, cfg);
        for (cluster.indices.items) |entity_index| {
            try mention_refs.append(allocator, .{
                .normalized_text = try allocator.dupe(u8, normalized_cache[entity_index]),
                .label = entities[entity_index].entity.label,
                .entity_id = resolved[i].id,
                .text_index = entities[entity_index].text_index,
            });
        }
    }

    return resolved;
}

fn buildResolvedEntity(
    allocator: std.mem.Allocator,
    cluster_index: usize,
    indices: []const usize,
    entities: []const IndexedEntity,
    normalized_cache: []const []const u8,
    cfg: ResolverConfig,
) !ResolvedEntity {
    var best_index = indices[0];
    var max_score: f32 = 0.0;

    var mention_values = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (mention_values.items) |mention| allocator.free(mention);
        mention_values.deinit(allocator);
    }

    for (indices) |idx| {
        const entity = entities[idx].entity;
        if (entity.score > max_score) max_score = entity.score;

        const current_best = entities[best_index].entity;
        if (entity.text.len > current_best.text.len or
            (entity.text.len == current_best.text.len and entity.score > current_best.score))
        {
            best_index = idx;
        }
    }

    if (cfg.track_provenance) {
        var seen = std.ArrayListUnmanaged([]const u8).empty;
        defer seen.deinit(allocator);
        for (indices) |idx| {
            const normalized = normalized_cache[idx];
            var already_seen = false;
            for (seen.items) |existing| {
                if (std.mem.eql(u8, existing, normalized)) {
                    already_seen = true;
                    break;
                }
            }
            if (already_seen) continue;
            try seen.append(allocator, normalized);
            try mention_values.append(allocator, try allocator.dupe(u8, entities[idx].entity.text));
        }
    }

    var text_indices = std.ArrayListUnmanaged(usize).empty;
    errdefer text_indices.deinit(allocator);
    for (indices) |idx| try appendUniqueUsize(allocator, &text_indices, entities[idx].text_index);

    return .{
        .id = try std.fmt.allocPrint(allocator, "entity-{d}", .{cluster_index}),
        .canonical_name = try allocator.dupe(u8, entities[best_index].entity.text),
        .label = try allocator.dupe(u8, entities[best_index].entity.label),
        .score = max_score,
        .mentions = if (cfg.track_provenance) try mention_values.toOwnedSlice(allocator) else null,
        .text_indices = try text_indices.toOwnedSlice(allocator),
    };
}

fn lookupEntityID(
    allocator: std.mem.Allocator,
    mention_refs: []const MentionRef,
    entity: Entity,
    cfg: ResolverConfig,
) ![]const u8 {
    const normalized = try normalizeText(allocator, entity.text);
    defer allocator.free(normalized);

    for (mention_refs) |mention| {
        if (cfg.type_must_match and !std.mem.eql(u8, mention.label, entity.label)) continue;
        if (std.mem.eql(u8, mention.normalized_text, normalized)) {
            return try allocator.dupe(u8, mention.entity_id);
        }
    }

    var best_similarity: f64 = 0.0;
    var best_id: ?[]const u8 = null;
    for (mention_refs) |mention| {
        if (cfg.type_must_match and !std.mem.eql(u8, mention.label, entity.label)) continue;
        const similarity = entitySimilarity(normalized, mention.normalized_text);
        if (similarity >= cfg.similarity_threshold and similarity > best_similarity) {
            best_similarity = similarity;
            best_id = mention.entity_id;
        }
    }

    if (best_id) |id| return try allocator.dupe(u8, id);
    return try allocator.alloc(u8, 0);
}

fn deduplicateRelations(allocator: std.mem.Allocator, relations: []const ResolvedRelation) ![]ResolvedRelation {
    const deduped = try allocator.alloc(ResolvedRelation, relations.len);
    var count: usize = 0;
    errdefer {
        for (deduped[0..count]) |*relation| relation.deinit(allocator);
        allocator.free(deduped);
    }

    for (relations) |relation| {
        var existing_index: ?usize = null;
        for (deduped[0..count], 0..) |existing, i| {
            if (std.mem.eql(u8, existing.head_id, relation.head_id) and
                std.mem.eql(u8, existing.tail_id, relation.tail_id) and
                std.mem.eql(u8, existing.label, relation.label))
            {
                existing_index = i;
                break;
            }
        }

        if (existing_index) |idx| {
            if (relation.score > deduped[idx].score) deduped[idx].score = relation.score;
            if (relation.text_indices) |text_indices| {
                var merged = std.ArrayListUnmanaged(usize).empty;
                errdefer merged.deinit(allocator);
                const existing_indices = deduped[idx].text_indices;
                if (existing_indices) |indices| {
                    for (indices) |text_index| try appendUniqueUsize(allocator, &merged, text_index);
                }
                for (text_indices) |text_index| try appendUniqueUsize(allocator, &merged, text_index);
                const merged_indices = try merged.toOwnedSlice(allocator);
                if (existing_indices) |indices| allocator.free(indices);
                deduped[idx].text_indices = merged_indices;
            }
            continue;
        }

        const text_indices = if (relation.text_indices) |indices| try allocator.dupe(usize, indices) else null;
        errdefer if (text_indices) |indices| allocator.free(indices);

        deduped[count] = .{
            .head_id = try allocator.dupe(u8, relation.head_id),
            .tail_id = try allocator.dupe(u8, relation.tail_id),
            .label = try allocator.dupe(u8, relation.label),
            .score = relation.score,
            .text_indices = text_indices,
        };
        count += 1;
    }

    std.mem.sort(ResolvedRelation, deduped[0..count], {}, struct {
        fn lessThan(_: void, a: ResolvedRelation, b: ResolvedRelation) bool {
            const head_order = std.mem.order(u8, a.head_id, b.head_id);
            if (head_order != .eq) return head_order == .lt;
            const tail_order = std.mem.order(u8, a.tail_id, b.tail_id);
            if (tail_order != .eq) return tail_order == .lt;
            return std.mem.order(u8, a.label, b.label) == .lt;
        }
    }.lessThan);

    return allocator.realloc(deduped, count);
}

fn appendUniqueUsize(
    allocator: std.mem.Allocator,
    values: *std.ArrayListUnmanaged(usize),
    value: usize,
) !void {
    for (values.items) |existing| {
        if (existing == value) return;
    }
    try values.append(allocator, value);
}

pub fn entitySimilarity(a: []const u8, b: []const u8) f64 {
    if (std.mem.eql(u8, a, b)) return 1.0;
    if (a.len == 0 or b.len == 0) return 0.0;

    const jw = jaroWinkler(a, b);
    const shorter, const longer = if (a.len <= b.len) .{ a, b } else .{ b, a };

    var shorter_tokens = std.ArrayListUnmanaged([]const u8).empty;
    defer shorter_tokens.deinit(std.heap.page_allocator);
    tokenizeWords(std.heap.page_allocator, shorter, &shorter_tokens) catch return jw;

    var longer_tokens = std.ArrayListUnmanaged([]const u8).empty;
    defer longer_tokens.deinit(std.heap.page_allocator);
    tokenizeWords(std.heap.page_allocator, longer, &longer_tokens) catch return jw;

    if (shorter_tokens.items.len == 0 or longer_tokens.items.len == 0) return jw;

    var matched: usize = 0;
    for (shorter_tokens.items) |short_token| {
        for (longer_tokens.items) |long_token| {
            if (std.mem.eql(u8, short_token, long_token)) {
                matched += 1;
                break;
            }
        }
    }

    if (matched == shorter_tokens.items.len) {
        const ratio = @as(f64, @floatFromInt(shorter.len)) / @as(f64, @floatFromInt(longer.len));
        const containment = 0.7 + 0.3 * ratio;
        return @max(containment, jw);
    }

    return jw;
}

pub fn jaroWinkler(a: []const u8, b: []const u8) f64 {
    if (std.mem.eql(u8, a, b)) return 1.0;
    if (a.len == 0 or b.len == 0) return 0.0;

    const jaro = jaroSimilarity(a, b);

    var prefix_len: usize = 0;
    const max_prefix = @min(@min(a.len, b.len), 4);
    while (prefix_len < max_prefix and a[prefix_len] == b[prefix_len]) : (prefix_len += 1) {}

    return jaro + @as(f64, @floatFromInt(prefix_len)) * 0.1 * (1.0 - jaro);
}

fn jaroSimilarity(a: []const u8, b: []const u8) f64 {
    if (std.mem.eql(u8, a, b)) return 1.0;

    const max_len = @max(a.len, b.len);
    const max_dist: usize = if (max_len > 1) max_len / 2 - 1 else 0;

    const allocator = std.heap.page_allocator;
    const a_matches = allocator.alloc(bool, a.len) catch return 0.0;
    defer allocator.free(a_matches);
    @memset(a_matches, false);

    const b_matches = allocator.alloc(bool, b.len) catch return 0.0;
    defer allocator.free(b_matches);
    @memset(b_matches, false);

    var matches: usize = 0;
    for (a, 0..) |a_char, i| {
        const start = i -| max_dist;
        const end = @min(b.len, i + max_dist + 1);
        var j = start;
        while (j < end) : (j += 1) {
            if (b_matches[j] or a_char != b[j]) continue;
            a_matches[i] = true;
            b_matches[j] = true;
            matches += 1;
            break;
        }
    }

    if (matches == 0) return 0.0;

    var transpositions: usize = 0;
    var j: usize = 0;
    for (a, 0..) |a_char, i| {
        if (!a_matches[i]) continue;
        while (j < b.len and !b_matches[j]) : (j += 1) {}
        if (j < b.len and a_char != b[j]) transpositions += 1;
        j += 1;
    }

    const matches_f = @as(f64, @floatFromInt(matches));
    const a_len_f = @as(f64, @floatFromInt(a.len));
    const b_len_f = @as(f64, @floatFromInt(b.len));
    const transpositions_f = @as(f64, @floatFromInt(transpositions)) / 2.0;

    return ((matches_f / a_len_f) + (matches_f / b_len_f) + ((matches_f - transpositions_f) / matches_f)) / 3.0;
}

fn normalizeText(allocator: std.mem.Allocator, text: []const u8) ![]const u8 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    const normalized = try allocator.alloc(u8, trimmed.len);
    for (trimmed, 0..) |c, i| {
        normalized[i] = std.ascii.toLower(c);
    }
    return normalized;
}

fn tokenizeWords(
    allocator: std.mem.Allocator,
    text: []const u8,
    out: *std.ArrayListUnmanaged([]const u8),
) !void {
    var iter = std.mem.tokenizeAny(u8, text, " \t\r\n");
    while (iter.next()) |token| {
        try out.append(allocator, token);
    }
}

test "jaroWinkler handles identical and empty strings" {
    try std.testing.expectEqual(@as(f64, 1.0), jaroWinkler("hello", "hello"));
    try std.testing.expectEqual(@as(f64, 0.0), jaroWinkler("", "hello"));
    try std.testing.expectEqual(@as(f64, 0.0), jaroWinkler("hello", ""));
}

test "entitySimilarity boosts token containment" {
    try std.testing.expect(entitySimilarity("elon musk", "musk") >= 0.7);
    try std.testing.expect(entitySimilarity("spacex", "spacex inc") >= 0.85);
}

test "buildKnowledgeGraph resolves entities and deduplicates relations" {
    const allocator = std.testing.allocator;

    const entities = [_][]const Entity{
        &.{
            .{ .text = "Elon Musk", .label = "person", .score = 0.95, .start = 0, .end = 9 },
            .{ .text = "SpaceX", .label = "organization", .score = 0.92, .start = 18, .end = 24 },
        },
        &.{
            .{ .text = "Musk", .label = "person", .score = 0.88, .start = 0, .end = 4 },
            .{ .text = "SpaceX", .label = "organization", .score = 0.90, .start = 20, .end = 26 },
        },
    };

    const relations = [_][]const Relation{
        &.{
            .{
                .head = .{ .text = "Elon Musk", .label = "person", .score = 0.95, .start = 0, .end = 9 },
                .tail = .{ .text = "SpaceX", .label = "organization", .score = 0.92, .start = 18, .end = 24 },
                .label = "founded",
                .score = 0.80,
            },
        },
        &.{
            .{
                .head = .{ .text = "Musk", .label = "person", .score = 0.88, .start = 0, .end = 4 },
                .tail = .{ .text = "SpaceX", .label = "organization", .score = 0.90, .start = 20, .end = 26 },
                .label = "founded",
                .score = 0.90,
            },
        },
    };

    var cfg = ResolverConfig{};
    cfg.similarity_threshold = 0.7;

    var kg = try buildKnowledgeGraph(allocator, &entities, &relations, cfg);
    defer kg.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), kg.entities.len);
    try std.testing.expectEqual(@as(usize, 1), kg.relations.len);
    try std.testing.expectEqualStrings("Elon Musk", kg.entities[0].canonical_name);
    try std.testing.expectEqual(@as(f32, 0.90), kg.relations[0].score);
}
