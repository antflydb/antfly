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

pub const Entity = struct {
    text: []const u8,
    label: []const u8,
    start: usize,
    end: usize,
    score: f32,
};

pub const CleanupConfig = struct {
    min_validity_score: f32 = 0.5,
    dedup_similarity_threshold: f32 = 0.9,
    type_must_match: bool = true,
    track_provenance: bool = true,
};

pub const FeatureConfig = struct {
    feature_dim: usize = 128,
    context_window: usize = 24,
};

pub const CleanupMention = struct {
    entity: Entity,
    validity_score: f32,
    representative_score: f32,
    embedding: []const f32,

    pub fn deinit(self: *CleanupMention, allocator: std.mem.Allocator) void {
        allocator.free(self.entity.text);
        allocator.free(self.entity.label);
        allocator.free(self.embedding);
        self.* = undefined;
    }
};

pub const DroppedMention = struct {
    text: []const u8,
    label: []const u8,
    start: usize,
    end: usize,
    detect_score: f32,
    validity_score: f32,

    pub fn deinit(self: *DroppedMention, allocator: std.mem.Allocator) void {
        allocator.free(self.text);
        allocator.free(self.label);
        self.* = undefined;
    }
};

pub const ResolvedEntity = struct {
    id: []const u8,
    text: []const u8,
    label: []const u8,
    start: usize,
    end: usize,
    detect_score: f32,
    validity_score: f32,
    representative_score: f32,
    mentions: ?[]const []const u8 = null,

    pub fn deinit(self: *ResolvedEntity, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.text);
        allocator.free(self.label);
        if (self.mentions) |mentions| {
            for (mentions) |mention| allocator.free(mention);
            allocator.free(mentions);
        }
        self.* = undefined;
    }
};

pub const CleanupResult = struct {
    dropped_mentions: []DroppedMention,
    resolved_entities: []ResolvedEntity,

    pub fn deinit(self: *CleanupResult, allocator: std.mem.Allocator) void {
        for (self.dropped_mentions) |*mention| mention.deinit(allocator);
        allocator.free(self.dropped_mentions);
        for (self.resolved_entities) |*entity| entity.deinit(allocator);
        allocator.free(self.resolved_entities);
        self.* = undefined;
    }
};

const Cluster = struct {
    indices: std.ArrayListUnmanaged(usize) = .empty,

    fn deinit(self: *Cluster, allocator: std.mem.Allocator) void {
        self.indices.deinit(allocator);
    }
};

pub fn cleanupMentions(
    allocator: std.mem.Allocator,
    mentions: []const CleanupMention,
    cfg: CleanupConfig,
) !CleanupResult {
    var dropped = std.ArrayListUnmanaged(DroppedMention).empty;
    errdefer {
        for (dropped.items) |*mention| mention.deinit(allocator);
        dropped.deinit(allocator);
    }

    var kept_indices = std.ArrayListUnmanaged(usize).empty;
    defer kept_indices.deinit(allocator);

    for (mentions, 0..) |mention, idx| {
        if (mention.validity_score < cfg.min_validity_score) {
            try dropped.append(allocator, .{
                .text = try allocator.dupe(u8, mention.entity.text),
                .label = try allocator.dupe(u8, mention.entity.label),
                .start = mention.entity.start,
                .end = mention.entity.end,
                .detect_score = mention.entity.score,
                .validity_score = mention.validity_score,
            });
            continue;
        }
        try kept_indices.append(allocator, idx);
    }

    const clusters = try buildClusters(allocator, mentions, kept_indices.items, cfg);
    defer {
        for (clusters) |*cluster| cluster.deinit(allocator);
        allocator.free(clusters);
    }

    const resolved = try allocator.alloc(ResolvedEntity, clusters.len);
    var resolved_count: usize = 0;
    errdefer {
        for (resolved[0..resolved_count]) |*entity| entity.deinit(allocator);
        allocator.free(resolved);
    }

    for (clusters, 0..) |cluster, idx| {
        resolved[idx] = try buildResolvedEntity(allocator, mentions, cluster.indices.items, cfg, idx);
        resolved_count += 1;
    }

    return .{
        .dropped_mentions = try dropped.toOwnedSlice(allocator),
        .resolved_entities = resolved,
    };
}

fn buildClusters(
    allocator: std.mem.Allocator,
    mentions: []const CleanupMention,
    kept_indices: []const usize,
    cfg: CleanupConfig,
) ![]Cluster {
    var clusters = std.ArrayListUnmanaged(Cluster).empty;
    errdefer {
        for (clusters.items) |*cluster| cluster.deinit(allocator);
        clusters.deinit(allocator);
    }

    const assignments = try allocator.alloc(i32, mentions.len);
    defer allocator.free(assignments);
    @memset(assignments, -1);

    for (kept_indices) |mention_idx| {
        if (assignments[mention_idx] >= 0) continue;

        const cluster_idx = clusters.items.len;
        try clusters.append(allocator, .{});
        try clusters.items[cluster_idx].indices.append(allocator, mention_idx);
        assignments[mention_idx] = @intCast(cluster_idx);

        var cursor: usize = 0;
        while (cursor < clusters.items[cluster_idx].indices.items.len) : (cursor += 1) {
            const seed_idx = clusters.items[cluster_idx].indices.items[cursor];
            for (kept_indices) |other_idx| {
                if (assignments[other_idx] >= 0) continue;
                if (cfg.type_must_match and !std.mem.eql(u8, mentions[seed_idx].entity.label, mentions[other_idx].entity.label)) continue;

                const similarity = cosineSimilarity(mentions[seed_idx].embedding, mentions[other_idx].embedding);
                if (similarity >= cfg.dedup_similarity_threshold) {
                    try clusters.items[cluster_idx].indices.append(allocator, other_idx);
                    assignments[other_idx] = @intCast(cluster_idx);
                }
            }
        }
    }

    return try clusters.toOwnedSlice(allocator);
}

fn buildResolvedEntity(
    allocator: std.mem.Allocator,
    mentions: []const CleanupMention,
    indices: []const usize,
    cfg: CleanupConfig,
    cluster_idx: usize,
) !ResolvedEntity {
    var best_idx = indices[0];
    for (indices[1..]) |idx| {
        if (isBetterRepresentative(mentions[idx], mentions[best_idx])) {
            best_idx = idx;
        }
    }

    var mention_values = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (mention_values.items) |value| allocator.free(value);
        mention_values.deinit(allocator);
    }

    if (cfg.track_provenance) {
        for (indices) |idx| {
            try mention_values.append(allocator, try allocator.dupe(u8, mentions[idx].entity.text));
        }
    }

    return .{
        .id = try std.fmt.allocPrint(allocator, "cleanup-entity-{d}", .{cluster_idx}),
        .text = try allocator.dupe(u8, mentions[best_idx].entity.text),
        .label = try allocator.dupe(u8, mentions[best_idx].entity.label),
        .start = mentions[best_idx].entity.start,
        .end = mentions[best_idx].entity.end,
        .detect_score = mentions[best_idx].entity.score,
        .validity_score = mentions[best_idx].validity_score,
        .representative_score = mentions[best_idx].representative_score,
        .mentions = if (cfg.track_provenance) try mention_values.toOwnedSlice(allocator) else null,
    };
}

fn isBetterRepresentative(candidate: CleanupMention, current: CleanupMention) bool {
    if (candidate.representative_score != current.representative_score) {
        return candidate.representative_score > current.representative_score;
    }
    if (candidate.validity_score != current.validity_score) {
        return candidate.validity_score > current.validity_score;
    }
    return candidate.entity.score > current.entity.score;
}

fn cosineSimilarity(a: []const f32, b: []const f32) f32 {
    if (a.len == 0 or b.len == 0 or a.len != b.len) return 0;

    var dot: f32 = 0;
    var a_norm: f32 = 0;
    var b_norm: f32 = 0;
    for (a, b) |av, bv| {
        dot += av * bv;
        a_norm += av * av;
        b_norm += bv * bv;
    }

    if (a_norm <= 0 or b_norm <= 0) return 0;
    return dot / (@sqrt(a_norm) * @sqrt(b_norm));
}

pub fn buildFeatureVector(
    allocator: std.mem.Allocator,
    source_text: []const u8,
    entity: Entity,
    cfg: FeatureConfig,
) ![]f32 {
    const feature_dim = @max(cfg.feature_dim, 16);
    const features = try allocator.alloc(f32, feature_dim);
    @memset(features, 0);

    const mention_text = if (entity.end <= source_text.len and entity.start < entity.end)
        source_text[entity.start..entity.end]
    else
        entity.text;
    const left_start = entity.start -| @min(entity.start, cfg.context_window);
    const right_end = @min(source_text.len, entity.end + cfg.context_window);
    const left_context = source_text[left_start..@min(entity.start, source_text.len)];
    const right_context = if (entity.end <= source_text.len) source_text[entity.end..right_end] else "";

    // Reserved scalar features.
    features[0] = clampUnit(@as(f32, @floatFromInt(mention_text.len)) / 32.0);
    features[1] = entity.score;
    features[2] = if (containsDigit(mention_text)) 1.0 else 0.0;
    features[3] = uppercaseRatio(mention_text);
    features[4] = whitespaceRatio(mention_text);
    features[5] = punctuationRatio(mention_text);
    features[6] = clampUnit(@as(f32, @floatFromInt(left_context.len)) / @as(f32, @floatFromInt(@max(cfg.context_window, 1))));
    features[7] = clampUnit(@as(f32, @floatFromInt(right_context.len)) / @as(f32, @floatFromInt(@max(cfg.context_window, 1))));

    addHashedSignal(features, feature_dim, mention_text, 8, "mention");
    addHashedSignal(features, feature_dim, entity.label, 8, "label");
    addHashedSignal(features, feature_dim, left_context, 8, "left");
    addHashedSignal(features, feature_dim, right_context, 8, "right");
    addHashedNgrams(features, feature_dim, mention_text, 8, "tri");

    return features;
}

fn addHashedSignal(features: []f32, feature_dim: usize, text: []const u8, offset: usize, namespace: []const u8) void {
    if (offset >= feature_dim or text.len == 0) return;
    var lower_buf: [256]u8 = undefined;
    const lowered = lowercaseInto(text, &lower_buf);
    var iter = std.mem.tokenizeAny(u8, lowered, " \t\r\n,.;:!?()[]{}<>/\\\"'");
    while (iter.next()) |token| {
        if (token.len == 0) continue;
        const idx = hashToFeature(namespace, token, feature_dim, offset);
        features[idx] += 1.0;
    }
}

fn addHashedNgrams(features: []f32, feature_dim: usize, text: []const u8, offset: usize, namespace: []const u8) void {
    if (offset >= feature_dim or text.len == 0) return;
    var lower_buf: [256]u8 = undefined;
    const lowered = lowercaseInto(text, &lower_buf);
    if (lowered.len < 3) {
        const idx = hashToFeature(namespace, lowered, feature_dim, offset);
        features[idx] += 1.0;
        return;
    }
    var i: usize = 0;
    while (i + 3 <= lowered.len) : (i += 1) {
        const ngram = lowered[i .. i + 3];
        const idx = hashToFeature(namespace, ngram, feature_dim, offset);
        features[idx] += 1.0;
    }
}

fn hashToFeature(namespace: []const u8, text: []const u8, feature_dim: usize, offset: usize) usize {
    var hash = std.hash.Wyhash.init(0);
    hash.update(namespace);
    hash.update(text);
    const width = feature_dim - offset;
    if (width == 0) return 0;
    return offset + @as(usize, @intCast(hash.final() % width));
}

fn lowercaseInto(text: []const u8, buf: []u8) []const u8 {
    const len = @min(text.len, buf.len);
    for (text[0..len], 0..) |ch, idx| {
        buf[idx] = std.ascii.toLower(ch);
    }
    return buf[0..len];
}

fn containsDigit(text: []const u8) bool {
    for (text) |ch| {
        if (std.ascii.isDigit(ch)) return true;
    }
    return false;
}

fn uppercaseRatio(text: []const u8) f32 {
    var total: usize = 0;
    var upper: usize = 0;
    for (text) |ch| {
        if (!std.ascii.isAlphabetic(ch)) continue;
        total += 1;
        if (std.ascii.isUpper(ch)) upper += 1;
    }
    if (total == 0) return 0;
    return @as(f32, @floatFromInt(upper)) / @as(f32, @floatFromInt(total));
}

fn whitespaceRatio(text: []const u8) f32 {
    if (text.len == 0) return 0;
    var count: usize = 0;
    for (text) |ch| {
        if (std.ascii.isWhitespace(ch)) count += 1;
    }
    return @as(f32, @floatFromInt(count)) / @as(f32, @floatFromInt(text.len));
}

fn punctuationRatio(text: []const u8) f32 {
    if (text.len == 0) return 0;
    var count: usize = 0;
    for (text) |ch| {
        if (std.ascii.isPunctuation(ch)) count += 1;
    }
    return @as(f32, @floatFromInt(count)) / @as(f32, @floatFromInt(text.len));
}

fn clampUnit(value: f32) f32 {
    if (value < 0) return 0;
    if (value > 1) return 1;
    return value;
}

test "cleanup pipeline drops invalid mentions and clusters duplicates" {
    const allocator = std.testing.allocator;

    var mentions = [_]CleanupMention{
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "T1mKaye"),
                .label = try allocator.dupe(u8, "person"),
                .start = 0,
                .end = 8,
                .score = 0.72,
            },
            .validity_score = 0.10,
            .representative_score = 0.05,
            .embedding = try allocator.dupe(f32, &[_]f32{ 1.0, 0.0, 0.0 }),
        },
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "Tim Kaye"),
                .label = try allocator.dupe(u8, "person"),
                .start = 13,
                .end = 21,
                .score = 0.97,
            },
            .validity_score = 0.98,
            .representative_score = 0.96,
            .embedding = try allocator.dupe(f32, &[_]f32{ 0.9, 0.1, 0.0 }),
        },
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "TIM KAYE"),
                .label = try allocator.dupe(u8, "person"),
                .start = 30,
                .end = 38,
                .score = 0.88,
            },
            .validity_score = 0.92,
            .representative_score = 0.40,
            .embedding = try allocator.dupe(f32, &[_]f32{ 0.88, 0.12, 0.0 }),
        },
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "Apple"),
                .label = try allocator.dupe(u8, "organization"),
                .start = 42,
                .end = 47,
                .score = 0.95,
            },
            .validity_score = 0.97,
            .representative_score = 0.91,
            .embedding = try allocator.dupe(f32, &[_]f32{ 0.0, 1.0, 0.0 }),
        },
    };
    defer {
        for (&mentions) |*mention| mention.deinit(allocator);
    }

    var result = try cleanupMentions(allocator, &mentions, .{
        .min_validity_score = 0.5,
        .dedup_similarity_threshold = 0.98,
    });
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), result.dropped_mentions.len);
    try std.testing.expectEqualStrings("T1mKaye", result.dropped_mentions[0].text);

    try std.testing.expectEqual(@as(usize, 2), result.resolved_entities.len);
    try std.testing.expectEqualStrings("Tim Kaye", result.resolved_entities[0].text);
    try std.testing.expectEqualStrings("person", result.resolved_entities[0].label);
    try std.testing.expectEqual(@as(usize, 2), result.resolved_entities[0].mentions.?.len);
    try std.testing.expectEqualStrings("Apple", result.resolved_entities[1].text);
}

test "representative selection prefers learned score over detect score" {
    const allocator = std.testing.allocator;

    var mentions = [_]CleanupMention{
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "TimKaye"),
                .label = try allocator.dupe(u8, "person"),
                .start = 0,
                .end = 8,
                .score = 0.99,
            },
            .validity_score = 0.95,
            .representative_score = 0.20,
            .embedding = try allocator.dupe(f32, &[_]f32{ 1.0, 0.0 }),
        },
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "Tim Kaye"),
                .label = try allocator.dupe(u8, "person"),
                .start = 10,
                .end = 18,
                .score = 0.80,
            },
            .validity_score = 0.96,
            .representative_score = 0.98,
            .embedding = try allocator.dupe(f32, &[_]f32{ 0.999, 0.001 }),
        },
    };
    defer {
        for (&mentions) |*mention| mention.deinit(allocator);
    }

    var result = try cleanupMentions(allocator, &mentions, .{
        .min_validity_score = 0.5,
        .dedup_similarity_threshold = 0.99,
    });
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), result.resolved_entities.len);
    try std.testing.expectEqualStrings("Tim Kaye", result.resolved_entities[0].text);
}

test "cleanup pipeline clusters transitive duplicate chains" {
    const allocator = std.testing.allocator;

    var mentions = [_]CleanupMention{
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "TimKaye"),
                .label = try allocator.dupe(u8, "person"),
                .start = 0,
                .end = 8,
                .score = 0.82,
            },
            .validity_score = 0.95,
            .representative_score = 0.20,
            .embedding = try allocator.dupe(f32, &[_]f32{ 1.0, 0.0 }),
        },
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "Tim Kaye"),
                .label = try allocator.dupe(u8, "person"),
                .start = 10,
                .end = 18,
                .score = 0.90,
            },
            .validity_score = 0.97,
            .representative_score = 0.99,
            .embedding = try allocator.dupe(f32, &[_]f32{ 0.8, 0.6 }),
        },
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "TIM KAYE"),
                .label = try allocator.dupe(u8, "person"),
                .start = 20,
                .end = 28,
                .score = 0.84,
            },
            .validity_score = 0.96,
            .representative_score = 0.50,
            .embedding = try allocator.dupe(f32, &[_]f32{ 0.28, 0.96 }),
        },
    };
    defer {
        for (&mentions) |*mention| mention.deinit(allocator);
    }

    var result = try cleanupMentions(allocator, &mentions, .{
        .min_validity_score = 0.5,
        .dedup_similarity_threshold = 0.75,
    });
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), result.resolved_entities.len);
    try std.testing.expectEqual(@as(usize, 3), result.resolved_entities[0].mentions.?.len);
    try std.testing.expectEqualStrings("Tim Kaye", result.resolved_entities[0].text);
}

test "cleanup pipeline does not bridge duplicate chains across labels" {
    const allocator = std.testing.allocator;

    var mentions = [_]CleanupMention{
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "TimKaye"),
                .label = try allocator.dupe(u8, "person"),
                .start = 0,
                .end = 8,
                .score = 0.82,
            },
            .validity_score = 0.95,
            .representative_score = 0.20,
            .embedding = try allocator.dupe(f32, &[_]f32{ 1.0, 0.0 }),
        },
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "Apple"),
                .label = try allocator.dupe(u8, "organization"),
                .start = 10,
                .end = 15,
                .score = 0.90,
            },
            .validity_score = 0.97,
            .representative_score = 0.99,
            .embedding = try allocator.dupe(f32, &[_]f32{ 0.8, 0.6 }),
        },
        .{
            .entity = .{
                .text = try allocator.dupe(u8, "TIM KAYE"),
                .label = try allocator.dupe(u8, "person"),
                .start = 20,
                .end = 28,
                .score = 0.84,
            },
            .validity_score = 0.96,
            .representative_score = 0.50,
            .embedding = try allocator.dupe(f32, &[_]f32{ 0.28, 0.96 }),
        },
    };
    defer {
        for (&mentions) |*mention| mention.deinit(allocator);
    }

    var result = try cleanupMentions(allocator, &mentions, .{
        .min_validity_score = 0.5,
        .dedup_similarity_threshold = 0.75,
        .type_must_match = true,
    });
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), result.resolved_entities.len);
}

test "feature vector captures mention and context signal" {
    const allocator = std.testing.allocator;
    const source = "T1mKaye met Tim Kaye at Apple.";
    const entity = Entity{
        .text = "Tim Kaye",
        .label = "person",
        .start = 13,
        .end = 21,
        .score = 0.9,
    };
    const features = try buildFeatureVector(allocator, source, entity, .{
        .feature_dim = 32,
        .context_window = 10,
    });
    defer allocator.free(features);

    try std.testing.expectEqual(@as(usize, 32), features.len);
    try std.testing.expect(features[0] > 0);
    try std.testing.expect(features[1] == 0.9);
}
