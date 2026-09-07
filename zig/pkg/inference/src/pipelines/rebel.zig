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

// REBEL relation extraction pipeline built on top of the shared encoder-decoder
// generation loop.
//
// Matches the Go REBEL recognizer at a high level:
//   - generate triplets from input text with a seq2seq model
//   - parse generated triplets from special-token or plain-text output
//   - convert triplets to recognize-style entities and relations

const std = @import("std");
const c_file = @import("../util/c_file.zig");
const tokenizer_mod = @import("inference_tokenizer");
const enc_dec_mod = @import("encoder_decoder.zig");
const ner_mod = @import("ner.zig");
const gliner_mod = @import("gliner.zig");

pub const Entity = ner_mod.Entity;
pub const Relation = gliner_mod.Relation;

/// Release REBEL entity results. Entity text and batch containers are owned by
/// the caller, while the generic entity label is borrowed static storage.
pub fn deinitEntityBatches(allocator: std.mem.Allocator, all_entities: []const []Entity) void {
    for (all_entities) |entities| {
        for (entities) |entity| allocator.free(entity.text);
        allocator.free(entities);
    }
    allocator.free(all_entities);
}

/// REBEL emits a single generic entity type. It can constrain generated
/// relation names, but typed entity or endpoint schemas require a model that
/// performs entity typing.
pub fn validateSchemaSupport(
    entity_labels: ?[]const []const u8,
    relation_labels: ?[]const []const u8,
) !void {
    if (entity_labels) |labels| {
        for (labels) |label| {
            if (!std.ascii.eqlIgnoreCase(label, "ENTITY"))
                return error.UnsupportedRebelEntityLabels;
        }
    }

    if (relation_labels) |labels| {
        for (labels) |constraint| {
            const first_separator = std.mem.indexOf(u8, constraint, "::") orelse continue;
            if (!std.ascii.eqlIgnoreCase(constraint[0..first_separator], "ENTITY"))
                return error.UnsupportedRebelRelationEndpoints;

            const remainder = constraint[first_separator + 2 ..];
            const second_separator = std.mem.indexOf(u8, remainder, "::") orelse continue;
            if (!std.ascii.eqlIgnoreCase(remainder[second_separator + 2 ..], "ENTITY"))
                return error.UnsupportedRebelRelationEndpoints;
        }
    }
}

pub const RebelConfig = struct {
    allocator: std.mem.Allocator,
    max_length: usize,
    num_beams: usize,
    triplet_token: []const u8,
    subject_token: []const u8,
    object_token: []const u8,

    pub fn initDefault(allocator: std.mem.Allocator) !RebelConfig {
        return .{
            .allocator = allocator,
            .max_length = 256,
            .num_beams = 3,
            .triplet_token = try allocator.dupe(u8, "<triplet>"),
            .subject_token = try allocator.dupe(u8, "<subj>"),
            .object_token = try allocator.dupe(u8, "<obj>"),
        };
    }

    pub fn deinit(self: *RebelConfig) void {
        self.allocator.free(self.triplet_token);
        self.allocator.free(self.subject_token);
        self.allocator.free(self.object_token);
    }
};

const Triplet = struct {
    subject: []const u8,
    object: []const u8,
    relation: []const u8,
    score: f32,

    fn deinit(self: *const Triplet, allocator: std.mem.Allocator) void {
        allocator.free(self.subject);
        allocator.free(self.object);
        allocator.free(self.relation);
    }
};

pub const RebelPipeline = struct {
    allocator: std.mem.Allocator,
    enc_dec: enc_dec_mod.EncoderDecoderPipeline,
    tokenizer: tokenizer_mod.Tokenizer,
    config: RebelConfig,

    /// Returns the exact (post-truncation) encoder length used for one input.
    pub fn inputTokenCount(self: *RebelPipeline, text: []const u8) !usize {
        const token_ids = try self.tokenizer.encode(self.allocator, text);
        defer self.allocator.free(token_ids);
        return @min(token_ids.len, self.config.max_length);
    }

    pub fn recognizeBatch(self: *RebelPipeline, texts: []const []const u8) ![][]Entity {
        const extracted = try self.extractRelationsBatch(texts, null, null);
        errdefer deinitEntityBatches(self.allocator, extracted.entities);

        for (extracted.relations) |relations| {
            for (relations) |*relation| relation.deinit(self.allocator);
            self.allocator.free(relations);
        }
        self.allocator.free(extracted.relations);

        return extracted.entities;
    }

    pub fn extractRelationsBatch(
        self: *RebelPipeline,
        texts: []const []const u8,
        entity_labels: ?[]const []const u8,
        relation_labels: ?[]const []const u8,
    ) !struct { entities: [][]Entity, relations: [][]Relation } {
        try validateSchemaSupport(entity_labels, relation_labels);

        const alloc = self.allocator;
        const all_entities = try alloc.alloc([]Entity, texts.len);
        var initialized_entities: usize = 0;
        errdefer {
            for (all_entities[0..initialized_entities]) |entities| {
                for (entities) |entity| alloc.free(entity.text);
                alloc.free(entities);
            }
            alloc.free(all_entities);
        }

        const all_relations = try alloc.alloc([]Relation, texts.len);
        var initialized_relations: usize = 0;
        errdefer {
            for (all_relations[0..initialized_relations]) |relations| {
                for (relations) |*relation| relation.deinit(alloc);
                alloc.free(relations);
            }
            alloc.free(all_relations);
        }

        for (texts, 0..) |text, i| {
            const generated_text = try self.generate(text);
            defer alloc.free(generated_text);

            const triplets = try parseTriplets(alloc, generated_text, self.config, 1.0);
            defer {
                for (triplets) |triplet| triplet.deinit(alloc);
                alloc.free(triplets);
            }

            const extracted = try tripletsToRecognizeOutput(alloc, text, triplets, relation_labels);
            all_entities[i] = extracted.entities;
            initialized_entities += 1;
            all_relations[i] = extracted.relations;
            initialized_relations += 1;
        }

        return .{
            .entities = all_entities,
            .relations = all_relations,
        };
    }

    fn generate(self: *RebelPipeline, text: []const u8) ![]const u8 {
        const alloc = self.allocator;

        const token_ids_i32 = try self.tokenizer.encode(alloc, text);
        defer alloc.free(token_ids_i32);

        const seq_len = @min(token_ids_i32.len, self.config.max_length);
        const input_ids = try alloc.alloc(i64, seq_len);
        defer alloc.free(input_ids);
        for (0..seq_len) |i| {
            input_ids[i] = @intCast(token_ids_i32[i]);
        }

        const encoder_outputs = try self.enc_dec.encode(alloc, input_ids, seq_len);
        defer {
            for (encoder_outputs) |*output| output.deinit();
            alloc.free(encoder_outputs);
        }

        const enc_mask = try alloc.alloc(i64, seq_len);
        defer alloc.free(enc_mask);
        @memset(enc_mask, 1);

        var gen_result = try self.enc_dec.greedyDecode(alloc, encoder_outputs, enc_mask, seq_len);
        defer gen_result.deinit();

        return try self.tokenizer.decode(alloc, gen_result.text_ids);
    }

    pub fn deinit(self: *RebelPipeline) void {
        self.enc_dec.deinit();
        self.config.deinit();
    }
};

/// REBEL generates an unconstrained relation vocabulary, so request schemas
/// are enforced as a post-generation filter. Endpoint-qualified labels can be
/// honored only when they name REBEL's explicit generic entity type.
fn tripletMatchesRequestedRelation(triplet: Triplet, requested: []const []const u8) bool {
    for (requested) |constraint| {
        const first_separator = std.mem.indexOf(u8, constraint, "::") orelse {
            if (std.ascii.eqlIgnoreCase(constraint, triplet.relation)) return true;
            continue;
        };
        const source_label = constraint[0..first_separator];
        const remainder = constraint[first_separator + 2 ..];
        const second_separator = std.mem.indexOf(u8, remainder, "::");
        const relation_label = if (second_separator) |index| remainder[0..index] else remainder;
        if (!std.ascii.eqlIgnoreCase(source_label, "ENTITY") or
            !std.ascii.eqlIgnoreCase(relation_label, triplet.relation)) continue;
        if (second_separator) |index| {
            const target_label = remainder[index + 2 ..];
            if (!std.ascii.eqlIgnoreCase(target_label, "ENTITY")) continue;
        }
        return true;
    }
    return false;
}

pub fn loadConfig(allocator: std.mem.Allocator, model_dir: []const u8) !RebelConfig {
    var config = try RebelConfig.initDefault(allocator);
    errdefer config.deinit();

    if (!c_file.fileExistsInDir(allocator, model_dir, "rebel_config.json")) return config;

    const data = try c_file.readFileFromDir(allocator, model_dir, "rebel_config.json");
    defer allocator.free(data);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, data, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    if (obj.get("max_length")) |v| {
        switch (v) {
            .integer => |i| {
                if (i > 0) config.max_length = @intCast(i);
            },
            else => {},
        }
    }
    if (obj.get("num_beams")) |v| {
        switch (v) {
            .integer => |i| {
                if (i > 0) config.num_beams = @intCast(i);
            },
            else => {},
        }
    }
    if (obj.get("triplet_token")) |v| {
        if (v == .string and v.string.len > 0) {
            config.allocator.free(config.triplet_token);
            config.triplet_token = try allocator.dupe(u8, v.string);
        }
    }
    if (obj.get("subject_token")) |v| {
        if (v == .string and v.string.len > 0) {
            config.allocator.free(config.subject_token);
            config.subject_token = try allocator.dupe(u8, v.string);
        }
    }
    if (obj.get("object_token")) |v| {
        if (v == .string and v.string.len > 0) {
            config.allocator.free(config.object_token);
            config.object_token = try allocator.dupe(u8, v.string);
        }
    }

    return config;
}

pub fn isRebelModel(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    if (c_file.fileExistsInDir(allocator, model_dir, "rebel_config.json")) return true;

    const basename = std.fs.path.basename(model_dir);
    if (!containsAsciiIgnoreCase(basename, "rebel")) return false;
    return enc_dec_mod.isEncoderDecoderModel(model_dir);
}

fn parseTriplets(
    allocator: std.mem.Allocator,
    generated_text: []const u8,
    config: RebelConfig,
    score: f32,
) ![]Triplet {
    const cleaned = try sanitizeGeneratedText(allocator, generated_text);
    defer allocator.free(cleaned);

    var triplets = std.ArrayListUnmanaged(Triplet).empty;
    errdefer {
        for (triplets.items) |triplet| triplet.deinit(allocator);
        triplets.deinit(allocator);
    }

    const has_special_tokens =
        std.mem.indexOf(u8, cleaned, config.triplet_token) != null or
        std.mem.indexOf(u8, cleaned, config.subject_token) != null or
        std.mem.indexOf(u8, cleaned, config.object_token) != null;

    if (has_special_tokens) {
        var parts = std.mem.splitSequence(u8, cleaned, config.triplet_token);
        while (parts.next()) |raw_part| {
            const part = std.mem.trim(u8, raw_part, " \t\r\n");
            if (part.len == 0) continue;
            const maybe_triplet = try parseSpecialTokenTriplet(allocator, part, config, score);
            if (maybe_triplet) |triplet| {
                errdefer triplet.deinit(allocator);
                try triplets.append(allocator, triplet);
            }
        }
    } else {
        try parseTripletsWithoutTokens(allocator, cleaned, score, &triplets);
    }

    return try triplets.toOwnedSlice(allocator);
}

fn sanitizeGeneratedText(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    var i: usize = 0;
    while (i < input.len) {
        if (std.mem.startsWith(u8, input[i..], "<s>")) {
            i += 3;
            continue;
        }
        if (std.mem.startsWith(u8, input[i..], "</s>")) {
            i += 4;
            continue;
        }
        if (std.mem.startsWith(u8, input[i..], "<pad>")) {
            i += 5;
            continue;
        }
        try buf.append(allocator, input[i]);
        i += 1;
    }

    const trimmed = std.mem.trim(u8, buf.items, " \t\r\n");
    return try allocator.dupe(u8, trimmed);
}

fn parseSpecialTokenTriplet(
    allocator: std.mem.Allocator,
    part: []const u8,
    config: RebelConfig,
    score: f32,
) !?Triplet {
    const subj_idx = std.mem.indexOf(u8, part, config.subject_token) orelse return null;
    const obj_start = subj_idx + config.subject_token.len;
    const obj_rel = part[obj_start..];
    const obj_idx_rel = std.mem.indexOf(u8, obj_rel, config.object_token) orelse return null;
    const obj_idx = obj_start + obj_idx_rel;

    const subject = std.mem.trim(u8, part[0..subj_idx], " \t\r\n");
    const object = std.mem.trim(u8, part[obj_start..obj_idx], " \t\r\n");
    const relation = std.mem.trim(u8, part[obj_idx + config.object_token.len ..], " \t\r\n");

    if (subject.len == 0 or object.len == 0 or relation.len == 0) return null;

    return .{
        .subject = try allocator.dupe(u8, subject),
        .object = try allocator.dupe(u8, object),
        .relation = try allocator.dupe(u8, relation),
        .score = score,
    };
}

fn parseTripletsWithoutTokens(
    allocator: std.mem.Allocator,
    text: []const u8,
    score: f32,
    triplets: *std.ArrayListUnmanaged(Triplet),
) !void {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(allocator);

    var iter = std.mem.splitSequence(u8, text, "  ");
    while (iter.next()) |raw_part| {
        const part = std.mem.trim(u8, raw_part, " \t\r\n");
        if (part.len > 0) try parts.append(allocator, part);
    }

    var i: usize = 0;
    while (i + 2 < parts.items.len) : (i += 3) {
        const triplet = Triplet{
            .subject = try allocator.dupe(u8, parts.items[i]),
            .object = try allocator.dupe(u8, parts.items[i + 1]),
            .relation = try allocator.dupe(u8, parts.items[i + 2]),
            .score = score,
        };
        errdefer triplet.deinit(allocator);
        try triplets.append(allocator, triplet);
    }
}

fn tripletsToRecognizeOutput(
    allocator: std.mem.Allocator,
    text: []const u8,
    triplets: []const Triplet,
    requested_relations: ?[]const []const u8,
) !struct { entities: []Entity, relations: []Relation } {
    var entities = std.ArrayListUnmanaged(Entity).empty;
    errdefer {
        for (entities.items) |entity| allocator.free(entity.text);
        entities.deinit(allocator);
    }

    var relations = std.ArrayListUnmanaged(Relation).empty;
    errdefer {
        for (relations.items) |*relation| relation.deinit(allocator);
        relations.deinit(allocator);
    }

    for (triplets) |triplet| {
        if (requested_relations) |requested| {
            if (requested.len > 0 and !tripletMatchesRequestedRelation(triplet, requested)) continue;
        }
        const subject_idx = try getOrCreateEntity(allocator, text, triplet.subject, triplet.score, &entities);
        const object_idx = try getOrCreateEntity(allocator, text, triplet.object, triplet.score, &entities);

        var relation = try cloneRelation(
            allocator,
            entities.items[subject_idx],
            entities.items[object_idx],
            triplet.relation,
            triplet.score,
        );
        relations.append(allocator, relation) catch |err| {
            relation.deinit(allocator);
            return err;
        };
    }

    const owned_entities = try entities.toOwnedSlice(allocator);
    errdefer {
        for (owned_entities) |entity| allocator.free(entity.text);
        allocator.free(owned_entities);
    }
    const owned_relations = try relations.toOwnedSlice(allocator);
    return .{
        .entities = owned_entities,
        .relations = owned_relations,
    };
}

fn getOrCreateEntity(
    allocator: std.mem.Allocator,
    full_text: []const u8,
    entity_text: []const u8,
    score: f32,
    entities: *std.ArrayListUnmanaged(Entity),
) !usize {
    for (entities.items, 0..) |entity, i| {
        if (std.mem.eql(u8, entity.text, entity_text)) return i;
    }

    const span = findSpan(full_text, entity_text);
    const owned_text = try allocator.dupe(u8, entity_text);
    entities.append(allocator, .{
        .text = owned_text,
        .label = "ENTITY",
        .start = span.start,
        .end = span.end,
        .score = score,
    }) catch |err| {
        allocator.free(owned_text);
        return err;
    };
    return entities.items.len - 1;
}

fn cloneRelation(
    allocator: std.mem.Allocator,
    head_entity: Entity,
    tail_entity: Entity,
    label: []const u8,
    score: f32,
) !Relation {
    const head = try cloneEntity(allocator, head_entity);
    errdefer allocator.free(head.text);
    const tail = try cloneEntity(allocator, tail_entity);
    errdefer allocator.free(tail.text);
    const owned_label = try allocator.dupe(u8, label);
    return .{
        .head = head,
        .tail = tail,
        .label = owned_label,
        .score = score,
    };
}

fn cloneEntity(allocator: std.mem.Allocator, entity: Entity) !Entity {
    return .{
        .text = try allocator.dupe(u8, entity.text),
        .label = entity.label,
        .start = entity.start,
        .end = entity.end,
        .score = entity.score,
    };
}

fn findSpan(text: []const u8, needle: []const u8) struct { start: usize, end: usize } {
    if (needle.len == 0 or needle.len > text.len) {
        return .{ .start = 0, .end = 0 };
    }

    const last_start = text.len - needle.len;
    for (0..last_start + 1) |i| {
        if (std.ascii.eqlIgnoreCase(text[i .. i + needle.len], needle)) {
            return .{
                .start = i,
                .end = i + needle.len,
            };
        }
    }

    return .{ .start = 0, .end = 0 };
}

fn containsAsciiIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (needle.len > haystack.len) return false;

    const last_start = haystack.len - needle.len;
    for (0..last_start + 1) |i| {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

test "parseTriplets handles special-token REBEL output" {
    const allocator = std.testing.allocator;
    var config = try RebelConfig.initDefault(allocator);
    defer config.deinit();

    const triplets = try parseTriplets(
        allocator,
        "<s><triplet> John Smith <subj> Google <obj> works for <triplet> Google <subj> Mountain View <obj> located in </s>",
        config,
        0.9,
    );
    defer {
        for (triplets) |triplet| triplet.deinit(allocator);
        allocator.free(triplets);
    }

    try std.testing.expectEqual(@as(usize, 2), triplets.len);
    try std.testing.expectEqualStrings("John Smith", triplets[0].subject);
    try std.testing.expectEqualStrings("Google", triplets[0].object);
    try std.testing.expectEqualStrings("works for", triplets[0].relation);
    try std.testing.expectEqualStrings("located in", triplets[1].relation);
}

test "parseTriplets handles plain-text fallback output" {
    const allocator = std.testing.allocator;
    var config = try RebelConfig.initDefault(allocator);
    defer config.deinit();

    const triplets = try parseTriplets(
        allocator,
        "John Smith  Google  works for  Google  Mountain View  located in",
        config,
        1.0,
    );
    defer {
        for (triplets) |triplet| triplet.deinit(allocator);
        allocator.free(triplets);
    }

    try std.testing.expectEqual(@as(usize, 2), triplets.len);
    try std.testing.expectEqualStrings("John Smith", triplets[0].subject);
    try std.testing.expectEqualStrings("Google", triplets[1].subject);
}

test "REBEL relation schemas filter generated triplets and enforce endpoint constraints" {
    const triplets = [_]Triplet{
        .{ .subject = "Grace Hopper", .object = "Navy", .relation = "served in", .score = 0.9 },
        .{ .subject = "Grace Hopper", .object = "COBOL", .relation = "helped create", .score = 0.8 },
    };

    try std.testing.expect(tripletMatchesRequestedRelation(triplets[0], &.{"served in"}));
    try std.testing.expect(!tripletMatchesRequestedRelation(triplets[1], &.{"served in"}));
    try std.testing.expect(tripletMatchesRequestedRelation(
        triplets[0],
        &.{"ENTITY::served in::ENTITY"},
    ));
    try std.testing.expect(!tripletMatchesRequestedRelation(
        triplets[0],
        &.{"person::served in::organization"},
    ));
    try validateSchemaSupport(&.{"ENTITY"}, &.{ "served in", "ENTITY::served in::ENTITY" });
    try std.testing.expectError(
        error.UnsupportedRebelEntityLabels,
        validateSchemaSupport(&.{"person"}, &.{"served in"}),
    );
    try std.testing.expectError(
        error.UnsupportedRebelRelationEndpoints,
        validateSchemaSupport(null, &.{"person::served in::organization"}),
    );

    const extracted = try tripletsToRecognizeOutput(
        std.testing.allocator,
        "Grace Hopper served in the Navy and helped create COBOL.",
        &triplets,
        &.{"served in"},
    );
    defer {
        for (extracted.entities) |entity| std.testing.allocator.free(entity.text);
        std.testing.allocator.free(extracted.entities);
        for (extracted.relations) |*relation| relation.deinit(std.testing.allocator);
        std.testing.allocator.free(extracted.relations);
    }
    try std.testing.expectEqual(@as(usize, 2), extracted.entities.len);
    try std.testing.expectEqual(@as(usize, 1), extracted.relations.len);
    try std.testing.expectEqualStrings("served in", extracted.relations[0].label);
}

test "REBEL entity cleanup preserves borrowed generic labels" {
    const allocator = std.testing.allocator;
    const borrowed_label = "ENTITY";
    const batches = try allocator.alloc([]Entity, 1);
    batches[0] = try allocator.alloc(Entity, 1);
    batches[0][0] = .{
        .text = try allocator.dupe(u8, "Grace Hopper"),
        .label = borrowed_label,
        .start = 0,
        .end = 12,
        .score = 0.9,
    };

    deinitEntityBatches(allocator, batches);
    try std.testing.expectEqualStrings("ENTITY", borrowed_label);
}

test "tripletsToRecognizeOutput deduplicates entities and preserves spans" {
    const allocator = std.testing.allocator;
    const triplets = [_]Triplet{
        .{
            .subject = try allocator.dupe(u8, "John Smith"),
            .object = try allocator.dupe(u8, "Google"),
            .relation = try allocator.dupe(u8, "works for"),
            .score = 0.8,
        },
        .{
            .subject = try allocator.dupe(u8, "John Smith"),
            .object = try allocator.dupe(u8, "Mountain View"),
            .relation = try allocator.dupe(u8, "lives in"),
            .score = 0.7,
        },
    };
    defer {
        for (triplets) |triplet| triplet.deinit(allocator);
    }

    const extracted = try tripletsToRecognizeOutput(
        allocator,
        "John Smith works at Google in Mountain View.",
        &triplets,
        null,
    );
    defer {
        for (extracted.entities) |entity| allocator.free(entity.text);
        allocator.free(extracted.entities);
        for (extracted.relations) |*relation| relation.deinit(allocator);
        allocator.free(extracted.relations);
    }

    try std.testing.expectEqual(@as(usize, 3), extracted.entities.len);
    try std.testing.expectEqual(@as(usize, 2), extracted.relations.len);
    try std.testing.expectEqualStrings("ENTITY", extracted.entities[0].label);
    try std.testing.expect(extracted.entities[0].end > extracted.entities[0].start);
}

test "tripletsToRecognizeOutput releases every partial allocation" {
    const Runner = struct {
        fn run(allocator: std.mem.Allocator) !void {
            const triplets = [_]Triplet{
                .{
                    .subject = "Grace Hopper",
                    .object = "Navy",
                    .relation = "served in",
                    .score = 0.9,
                },
                .{
                    .subject = "Grace Hopper",
                    .object = "COBOL",
                    .relation = "created",
                    .score = 0.8,
                },
            };
            const extracted = try tripletsToRecognizeOutput(
                allocator,
                "Grace Hopper served in the Navy and helped create COBOL.",
                &triplets,
                null,
            );
            defer {
                for (extracted.entities) |entity| allocator.free(entity.text);
                allocator.free(extracted.entities);
                for (extracted.relations) |*relation| relation.deinit(allocator);
                allocator.free(extracted.relations);
            }
            try std.testing.expectEqual(@as(usize, 3), extracted.entities.len);
            try std.testing.expectEqual(@as(usize, 2), extracted.relations.len);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}
