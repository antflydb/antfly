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

// Named Entity Recognition pipeline: token classification with BIO aggregation.
//
// Runs text through a BERT encoder with a token classification head.
// Model outputs [batch, seq_len, num_labels] logits. For each token,
// argmax gives the predicted label (BIO-tagged). Consecutive B-/I- tokens
// with the same entity type are aggregated into entity spans.
//
// Also supports GLiNER-style zero-shot NER where entity labels are provided
// at inference time (not implemented yet — requires a different model arch).

const std = @import("std");
const backends = @import("../backends/backends.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Tensor = backends.Tensor;
const runtime = @import("../runtime/root.zig");

pub const Entity = struct {
    text: []const u8,
    label: []const u8,
    start: usize, // character offset in original text
    end: usize, // character offset (exclusive)
    score: f32,
};

pub const NerConfig = struct {
    max_length: usize = 512,
    /// Label names indexed by model output class ID.
    /// Typically BIO format: ["O", "B-PER", "I-PER", "B-ORG", "I-ORG", ...]
    id2label: ?[]const []const u8 = null,
    /// Minimum confidence threshold for entity detection
    threshold: f32 = 0.0,
    distributed: runtime.distributed.Config = .{},
};

pub const NerPipeline = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    tok: Tokenizer,
    config: NerConfig,

    pub fn usesDistributedMlx(self: *const NerPipeline) bool {
        return self.config.distributed.enabled and
            self.config.distributed.world_size > 1 and
            self.session.backend().usesGpuHostedSession();
    }

    pub fn usesTensorParallelMlx(self: *const NerPipeline) bool {
        return self.usesDistributedMlx() and self.config.distributed.mode == .tensor_parallel;
    }

    pub fn init(
        allocator: std.mem.Allocator,
        session: backends.Session,
        tok: Tokenizer,
        config: NerConfig,
    ) NerPipeline {
        return .{
            .allocator = allocator,
            .session = session,
            .tok = tok,
            .config = config,
        };
    }

    /// Recognize entities in a single text. Caller owns the returned slice.
    pub fn recognize(self: *NerPipeline, text: []const u8) ![]Entity {
        const alloc = self.allocator;
        const max_len = self.config.max_length;

        // Tokenize
        var enc = try self.tok.encodeForModel(alloc, text, max_len);
        defer enc.deinit();

        // Convert i32 -> i64
        const ids_i64 = try alloc.alloc(i64, max_len);
        defer alloc.free(ids_i64);
        const mask_i64 = try alloc.alloc(i64, max_len);
        defer alloc.free(mask_i64);
        for (0..max_len) |j| {
            ids_i64[j] = @intCast(enc.ids[j]);
            mask_i64[j] = @intCast(enc.attention_mask[j]);
        }

        // Build input tensors (batch=1)
        const shape = [_]i64{ 1, @intCast(max_len) };
        var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape, ids_i64);
        defer input_ids_tensor.deinit();
        var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape, mask_i64);
        defer attention_mask_tensor.deinit();

        var token_type_tensor: ?Tensor = null;
        defer if (token_type_tensor) |*t| t.deinit();

        const input_info = self.session.inputInfo();
        var needs_token_type = false;
        for (input_info) |info| {
            if (std.mem.eql(u8, info.name, "token_type_ids")) {
                needs_token_type = true;
                break;
            }
        }

        const inputs = if (needs_token_type) blk: {
            const zeros = try alloc.alloc(i64, max_len);
            defer alloc.free(zeros);
            @memset(zeros, 0);
            token_type_tensor = try Tensor.initInt64(alloc, "token_type_ids", &shape, zeros);
            break :blk &[_]Tensor{ input_ids_tensor, attention_mask_tensor, token_type_tensor.? };
        } else &[_]Tensor{ input_ids_tensor, attention_mask_tensor };

        // Run inference
        var outputs = try self.session.run(inputs, alloc);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        if (outputs.len == 0) return error.NoOutputTensors;

        // Output shape: [1, seq_len, num_labels]
        const output = &outputs[0];
        const output_shape = output.shape;
        if (output_shape.len != 3) return error.UnexpectedOutputShape;

        const seq_len: usize = @intCast(output_shape[1]);
        const num_labels: usize = @intCast(output_shape[2]);
        const data = output.asFloat32();

        // Decode token predictions into entities
        return self.aggregateEntities(text, enc.offsets, enc.attention_mask, data, seq_len, num_labels);
    }

    /// Recognize entities in a batch of texts.
    pub fn recognizeBatch(self: *NerPipeline, texts: []const []const u8) ![][]Entity {
        const alloc = self.allocator;
        const results = try alloc.alloc([]Entity, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |r| {
                for (r) |e| {
                    alloc.free(e.text);
                }
                alloc.free(r);
            }
            alloc.free(results);
        }

        for (texts, 0..) |text, i| {
            results[i] = try self.recognize(text);
            initialized += 1;
        }

        return results;
    }

    /// Aggregate per-token BIO predictions into entity spans.
    fn aggregateEntities(
        self: *NerPipeline,
        text: []const u8,
        offsets: ?[]const [2]u32,
        attention_mask: []const i32,
        logits: []const f32,
        seq_len: usize,
        num_labels: usize,
    ) ![]Entity {
        const alloc = self.allocator;
        const id2label = self.config.id2label orelse return error.NoLabelMapping;

        var entities = std.ArrayListUnmanaged(Entity).empty;
        errdefer {
            for (entities.items) |e| alloc.free(e.text);
            entities.deinit(alloc);
        }

        // Track current entity being built
        var cur_label: ?[]const u8 = null;
        var cur_start: usize = 0;
        var cur_end: usize = 0;
        var cur_score_sum: f32 = 0;
        var cur_token_count: usize = 0;

        // Skip [CLS] (index 0) and process tokens
        var i: usize = 1;
        while (i < seq_len) : (i += 1) {
            if (attention_mask[i] == 0) break; // padding
            // Skip [SEP] token (last real token)
            if (i + 1 < seq_len and attention_mask[i + 1] == 0) break;

            // Argmax over labels
            const offset = i * num_labels;
            var best_label: usize = 0;
            var best_score: f32 = logits[offset];
            for (1..num_labels) |l| {
                if (logits[offset + l] > best_score) {
                    best_score = logits[offset + l];
                    best_label = l;
                }
            }

            // Convert to probability via softmax (approximate: just use max logit)
            var exp_sum: f32 = 0;
            for (0..num_labels) |l| {
                exp_sum += @exp(logits[offset + l] - best_score);
            }
            const prob = 1.0 / exp_sum;

            if (best_label >= id2label.len) continue;
            const label_str = id2label[best_label];

            // Parse BIO tag
            const tag = parseBioTag(label_str);

            // Get character offsets for this token
            const char_start: usize = if (offsets) |off| off[i][0] else 0;
            const char_end: usize = if (offsets) |off| off[i][1] else 0;

            switch (tag.kind) {
                .O => {
                    // Outside — flush current entity
                    if (cur_label != null) {
                        try self.flushEntity(&entities, text, cur_label.?, cur_start, cur_end, cur_score_sum, cur_token_count);
                        cur_label = null;
                    }
                },
                .B, .I => {
                    // Match Go aggregation behavior: merge consecutive predictions
                    // with the same entity type even when the model emits repeated
                    // B-* labels across subword pieces.
                    if (cur_label != null and shouldContinueEntity(cur_label.?, tag.entity_type, cur_end, char_start)) {
                        cur_label = preferredEntityType(cur_label.?, tag.entity_type);
                        cur_end = char_end;
                        cur_score_sum += prob;
                        cur_token_count += 1;
                    } else {
                        if (cur_label != null) {
                            try self.flushEntity(&entities, text, cur_label.?, cur_start, cur_end, cur_score_sum, cur_token_count);
                        }
                        cur_label = tag.entity_type;
                        cur_start = char_start;
                        cur_end = char_end;
                        cur_score_sum = prob;
                        cur_token_count = 1;
                    }
                },
            }
        }

        // Flush final entity
        if (cur_label != null) {
            try self.flushEntity(&entities, text, cur_label.?, cur_start, cur_end, cur_score_sum, cur_token_count);
        }

        return try entities.toOwnedSlice(alloc);
    }

    fn flushEntity(
        self: *NerPipeline,
        entities: *std.ArrayListUnmanaged(Entity),
        text: []const u8,
        label: []const u8,
        start: usize,
        end: usize,
        score_sum: f32,
        token_count: usize,
    ) !void {
        const avg_score = if (token_count > 0) score_sum / @as(f32, @floatFromInt(token_count)) else 0.0;
        if (avg_score < self.config.threshold) return;

        // Extract entity text from original
        const clamped_start = @min(start, text.len);
        const clamped_end = @min(end, text.len);
        const entity_text = if (clamped_end > clamped_start)
            try self.allocator.dupe(u8, text[clamped_start..clamped_end])
        else
            try self.allocator.dupe(u8, "");

        try entities.append(self.allocator, .{
            .text = entity_text,
            .label = label,
            .start = clamped_start,
            .end = clamped_end,
            .score = avg_score,
        });
    }
};

const BioTagKind = enum { O, B, I };

const BioTag = struct {
    kind: BioTagKind,
    entity_type: []const u8,
};

/// Parse a BIO tag string like "B-PER", "I-ORG", "O" into components.
fn parseBioTag(label: []const u8) BioTag {
    if (label.len == 0 or std.mem.eql(u8, label, "O")) {
        return .{ .kind = .O, .entity_type = "" };
    }
    if (label.len >= 2 and label[1] == '-') {
        const entity_type = label[2..];
        return switch (label[0]) {
            'B' => .{ .kind = .B, .entity_type = entity_type },
            'I' => .{ .kind = .I, .entity_type = entity_type },
            else => .{ .kind = .O, .entity_type = "" },
        };
    }
    // No prefix — treat as B-tag (some models don't use BIO)
    return .{ .kind = .B, .entity_type = label };
}

fn shouldContinueEntity(current_type: []const u8, next_type: []const u8, current_end: usize, next_start: usize) bool {
    if (std.mem.eql(u8, current_type, next_type)) {
        return true;
    }
    // Some PII models switch between a coarse label and a subtype label
    // across adjacent pieces, for example IP <-> IPV4 around punctuation.
    return next_start <= current_end and areCompatibleEntityTypes(current_type, next_type);
}

fn preferredEntityType(current_type: []const u8, next_type: []const u8) []const u8 {
    if (next_type.len > current_type.len) return next_type;
    return current_type;
}

fn areCompatibleEntityTypes(a: []const u8, b: []const u8) bool {
    if (a.len == 0 or b.len == 0) return false;
    if (std.mem.eql(u8, a, b)) return true;

    const min_len = @min(a.len, b.len);
    if (min_len < 2) return false;

    var prefix_len: usize = 0;
    while (prefix_len < min_len and a[prefix_len] == b[prefix_len]) : (prefix_len += 1) {}
    return prefix_len == min_len;
}

test "parseBioTag" {
    const t1 = parseBioTag("B-PER");
    try std.testing.expectEqual(BioTagKind.B, t1.kind);
    try std.testing.expectEqualStrings("PER", t1.entity_type);

    const t2 = parseBioTag("I-ORG");
    try std.testing.expectEqual(BioTagKind.I, t2.kind);
    try std.testing.expectEqualStrings("ORG", t2.entity_type);

    const t3 = parseBioTag("O");
    try std.testing.expectEqual(BioTagKind.O, t3.kind);

    const t4 = parseBioTag("LOC");
    try std.testing.expectEqual(BioTagKind.B, t4.kind);
    try std.testing.expectEqualStrings("LOC", t4.entity_type);
}

test "shouldContinueEntity allows adjacent compatible subtype labels" {
    try std.testing.expect(shouldContinueEntity("IPV4", "IP", 3, 3));
    try std.testing.expect(shouldContinueEntity("IP", "IPV4", 4, 4));
    try std.testing.expect(!shouldContinueEntity("IP", "IPV6", 4, 5));
    try std.testing.expect(!shouldContinueEntity("ORG", "PERSON", 10, 10));
}

test "aggregateEntities merges consecutive B-label fragments with the same type" {
    const alloc = std.testing.allocator;

    var pipeline = NerPipeline{
        .allocator = alloc,
        .session = undefined,
        .tok = undefined,
        .config = .{
            .max_length = 16,
            .id2label = &.{ "O", "B-EMAIL" },
            .threshold = 0.0,
        },
    };

    const text = "Reach jane.smith@example.org now.";
    const offsets = [_][2]u32{
        .{ 0, 0 }, // [CLS]
        .{ 6, 10 }, // jane
        .{ 10, 11 }, // .
        .{ 11, 16 }, // smith
        .{ 16, 17 }, // @
        .{ 17, 24 }, // example
        .{ 24, 25 }, // .
        .{ 25, 28 }, // org
        .{ 0, 0 }, // [SEP]
    };
    const attention_mask = [_]i32{ 1, 1, 1, 1, 1, 1, 1, 1, 1 };

    // Predict B-EMAIL for each sub-piece so the aggregator has to merge them.
    const logits = [_]f32{
        5, 0,
        0, 5,
        0, 5,
        0, 5,
        0, 5,
        0, 5,
        0, 5,
        0, 5,
        5, 0,
    };

    const entities = try pipeline.aggregateEntities(
        text,
        &offsets,
        &attention_mask,
        &logits,
        offsets.len,
        2,
    );
    defer {
        for (entities) |entity| alloc.free(entity.text);
        alloc.free(entities);
    }

    try std.testing.expectEqual(@as(usize, 1), entities.len);
    try std.testing.expectEqualStrings("EMAIL", entities[0].label);
    try std.testing.expectEqual(@as(usize, 6), entities[0].start);
    try std.testing.expectEqual(@as(usize, 28), entities[0].end);
    try std.testing.expectEqualStrings("jane.smith@example.org", entities[0].text);
}

test "aggregateEntities merges adjacent compatible label variants" {
    const alloc = std.testing.allocator;

    var pipeline = NerPipeline{
        .allocator = alloc,
        .session = undefined,
        .tok = undefined,
        .config = .{
            .max_length = 16,
            .id2label = &.{ "O", "B-IPV4", "B-IP" },
            .threshold = 0.0,
        },
    };

    const text = "Server 203.0.113.42";
    const offsets = [_][2]u32{
        .{ 0, 0 }, // [CLS]
        .{ 7, 10 }, // 203
        .{ 10, 11 }, // .
        .{ 11, 19 }, // 0.113.42
        .{ 0, 0 }, // [SEP]
    };
    const attention_mask = [_]i32{ 1, 1, 1, 1, 1 };
    const logits = [_]f32{
        5, 0, 0,
        0, 5, 0,
        0, 0, 5,
        0, 5, 0,
        5, 0, 0,
    };

    const entities = try pipeline.aggregateEntities(
        text,
        &offsets,
        &attention_mask,
        &logits,
        offsets.len,
        3,
    );
    defer {
        for (entities) |entity| alloc.free(entity.text);
        alloc.free(entities);
    }

    try std.testing.expectEqual(@as(usize, 1), entities.len);
    try std.testing.expectEqualStrings("IPV4", entities[0].label);
    try std.testing.expectEqualStrings("203.0.113.42", entities[0].text);
    try std.testing.expectEqual(@as(usize, 7), entities[0].start);
    try std.testing.expectEqual(@as(usize, 19), entities[0].end);
}
