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

//! Offline decoded evaluation of a ModernBERT span artifact. Explicit evaluation
//! does not grant production capabilities or inherit the original weight pin.
const std = @import("std");
const job = @import("job.zig");
const data = @import("../gliner/boundary_dataset.zig");
const features = @import("features.zig");
const factory = @import("../../architectures/session_factory.zig");
const core = @import("../../extractors/gliner_span_extraction.zig");
const processor = @import("../../pipelines/gliner_boundary_processor.zig");
const schema = @import("../../pipelines/extraction_schema.zig");
const pipeline = @import("../../pipelines/gliner_boundary_pipeline.zig");
const hf = @import("inference_hf_tokenizer");
const qualification = @import("../../models/gliner_decide_qualification.zig");
const file = @import("../../util/c_file.zig");
const Control = @import("../../execution_control.zig").InferenceExecutionControl;
const Budget = @import("../../runtime/bounded_allocator.zig").BoundedAllocator;
const A = std.mem.Allocator;
pub const Config = struct {
    version: u32 = 1,
    model_dir: []const u8,
    eval_file: []const u8,
    output_file: []const u8,
    expected_weights_sha256: []const u8,
    backend: enum { native, metal } = .native,
    threshold: f32 = 0.5,
    max_examples: usize = 1024,
    max_sequence_tokens: usize = 1024,
    max_host_bytes: usize = 12 * 1024 * 1024 * 1024,
    timeout_seconds: u32 = 4 * 60 * 60,
};
const Scores = struct {
    true_positive: usize = 0,
    false_positive: usize = 0,
    false_negative: usize = 0,
    precision: f64 = 0,
    recall: f64 = 0,
    f1: f64 = 0,
    fn finish(self: *Scores) void {
        if (self.true_positive + self.false_positive > 0) self.precision = @as(f64, @floatFromInt(self.true_positive)) / @as(f64, @floatFromInt(self.true_positive + self.false_positive));
        if (self.true_positive + self.false_negative > 0) self.recall = @as(f64, @floatFromInt(self.true_positive)) / @as(f64, @floatFromInt(self.true_positive + self.false_negative));
        if (self.precision + self.recall > 0) self.f1 = 2 * self.precision * self.recall / (self.precision + self.recall);
    }
};
const PredictedEntity = struct { label: []const u8, start: usize, end: usize, confidence: f32 };
const PredictedRelation = struct { type: []const u8, head: features.Span, tail: features.Span, confidence: f32 };
const Row = struct { id: []const u8, prompt_tokens: usize, input_ids: []const i64, entities: []const PredictedEntity, relations: []const PredictedRelation };
fn span(value: pipeline.Value) !features.Span {
    const source = value.source orelse return error.MissingSpanEvaluationSource;
    return .{ .start = source.byte_start, .end = source.byte_end };
}

pub fn execute(backing: A, io: std.Io, c: Config, control: Control) !void {
    if (c.version != 1 or c.expected_weights_sha256.len != 64 or c.max_examples == 0 or c.max_examples > 10000 or c.max_sequence_tokens == 0 or c.max_sequence_tokens > 1024 or !std.math.isFinite(c.threshold) or c.threshold <= 0 or c.threshold >= 1) return error.InvalidSpanEvaluationConfig;
    for ([_][]const u8{ c.model_dir, c.eval_file, c.output_file }) |name| if (!std.fs.path.isAbsolute(name)) return error.SpanTrainingRequiresAbsolutePaths;
    var budget = Budget{ .backing = backing, .limit = c.max_host_bytes };
    const a = budget.allocator();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const p = arena.allocator();
    var dataset = try data.Dataset.open(a, c.eval_file, .{ .limits = .{ .max_examples = c.max_examples } }, control, null);
    defer dataset.deinit();
    const records = try job.records(p, &dataset);
    const session = switch (c.backend) {
        .native => try factory.createNativeSession(a, c.model_dir),
        .metal => try factory.createMetalSession(a, c.model_dir),
    };
    defer session.close();
    const config = try factory.getGlinerSpanConfig(session);
    if (config != .modern_bert) return error.UnsupportedSpanEvaluationModel;
    const tokenizer_bytes = try file.readFileMax(p, try std.fs.path.join(p, &.{ c.model_dir, "tokenizer.json" }), 64 * 1024 * 1024);
    const tokenizer = try hf.HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    try factory.sealGlinerDecisionTokenizerDigest(session, qualification.Digest.of(tokenizer_bytes));
    const identity = try factory.getGlinerDecisionIdentity(session);
    const pinned = qualification.pinned_fp32_identity;
    if (identity.weight_companion != null or !identity.inventory.all_f32 or identity.inventory.count != pinned.inventory.count or !std.meta.eql(identity.geometry, pinned.geometry) or !std.meta.eql(identity.markers, pinned.markers) or !std.meta.eql(identity.sidecars, pinned.sidecars) or !std.mem.eql(u8, &identity.weight.sha256, c.expected_weights_sha256)) return error.SpanEvaluationArtifactMismatch;
    var entities = Scores{};
    var relations = Scores{};
    var rows: std.ArrayList(Row) = .empty;
    for (records) |record| {
        try control.check();
        var temporary = std.heap.ArenaAllocator.init(a);
        defer temporary.deinit();
        const t = temporary.allocator();
        var compiled = try schema.compile(t, record.schema_json, .{});
        defer compiled.deinit();
        var prepared = try processor.prepare(t, tokenizer.tokenizer(), &.{.{ .text = record.text, .schema = &compiled }}, .{ .max_sequence_tokens = c.max_sequence_tokens, .max_batch_tokens = c.max_sequence_tokens, .control = control });
        defer prepared.deinit();
        var managed = try factory.getManagedComputeBackend(session, a, null, control);
        defer managed.deinit();
        var decoded = try core.run(&managed.backend, a, config.modern_bert, prepared.samples[0], &compiled, .{ .threshold = c.threshold, .offset_unit = .utf8_bytes, .control = control });
        defer decoded.deinit();
        const output = decoded.samples[0];
        var predicted_entities: std.ArrayList(PredictedEntity) = .empty;
        var predicted_relations: std.ArrayList(PredictedRelation) = .empty;
        const entity_used = try t.alloc(bool, record.entities.len);
        const relation_used = try t.alloc(bool, record.relations.len);
        @memset(entity_used, false);
        @memset(relation_used, false);
        for (output.entities) |group| for (group.values) |value| {
            const source = try span(value);
            if (source.start >= source.end or source.end > record.text.len or !std.mem.eql(u8, value.text, record.text[source.start..source.end])) return error.InvalidSpanEvaluationSource;
            var matched = false;
            for (record.entities, entity_used) |gold, *used| if (!used.* and std.mem.eql(u8, group.name, gold.label) and source.start == gold.start and source.end == gold.end) {
                used.* = true;
                matched = true;
                break;
            };
            if (matched) entities.true_positive += 1 else entities.false_positive += 1;
            try predicted_entities.append(p, .{ .label = try p.dupe(u8, group.name), .start = source.start, .end = source.end, .confidence = value.confidence });
        };
        for (output.relations) |value| {
            const first = try span(value.head);
            const last = try span(value.tail);
            var matched = false;
            for (record.relations, relation_used) |gold, *used| if (!used.* and std.mem.eql(u8, value.name, gold.type) and std.meta.eql(first, gold.head) and std.meta.eql(last, gold.tail)) {
                used.* = true;
                matched = true;
                break;
            };
            if (matched) relations.true_positive += 1 else relations.false_positive += 1;
            try predicted_relations.append(p, .{ .type = try p.dupe(u8, value.name), .head = first, .tail = last, .confidence = value.confidence });
        }
        for (entity_used) |used| entities.false_negative += @intFromBool(!used);
        for (relation_used) |used| relations.false_negative += @intFromBool(!used);
        try rows.append(p, .{ .id = record.id, .prompt_tokens = prepared.input_ids.len, .input_ids = try p.dupe(i64, prepared.samples[0].input_ids), .entities = predicted_entities.items, .relations = predicted_relations.items });
        std.debug.print("GLiNER span decoded: {s}, entities={d}, relations={d}\n", .{ record.id, predicted_entities.items.len, predicted_relations.items.len });
    }
    entities.finish();
    relations.finish();
    const bytes = try std.json.Stringify.valueAlloc(p, .{ .scope = "offline decoded entity/relation exact UTF-8 span and label evaluation", .runtime_qualified = false, .weights_sha256 = identity.weight.sha256[0..], .dataset_sha256 = std.fmt.bytesToHex(dataset.sha256, .lower), .backend = c.backend, .threshold = c.threshold, .entities = entities, .relations = relations, .rows = rows.items }, .{ .whitespace = .indent_2 });
    var output = try std.Io.Dir.cwd().createFile(io, c.output_file, .{ .exclusive = true });
    defer output.close(io);
    try output.writeStreamingAll(io, bytes);
    try output.sync(io);
}
