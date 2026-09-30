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

//! The Laya subset of extraction v2. Caller owns a bounded request arena.
const std = @import("std");
const v2 = @import("extraction_v2.zig");
const pipeline = @import("../pipelines/laya.zig");
const Value = std.json.Value;
const Object = std.json.ObjectMap;
pub const Item = struct { id: ?[]const u8, first: usize, count: usize };
pub const Request = struct { model: []const u8, items: []Item, tasks: []pipeline.Task, schema_bytes: usize };
fn object(value: Value) !Object {
    return if (value == .object) value.object else error.InvalidExtractionRequest;
}
fn string(value: Value) ![]const u8 {
    if (value != .string or !std.unicode.utf8ValidateSlice(value.string)) return error.InvalidExtractionRequest;
    return value.string;
}
fn required(obj: Object, key: []const u8) !Value {
    return obj.get(key) orelse error.InvalidExtractionRequest;
}
fn keys(obj: Object, allowed: []const []const u8) !void {
    for (obj.keys()) |key| {
        for (allowed) |candidate| {
            if (std.mem.eql(u8, key, candidate)) break;
        } else return error.UnsupportedExtractionFeature;
    }
}
fn nonempty(value: Value) ![]const u8 {
    const text = try string(value);
    if (std.mem.trim(u8, text, " \r\n\t").len == 0) return error.InvalidLayaQuestion;
    return text;
}

pub fn parse(a: std.mem.Allocator, value: Value) !Request {
    if (try v2.version(value) != 2) return error.UnsupportedExtractionSchemaVersion;
    const root = try object(value);
    try keys(root, &.{ "model", "schema_version", "inputs", "schema", "options" });
    const name = try nonempty(try required(root, "model"));
    const inputs = try required(root, "inputs");
    if (inputs != .array or inputs.array.items.len == 0 or inputs.array.items.len > 128) return error.ExtractionRequestLimitExceeded;
    const schema = try required(root, "schema");
    _ = try object(schema);
    const shared_options = root.get("options") orelse Value{ .object = .empty };
    try validateOptions(shared_options);
    const items = try a.alloc(Item, inputs.array.items.len);
    var tasks: std.ArrayList(pipeline.Task) = .empty;
    var total_text: usize = 0;
    var schema_bytes: usize = 0;
    for (inputs.array.items, items) |raw, *item| {
        const input = try object(raw);
        try keys(input, &.{ "id", "content", "metadata", "schema", "options", "tokens" });
        if (input.get("metadata")) |meta| _ = try object(meta);
        if (input.get("tokens")) |tokens| if (tokens != .array or tokens.array.items.len != 0) return error.UnsupportedExtractionInput;
        try validateOptions(input.get("options") orelse shared_options);
        const text = try v2.textContent(a, try required(input, "content"), 1024 * 1024);
        total_text += text.len;
        if (total_text > 16 * 1024 * 1024) return error.ExtractionTextLimitExceeded;
        const selected_schema = input.get("schema") orelse schema;
        const encoded_schema = try std.json.Stringify.valueAlloc(a, selected_schema, .{});
        schema_bytes = @max(schema_bytes, encoded_schema.len);
        a.free(encoded_schema);
        const fields = try object(selected_schema);
        try keys(fields, &.{"classifications"});
        const classifications = try required(fields, "classifications");
        if (classifications != .array or classifications.array.items.len == 0 or classifications.array.items.len > 64) return error.InvalidLayaQuestion;
        if (tasks.items.len + classifications.array.items.len > 512) return error.ExtractionRequestLimitExceeded;
        item.* = .{ .id = if (input.get("id")) |id| try string(id) else null, .first = tasks.items.len, .count = classifications.array.items.len };
        for (classifications.array.items) |classification| {
            const q = try parseQuestion(a, classification);
            for (tasks.items[item.first..]) |other| if (std.mem.eql(u8, other.question.name, q.name)) return error.InvalidLayaQuestion;
            try tasks.append(a, .{ .text = text, .question = q });
        }
    }
    return .{ .model = name, .items = items, .tasks = try tasks.toOwnedSlice(a), .schema_bytes = schema_bytes };
}
fn validateOptions(value: Value) !void {
    const opts = try object(value);
    try keys(opts, &.{ "long_document", "include_confidence" });
    if (opts.get("include_confidence")) |v| if (v != .bool) return error.InvalidExtractionOptions;
    if (opts.get("long_document")) |v| {
        const doc = try object(v);
        try keys(doc, &.{"mode"});
        if (!std.mem.eql(u8, try string(try required(doc, "mode")), "reject")) return error.UnsupportedExtractionFeature;
    }
}
fn parseQuestion(a: std.mem.Allocator, value: Value) !pipeline.Question {
    const obj = try object(value);
    try keys(obj, &.{ "name", "labels", "mode", "prompt", "instruction", "label_definitions", "multi_label", "top_k" });
    if (obj.get("multi_label")) |v| if (v != .bool or v.bool) return error.UnsupportedExtractionFeature;
    if (obj.get("top_k")) |v| if (v != .integer or v.integer != 1) return error.UnsupportedExtractionFeature;
    if (obj.contains("prompt") and obj.contains("instruction")) return error.InvalidLayaQuestion;
    const instruction = try nonempty(obj.get("prompt") orelse obj.get("instruction") orelse return error.InvalidLayaQuestion);
    const mode = if (obj.get("mode")) |v| try string(v) else "single";
    const kind: @import("../models/laya.zig").QuestionType = if (std.mem.eql(u8, mode, "single")) .choice else if (std.mem.eql(u8, mode, "ordinal")) .score else if (std.mem.eql(u8, mode, "boolean")) .noul else return error.UnsupportedExtractionFeature;
    const raw_labels = try required(obj, "labels");
    // The model enforces its own option limit (20, or 255 with candidate packing).
    if (raw_labels != .array or raw_labels.array.items.len < 2 or raw_labels.array.items.len > @import("../models/laya.zig").max_packed_options) return error.InvalidLayaQuestion;
    const labels = try a.alloc([]const u8, raw_labels.array.items.len);
    const descriptions = try a.alloc([]const u8, labels.len);
    const definitions = if (obj.get("label_definitions")) |v| try object(v) else Object.empty;
    for (raw_labels.array.items, labels, descriptions, 0..) |label, *dest, *description, i| {
        dest.* = try nonempty(label);
        for (labels[0..i]) |prior| if (std.mem.eql(u8, prior, dest.*)) return error.InvalidLayaQuestion;
        description.* = "";
        if (definitions.get(dest.*)) |def| {
            const fields = try object(def);
            try keys(fields, &.{"description"});
            if (fields.get("description")) |v| description.* = try string(v);
        }
    }
    for (definitions.keys()) |key| {
        for (labels) |label| {
            if (std.mem.eql(u8, key, label)) break;
        } else return error.InvalidLayaQuestion;
    }
    if (kind == .noul and (labels.len != 2 or !std.mem.eql(u8, labels[0], "false") or !std.mem.eql(u8, labels[1], "true"))) return error.InvalidLayaQuestion;
    return .{ .name = try nonempty(try required(obj, "name")), .kind = kind, .instruction = instruction, .labels = labels, .descriptions = descriptions };
}

const Probability = struct { label: []const u8, probability: f32 };
const Decision = struct {
    name: []const u8,
    type: []const u8,
    label: []const u8,
    probabilities: []Probability,
    confidence: f32,
    confidence_method: []const u8,
    expected_value: ?f32,
    true_probability: ?f32,
    act_probability: f32,
};
const Classification = struct { name: []const u8, label: []const u8, score: f32 };
const Output = struct { id: ?[]const u8, classifications: []Classification, decisions: []Decision };
pub fn response(a: std.mem.Allocator, request: Request, result: pipeline.Result, limit: usize) ![]u8 {
    const data = try a.alloc(Output, request.items.len);
    for (request.items, data) |item, *out| {
        const decisions = try a.alloc(Decision, item.count);
        const classifications = try a.alloc(Classification, item.count);
        for (result.decisions[item.first..][0..item.count], decisions, classifications) |d, *decision, *classification| {
            const probabilities = try a.alloc(Probability, d.labels.len);
            var selected: f32 = 0;
            for (d.labels, d.probabilities, probabilities) |label, probability, *entry| {
                entry.* = .{ .label = label, .probability = probability };
                if (std.mem.eql(u8, label, d.label)) selected = probability;
            }
            classification.* = .{ .name = d.name, .label = d.label, .score = selected };
            decision.* = .{ .name = d.name, .type = if (d.kind == .noul) "boolean" else @tagName(d.kind), .label = d.label, .probabilities = probabilities, .confidence = d.confidence, .confidence_method = if (d.kind == .noul) "max_probability" else "normalized_inverse_entropy", .expected_value = d.expected_value, .true_probability = d.true_probability, .act_probability = d.act_probability };
        }
        out.* = .{ .id = item.id, .classifications = classifications, .decisions = decisions };
    }
    const envelope = .{ .object = "extraction", .model = request.model, .schema_version = 2, .data = data, .usage = .{ .prompt_tokens = result.prompt_tokens, .completion_tokens = 0, .total_tokens = result.prompt_tokens } };
    var buffer: [256]u8 = undefined;
    var counter = std.Io.Writer.Discarding.init(&buffer);
    try std.json.Stringify.value(envelope, .{ .emit_null_optional_fields = false }, &counter.writer);
    if (counter.fullCount() > limit) return error.ExtractionOutputLimitExceeded;
    return std.json.Stringify.valueAlloc(a, envelope, .{ .emit_null_optional_fields = false });
}

test "laya extraction validates boolean type and rejects unsupported features" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const json = "{\"schema_version\":2,\"model\":\"laya\",\"inputs\":[{\"content\":\"hello\"}],\"schema\":{\"classifications\":[{\"name\":\"tool\",\"mode\":\"boolean\",\"instruction\":\"Need a tool?\",\"labels\":[\"false\",\"true\"]}]}}";
    const parsed = try std.json.parseFromSlice(Value, a, json, .{});
    const request = try parse(a, parsed.value);
    try std.testing.expectEqual(@import("../models/laya.zig").QuestionType.noul, request.tasks[0].question.kind);
    const bad = try std.mem.replaceOwned(u8, a, json, "boolean", "multi");
    const invalid = try std.json.parseFromSlice(Value, a, bad, .{});
    try std.testing.expectError(error.UnsupportedExtractionFeature, parse(a, invalid.value));
    const missing = try std.mem.replaceOwned(u8, a, json, "\"false\",\"true\"", "\"yes\",\"no\"");
    const invalid_labels = try std.json.parseFromSlice(Value, a, missing, .{});
    try std.testing.expectError(error.InvalidLayaQuestion, parse(a, invalid_labels.value));
}
