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

//! Embedding similarity subset of extraction v2. Probability thresholds,
//! ordinal decisions, generated entities and trained heads are separate APIs.
const std = @import("std");
const v2 = @import("extraction_v2.zig");
const decide = @import("antfly_decisions").legacy;
const scoring = @import("antfly_decisions").scoring;
const V = std.json.Value;
const O = std.json.ObjectMap;
pub const Mode = enum { single, multi };
pub const Task = struct { text: []const u8, question: decide.Question, mode: Mode, thresholds: []const f64, top_k: usize };
pub const Item = struct { id: ?[]const u8, first: usize, count: usize };
pub const Request = struct { model: []const u8, items: []Item, tasks: []Task, options: scoring.Options, schema_bytes: usize };

fn object(value: V) !O {
    return if (value == .object) value.object else error.InvalidExtractionRequest;
}
fn text(value: V) ![]const u8 {
    if (value != .string or !std.unicode.utf8ValidateSlice(value.string) or std.mem.trim(u8, value.string, " \r\n\t").len == 0) return error.InvalidExtractionRequest;
    return value.string;
}
fn keys(o: O, allowed: []const []const u8) !void {
    for (o.keys()) |key| {
        for (allowed) |candidate| {
            if (std.mem.eql(u8, key, candidate)) break;
        } else return error.UnsupportedExtractionFeature;
    }
}
fn number(value: V) !f64 {
    const n: f64 = switch (value) {
        .float => value.float,
        .integer => @floatFromInt(value.integer),
        else => return error.InvalidExtractionRequest,
    };
    if (!std.math.isFinite(n) or n < -1 or n > 1) return error.InvalidExtractionRequest;
    return n;
}

pub fn parse(a: std.mem.Allocator, value: V) !Request {
    if (try v2.version(value) != 2) return error.UnsupportedExtractionSchemaVersion;
    const root = try object(value);
    try keys(root, &.{ "model", "schema_version", "inputs", "schema", "options" });
    const model = try text(root.get("model") orelse return error.InvalidExtractionRequest);
    const inputs = root.get("inputs") orelse return error.InvalidExtractionRequest;
    if (inputs != .array or inputs.array.items.len == 0 or inputs.array.items.len > 128) return error.ExtractionRequestLimitExceeded;
    var options = scoring.Options{};
    if (root.get("options")) |raw| {
        const opts = try object(raw);
        try keys(opts, &.{ "embedding", "include_confidence", "long_document" });
        if (opts.get("embedding")) |embedding| options = scoring.Options.parse(embedding) catch return error.InvalidExtractionOptions;
        if (opts.get("include_confidence")) |confidence| if (confidence != .bool or confidence.bool) return error.UnsupportedExtractionFeature;
        if (opts.get("long_document")) |document| {
            const o = try object(document);
            try keys(o, &.{"mode"});
            if (!std.mem.eql(u8, try text(o.get("mode") orelse return error.InvalidExtractionOptions), "reject")) return error.UnsupportedExtractionFeature;
        }
    }
    const shared_schema = root.get("schema");
    const items = try a.alloc(Item, inputs.array.items.len);
    var tasks: std.ArrayListUnmanaged(Task) = .empty;
    var total_bytes: usize = 0;
    var total_encodings: usize = 0;
    var schema_bytes: usize = 0;
    for (inputs.array.items, items) |raw, *item| {
        const input = try object(raw);
        try keys(input, &.{ "id", "content", "metadata", "schema" });
        if (input.get("metadata")) |metadata| _ = try object(metadata);
        const state = try v2.textContent(a, input.get("content") orelse return error.InvalidExtractionRequest, 1024 * 1024);
        total_bytes += state.len;
        if (total_bytes > 16 * 1024 * 1024) return error.ExtractionTextLimitExceeded;
        const schema = input.get("schema") orelse shared_schema orelse return error.InvalidExtractionRequest;
        const fields = try object(schema);
        try keys(fields, &.{"classifications"});
        const encoded = try std.json.Stringify.valueAlloc(a, schema, .{});
        schema_bytes = @max(schema_bytes, encoded.len);
        a.free(encoded);
        const classifications = fields.get("classifications") orelse return error.InvalidExtractionRequest;
        if (classifications != .array or classifications.array.items.len == 0 or classifications.array.items.len > 64 or tasks.items.len + classifications.array.items.len > 512) return error.ExtractionRequestLimitExceeded;
        item.* = .{ .id = if (input.get("id")) |id| try text(id) else null, .first = tasks.items.len, .count = classifications.array.items.len };
        for (classifications.array.items) |classification| {
            const task = try parseTask(a, state, classification, options);
            for (tasks.items[item.first..]) |previous| if (std.mem.eql(u8, previous.question.name, task.question.name)) return error.InvalidExtractionRequest;
            total_encodings += 1;
            for (task.question.examples) |examples| total_encodings += @max(examples.len, 1);
            if (total_encodings > 4096) return error.ExtractionRequestLimitExceeded;
            try tasks.append(a, task);
        }
    }
    return .{ .model = model, .items = items, .tasks = try tasks.toOwnedSlice(a), .options = options, .schema_bytes = schema_bytes };
}

fn parseTask(a: std.mem.Allocator, state: []const u8, raw: V, options: scoring.Options) !Task {
    const o = try object(raw);
    try keys(o, &.{ "name", "labels", "prompt", "instruction", "mode", "multi_label", "label_definitions", "examples", "similarity_thresholds", "top_k" });
    if (o.contains("prompt") and o.contains("instruction")) return error.InvalidExtractionRequest;
    const instruction = if (o.get("prompt") orelse o.get("instruction")) |v| try text(v) else "Classify the input.";
    var mode: Mode = .single;
    if (o.get("mode")) |v| mode = std.meta.stringToEnum(Mode, try text(v)) orelse return error.UnsupportedEmbeddingDecisionKind;
    if (o.get("multi_label")) |v| {
        if (v != .bool) return error.InvalidExtractionRequest;
        const requested: Mode = if (v.bool) .multi else .single;
        if (o.contains("mode") and mode != requested) return error.InvalidExtractionRequest;
        mode = requested;
    }
    if (mode == .multi and options.min_margin != null) return error.UnsupportedExtractionFeature;
    const raw_labels = o.get("labels") orelse return error.InvalidExtractionRequest;
    if (raw_labels != .array or raw_labels.array.items.len < @as(usize, if (mode == .single) 2 else 1) or raw_labels.array.items.len > 64) return error.InvalidExtractionRequest;
    const labels = try a.alloc([]const u8, raw_labels.array.items.len);
    const descriptions = try a.alloc([]const u8, labels.len);
    const examples = try a.alloc([]const []const u8, labels.len);
    const example_lists = try a.alloc(std.ArrayListUnmanaged([]const u8), labels.len);
    @memset(example_lists, .empty);
    const definitions = if (o.get("label_definitions")) |v| try object(v) else O.empty;
    for (raw_labels.array.items, labels, descriptions, 0..) |label, *dest, *description, i| {
        dest.* = try text(label);
        for (labels[0..i]) |previous| if (std.mem.eql(u8, previous, dest.*)) return error.InvalidExtractionRequest;
        description.* = dest.*;
        if (definitions.get(dest.*)) |definition| {
            const fields = try object(definition);
            try keys(fields, &.{"description"});
            if (fields.get("description")) |v| description.* = try text(v);
        }
    }
    for (definitions.keys()) |key| _ = try labelIndex(labels, key);
    if (o.get("examples")) |raw_examples| {
        if (raw_examples != .array or raw_examples.array.items.len > 128) return error.InvalidExtractionRequest;
        for (raw_examples.array.items) |example| {
            const pair: [2]V = if (example == .array) blk: {
                if (example.array.items.len != 2) return error.InvalidExtractionRequest;
                break :blk .{ example.array.items[0], example.array.items[1] };
            } else blk: {
                const fields = try object(example);
                try keys(fields, &.{ "input", "label" });
                break :blk .{ fields.get("input") orelse return error.InvalidExtractionRequest, fields.get("label") orelse return error.InvalidExtractionRequest };
            };
            const index = try labelIndex(labels, try text(pair[1]));
            if (example_lists[index].items.len >= 32) return error.ExtractionRequestLimitExceeded;
            try example_lists[index].append(a, try text(pair[0]));
        }
    }
    for (example_lists, examples) |*list, *dest| dest.* = try list.toOwnedSlice(a);
    const thresholds = try a.alloc(f64, if (mode == .multi) labels.len else 0);
    if (o.get("similarity_thresholds")) |raw_thresholds| {
        if (options.calibration_id != null) return error.InvalidExtractionOptions;
        if (mode != .multi) return error.UnsupportedExtractionFeature;
        if (raw_thresholds == .object) {
            if (raw_thresholds.object.count() != labels.len) return error.InvalidExtractionRequest;
            for (labels, thresholds) |label, *threshold| threshold.* = try number(raw_thresholds.object.get(label) orelse return error.InvalidExtractionRequest);
        } else @memset(thresholds, try number(raw_thresholds));
    } else if (mode == .multi and options.calibration_id == null) return error.EmbeddingMultiLabelThresholdRequired;
    var top_k = labels.len;
    if (o.get("top_k")) |v| {
        if (options.calibration_id != null and mode == .multi) return error.InvalidExtractionOptions;
        if (v != .integer or v.integer < 1 or v.integer > labels.len or (mode == .single and v.integer != 1)) return error.InvalidExtractionRequest;
        top_k = @intCast(v.integer);
    }
    return .{ .text = state, .question = .{ .name = try text(o.get("name") orelse return error.InvalidExtractionRequest), .kind = .choice, .instructions = instruction, .labels = labels, .descriptions = descriptions, .examples = examples }, .mode = mode, .thresholds = thresholds, .top_k = top_k };
}
fn labelIndex(labels: []const []const u8, label: []const u8) !usize {
    for (labels, 0..) |candidate, i| if (std.mem.eql(u8, candidate, label)) return i;
    return error.InvalidExtractionRequest;
}

pub fn selected(a: std.mem.Allocator, task: Task, scores: []const f64, options: scoring.Options) ![]usize {
    if (scores.len != task.question.labels.len) return error.InvalidEmbeddingDecisionOutput;
    if (task.mode == .single) {
        const selection = try scoring.select(scores, options);
        return if (selection.selected) |index| a.dupe(usize, &.{index}) else a.alloc(usize, 0);
    }
    var indices: std.ArrayListUnmanaged(usize) = .empty;
    for (scores, task.thresholds, 0..) |score, threshold, i| {
        if (!std.math.isFinite(score) or score < -1 or score > 1) return error.InvalidEmbeddingDecisionOutput;
        if (score >= threshold and (options.min_similarity == null or score >= options.min_similarity.?)) try indices.append(a, i);
    }
    const Sort = struct {
        scores: []const f64,
        labels: []const []const u8,
        fn less(self: @This(), lhs: usize, rhs: usize) bool {
            return if (self.scores[lhs] != self.scores[rhs]) self.scores[lhs] > self.scores[rhs] else std.mem.order(u8, self.labels[lhs], self.labels[rhs]) == .lt;
        }
    };
    std.mem.sort(usize, indices.items, Sort{ .scores = scores, .labels = task.question.labels }, Sort.less);
    // A top_k boundary tie is ambiguous; return no labels instead of an
    // arbitrary subset. Unbounded multi-label selection retains all matches.
    if (indices.items.len > task.top_k and @abs(scores[indices.items[task.top_k - 1]] - scores[indices.items[task.top_k]]) <= 1e-6) return a.alloc(usize, 0);
    return a.dupe(usize, indices.items[0..@min(indices.items.len, task.top_k)]);
}

test "embeddinggemma2 multi-label requires explicit raw cosine thresholds" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const value = try std.json.parseFromSliceLeaky(V, a, "{\"schema_version\":2,\"model\":\"test\",\"inputs\":[{\"content\":\"state\"}],\"schema\":{\"classifications\":[{\"name\":\"tags\",\"mode\":\"multi\",\"labels\":[\"a\",\"b\"],\"similarity_thresholds\":{\"a\":-0.2,\"b\":0.5}}]}}", .{});
    const request = try parse(a, value);
    const indices = try selected(a, request.tasks[0], &.{ -0.1, 0.4 }, request.options);
    try std.testing.expectEqualSlices(usize, &.{0}, indices);
    const missing = try std.json.parseFromSliceLeaky(V, a, "{\"schema_version\":2,\"model\":\"test\",\"inputs\":[{\"content\":\"state\"}],\"schema\":{\"classifications\":[{\"name\":\"tags\",\"mode\":\"multi\",\"labels\":[\"a\",\"b\"]}]}}", .{});
    try std.testing.expectError(error.EmbeddingMultiLabelThresholdRequired, parse(a, missing));
}
