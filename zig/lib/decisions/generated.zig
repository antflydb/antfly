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

//! Generated decision schemas and typed response construction. Generation
//! execution and validation/repair are shared with ordinary generator assets.
const std = @import("std");
const Kind = enum { choice, multi_choice, score, noul };
const c = @import("antfly_structured");
const evidence = @import("antfly_structured").token_evidence;
const A = std.mem.Allocator;
const V = std.json.Value;

pub fn questionSchema(a: A, questions: V) !V {
    var properties = c.object();
    var required: V = .{ .array = std.json.Array.init(a) };
    for (questions.object.keys(), questions.object.values()) |name, question| {
        var field = c.object();
        const kind = std.meta.stringToEnum(Kind, question.object.get("type").?.string).?;
        try c.put(a, &field, "type", .{ .string = switch (kind) {
            .choice => "string",
            .multi_choice => "array",
            .score => "number",
            .noul => "boolean",
        } });
        try c.put(a, &field, "description", .{ .string = try std.json.Stringify.valueAlloc(a, question, .{}) });
        if (kind == .choice or kind == .multi_choice) {
            var labels: V = .{ .array = std.json.Array.init(a) };
            for (question.object.get("criteria").?.object.keys()) |label| try labels.array.append(.{ .string = label });
            if (kind == .choice) {
                try c.put(a, &field, "enum", labels);
            } else {
                var items = c.object();
                try c.put(a, &items, "type", .{ .string = "string" });
                try c.put(a, &items, "enum", labels);
                try c.put(a, &field, "items", items);
                try c.put(a, &field, "uniqueItems", .{ .bool = true });
            }
        } else if (kind == .score) {
            try c.put(a, &field, "minimum", .{ .integer = 0 });
            try c.put(a, &field, "maximum", .{ .integer = 1 });
        }
        try c.put(a, &properties, name, field);
        try required.array.append(.{ .string = name });
    }
    var root = c.object();
    try c.put(a, &root, "type", .{ .string = "object" });
    try c.put(a, &root, "properties", properties);
    try c.put(a, &root, "required", required);
    try c.put(a, &root, "additionalProperties", .{ .bool = false });
    return root;
}

pub const Options = struct { model: []const u8, probabilities: c.Probabilities };

/// Used by the shared item validator before an answer is committed, so invalid
/// evidence is repaired together with invalid JSON rather than failing siblings.
pub fn responseValue(a: A, cfg: Options, questions: V, value: V, proof: ?c.TokenEvidence, index: usize, input_tokens: u64, output_tokens: u64) !V {
    var answers = c.object();
    for (questions.object.keys(), questions.object.values()) |name, question| {
        const kind = std.meta.stringToEnum(Kind, question.object.get("type").?.string).?;
        const raw = value.object.get(name) orelse return error.InvalidDecisionOutput;
        var answer = c.object();
        try c.put(a, &answer, "type", .{ .string = @tagName(kind) });
        try c.put(a, &answer, "decision_method", .{ .string = "generated" });
        try c.put(a, &answer, "probability_source", .{ .string = if (cfg.probabilities == .verdict_only) "verdict" else "token_logprobs" });
        try c.put(a, &answer, "raw_structured_answer", raw);
        switch (kind) {
            .choice, .multi_choice => {
                const criteria = question.object.get("criteria").?.object;
                var probabilities = c.object();
                const measured = if (cfg.probabilities == .token_logprobs) blk: {
                    if (kind != .choice) return error.UnsupportedConstrainedProbabilities;
                    const labels = try a.alloc(V, criteria.count());
                    for (labels, criteria.keys()) |*label, text| label.* = .{ .string = text };
                    break :blk try evidence.distribution(a, proof orelse return error.InvalidConstrainedProbabilities, index, &.{name}, labels);
                } else null;
                for (criteria.keys(), 0..) |label, i| {
                    var selected = kind == .choice and std.mem.eql(u8, raw.string, label);
                    if (kind == .multi_choice) for (raw.array.items) |entry| {
                        if (std.mem.eql(u8, entry.string, label)) selected = true;
                    };
                    try c.put(a, &probabilities, label, .{ .float = if (measured) |p| p[i] else if (selected) 1 else 0 });
                }
                try c.put(a, &answer, "probabilities", probabilities);
                try c.put(a, &answer, if (kind == .choice) "choice" else "choices", raw);
            },
            .noul => {
                const p: f64 = if (cfg.probabilities == .verdict_only) (if (raw.bool) 1 else 0) else (try evidence.distribution(a, proof orelse return error.InvalidConstrainedProbabilities, index, &.{name}, &.{ .{ .bool = false }, .{ .bool = true } }))[1];
                try c.put(a, &answer, "noul", .{ .float = p });
            },
            .score => {
                if (cfg.probabilities != .verdict_only) return error.UnsupportedConstrainedProbabilities;
                try c.put(a, &answer, "score", raw);
            },
        }
        try c.put(a, &answers, name, answer);
    }
    var root = c.object();
    try c.put(a, &root, "model", .{ .string = cfg.model });
    try c.put(a, &root, "answers", answers);
    var usage = c.object();
    // Batch usage belongs to the call, not every item; assign it exactly once.
    try c.put(a, &usage, "input_tokens", .{ .integer = @intCast(input_tokens) });
    try c.put(a, &usage, "output_tokens", .{ .integer = @intCast(output_tokens) });
    try c.put(a, &root, "usage", usage);
    return root;
}

pub const Request = struct {
    generator: V,
    model: []const u8,
    output: c.Output = .auto,
    probabilities: c.Probabilities = .token_logprobs,
    batch: c.Batch = .{},
    instructions: []const u8 = "",
    inputs: []const @import("root.zig").Item,
    questions: V,
    batched: bool,
};
fn fieldText(value: V) ![]const u8 {
    if (value != .string or value.string.len == 0 or !std.unicode.utf8ValidateSlice(value.string)) return error.InvalidDecideRequest;
    return value.string;
}
fn allowed(value: V, names: []const []const u8) !void {
    if (value != .object) return error.InvalidDecideRequest;
    for (value.object.keys()) |key| {
        for (names) |name| if (std.mem.eql(u8, name, key)) break else continue else return error.InvalidDecideRequest;
    }
}
/// Public generated questions share the named-array wire contract; numeric
/// scores are bounded [0,1], without a trained classifier's ordinal levels.
pub fn parseRequest(a: A, value: V) !Request {
    try allowed(value, &.{ "decision_method", "generator", "input", "inputs", "questions", "output", "probabilities", "batch", "instructions" });
    const root = value.object;
    const generator = root.get("generator") orelse return error.InvalidDecideRequest;
    if (generator != .object) return error.InvalidDecideRequest;
    const model = try fieldText(generator.object.get("model") orelse return error.InvalidDecideRequest);
    const batched = root.contains("inputs");
    if (batched == root.contains("input")) return error.InvalidDecideRequest;
    const items = if (batched) blk: {
        const raw = root.get("inputs").?;
        if (raw != .array or raw.array.items.len == 0 or raw.array.items.len > 128) return error.DecideRequestLimitExceeded;
        const items = try a.alloc(@import("root.zig").Item, raw.array.items.len);
        for (raw.array.items, items) |entry, *item| {
            try allowed(entry, &.{ "id", "input" });
            item.* = .{ .input = try fieldText(entry.object.get("input") orelse return error.InvalidDecideRequest), .id = if (entry.object.get("id")) |id| try fieldText(id) else null };
        }
        break :blk items;
    } else try a.dupe(@import("root.zig").Item, &.{.{ .input = try fieldText(root.get("input").?) }});
    for (items) |item| if (item.input.len > 1024 * 1024) return error.DecideRequestLimitExceeded;
    const raw_questions = root.get("questions") orelse return error.InvalidDecideRequest;
    if (raw_questions != .array or raw_questions.array.items.len == 0 or raw_questions.array.items.len > 64) return error.InvalidDecideRequest;
    const global_instructions = if (root.get("instructions")) |value_| try fieldText(value_) else "";
    var questions = c.object();
    for (raw_questions.array.items) |raw| {
        try allowed(raw, &.{ "name", "type", "instructions", "choices" });
        const name = try fieldText(raw.object.get("name") orelse return error.InvalidDecideRequest);
        if (questions.object.contains(name)) return error.InvalidDecideRequest;
        const kind = try fieldText(raw.object.get("type") orelse return error.InvalidDecideRequest);
        const instructions = if (raw.object.get("instructions")) |value_| try fieldText(value_) else if (global_instructions.len > 0) "Use the decider instructions and the named question." else return error.InvalidDecideRequest;
        if (instructions.len > 65536) return error.DecideRequestLimitExceeded;
        var question = c.object();
        try c.put(a, &question, "instructions", .{ .string = instructions });
        try c.put(a, &question, "type", .{ .string = if (std.mem.eql(u8, kind, "predicate")) "noul" else kind });
        if (std.mem.eql(u8, kind, "choice") or std.mem.eql(u8, kind, "multi_choice")) {
            const choices = raw.object.get("choices") orelse return error.InvalidDecideRequest;
            if (choices != .array or choices.array.items.len < 2 or choices.array.items.len > 64) return error.InvalidDecideRequest;
            var criteria = c.object();
            for (choices.array.items) |choice| {
                try allowed(choice, &.{ "value", "description" });
                const label = try fieldText(choice.object.get("value") orelse return error.InvalidDecideRequest);
                if (criteria.object.contains(label)) return error.InvalidDecideRequest;
                try c.put(a, &criteria, label, .{ .string = if (choice.object.get("description")) |description| try fieldText(description) else "" });
            }
            try c.put(a, &question, "criteria", criteria);
        } else if (!std.mem.eql(u8, kind, "predicate") and !std.mem.eql(u8, kind, "score")) return error.InvalidDecideRequest;
        try c.put(a, &questions, name, question);
    }
    var request = Request{ .generator = generator, .model = model, .inputs = items, .questions = questions, .batched = batched };
    if (root.get("output")) |output| request.output = std.meta.stringToEnum(c.Output, try fieldText(output)) orelse return error.InvalidDecideRequest;
    if (root.get("probabilities")) |policy| request.probabilities = std.meta.stringToEnum(c.Probabilities, try fieldText(policy)) orelse return error.InvalidDecideRequest;
    if (root.get("instructions")) |instructions| request.instructions = try fieldText(instructions);
    if (request.instructions.len > 65536) return error.DecideRequestLimitExceeded;
    if (root.get("batch")) |batch| {
        const parsed_batch = try std.json.parseFromValue(c.Batch, a, batch, .{});
        defer parsed_batch.deinit();
        request.batch = parsed_batch.value;
        try request.batch.validate();
    }
    if (request.probabilities == .token_logprobs) for (questions.object.values()) |question| {
        const kind = question.object.get("type").?.string;
        if (std.mem.eql(u8, kind, "multi_choice") or std.mem.eql(u8, kind, "score")) return error.UnsupportedConstrainedProbabilities;
    };
    return request;
}

test "generated constrained public questions cover choice multiple predicate and bounded score" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const value = try std.json.parseFromSliceLeaky(V, a,
        \\{"decision_method":"generated","generator":{"provider":"antfly","model":"fixture"},"probabilities":"verdict_only","input":"source","questions":[{"name":"choice","type":"choice","instructions":"select","choices":[{"value":"a"},{"value":"b"}]},{"name":"many","type":"multi_choice","instructions":"select","choices":[{"value":"a"},{"value":"b"}]},{"name":"yes","type":"predicate","instructions":"true?"},{"name":"score","type":"score","instructions":"rate"}]}
    , .{});
    const request = try parseRequest(a, value);
    try std.testing.expectEqual(@as(usize, 4), request.questions.object.count());
    const schema = try questionSchema(a, request.questions);
    try std.testing.expectEqual(@as(i64, 1), schema.object.get("properties").?.object.get("score").?.object.get("maximum").?.integer);
    const raw = try std.json.parseFromSliceLeaky(V, a, "{\"choice\":\"b\",\"many\":[\"a\",\"b\"],\"yes\":true,\"score\":0.75}", .{});
    const response = try responseValue(a, .{ .model = "fixture", .probabilities = .verdict_only }, request.questions, raw, null, 0, 10, 5);
    try std.testing.expectEqual(@as(f64, 1), response.object.get("answers").?.object.get("choice").?.object.get("probabilities").?.object.get("b").?.float);
    var unsupported = value;
    try c.put(a, &unsupported, "probabilities", .{ .string = "token_logprobs" });
    try std.testing.expectError(error.UnsupportedConstrainedProbabilities, parseRequest(a, unsupported));
}
