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

//! Translate the shared decision contract using types generated from OpenAI's
//! published OpenAPI schema. Allocations belong to a request arena.
const std = @import("std");
const api = @import("openai_api");
const d = @import("decisions.zig");

pub fn requestBody(a: std.mem.Allocator, model: []const u8, input: []const u8, questions: d.Json) ![]const u8 {
    try d.validateQuestions(questions, d.capabilities(.openai));
    const wire = try a.alloc(api.QuestionParam, questions.object.count());
    for (questions.object.keys(), questions.object.values(), wire) |name, question, *out| {
        const spec = question.object;
        const instructions = spec.get("instructions").?.string;
        const kind = std.meta.stringToEnum(d.Kind, spec.get("type").?.string).?;
        out.* = switch (kind) {
            .noul => .{ .question_param_predicate = .{ .type = "predicate", .name = name, .instructions = instructions } },
            .choice => blk: {
                const criteria = spec.get("criteria").?.object;
                const choices = try a.alloc(api.ChoiceOptionParam, criteria.count());
                for (criteria.keys(), criteria.values(), choices) |id, description, *choice| {
                    choice.* = .{ .value = .{ .string = id }, .description = description.string };
                }
                break :blk .{ .question_param_choice = .{ .type = "choice", .name = name, .instructions = instructions, .choices = choices } };
            },
            .score => blk: {
                const criteria = spec.get("criteria").?.array.items;
                const levels = try a.alloc(api.ScoreLevelParam, criteria.len);
                for (criteria, levels, 0..) |description, *level, index| {
                    // Stable labels preserve ordinal identities even when descriptions repeat.
                    level.* = .{ .label = try std.fmt.allocPrint(a, "{d}", .{index}), .description = description.string };
                }
                break :blk .{ .question_param_score = .{ .type = "score", .name = name, .instructions = instructions, .levels = levels } };
            },
        };
    }
    return std.json.Stringify.valueAlloc(a, api.DecisionRequest{ .model = model, .input = .{ .string = input }, .questions = wire }, .{});
}

fn checkName(actual: ?[]const u8, expected: []const u8) !void {
    if (!std.mem.eql(u8, actual orelse return error.InvalidDecisionOutput, expected)) return error.InvalidDecisionOutput;
}

fn addProbability(a: std.mem.Allocator, distribution: *d.Json, key: []const u8, probability: f64) !void {
    if (distribution.object.contains(key)) return error.InvalidDecisionOutput;
    try d.put(a, distribution, key, .{ .float = probability });
}

pub fn response(a: std.mem.Allocator, questions: d.Json, source: d.Json) !d.Json {
    const parsed = std.json.parseFromValueLeaky(api.DecisionResponse, a, source, .{ .ignore_unknown_fields = true }) catch |err| {
        return if (err == error.OutOfMemory) err else error.InvalidDecisionOutput;
    };
    if (parsed.answers.len != questions.object.count()) return error.InvalidDecisionOutput;
    var answers = d.jsonObject();
    for (questions.object.keys(), questions.object.values(), parsed.answers) |name, question, wire| {
        const kind = std.meta.stringToEnum(d.Kind, question.object.get("type").?.string).?;
        var answer = d.jsonObject();
        try d.put(a, &answer, "type", .{ .string = @tagName(kind) });
        switch (wire) {
            .answer_resource_predicate => |predicate| {
                try checkName(predicate.name, name);
                if (kind != .noul) return error.InvalidDecisionOutput;
                try d.put(a, &answer, "noul", .{ .float = predicate.probability });
            },
            .answer_resource_choice => |choice| {
                try checkName(choice.name, name);
                if (kind != .choice or choice.choice != .string) return error.InvalidDecisionOutput;
                var distribution = d.jsonObject();
                for (choice.probabilities) |entry| {
                    // Antfly supplies string option IDs; a boolean is a different value.
                    if (entry.value != .string) return error.InvalidDecisionOutput;
                    try addProbability(a, &distribution, entry.value.string, entry.probability);
                }
                try d.put(a, &answer, "choice", choice.choice);
                try d.put(a, &answer, "probabilities", distribution);
                try d.put(a, &answer, "confidence", .{ .float = choice.confidence });
            },
            .answer_resource_score => |score| {
                try checkName(score.name, name);
                if (kind != .score) return error.InvalidDecisionOutput;
                const count = question.object.get("criteria").?.array.items.len;
                var distribution = d.jsonObject();
                for (score.probabilities) |entry| {
                    if (entry.value < 0 or entry.value >= count) return error.InvalidDecisionOutput;
                    const key = try std.fmt.allocPrint(a, "{d}", .{entry.value});
                    if (!std.mem.eql(u8, entry.label, key)) return error.InvalidDecisionOutput;
                    try addProbability(a, &distribution, key, entry.probability);
                }
                try d.put(a, &answer, "score", .{ .float = score.score });
                try d.put(a, &answer, "probabilities", distribution);
                try d.put(a, &answer, "confidence", .{ .float = score.confidence });
            },
            // The shared contract requires a scored answer for every question.
            // Refusals fail the query through its existing invalid-output path.
            .answer_resource_refusal => return error.InvalidDecisionOutput,
        }
        try d.put(a, &answers, name, answer);
    }
    var result = source;
    try d.put(a, &result, "answers", answers);
    // Validate probabilities and compute choice/expected ordinal score centrally.
    return d.normalizeResponse(a, questions, result);
}

const test_questions =
    \\{"safe":{"type":"noul","instructions":"The command is safe."},"intent":{"type":"choice","instructions":"Intent?","criteria":{"read":"Read files","write":"Modify files"}},"risk":{"type":"score","instructions":"Risk?","criteria":["Low","High"]}}
;
const test_usage =
    \\"usage":{"input_tokens":42,"input_tokens_details":{"cached_tokens":4,"cache_write_tokens":0},"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":42}
;
const test_response =
    \\{"model":"resolved-model","answers":[{"type":"predicate","name":"safe","probability":0.95},{"type":"choice","name":"intent","choice":"write","confidence":0.7,"probabilities":[{"value":"write","probability":0.2},{"value":"read","probability":0.8}]},{"type":"score","name":"risk","score":99,"confidence":0.6,"probabilities":[{"label":"1","value":1,"probability":0.3},{"label":"0","value":0,"probability":0.7}]}],
++ test_usage ++ "}";

test "decision functions OpenAI generated wire types preserve instructions and ordinal identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const questions = try std.json.parseFromSliceLeaky(d.Json, a, test_questions, .{});
    const body = try requestBody(a, "gpt-6-luna", "command: ls; cwd: /tmp; request: list files", questions);
    const parsed = try std.json.parseFromSliceLeaky(api.DecisionRequest, a, body, .{});
    try std.testing.expectEqualStrings("gpt-6-luna", parsed.model);
    try std.testing.expectEqualStrings("command: ls; cwd: /tmp; request: list files", parsed.input.string);
    try std.testing.expectEqual(@as(usize, 3), parsed.questions.len);
    const predicate = parsed.questions[0].question_param_predicate;
    try std.testing.expectEqualStrings("safe", predicate.name.?);
    try std.testing.expectEqualStrings("The command is safe.", predicate.instructions);
    const choice = parsed.questions[1].question_param_choice;
    try std.testing.expectEqualStrings("read", choice.choices[0].value.string);
    try std.testing.expectEqualStrings("Read files", choice.choices[0].description.?);
    const score = parsed.questions[2].question_param_score;
    try std.testing.expectEqualStrings("0", score.levels[0].label);
    try std.testing.expectEqualStrings("Low", score.levels[0].description.?);
    try std.testing.expectEqualStrings("1", score.levels[1].label);
    try std.testing.expectEqualStrings("High", score.levels[1].description.?);
    try std.testing.expect(parsed.safety_identifier == .absent);

    const source = try std.json.parseFromSliceLeaky(d.Json, a, test_response, .{});
    const result = try response(a, questions, source);
    try std.testing.expectEqualStrings("resolved-model", result.object.get("model").?.string);
    const answers = result.object.get("answers").?.object;
    try std.testing.expectApproxEqAbs(@as(f64, 0.95), answers.get("safe").?.object.get("noul").?.float, 1e-9);
    // The shared normalizer derives values from complete distributions.
    try std.testing.expectEqualStrings("read", answers.get("intent").?.object.get("choice").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 0.3), answers.get("risk").?.object.get("score").?.float, 1e-9);
    try std.testing.expectEqualStrings("High", answers.get("risk").?.object.get("legend").?.object.get("1").?.string);
    const usage = result.object.get("usage").?.object;
    try std.testing.expectEqual(@as(i64, 42), usage.get("input_tokens").?.integer);
    try std.testing.expectEqual(@as(i64, 4), usage.get("input_tokens_details").?.object.get("cached_tokens").?.integer);
}

test "decision functions OpenAI refuses incomplete ambiguous or invalid distributions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const predicate = try d.questionsFor(a, .ai_probability, &.{ .{ .string = "context" }, .{ .string = "Safe?" }, .{ .string = "openai" } });
    const choice = try d.questionsFor(a, .ai_choice, &.{ .{ .string = "context" }, .{ .string = "Intent?" }, .{ .string = "{\"read\":\"Read\",\"write\":\"Write\"}" }, .{ .string = "openai" } });
    const score = try d.questionsFor(a, .ai_score, &.{ .{ .string = "context" }, .{ .string = "Risk?" }, .{ .string = "[\"Low\",\"High\"]" }, .{ .string = "openai" } });
    const Case = struct { questions: d.Json, answers: []const u8 };
    for ([_]Case{
        .{ .questions = predicate, .answers = "[]" },
        .{ .questions = predicate, .answers = "[{\"type\":\"refusal\",\"name\":\"answer\"}]" },
        .{ .questions = predicate, .answers = "[{\"type\":\"unknown\",\"name\":\"answer\"}]" },
        .{ .questions = predicate, .answers = "[{\"type\":\"predicate\",\"name\":null,\"probability\":0.9}]" },
        .{ .questions = predicate, .answers = "[{\"type\":\"predicate\",\"name\":\"wrong\",\"probability\":0.9}]" },
        .{ .questions = predicate, .answers = "[{\"type\":\"predicate\",\"name\":\"answer\",\"probability\":1.1}]" },
        .{ .questions = predicate, .answers = "[{\"type\":\"predicate\",\"name\":\"answer\"}]" },
        .{ .questions = choice, .answers = "[{\"type\":\"predicate\",\"name\":\"answer\",\"probability\":0.9}]" },
        .{ .questions = choice, .answers = "[{\"type\":\"choice\",\"name\":\"answer\",\"choice\":\"read\",\"confidence\":0.5,\"probabilities\":[{\"value\":\"read\",\"probability\":1}]}]" },
        .{ .questions = choice, .answers = "[{\"type\":\"choice\",\"name\":\"answer\",\"choice\":\"read\",\"confidence\":0.5,\"probabilities\":[{\"value\":\"read\",\"probability\":0.5},{\"value\":\"read\",\"probability\":0.5}]}]" },
        .{ .questions = choice, .answers = "[{\"type\":\"choice\",\"name\":\"answer\",\"choice\":true,\"confidence\":0.5,\"probabilities\":[{\"value\":true,\"probability\":0.5},{\"value\":false,\"probability\":0.5}]}]" },
        .{ .questions = choice, .answers = "[{\"type\":\"choice\",\"name\":\"answer\",\"choice\":\"read\",\"confidence\":0.5,\"probabilities\":[{\"value\":\"read\",\"probability\":0.3},{\"value\":\"write\",\"probability\":0.3}]}]" },
        .{ .questions = score, .answers = "[{\"type\":\"score\",\"name\":\"answer\",\"score\":0.5,\"confidence\":0.5,\"probabilities\":[{\"value\":0,\"label\":\"0\",\"probability\":0.5},{\"value\":2,\"label\":\"2\",\"probability\":0.5}]}]" },
        .{ .questions = score, .answers = "[{\"type\":\"score\",\"name\":\"answer\",\"score\":0.5,\"confidence\":0.5,\"probabilities\":[{\"value\":0,\"label\":\"1\",\"probability\":0.5},{\"value\":1,\"label\":\"0\",\"probability\":0.5}]}]" },
    }) |case| {
        const bytes = try std.fmt.allocPrint(a, "{{\"model\":\"test\",\"answers\":{s},{s}}}", .{ case.answers, test_usage });
        const source = try std.json.parseFromSliceLeaky(d.Json, a, bytes, .{});
        try std.testing.expectError(error.InvalidDecisionOutput, response(a, case.questions, source));
    }
    const all = try std.json.parseFromSliceLeaky(d.Json, a, test_questions, .{});
    const reordered = try std.json.parseFromSliceLeaky(d.Json, a, test_response, .{});
    const items = reordered.object.get("answers").?.array.items;
    std.mem.swap(d.Json, &items[0], &items[1]);
    try std.testing.expectError(error.InvalidDecisionOutput, response(a, all, reordered));
}
