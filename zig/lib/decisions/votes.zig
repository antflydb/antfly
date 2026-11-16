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

//! Allocation-free validation of public choice/predicate vote evidence.
//! Shared by local execution, upstream trace consumers, and resolver thresholds.
const std = @import("std");
const V = std.json.Value;
pub const Choice = struct {
    selected: []const u8,
    probability: f64,
    distribution: []const V,
    generated: bool,
    verdict: bool,
};
pub const Vote = union(enum) { choice: Choice, predicate: f64 };
fn text(v: V) ![]const u8 {
    if (v != .string or v.string.len == 0 or !std.unicode.utf8ValidateSlice(v.string)) return error.InvalidDecisionOutput;
    return v.string;
}
fn probability(v: V) !f64 {
    const p: f64 = switch (v) {
        .float => v.float,
        .integer => @floatFromInt(v.integer),
        else => return error.InvalidDecisionOutput,
    };
    if (!std.math.isFinite(p) or p < 0 or p > 1) return error.InvalidDecisionOutput;
    return p;
}
fn unique(names: [][]const u8) !void {
    std.mem.sort([]const u8, names, {}, struct {
        fn less(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.less);
    for (names[1..], names[0 .. names.len - 1]) |name, previous| if (std.mem.eql(u8, name, previous)) return error.InvalidDecisionOutput;
}
pub fn validateAnswer(answer: V) !Vote {
    if (answer != .object) return error.InvalidDecisionOutput;
    const fields = answer.object;
    _ = try text(fields.get("name") orelse return error.InvalidDecisionOutput);
    const kind = try text(fields.get("type") orelse return error.InvalidDecisionOutput);
    const method = try text(fields.get("decision_method") orelse return error.InvalidDecisionOutput);
    const generated = std.mem.eql(u8, method, "generated");
    if (!generated and !std.mem.eql(u8, method, "typed")) return error.InvalidDecisionOutput;
    var verdict = false;
    if (generated) {
        const source = try text(fields.get("probability_source") orelse return error.InvalidDecisionOutput);
        verdict = std.mem.eql(u8, source, "verdict");
        if (!verdict and !std.mem.eql(u8, source, "token_logprobs")) return error.InvalidDecisionOutput;
    }
    for ([_][]const u8{ "confidence", "act_probability" }) |key| if (fields.get(key)) |value| {
        _ = try probability(value);
    };
    if (std.mem.eql(u8, kind, "predicate")) {
        const p = try probability(fields.get("probability") orelse return error.InvalidDecisionOutput);
        if (generated) {
            const raw = fields.get("raw_structured_answer") orelse return error.InvalidDecisionOutput;
            if (raw != .bool or (verdict and p != @as(f64, if (raw.bool) 1 else 0))) return error.InvalidDecisionOutput;
        }
        return .{ .predicate = p };
    }
    if (!std.mem.eql(u8, kind, "choice")) return error.InvalidDecisionOutput;
    const selected = try text(fields.get("choice") orelse return error.InvalidDecisionOutput);
    if (generated and !std.mem.eql(u8, selected, try text(fields.get("raw_structured_answer") orelse return error.InvalidDecisionOutput))) return error.InvalidDecisionOutput;
    const dist = fields.get("probabilities") orelse return error.InvalidDecisionOutput;
    if (dist != .array or dist.array.items.len < 2 or dist.array.items.len > 64) return error.InvalidDecisionOutput;
    var names: [64][]const u8 = undefined;
    var selected_probability: ?f64 = null;
    var total: f64 = 0;
    var maximum: f64 = 0;
    for (dist.array.items, names[0..dist.array.items.len]) |item, *name| {
        if (item != .object) return error.InvalidDecisionOutput;
        name.* = try text(item.object.get("value") orelse return error.InvalidDecisionOutput);
        const p = try probability(item.object.get("probability") orelse return error.InvalidDecisionOutput);
        const chosen = std.mem.eql(u8, selected, name.*);
        if (chosen) selected_probability = p;
        if (verdict and p != @as(f64, if (chosen) 1 else 0)) return error.InvalidDecisionOutput;
        maximum = @max(maximum, p);
        total += p;
    }
    try unique(names[0..dist.array.items.len]);
    const p = selected_probability orelse return error.InvalidDecisionOutput;
    if (@abs(total - 1) > 1e-6 or (!generated and p != maximum)) return error.InvalidDecisionOutput;
    return .{ .choice = .{ .selected = selected, .probability = p, .distribution = dist.array.items, .generated = generated, .verdict = verdict } };
}
pub fn validateResponse(response: V, generated_required: bool) !void {
    if (response != .object) return error.InvalidDecisionOutput;
    const model = try text(response.object.get("model") orelse return error.InvalidDecisionOutput);
    if (model.len > 2048) return error.InvalidDecisionOutput;
    if (response.object.get("model_revision")) |raw| {
        const revision = try text(raw);
        if (revision.len != 64) return error.InvalidDecisionOutput;
        for (revision) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return error.InvalidDecisionOutput;
    }
    const answers = response.object.get("answers") orelse return error.InvalidDecisionOutput;
    try validateAnswers(answers);
    if (generated_required) for (answers.array.items) |answer| {
        if (!std.mem.eql(u8, answer.object.get("decision_method").?.string, "generated")) return error.InvalidDecisionOutput;
    };
}
pub fn validateAnswers(answers: V) !void {
    if (answers != .array or answers.array.items.len == 0 or answers.array.items.len > 64) return error.InvalidDecisionOutput;
    var names: [64][]const u8 = undefined;
    for (answers.array.items, names[0..answers.array.items.len]) |answer, *name| {
        _ = try validateAnswer(answer);
        name.* = answer.object.get("name").?.string;
    }
    try unique(names[0..answers.array.items.len]);
}

fn example(a: std.mem.Allocator) !V {
    return std.json.parseFromSliceLeaky(V, a,
        \\{"name":"same","type":"choice","decision_method":"typed","choice":"same","probabilities":[{"value":"same","probability":0.97},{"value":"different","probability":0.03}]}
    , .{});
}

test "vote evidence validates typed argmax and sampled generated choices without allocation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var answer = try example(a);
    try std.testing.expectEqual(@as(f64, 0.97), (try validateAnswer(answer)).choice.probability);
    try answer.object.put(a, "choice", .{ .string = "different" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
    try answer.object.put(a, "decision_method", .{ .string = "generated" });
    try answer.object.put(a, "raw_structured_answer", .{ .string = "different" });
    try answer.object.put(a, "probability_source", .{ .string = "token_logprobs" });
    try std.testing.expectEqual(@as(f64, 0.03), (try validateAnswer(answer)).choice.probability);
    try answer.object.put(a, "probability_source", .{ .string = "verdict" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
    const dist = answer.object.getPtr("probabilities").?.array.items;
    try dist[0].object.put(a, "probability", .{ .integer = 0 });
    try dist[1].object.put(a, "probability", .{ .integer = 1 });
    _ = try validateAnswer(answer);
    try answer.object.put(a, "raw_structured_answer", .{ .string = "same" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
    try answer.object.put(a, "raw_structured_answer", .{ .string = "different" });
    try answer.object.put(a, "probability_source", .{ .string = "invented" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
}

test "vote evidence rejects malformed distributions labels and question sets" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]f64{ -0.1, 1.1, std.math.inf(f64), std.math.nan(f64), 0.5 }) |bad| {
        const answer = try example(a);
        try answer.object.getPtr("probabilities").?.array.items[0].object.put(a, "probability", .{ .float = bad });
        try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
    }
    var answer = try example(a);
    try answer.object.getPtr("probabilities").?.array.items[1].object.put(a, "value", .{ .string = "same" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
    answer = try example(a);
    try answer.object.put(a, "choice", .{ .string = "unknown" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
    var list = std.json.Array.init(a);
    try list.append(try example(a));
    try validateAnswers(.{ .array = list });
    try list.append(try example(a));
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswers(.{ .array = list }));
    while (list.items.len < 65) try list.append(try example(a));
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswers(.{ .array = list }));
    var huge = std.json.Array.init(a);
    for (0..65) |_| try huge.append(answer.object.get("probabilities").?.array.items[0]);
    try answer.object.put(a, "probabilities", .{ .array = huge });
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
}

test "vote evidence validates predicate probabilities and categorical raw votes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var answer = try std.json.parseFromSliceLeaky(V, a,
        \\{"name":"p","type":"predicate","decision_method":"typed","probability":0.5}
    , .{});
    try std.testing.expectEqual(@as(f64, 0.5), (try validateAnswer(answer)).predicate);
    try answer.object.put(a, "decision_method", .{ .string = "generated" });
    try answer.object.put(a, "probability_source", .{ .string = "token_logprobs" });
    try answer.object.put(a, "raw_structured_answer", .{ .bool = false });
    _ = try validateAnswer(answer);
    try answer.object.put(a, "probability_source", .{ .string = "verdict" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
    try answer.object.put(a, "probability", .{ .integer = 0 });
    _ = try validateAnswer(answer);
    try answer.object.put(a, "raw_structured_answer", .{ .string = "false" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
    try answer.object.put(a, "decision_method", .{ .string = "unknown" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateAnswer(answer));
}

test "vote response validation binds the model revision and declared generated contract" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var answer = try example(a);
    var list = std.json.Array.init(a);
    try list.append(answer);
    var response = V{ .object = .empty };
    try response.object.put(a, "model", .{ .string = "model" });
    try response.object.put(a, "answers", .{ .array = list });
    try validateResponse(response, false);
    try std.testing.expectError(error.InvalidDecisionOutput, validateResponse(response, true));
    try answer.object.put(a, "decision_method", .{ .string = "generated" });
    try answer.object.put(a, "raw_structured_answer", .{ .string = "same" });
    try answer.object.put(a, "probability_source", .{ .string = "token_logprobs" });
    list.items[0] = answer;
    try validateResponse(response, true);
    for ([_][]const u8{ "short", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" }) |bad| {
        try response.object.put(a, "model_revision", .{ .string = bad });
        try std.testing.expectError(error.InvalidDecisionOutput, validateResponse(response, false));
    }
    try response.object.put(a, "model_revision", .{ .string = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" });
    try validateResponse(response, true);
    try response.object.put(a, "model", .{ .string = "" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateResponse(response, true));
}
