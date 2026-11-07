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

//! Translate upstream captured decision fixtures at the public API boundary.
const std = @import("std");
const decisions = @import("antfly_decisions");

pub fn requestJson(a: std.mem.Allocator, captured: []const u8, model: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, captured, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const root = parsed.value.object;
    const questions = root.get("questions").?;
    // Captured ordinal criteria use their descriptions as display labels.
    for (questions.object.values()) |*question| {
        if (std.mem.eql(u8, question.object.get("type").?.string, "score"))
            try question.object.put(parsed.arena.allocator(), "level_labels", question.object.get("criteria").?);
    }
    return decisions.requestJson(a, model, root.get("state").?.string, questions, null, null);
}

fn validateConfidence(answer: std.json.Value) !void {
    if (answer != .object) return error.InvalidDecideOutput;
    const raw = answer.object.get("confidence") orelse return error.InvalidDecideOutput;
    const confidence: f64 = switch (raw) {
        .float => |value| value,
        .integer => |value| @floatFromInt(value),
        else => return error.InvalidDecideOutput,
    };
    if (!std.math.isFinite(confidence) or confidence < 0 or confidence > 1) return error.InvalidDecideOutput;
    const method = answer.object.get("confidence_method") orelse return error.InvalidDecideOutput;
    if (method != .string or !(std.mem.eql(u8, method.string, "normalized_inverse_entropy") or
        std.mem.eql(u8, method.string, "max_probability"))) return error.InvalidDecideOutput;
}

pub fn comparisonValue(a: std.mem.Allocator, response: std.json.Value) !std.json.Value {
    if (response != .object) return error.InvalidDecideOutput;
    const answers = response.object.get("answers") orelse return error.InvalidDecideOutput;
    const public = answers == .array;
    switch (answers) {
        .array => |rows| for (rows.items) |answer| {
            try validateConfidence(answer);
            const method = answer.object.get("decision_method") orelse return error.InvalidDecideOutput;
            if (method != .string or !std.mem.eql(u8, method.string, "typed")) return error.InvalidDecideOutput;
        },
        .object => |rows| for (rows.values()) |answer| try validateConfidence(answer),
        else => return error.InvalidDecideOutput,
    }
    const lowered = try decisions.internalResponse(a, response);
    var normalized_answers: std.json.ObjectMap = .empty;
    for (lowered.object.get("answers").?.object.keys(), lowered.object.get("answers").?.object.values()) |name, answer| {
        var projected: std.json.ObjectMap = .empty;
        for (answer.object.keys(), answer.object.values()) |key, value| {
            // Both current private and public producers emit these diagnostics.
            // Historical learned-output captures do not. Validate them above,
            // then remove only the known presentation fields for comparison.
            if (std.mem.eql(u8, key, "confidence") or std.mem.eql(u8, key, "confidence_method")) continue;
            if (public and (std.mem.eql(u8, key, "name") or std.mem.eql(u8, key, "decision_method"))) continue;
            try projected.put(a, key, value);
        }
        try normalized_answers.put(a, name, .{ .object = projected });
    }
    var normalized: std.json.ObjectMap = .empty;
    for (lowered.object.keys(), lowered.object.values()) |key, value|
        try normalized.put(a, key, if (std.mem.eql(u8, key, "answers")) .{ .object = normalized_answers } else value);
    return .{ .object = normalized };
}

test "GLiNER captured decisions translate to the public contract and retain reference labels" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try decisions.parse(a, try requestJson(a,
        \\{"model":"upstream","state":"incident","questions":{"risk":{"type":"score","instructions":"Risk?","criteria":["Low","High"]},"act":{"type":"noul","instructions":"Act?"}}}
    , "model"));
    try std.testing.expectEqualStrings("incident", request.items[0].input);
    try std.testing.expectEqualStrings("Low", request.policies[0].level_labels[0]);
    const Label = struct { label: []const u8, confidence: f32 };
    const Classification = struct { name: []const u8, labels: []const Label };
    const classifications: []const Classification = &.{
        .{ .name = "risk", .labels = &.{ .{ .label = "0", .confidence = 0.25 }, .{ .label = "1", .confidence = 0.75 } } },
        .{ .name = "act", .labels = &.{ .{ .label = "false", .confidence = 0.25 }, .{ .label = "true", .confidence = 0.75 } } },
    };
    const response = try std.json.parseFromSliceLeaky(std.json.Value, a, try decisions.trainedClassifications(a, request, classifications, 10), .{});
    try std.testing.expect(response.object.get("answers").? == .array);
    const normalized = try comparisonValue(a, response);
    const answers = normalized.object.get("answers").?.object;
    const risk = answers.get("risk").?.object;
    try std.testing.expectEqual(@as(usize, 4), risk.count());
    try std.testing.expectEqual(@as(usize, 2), answers.get("act").?.object.count());
    try std.testing.expectEqualStrings("Low", risk.get("legend").?.object.get("0").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 0.75), risk.get("score").?.float, 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 0.75), answers.get("act").?.object.get("noul").?.float, 1e-9);
    try std.testing.expect(!risk.contains("decision_method"));
}

test "GLiNER captured private decisions validate diagnostics and preserve exact learned fields" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try decisions.legacy.parse(a,
        \\{"model":"model","state":"incident","questions":{"route":{"type":"choice","instructions":"Route?","criteria":{"low":"Low","high":"High"}}}}
    );
    const bytes = try decisions.legacy.responseJson(a, request,
        \\{"data":[{"classifications":[{"name":"route","label":"low","score":0.25},{"name":"route","label":"high","score":0.75}]}],"usage":{"prompt_tokens":10,"completion_tokens":0}}
    , .span_marker);
    const response = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{});
    const original = response.object.get("answers").?.object.get("route").?.object;
    try std.testing.expectEqual(@as(usize, 5), original.count());
    const normalized = try comparisonValue(a, response);
    const learned = normalized.object.get("answers").?.object.get("route").?.object;
    try std.testing.expectEqual(@as(usize, 3), learned.count());
    try std.testing.expectEqualStrings("choice", learned.get("type").?.string);
    try std.testing.expectEqualStrings("high", learned.get("choice").?.string);
    try std.testing.expectEqual(@as(usize, 2), learned.get("probabilities").?.object.count());
    try std.testing.expectEqual(@as(f64, 0.75), learned.get("probabilities").?.object.get("high").?.float);
    // Projection must neither mutate the response nor erase unexpected learned
    // fields that the original strict reference-fieldset comparison must reject.
    try std.testing.expectEqual(@as(usize, 5), response.object.get("answers").?.object.get("route").?.object.count());
    const extra = response;
    const answer = extra.object.get("answers").?.object.getPtr("route").?;
    try answer.object.put(a, "unexpected_learned", .{ .integer = 7 });
    const with_extra = try comparisonValue(a, extra);
    try std.testing.expectEqual(@as(usize, 4), with_extra.object.get("answers").?.object.get("route").?.object.count());
    try std.testing.expectEqual(@as(i64, 7), with_extra.object.get("answers").?.object.get("route").?.object.get("unexpected_learned").?.integer);
}

test "GLiNER captured decision confidence metadata rejects malformed private and public forms" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const malformed = [_][]const u8{
        "{}",
        "{\"confidence\":0.5}",
        "{\"confidence\":-0.1,\"confidence_method\":\"max_probability\"}",
        "{\"confidence\":1.1,\"confidence_method\":\"max_probability\"}",
        "{\"confidence\":\"0.5\",\"confidence_method\":\"max_probability\"}",
        "{\"confidence\":0.5,\"confidence_method\":7}",
        "{\"confidence\":0.5,\"confidence_method\":\"unqualified\"}",
    };
    for ([_]bool{ false, true }) |public| for (malformed) |raw| {
        var answer = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
        if (public) {
            try answer.object.put(a, "name", .{ .string = "route" });
            try answer.object.put(a, "decision_method", .{ .string = "typed" });
        }
        var root: std.json.ObjectMap = .empty;
        if (public) {
            var rows: std.array_list.Managed(std.json.Value) = .init(a);
            try rows.append(answer);
            try root.put(a, "answers", .{ .array = rows });
        } else {
            var rows: std.json.ObjectMap = .empty;
            try rows.put(a, "route", answer);
            try root.put(a, "answers", .{ .object = rows });
        }
        try std.testing.expectError(error.InvalidDecideOutput, comparisonValue(a, .{ .object = root }));
    };
    var nonfinite: std.json.ObjectMap = .empty;
    try nonfinite.put(a, "confidence", .{ .float = std.math.nan(f64) });
    try nonfinite.put(a, "confidence_method", .{ .string = "max_probability" });
    try std.testing.expectError(error.InvalidDecideOutput, validateConfidence(.{ .object = nonfinite }));
    const wrong_public_method = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"answers":[{"name":"route","decision_method":"legacy","confidence":1,"confidence_method":"max_probability"}]}
    , .{});
    try std.testing.expectError(error.InvalidDecideOutput, comparisonValue(a, wrong_public_method));
}
