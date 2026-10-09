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

pub fn comparisonValue(a: std.mem.Allocator, response: std.json.Value) !std.json.Value {
    const answers = response.object.get("answers").?;
    if (answers == .object) return response; // Direct private-kernel reference.
    for (answers.array.items) |answer| {
        try std.testing.expectEqualStrings("typed", answer.object.get("decision_method").?.string);
        const confidence_value = answer.object.get("confidence") orelse return error.InvalidDecideOutput;
        const confidence: f64 = switch (confidence_value) {
            .float => |value| value,
            .integer => |value| @floatFromInt(value),
            else => return error.InvalidDecideOutput,
        };
        try std.testing.expect(std.math.isFinite(confidence) and confidence >= 0 and confidence <= 1);
        const method = answer.object.get("confidence_method") orelse return error.InvalidDecideOutput;
        try std.testing.expect(method == .string);
        try std.testing.expect(std.mem.eql(u8, method.string, "normalized_inverse_entropy") or std.mem.eql(u8, method.string, "max_probability"));
    }
    var lowered = try decisions.internalResponse(a, response);
    for (lowered.object.get("answers").?.object.values()) |*answer| {
        _ = answer.object.swapRemove("decision_method");
        // These diagnostics belong to the public contract. The historical
        // PyTorch captures contain only the learned decisions/distributions.
        _ = answer.object.swapRemove("name");
        _ = answer.object.swapRemove("confidence");
        _ = answer.object.swapRemove("confidence_method");
    }
    return lowered;
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
