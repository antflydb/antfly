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

//! Allocation-free predicate validation shared by Decisions clients. Optional
//! Antfly metadata cannot change the meaning of an OpenAI predicate answer.
const std = @import("std");
const V = std.json.Value;
pub const max_questions = 64;
pub fn field(value: V, name: []const u8) ?V {
    return if (value == .object) value.object.get(name) else null;
}
fn is(value: ?V, expected: []const u8) bool {
    const v = value orelse return false;
    return v == .string and std.mem.eql(u8, v.string, expected);
}
pub fn probabilities(row: V, names: []const []const u8, output: []f64) !void {
    if (names.len == 0 or names.len > max_questions or output.len != names.len) return error.InvalidDecisionResponse;
    const answers = field(row, "answers") orelse return error.InvalidDecisionResponse;
    if (answers != .array or answers.array.items.len != names.len) return error.InvalidDecisionResponse;
    // At most 64 entries: bounded stack storage, no per-answer allocations.
    var seen: u64 = 0;
    for (answers.array.items) |answer| {
        const name = field(answer, "name") orelse return error.InvalidDecisionResponse;
        if (name != .string) return error.InvalidDecisionResponse;
        const index = for (names, 0..) |wanted, i| {
            if (std.mem.eql(u8, name.string, wanted)) break i;
        } else return error.InvalidDecisionResponse;
        const bit = @as(u64, 1) << @as(u6, @intCast(index));
        if (seen & bit != 0) return error.InvalidDecisionResponse;
        seen |= bit;
        if (is(field(answer, "type"), "refusal")) return error.DecisionRefused;
        if (!is(field(answer, "type"), "predicate")) return error.InvalidDecisionResponse;
        if (field(answer, "decision_method")) |method| if (!is(method, "typed")) return error.InvalidDecisionResponse;
        if (is(field(answer, "status"), "abstained")) return error.DecisionAbstained;
        if (field(answer, "abstained")) |abstained| {
            if (abstained != .bool) return error.InvalidDecisionResponse;
            if (abstained.bool) return error.DecisionAbstained;
        }
        const raw = field(answer, "probability") orelse return error.InvalidDecisionResponse;
        const probability: f64 = switch (raw) {
            .float => raw.float,
            .integer => @floatFromInt(raw.integer),
            else => return error.InvalidDecisionResponse,
        };
        if (!std.math.isFinite(probability) or probability < 0 or probability > 1) return error.InvalidDecisionResponse;
        output[index] = probability;
    }
}

test "common predicate answers omit metadata and preserve incomplete evidence" {
    const a = std.testing.allocator;
    const cases = [_]struct { body: []const u8, err: ?anyerror }{
        .{ .body = "{\"answers\":[{\"type\":\"predicate\",\"name\":\"yes\",\"probability\":0.75}]}", .err = null },
        .{ .body = "{\"answers\":[{\"type\":\"refusal\",\"name\":\"yes\"}]}", .err = error.DecisionRefused },
        .{ .body = "{\"answers\":[{\"type\":\"predicate\",\"name\":\"yes\",\"abstained\":true}]}", .err = error.DecisionAbstained },
        .{ .body = "{\"answers\":[{\"type\":\"predicate\",\"name\":\"yes\",\"decision_method\":\"similarity\",\"probability\":0.75}]}", .err = error.InvalidDecisionResponse },
    };
    for (cases) |case| {
        const parsed = try std.json.parseFromSlice(V, a, case.body, .{ .duplicate_field_behavior = .@"error" });
        defer parsed.deinit();
        var scores: [1]f64 = undefined;
        if (case.err) |err| try std.testing.expectError(err, probabilities(parsed.value, &.{"yes"}, &scores)) else {
            try probabilities(parsed.value, &.{"yes"}, &scores);
            try std.testing.expectEqual(@as(f64, 0.75), scores[0]);
        }
    }
}

test "predicate decoding validates reordered names duplicates and numeric domains" {
    const a = std.testing.allocator;
    const cases = [_]struct { body: []const u8, valid: bool }{
        .{ .body = "{\"answers\":[{\"name\":\"b\",\"type\":\"predicate\",\"probability\":1},{\"name\":\"a\",\"type\":\"predicate\",\"probability\":0}]}", .valid = true },
        .{ .body = "{\"answers\":[{\"name\":\"a\",\"type\":\"predicate\",\"probability\":1},{\"name\":\"a\",\"type\":\"predicate\",\"probability\":0}]}", .valid = false },
        .{ .body = "{\"answers\":[{\"name\":\"a\",\"type\":\"predicate\",\"probability\":1}]}", .valid = false },
        .{ .body = "{\"answers\":[{\"name\":\"a\",\"type\":\"predicate\",\"probability\":-0.1},{\"name\":\"b\",\"type\":\"predicate\",\"probability\":0}]}", .valid = false },
        .{ .body = "{\"answers\":[{\"name\":\"a\",\"type\":\"predicate\",\"probability\":\"0.9\"},{\"name\":\"b\",\"type\":\"predicate\",\"probability\":0}]}", .valid = false },
        .{ .body = "{\"answers\":[{\"name\":\"a\",\"type\":\"predicate\",\"probability\":1e1000},{\"name\":\"b\",\"type\":\"predicate\",\"probability\":0}]}", .valid = false },
    };
    for (cases) |case| {
        const parsed = try std.json.parseFromSlice(V, a, case.body, .{});
        defer parsed.deinit();
        var scores: [2]f64 = undefined;
        if (!case.valid) try std.testing.expectError(error.InvalidDecisionResponse, probabilities(parsed.value, &.{ "a", "b" }, &scores)) else {
            try probabilities(parsed.value, &.{ "a", "b" }, &scores);
            try std.testing.expectEqualSlices(f64, &.{ 0, 1 }, &scores);
        }
    }
}
