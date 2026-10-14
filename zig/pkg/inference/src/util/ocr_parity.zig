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

const std = @import("std");

pub const OcrLogitSummary = struct { token: u32, runner_up: u32, margin: f32 };

pub fn ocrLogitSummary(logits: []const f32, suppressed: []const i32) !OcrLogitSummary {
    var first: ?usize = null;
    var second: ?usize = null;
    for (logits, 0..) |value, token| {
        if (!std.math.isFinite(value)) return error.InvalidModelOutput;
        if (token <= std.math.maxInt(i32) and std.mem.indexOfScalar(i32, suppressed, @intCast(token)) != null) continue;
        if (first == null or value > logits[first.?]) {
            second = first;
            first = token;
        } else if (second == null or value > logits[second.?]) second = token;
    }
    const top = first orelse return error.InvalidModelOutput;
    const next = second orelse return error.InvalidModelOutput;
    return .{ .token = @intCast(top), .runner_up = @intCast(next), .margin = logits[top] - logits[next] };
}
