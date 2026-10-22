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
const shared = @import("antfly_extracting").text_processing.alignment;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Mention = shared.Mention;
pub const regexCaseEquivalent = shared.regexCaseEquivalent;
pub const Options = struct { max_steps: usize = 10000000, max_mentions: usize = 65536, control: ?Control = null };
fn checkpoint(ptr: *const anyopaque) !void {
    const control: *const Control = @ptrCast(@alignCast(ptr));
    try control.check();
}
pub fn findLiteralMentions(a: std.mem.Allocator, text: []const u8, choices: []const []const u8, options: Options) ![]Mention {
    return shared.findLiteralMentions(a, text, choices, .{ .max_steps = options.max_steps, .max_mentions = options.max_mentions, .control = if (options.control) |*control| .{ .ptr = control, .check_fn = checkpoint } else null });
}

test "record enum literals match pinned Python Unicode regex fixture" {
    const Fixture = struct { format_version: u32, provenance: std.json.Value, pairs: usize, cases: []const struct { text: []const u8, choices: []const []const u8, matches: []const Mention } };
    const bytes = try @import("../architectures/gliner/boundary_parity_test.zig").fixtureBytes(std.testing.allocator, "case_equivalence.json");
    defer std.testing.allocator.free(bytes);
    const parsed = try std.json.parseFromSlice(Fixture, std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    for (parsed.value.cases) |case| {
        const got = try findLiteralMentions(std.testing.allocator, case.text, case.choices, .{});
        defer std.testing.allocator.free(got);
        try std.testing.expectEqualDeep(case.matches, got);
    }
    try std.testing.expectError(error.EnumMatchingLimitExceeded, findLiteralMentions(std.testing.allocator, "text", &.{"text"}, .{ .max_steps = 0 }));
}
