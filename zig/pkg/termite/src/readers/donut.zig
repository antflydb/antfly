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

const std = @import("std");

pub fn docVqaPrompt(allocator: std.mem.Allocator, question: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "<s_docvqa><s_question>{s}</s_question><s_answer>", .{question});
}

pub fn cordPrompt() []const u8 {
    return "<s_cord-v2>";
}

pub fn rvlcdipPrompt() []const u8 {
    return "<s_rvlcdip>";
}

pub fn parseDocVqaAnswer(allocator: std.mem.Allocator, text: []const u8) ![]const u8 {
    const close_tag = "</s_answer>";
    const answer = if (std.mem.indexOf(u8, text, close_tag)) |idx|
        std.mem.trim(u8, text[0..idx], " \t\r\n")
    else
        std.mem.trim(u8, text, " \t\r\n");
    return allocator.dupe(u8, answer);
}

test "donut prompt helpers render expected task tokens" {
    const allocator = std.testing.allocator;
    const prompt = try docVqaPrompt(allocator, "What is the total?");
    defer allocator.free(prompt);

    try std.testing.expectEqualStrings("<s_docvqa><s_question>What is the total?</s_question><s_answer>", prompt);
    try std.testing.expectEqualStrings("<s_cord-v2>", cordPrompt());
    try std.testing.expectEqualStrings("<s_rvlcdip>", rvlcdipPrompt());
}

test "donut docvqa parser trims trailing answer wrapper" {
    const allocator = std.testing.allocator;
    const answer = try parseDocVqaAnswer(allocator, "Invoice</s_answer></s_docvqa>");
    defer allocator.free(answer);

    try std.testing.expectEqualStrings("Invoice", answer);
}
