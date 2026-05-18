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

pub const DefaultPrompt = "Describe this image.";
pub const SingleImagePromptTemplate =
    \\Describe this image in detail.
    \\
    \\Respond ONLY with valid JSON in this exact format:
    \\{{
    \\  "description": "A detailed description of the image",
    \\  "mood": "The emotional tone (e.g., happy, sad, funny, exciting)",
    \\  "possible_source": "Where this might be from (e.g., photo, artwork, screenshot)",
    \\  "tags": ["tag1", "tag2", "tag3"]
    \\}}
    \\
    \\{s}
;

pub fn buildSingleImagePrompt(allocator: std.mem.Allocator, user_prompt: ?[]const u8) ![]u8 {
    const prompt = if (user_prompt) |value|
        if (std.mem.trim(u8, value, " \t\r\n").len > 0) value else DefaultPrompt
    else
        DefaultPrompt;
    return std.fmt.allocPrint(allocator, SingleImagePromptTemplate, .{prompt});
}

test "moondream prompt builder uses default instruction" {
    const allocator = std.testing.allocator;
    const prompt = try buildSingleImagePrompt(allocator, null);
    defer allocator.free(prompt);

    try std.testing.expect(std.mem.indexOf(u8, prompt, "Describe this image.") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt, "\"description\"") != null);
}

test "moondream prompt builder embeds natural language prompt" {
    const allocator = std.testing.allocator;
    const prompt = try buildSingleImagePrompt(allocator, "Summarize this scene.");
    defer allocator.free(prompt);

    try std.testing.expect(std.mem.indexOf(u8, prompt, "Summarize this scene.") != null);
}
