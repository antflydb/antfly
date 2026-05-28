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

// Pix2Struct models use natural-language prompts directly for visual QA tasks.
pub fn docVqaPrompt(question: []const u8) []const u8 {
    return question;
}

pub fn chartQaPrompt(question: []const u8) []const u8 {
    return question;
}

pub fn infographicsPrompt(question: []const u8) []const u8 {
    return question;
}

test "pix2struct prompt helpers pass through natural language" {
    try std.testing.expectEqualStrings("What type of document is this?", docVqaPrompt("What type of document is this?"));
    try std.testing.expectEqualStrings("What is the highest value?", chartQaPrompt("What is the highest value?"));
    try std.testing.expectEqualStrings("What year had the most growth?", infographicsPrompt("What year had the most growth?"));
}
