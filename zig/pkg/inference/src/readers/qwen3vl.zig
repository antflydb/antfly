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

pub const DefaultPrompt = "Transcribe all visible text exactly. Preserve the original reading order, line breaks, punctuation, and accents. Return only the transcription.";

/// Use the OCR default for omitted or blank prompts; preserve custom prompts verbatim.
pub fn resolvePrompt(prompt: ?[]const u8) []const u8 {
    const value = prompt orelse return DefaultPrompt;
    return if (std.mem.trim(u8, value, " \t\r\n").len == 0) DefaultPrompt else value;
}

test "Qwen3-VL read prompt defaults to OCR and preserves document modes" {
    try std.testing.expectEqualStrings(DefaultPrompt, resolvePrompt(null));
    try std.testing.expectEqualStrings(DefaultPrompt, resolvePrompt(""));
    try std.testing.expectEqualStrings(DefaultPrompt, resolvePrompt(" \n\t\r"));
    try std.testing.expectEqualStrings("qwenvl markdown", resolvePrompt("qwenvl markdown"));
    try std.testing.expectEqualStrings("qwenvl html", resolvePrompt("qwenvl html"));
    try std.testing.expectEqualStrings("Extract the invoice number only.", resolvePrompt("Extract the invoice number only."));
    try std.testing.expectEqualStrings(" \nTranscribe this page.\t ", resolvePrompt(" \nTranscribe this page.\t "));
}
