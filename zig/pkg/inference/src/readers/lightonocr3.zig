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

//! LightOnOCR-3's trained prompt contract. Do not substitute a generic OCR
//! instruction: transcription is image-only, and grounding is an exact keyword.
const std = @import("std");

/// Per-page OCR completion budget when the caller omits max_tokens.
/// Request and deployment limits remain authoritative.
pub const default_max_tokens: usize = 4096;

pub const repository = "lightonai/LightOnOCR-3-0.8B";
pub const revision = "36d8636925327c671bd8e479c517a004f7566bd9";
pub const pinned_ref = repository ++ ":safetensors@" ++ revision;
pub const repository_4b = "lightonai/LightOnOCR-3-4B";
pub const revision_4b = "b71010f095dc3735de7ee6b9969bff8f8f50b39a";
pub const pinned_ref_4b = repository_4b ++ ":safetensors@" ++ revision_4b;
pub const manifest_json =
    \\{"type":"generator","tasks":["generate","read"],"inputs":["text","image"],"capabilities":["ocr","lightonocr3"]}
;

/// The upstream template includes tool macros unsupported by our Jinja
/// parser. This page-reader subset preserves its image and non-thinking
/// assistant framing exactly, without adding a system instruction.
pub const chat_template =
    "{% for message in messages %}" ++
    "{{ '<|im_start|>' + message['role'] + '\\n' }}" ++
    "{% if message['content'] is string %}{{ message['content'] }}" ++
    "{% else %}{% for item in message['content'] %}" ++
    "{% if item['type'] == 'image' %}{{ '<|vision_start|><|image_pad|><|vision_end|>' }}" ++
    "{% elif item['type'] == 'text' %}{{ item['text'] }}{% endif %}" ++
    "{% endfor %}{% endif %}{{ '<|im_end|>\\n' }}{% endfor %}" ++
    "{{ '<|im_start|>assistant\\n<think>\\n\\n</think>\\n\\n' }}";

pub fn isHubSource(owner: []const u8, name: []const u8, format: []const u8) bool {
    return std.mem.eql(u8, owner, "lightonai") and (std.mem.eql(u8, name, "LightOnOCR-3-0.8B") or std.mem.eql(u8, name, "LightOnOCR-3-4B")) and
        (std.mem.eql(u8, format, "auto") or std.mem.eql(u8, format, "safetensors"));
}

pub fn resolvePrompt(prompt: ?[]const u8) ![]const u8 {
    const value = std.mem.trim(u8, prompt orelse "", " \t\r\n");
    if (value.len == 0) return "";
    if (std.mem.eql(u8, value, "grounding")) return "grounding";
    return error.InvalidLightOnOcrPrompt;
}

test "LightOnOCR3 uses image-only transcription and the trained grounding prompt" {
    try std.testing.expectEqualStrings("", try resolvePrompt(null));
    try std.testing.expectEqualStrings("", try resolvePrompt(" \n\t"));
    try std.testing.expectEqualStrings("grounding", try resolvePrompt("grounding"));
    try std.testing.expectError(error.InvalidLightOnOcrPrompt, resolvePrompt("Transcribe this page."));
}

test "LightOnOCR3 installs its reader profile only for the canonical integrated checkpoint" {
    try std.testing.expect(isHubSource("lightonai", "LightOnOCR-3-0.8B", "auto"));
    try std.testing.expect(isHubSource("lightonai", "LightOnOCR-3-0.8B", "safetensors"));
    try std.testing.expect(!isHubSource("other", "LightOnOCR-3-0.8B", "safetensors"));
    try std.testing.expect(isHubSource("lightonai", "LightOnOCR-3-4B", "safetensors"));
    try std.testing.expect(!isHubSource("lightonai", "LightOnOCR-3-0.8B", "gguf"));
}
