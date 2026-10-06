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

const Allocator = std.mem.Allocator;

pub const default_native_batch_size: usize = 8;
pub const max_native_batch_size: usize = 64;

/// Applies the process-wide native reader microbatch policy without importing
/// an inference backend. Both the caller-side planner and concrete executor
/// supply the same environment value to this authority.
pub fn nativeBatchSize(configured: ?usize) usize {
    return std.math.clamp(configured orelse default_native_batch_size, 1, max_native_batch_size);
}

test "native reader batch policy defaults and clamps" {
    try std.testing.expectEqual(default_native_batch_size, nativeBatchSize(null));
    try std.testing.expectEqual(@as(usize, 1), nativeBatchSize(0));
    try std.testing.expectEqual(max_native_batch_size, nativeBatchSize(max_native_batch_size + 1));
}

/// Language-neutral reader configuration shared by admission and inference.
/// This module deliberately has no HTTP, provider SDK, or inference runtime
/// imports so storage-side validation cannot pull those graphs into codegen.
pub const Provider = enum {
    antfly,
    openai,
    vertex,
    apple,

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.write(switch (self) {
            .antfly => "antfly",
            .openai => "openai",
            .vertex => "vertex",
            .apple => "apple",
        });
    }

    pub fn jsonParse(_: Allocator, source: anytype, _: std.json.ParseOptions) !@This() {
        const raw = switch (try source.next()) {
            .string => |value| value,
            else => return error.UnexpectedToken,
        };
        if (std.mem.eql(u8, raw, "antfly")) return .antfly;
        if (std.mem.eql(u8, raw, "openai")) return .openai;
        if (std.mem.eql(u8, raw, "vertex")) return .vertex;
        if (std.mem.eql(u8, raw, "apple")) return .apple;
        return error.UnexpectedToken;
    }
};

pub const Config = struct {
    provider: Provider,
    model: ?[]const u8 = null,
    prompt: ?[]const u8 = null,
    max_tokens: ?i64 = null,
    api_key: ?[]const u8 = null,
    bearer_token: ?[]const u8 = null,
    capability_token: ?[]const u8 = null,
    capability_revision: ?[]const u8 = null,
    base_url: ?[]const u8 = null,
    url: ?[]const u8 = null,
    api_url: ?[]const u8 = null,
    project_id: ?[]const u8 = null,
    location: ?[]const u8 = null,
    credentials_path: ?[]const u8 = null,
    recognition_languages: []const []const u8 = &.{"en-US"},
    recognition_level: enum { accurate, fast } = .accurate,
    uses_language_correction: bool = false,

    pub fn validate(self: Config) !void {
        if (self.provider == .antfly) {
            const model = self.model orelse return error.InvalidReaderConfig;
            if (std.mem.trim(u8, model, " \t\r\n").len == 0)
                return error.InvalidReaderConfig;
        }
        if (self.provider == .apple) {
            if (self.model != null) return error.InvalidAppleReaderConfig;
            if (self.api_key != null or self.bearer_token != null or
                self.capability_token != null or self.capability_revision != null or
                self.base_url != null or self.url != null or self.api_url != null or
                self.project_id != null or self.location != null or self.credentials_path != null)
                return error.InvalidAppleReaderConfig;
            try validateAppleRequest(self.prompt, self.max_tokens);
            if (self.recognition_languages.len == 0 or self.recognition_languages.len > 16)
                return error.InvalidAppleReaderConfig;
            for (self.recognition_languages) |language| {
                if (language.len == 0 or language.len > 64) return error.InvalidAppleReaderConfig;
                for (language) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '-')
                    return error.InvalidAppleReaderConfig;
            }
        }
    }

    pub fn resolvedUrl(self: Config) ?[]const u8 {
        return self.url orelse self.api_url;
    }
};

/// The canonical OCR marker is supplied by the PDF pipeline. Vision does not
/// implement captioning, prompt following, or generated-token budgets.
pub fn validateAppleRequest(prompt: ?[]const u8, max_tokens: ?i64) !void {
    if (max_tokens != null) return error.UnsupportedAppleOcrOptions;
    if (prompt) |value| {
        const trimmed = std.mem.trim(u8, value, " \t\r\n");
        if (trimmed.len > 0 and !std.mem.eql(u8, trimmed, "<OCR>"))
            return error.UnsupportedAppleOcrOptions;
    }
}

test "apple OCR configuration validates native-only options" {
    try (Config{ .provider = .apple }).validate();
    try (Config{ .provider = .apple, .prompt = "<OCR>" }).validate();
    try std.testing.expectError(error.InvalidAppleReaderConfig, (Config{ .provider = .apple, .model = "vision-text" }).validate());
    try std.testing.expectError(error.InvalidAppleReaderConfig, (Config{ .provider = .apple, .model = "" }).validate());
    try std.testing.expectError(error.InvalidAppleReaderConfig, (Config{ .provider = .apple, .url = "http://localhost" }).validate());
    try std.testing.expectError(error.InvalidAppleReaderConfig, (Config{ .provider = .apple, .model = "system" }).validate());
    try std.testing.expectError(error.InvalidAppleReaderConfig, (Config{ .provider = .apple, .recognition_languages = &.{} }).validate());
    try std.testing.expectError(error.UnsupportedAppleOcrOptions, (Config{ .provider = .apple, .prompt = "Describe this image" }).validate());
    try std.testing.expectError(error.UnsupportedAppleOcrOptions, (Config{ .provider = .apple, .max_tokens = 256 }).validate());
}

test "antfly reader requires an explicit routing model" {
    try std.testing.expectError(error.InvalidReaderConfig, (Config{ .provider = .antfly }).validate());
    try std.testing.expectError(error.InvalidReaderConfig, (Config{ .provider = .antfly, .model = " \t" }).validate());
    try (Config{ .provider = .antfly, .model = "antflydb/Florence-2-base" }).validate();
    try (Config{ .provider = .openai }).validate();
}
