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

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.write(switch (self) {
            .antfly => "antfly",
            .openai => "openai",
            .vertex => "vertex",
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

    pub fn resolvedUrl(self: Config) ?[]const u8 {
        return self.url orelse self.api_url;
    }
};
