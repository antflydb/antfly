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
const Allocator = std.mem.Allocator;

pub const Provider = enum {
    antfly,
    openai,
    cloudflare,
    vertex,
    apple,

    /// These adapters consume an already acquired recording through
    /// RemoteOptions.resolved_audio. Antfly's local callback remains URL-based.
    pub fn supportsResolvedAudio(self: @This()) bool {
        return switch (self) {
            .openai, .vertex, .cloudflare, .apple => true,
            .antfly => false,
        };
    }

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.write(switch (self) {
            .antfly => "antfly",
            .cloudflare => "cloudflare",
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
        if (std.mem.eql(u8, raw, "cloudflare")) return .cloudflare;
        if (std.mem.eql(u8, raw, "openai")) return .openai;
        if (std.mem.eql(u8, raw, "vertex")) return .vertex;
        if (std.mem.eql(u8, raw, "apple")) return .apple;
        return error.UnexpectedToken;
    }
};
