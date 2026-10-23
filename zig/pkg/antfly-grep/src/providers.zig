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

//! Destination profiles for the common Decisions contract. Jev routing belongs
//! in the Antfly inference gateway, not in client-specific wire adapters.
const std = @import("std");
pub const Provider = struct { id: []const u8, label: []const u8, endpoint: []const u8, model: ?[]const u8 = null, protocol: enum { antfly, decisions, local } };
pub const catalog = [_]Provider{
    .{ .id = "antfly", .label = "Antfly", .endpoint = "http://127.0.0.1:8080/ai/v1/decisions", .protocol = .antfly },
    .{ .id = "openai", .label = "OpenAI", .endpoint = "https://api.openai.com/v1/decisions", .model = "gpt-6-luna", .protocol = .decisions },
    .{ .id = "local", .label = "Local inference", .endpoint = "", .protocol = .local },
    .{ .id = "custom", .label = "Custom Decisions endpoint", .endpoint = "", .protocol = .decisions },
};
pub fn get(id: []const u8) ?Provider {
    for (catalog) |provider| if (std.mem.eql(u8, id, provider.id)) return provider;
    return null;
}
pub fn removed(id: []const u8) bool {
    for ([_][]const u8{ "typesafe", "vercel", "openrouter", "opencode" }) |old| if (std.mem.eql(u8, id, old)) return true;
    return false;
}
pub fn validateCustom(endpoint: []const u8) !void {
    const uri = std.Uri.parse(endpoint) catch return error.InvalidProviderEndpoint;
    if (uri.user != null or uri.password != null or uri.query != null or uri.fragment != null or uri.host == null) return error.InvalidProviderEndpoint;
    const host = uri.host.?.percent_encoded;
    const local = std.ascii.eqlIgnoreCase(host, "localhost") or std.mem.eql(u8, host, "127.0.0.1") or std.mem.eql(u8, host, "[::1]");
    if (!std.ascii.eqlIgnoreCase(uri.scheme, "https") and !(std.ascii.eqlIgnoreCase(uri.scheme, "http") and local)) return error.InvalidProviderEndpoint;
}
