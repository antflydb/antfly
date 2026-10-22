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

//! Provider metadata is shared by setup and completions. Gateway routes use
//! the same native TypeSafe state/question protocol as the direct provider.
const std = @import("std");
pub const Provider = struct { id: []const u8, label: []const u8, endpoint: []const u8, model: ?[]const u8 = null, protocol: enum { antfly, typesafe, local } };
pub const catalog = [_]Provider{
    .{ .id = "antfly", .label = "Antfly", .endpoint = "http://127.0.0.1:8090/decisions", .protocol = .antfly },
    .{ .id = "typesafe", .label = "TypeSafe", .endpoint = "https://api.typesafe.ai/v1/systemone", .model = "jev-1.13.0", .protocol = .typesafe },
    .{ .id = "vercel", .label = "Vercel AI Gateway", .endpoint = "https://ai-gateway.vercel.sh/typesafe/v1/systemone", .model = "typesafe-ai/jev", .protocol = .typesafe },
    .{ .id = "openrouter", .label = "OpenRouter", .endpoint = "https://openrouter.ai/api/v1/systemone", .model = "typesafe/jev-1.13", .protocol = .typesafe },
    .{ .id = "opencode", .label = "OpenCode Zen", .endpoint = "https://opencode.ai/zen/v1/systemone", .model = "jev-1.13", .protocol = .typesafe },
    .{ .id = "local", .label = "Local inference", .endpoint = "", .protocol = .local },
    .{ .id = "custom", .label = "Custom TypeSafe gateway", .endpoint = "", .protocol = .typesafe },
};
pub fn get(id: []const u8) ?Provider {
    for (catalog) |provider| if (std.mem.eql(u8, id, provider.id)) return provider;
    return null;
}
pub fn validateCustom(endpoint: []const u8) !void {
    const uri = std.Uri.parse(endpoint) catch return error.InvalidProviderEndpoint;
    if (uri.user != null or uri.password != null or uri.query != null or uri.fragment != null or uri.host == null) return error.InvalidProviderEndpoint;
    const host = uri.host.?.percent_encoded;
    const local = std.ascii.eqlIgnoreCase(host, "localhost") or std.mem.eql(u8, host, "127.0.0.1") or std.mem.eql(u8, host, "[::1]");
    if (!std.ascii.eqlIgnoreCase(uri.scheme, "https") and !(std.ascii.eqlIgnoreCase(uri.scheme, "http") and local)) return error.InvalidProviderEndpoint;
}
