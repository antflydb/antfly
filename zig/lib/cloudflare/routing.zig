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
pub const wire = @import("cloudflare_api");

pub fn identifier(value: []const u8) !void {
    if (value.len == 0 or value.len > 64) return error.InvalidCloudflareConfig;
    for (value) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '-' and ch != '_') return error.InvalidCloudflareConfig;
}

pub fn modelId(model: []const u8) !void {
    if (model.len > 256 or (!std.mem.startsWith(u8, model, "@cf/") and !std.mem.startsWith(u8, model, "@hf/"))) return error.UnsupportedCloudflareModel;
    var parts = std.mem.splitScalar(u8, model[4..], '/');
    const owner = parts.next() orelse return error.UnsupportedCloudflareModel;
    const name = parts.next() orelse return error.UnsupportedCloudflareModel;
    if (parts.next() != null) return error.UnsupportedCloudflareModel;
    for ([_][]const u8{ owner, name }) |part| {
        if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return error.UnsupportedCloudflareModel;
        for (part) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '-' and ch != '_' and ch != '.') return error.UnsupportedCloudflareModel;
    }
}

/// All ordinary Cloudflare adapters resolve their account into the endpoint
/// before entering the existing pooled, quota-controlled provider execution.
pub fn baseUrl(a: std.mem.Allocator, account: ?[]const u8, override: ?[]const u8) ![]u8 {
    const id = account orelse return error.InvalidCloudflareConfig;
    try identifier(id);
    if (override) |url| if (url.len > 0) return a.dupe(u8, std.mem.trimEnd(u8, url, "/"));
    return std.fmt.allocPrint(a, "https://api.cloudflare.com/client/v4/accounts/{s}/ai/v1", .{id});
}

pub fn nativeUrl(a: std.mem.Allocator, account: []const u8, model: []const u8) ![]u8 {
    try identifier(account);
    try modelId(model);
    return std.fmt.allocPrint(a, "https://api.cloudflare.com/client/v4/accounts/{s}/ai/run/{s}", .{ account, model });
}
pub fn visionModel(model: []const u8) bool {
    // Qualified against the pinned image-bearing input schema. New vision
    // families need their own capability qualification before document use.
    return std.mem.eql(u8, model, "@cf/mistralai/mistral-small-3.1-24b-instruct");
}

test "cloudflare routing rejects path injection" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.InvalidCloudflareConfig, baseUrl(a, "../account", null));
    for ([_][]const u8{ "@cf/a/../x", "@cf/a/x?key=y", "@cf/a/..", "@cf//x" }) |model| try std.testing.expectError(error.UnsupportedCloudflareModel, modelId(model));
    const url = try baseUrl(a, "account", null);
    defer a.free(url);
    try std.testing.expectEqualStrings("https://api.cloudflare.com/client/v4/accounts/account/ai/v1", url);
}
