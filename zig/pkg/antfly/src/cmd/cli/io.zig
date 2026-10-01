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

pub const std = @import("std");
pub fn writeStdout(io: std.Io, bytes: []const u8) void {
    std.Io.File.stdout().writeStreamingAll(io, bytes) catch {};
}

pub fn readFileAlloc(io: std.Io, allocator: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    return try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes));
}

pub fn fatal(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("error: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

pub const platform = @import("antfly_platform");
pub const antfly_client = @import("antfly-client");
pub const httpx = @import("httpx");

pub const OutputFormat = enum { json, table_fmt };

pub const GlobalConfig = struct {
    url: []const u8 = "http://127.0.0.1:8080",
    token: ?[]const u8 = null,
    output: OutputFormat = .json,
};

pub fn parseGlobalFlags() GlobalConfig {
    var config = GlobalConfig{};
    if (platform.env.getenv("ANTFLY_URL")) |raw| {
        config.url = raw;
    }
    if (platform.env.getenv("ANTFLY_TOKEN")) |raw| {
        config.token = raw;
    }
    return config;
}

pub fn initClient(allocator: std.mem.Allocator, http: *httpx.Client, config: GlobalConfig) !antfly_client.AntflyClient {
    var client = try antfly_client.AntflyClient.init(allocator, http, config.url);
    if (config.token) |token| {
        try client.setBearer(token);
    }
    return client;
}

pub fn writeJson(allocator: std.mem.Allocator, io: std.Io, value: anytype) !void {
    const json = try std.json.Stringify.valueAlloc(allocator, value, .{ .whitespace = .indent_2 });
    defer allocator.free(json);
    writeStdout(io, json);
    writeStdout(io, "\n");
}

pub const backup = @import("backup_wait.zig");

pub fn expectHttpSuccess(resp: anytype) void {
    if (resp.status_code >= 400) {
        if (resp.err_body) |body| fatal("request failed with HTTP {d}: {s}", .{ resp.status_code, body });
        fatal("request failed with HTTP {d}", .{resp.status_code});
    }
}
