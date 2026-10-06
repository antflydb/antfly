// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
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
    config.username = platform.env.getenv("ANTFLY_USERNAME");
    config.password = platform.env.getenv("ANTFLY_PASSWORD");
    return config;
}

pub fn initClient(allocator: std.mem.Allocator, http: *httpx.Client, config: GlobalConfig) !antfly_client.AntflyClient {
    var client = try antfly_client.AntflyClient.init(allocator, http, config.url);
    errdefer client.deinit();
    if ((config.username == null) != (config.password == null)) return error.BasicAuthRequiresUsernameAndPassword;
    if (config.token != null and config.username != null) return error.ConflictingAuthentication;
    if (config.username) |username| try client.setBasicAuth(username, config.password.?);
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
