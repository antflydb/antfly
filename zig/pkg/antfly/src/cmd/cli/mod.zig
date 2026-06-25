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

const std = @import("std");
const platform = @import("antfly_platform");
const antfly_client = @import("antfly-client");
const httpx = @import("httpx");

pub const table = @import("table.zig");
pub const database_cmd = @import("database.zig");
pub const namespace_cmd = @import("namespace.zig");
pub const tablespace = @import("tablespace.zig");
pub const index = @import("index.zig");
pub const artifact = @import("artifact.zig");
pub const query = @import("query.zig");
pub const sql = @import("sql.zig");
pub const data = @import("data.zig");
pub const backup = @import("backup.zig");
pub const agents = @import("agents.zig");
pub const internal = @import("internal.zig");
pub const auth = @import("auth.zig");

pub const OutputFormat = enum { json, table_fmt };

pub const GlobalConfig = struct {
    url: []const u8 = "http://localhost:8080",
    token: ?[]const u8 = null,
    output: OutputFormat = .json,
};

pub const CatalogFlags = struct {
    database: ?[]const u8 = null,
    namespace: ?[]const u8 = null,

    pub const Explicit = struct {
        database: []const u8,
        namespace: []const u8,
    };

    pub fn defaultsFromEnv() CatalogFlags {
        return .{
            .database = platform.env.getenv("ANTFLY_DATABASE"),
            .namespace = platform.env.getenv("ANTFLY_NAMESPACE"),
        };
    }

    pub fn explicit(self: CatalogFlags) ?Explicit {
        if (self.database == null and self.namespace == null) return null;
        return .{
            .database = self.database orelse fatal("--database is required when --namespace is set or ANTFLY_NAMESPACE is configured", .{}),
            .namespace = self.namespace orelse fatal("--namespace is required when --database is set or ANTFLY_DATABASE is configured", .{}),
        };
    }

    pub fn databaseOrFatal(self: CatalogFlags) []const u8 {
        return self.database orelse fatal("--database is required or ANTFLY_DATABASE must be configured", .{});
    }
};

/// Build global CLI config from environment variables.
///
/// Supported env vars:
///   ANTFLY_URL    — server base URL (default http://localhost:8080)
///   ANTFLY_TOKEN  — bearer token for authentication
///   ANTFLY_DATABASE / ANTFLY_NAMESPACE — default catalog target for catalog-aware table commands
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

pub fn isCatalogFlag(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "--database") or
        std.mem.eql(u8, arg, "-d") or
        std.mem.eql(u8, arg, "--namespace") or
        std.mem.eql(u8, arg, "-n");
}

pub fn parseCatalogFlag(catalog: *CatalogFlags, arg: []const u8, args: *std.process.Args.Iterator) bool {
    if (std.mem.eql(u8, arg, "--database") or std.mem.eql(u8, arg, "-d")) {
        catalog.database = args.next() orelse fatal("--database requires a value", .{});
        return true;
    }
    if (std.mem.eql(u8, arg, "--namespace") or std.mem.eql(u8, arg, "-n")) {
        catalog.namespace = args.next() orelse fatal("--namespace requires a value", .{});
        return true;
    }
    return false;
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

pub fn printResponse(allocator: std.mem.Allocator, io: std.Io, resp: anytype) !void {
    if (resp.data) |parsed| {
        try writeJson(allocator, io, parsed.value);
        return;
    }
    expectHttpSuccess(resp);
    try writeJson(allocator, io, .{ .status = resp.status_code });
}

pub fn expectHttpSuccess(resp: anytype) void {
    if (resp.status_code >= 400) {
        if (resp.err_body) |body| fatal("request failed with HTTP {d}: {s}", .{ resp.status_code, body });
        fatal("request failed with HTTP {d}", .{resp.status_code});
    }
}

pub fn writeStdout(io: std.Io, bytes: []const u8) void {
    std.Io.File.stdout().writeStreamingAll(io, bytes) catch {};
}

pub fn readFileAlloc(io: std.Io, allocator: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    return try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes));
}

pub fn splitCommaListAlloc(allocator: std.mem.Allocator, raw: []const u8) ![]const []const u8 {
    var list: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer list.deinit(allocator);

    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |item| {
        const trimmed = std.mem.trim(u8, item, " \t\r\n");
        if (trimmed.len == 0) continue;
        try list.append(allocator, trimmed);
    }

    return try list.toOwnedSlice(allocator);
}

pub fn fatal(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("error: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

test "cli mod compiles" {
    _ = table;
    _ = database_cmd;
    _ = namespace_cmd;
    _ = tablespace;
    _ = index;
    _ = artifact;
    _ = query;
    _ = sql;
    _ = data;
    _ = backup;
    _ = agents;
    _ = internal;
    _ = auth;
}
