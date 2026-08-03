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

pub fn isHelpArg(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "help");
}

pub fn commandUsage(command: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, command, "query")) return
    \\usage: antfly query --table <table> [search options]
    \\
    \\  --full-text-search <query>       Full-text query string
    \\  --full-text-search-json <json>   Full-text query object
    \\  --semantic-search <text>         Semantic query text
    \\  --indexes <names>                Comma-separated semantic indexes
    \\  --fields <names>                 Comma-separated response fields
    \\  --filter-query <json>             Filter query
    \\  --exclusion-query <json>          Exclusion query
    \\  --aggregations <json>             Named aggregations
    \\  --reranker <json>                 Reranker configuration
    \\  --pruner <json>                   Result pruner configuration
    \\  --limit <n>                       Result limit
    \\  --offset <n>                      Result offset
    \\
    ;
    if (std.mem.eql(u8, command, "load")) return
    \\usage: antfly load --table <table> --file <ndjson> [options]
    \\
    \\  --size <n>                 Documents per batch
    \\  --batches <n>              Maximum in-flight batches
    \\  --batch-bytes <n>          Maximum bytes per batch
    \\  --id-field <field>         Source field used as document ID
    \\  --id-template <template>   Template used as document ID
    \\  --sync-level <level>       Write synchronization level
    \\  --checkpoint <path>        Checkpoint file path
    \\  --resume                   Resume from a checkpoint
    \\  --no-checkpoint            Disable checkpointing
    \\  --dry-run                  Validate input without writing
    \\  --max-errors <n>           Maximum rejected records
    \\  --strict                   Fail on the first rejected record
    \\
    ;
    if (std.mem.eql(u8, command, "table")) return "usage: antfly table <create|drop|list|get> [options]\n";
    if (std.mem.eql(u8, command, "index")) return "usage: antfly index <create|drop|list|get> --table <table> [options]\n";
    if (std.mem.eql(u8, command, "artifact")) return "usage: antfly artifact <list|get|put|delete|reprocess|job> [options]\n";
    if (std.mem.eql(u8, command, "lookup")) return "usage: antfly lookup --table <table> --key <key> [options]\n";
    if (std.mem.eql(u8, command, "insert")) return "usage: antfly insert --table <table> --key <key> --document <json> [options]\n";
    if (std.mem.eql(u8, command, "delete")) return "usage: antfly delete --table <table> --key <key> [options]\n";
    if (std.mem.eql(u8, command, "agents")) return "usage: antfly agents <retrieval|query-builder> [options]\n";
    if (std.mem.eql(u8, command, "backup")) return "usage: antfly backup --table <table> --location <uri> [options]\n";
    if (std.mem.eql(u8, command, "restore")) return "usage: antfly restore --location <uri> [options]\n";
    if (std.mem.eql(u8, command, "auth")) return "usage: antfly auth <me|users|permissions|roles|row-filters|subjects|api-keys> [options]\n";
    if (std.mem.eql(u8, command, "internal")) return "usage: antfly internal metadata status\n";
    return null;
}

pub fn printCommandUsage(command: []const u8) void {
    const usage = commandUsage(command) orelse return;
    std.debug.print("{s}", .{usage});
}

test "client commands expose help without a server" {
    try std.testing.expect(isHelpArg("--help"));
    try std.testing.expect(isHelpArg("-h"));
    try std.testing.expect(commandUsage("query") != null);
    try std.testing.expect(commandUsage("load") != null);
    try std.testing.expect(commandUsage("auth") != null);
    try std.testing.expect(commandUsage("unknown") == null);
}

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
