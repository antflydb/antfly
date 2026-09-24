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
const httpx = @import("httpx");
const exa = @import("exa_api").types;
const tavily = @import("tavily_api").types;
const api = @import("antfly_websearch_openapi");
const generating_api = @import("antfly_generating_api_openapi");
const metadata = @import("antfly_metadata_openapi");
const common = @import("../common/config.zig");
const secrets = @import("../common/secrets.zig");
const time = @import("antfly_platform").time;

pub const default_endpoint = "https://api.exa.ai/search";
pub const tavily_endpoint = "https://api.tavily.com/search";
pub const Provider = enum { exa, tavily };

pub const max_response_bytes = 1024 * 1024;
pub const max_text_bytes = 4000;

pub const Options = struct {
    connection: ?[]const u8 = null,
    inline_config: ?api.ExaSearchConfig = null,
    tavily_config: ?api.TavilySearchConfig = null,
};

/// Request-arena owned. This configuration must never enter model history,
/// step details, or inventory responses.
pub const Config = struct {
    provider: Provider = .exa,
    search_depth: []const u8 = "basic",
    include_answer: bool = false,
    include_raw_content: bool = false,
    endpoint: []const u8 = default_endpoint,
    api_key: ?[]const u8 = null,
    max_results: usize = 5,
    timeout_ms: u64 = 10000,
    safe_search: bool = true,
    include_content: bool = false,
    include_highlights: bool = false,
    search_type: []const u8 = "auto",
    region: ?[]const u8 = null,
    start_published_date: ?[]const u8 = null,
    end_published_date: ?[]const u8 = null,
    include_domains: []const []const u8 = &.{},
    exclude_domains: []const []const u8 = &.{},
};

/// Read provider-specific options from the selected tool scope, rejecting
/// conflicting connection identities instead of silently replacing authority.
pub fn parseOptions(global: ?generating_api.ChatToolsConfig, retrieval: ?generating_api.ChatToolsConfig) !?Options {
    const global_options = if (global) |tools| try optionsFromTools(tools) else null;
    const local_options = if (retrieval) |tools| try optionsFromTools(tools) else null;
    if (global_options != null and local_options != null) return error.InvalidRetrievalAgentRequest;
    return local_options orelse global_options;
}

fn optionsFromTools(tools: generating_api.ChatToolsConfig) !?Options {
    var options = Options{ .connection = tools.web_search_connection };
    if (tools.web_search_config) |provider| switch (provider) {
        .exa_search_config => |value| options.inline_config = value,
        .tavily_search_config => |value| options.tavily_config = value,
        else => return error.UnsupportedRetrievalAgentRequest,
    };
    if (options.connection) |name| if (name.len == 0) return error.InvalidRetrievalAgentRequest;
    return if (options.connection != null or options.inline_config != null or options.tavily_config != null) options else null;
}

/// Named connections supply authority. Request overrides can only narrow that
/// authority; an inline endpoint cannot redirect a server-held credential.
pub fn resolve(arena: std.mem.Allocator, node: ?*const common.Config, options: Options) !Config {
    if (options.inline_config != null and options.tavily_config != null) return error.InvalidRetrievalAgentRequest;
    var config = Config{};
    if (options.tavily_config != null) {
        config.provider = .tavily;
        config.endpoint = tavily_endpoint;
    }
    var named = false;
    if (options.connection) |name| {
        const cfg = node orelse return error.InvalidRetrievalAgentRequest;
        const connection = cfg.connections.get(name) orelse return error.InvalidRetrievalAgentRequest;
        if (connection.kind != .web_search) return error.InvalidRetrievalAgentRequest;
        const provider = std.meta.stringToEnum(Provider, connection.provider orelse "") orelse return error.InvalidRetrievalAgentRequest;
        if ((options.inline_config != null and provider != .exa) or (options.tavily_config != null and provider != .tavily)) return error.InvalidRetrievalAgentRequest;
        if (!contains(connection.capabilities, "web.search") or !contains(connection.capabilities, "agents.use")) return error.Forbidden;
        const web = connection.web_search orelse return error.InvalidRetrievalAgentRequest;
        config = .{
            .provider = provider,
            .endpoint = web.endpoint orelse (if (provider == .tavily) tavily_endpoint else default_endpoint),
            .api_key = web.api_key,
            .max_results = web.max_results orelse 5,
            .timeout_ms = web.timeout_ms orelse 10000,
            .safe_search = web.safe_search orelse true,
            .include_content = web.include_content orelse false,
            .include_highlights = web.include_highlights orelse false,
            .region = web.region,
            .include_domains = web.include_domains,
            .exclude_domains = web.exclude_domains,
        };
        if (web.language != null or web.service != null) return error.InvalidRetrievalAgentRequest;
        named = true;
    }
    if (options.inline_config) |inline_cfg| try applyInline(arena, &config, inline_cfg, named);
    if (options.tavily_config) |inline_cfg| try applyInline(arena, &config, inline_cfg, named);
    // Tavily has no ISO region or highlighted-passage equivalent. Reject rather
    // than silently ignoring configured policy.
    if (config.provider == .tavily and (config.region != null or config.include_highlights)) return error.InvalidRetrievalAgentRequest;
    if (config.max_results < 1 or config.max_results > 20 or config.timeout_ms < 1 or config.timeout_ms > 120000) return error.InvalidRetrievalAgentRequest;
    if (config.region) |region| if (region.len != 2 or !std.ascii.isAlphabetic(region[0]) or !std.ascii.isAlphabetic(region[1])) return error.InvalidRetrievalAgentRequest;
    for ([_][]const []const u8{ config.include_domains, config.exclude_domains }) |domains| {
        if (domains.len > 100) return error.InvalidRetrievalAgentRequest;
        for (domains) |domain| {
            if (domain.len == 0 or domain.len > 253 or std.mem.indexOfAny(u8, domain, "/:*?#@ \t\r\n") != null) return error.InvalidRetrievalAgentRequest;
        }
    }
    const uri = std.Uri.parse(config.endpoint) catch return error.InvalidRetrievalAgentRequest;
    if ((!std.mem.eql(u8, uri.scheme, "https") and !std.mem.eql(u8, uri.scheme, "http")) or uri.host == null or uri.user != null or uri.password != null or uri.query != null or uri.fragment != null) return error.InvalidRetrievalAgentRequest;
    return config;
}

fn applyInline(arena: std.mem.Allocator, config: *Config, inline_cfg: anytype, named: bool) !void {
    if (!std.mem.eql(u8, @tagName(inline_cfg.provider), @tagName(config.provider))) return error.InvalidRetrievalAgentRequest;
    if (inline_cfg.language != null or inline_cfg.project_id != null or inline_cfg.location != null or inline_cfg.data_store != null or inline_cfg.serving_config != null or inline_cfg.credentials_path != null) return error.InvalidRetrievalAgentRequest;
    if (named and (inline_cfg.api_key != null or inline_cfg.endpoint != null)) return error.Forbidden;
    if (!named) {
        config.api_key = inline_cfg.api_key;
        if (inline_cfg.endpoint) |endpoint| {
            if (!std.mem.eql(u8, endpoint, if (config.provider == .tavily) tavily_endpoint else default_endpoint)) return error.Forbidden;
        }
    }
    if (inline_cfg.max_results) |n| config.max_results = try narrowedCount(n, config.max_results, named);
    if (@hasField(@TypeOf(inline_cfg), "num_results")) {
        if (inline_cfg.num_results) |n| config.max_results = try narrowedCount(n, config.max_results, named or inline_cfg.max_results != null);
    }
    if (inline_cfg.timeout_ms) |ms| {
        if (ms < 1 or ms > 120000) return error.InvalidRetrievalAgentRequest;
        config.timeout_ms = if (named) @min(config.timeout_ms, @as(u64, @intCast(ms))) else @intCast(ms);
    }
    if (inline_cfg.safe_search) |value| {
        if (named and config.safe_search and !value) return error.Forbidden;
        config.safe_search = value;
    }
    if (inline_cfg.include_content) |value| {
        if (named and value and !config.include_content) return error.Forbidden;
        config.include_content = value;
    }
    if (inline_cfg.include_highlights) |value| {
        if (named and value and !config.include_highlights) return error.Forbidden;
        config.include_highlights = value;
    }
    if (inline_cfg.region) |region| {
        if (named and config.region != null and !std.mem.eql(u8, config.region.?, region)) return error.Forbidden;
        config.region = region;
    }
    if (@TypeOf(inline_cfg) == api.ExaSearchConfig) {
        if (inline_cfg.search_type) |kind| {
            if (!contains(&.{ "auto", "neural", "keyword" }, kind)) return error.InvalidRetrievalAgentRequest;
            config.search_type = kind;
        }
        config.start_published_date = inline_cfg.start_published_date;
        config.end_published_date = inline_cfg.end_published_date;
    } else {
        if (inline_cfg.search_depth) |depth| {
            if (!contains(&.{ "basic", "advanced" }, depth)) return error.InvalidRetrievalAgentRequest;
            config.search_depth = depth;
        }
        if (inline_cfg.include_answer) |value| config.include_answer = value;
        if (inline_cfg.include_raw_content) |value| {
            if (named and value and !config.include_content) return error.Forbidden;
            config.include_raw_content = value;
        }
    }
    if (inline_cfg.include_domains) |domains| {
        if (named and config.include_domains.len > 0) {
            if (domains.len == 0) return error.Forbidden;
            for (domains) |domain| if (!contains(config.include_domains, domain)) return error.Forbidden;
        }
        config.include_domains = domains;
    }
    if (inline_cfg.exclude_domains) |domains| {
        config.exclude_domains = try std.mem.concat(arena, []const u8, &.{ config.exclude_domains, domains });
    }
}

fn narrowedCount(n: i64, ceiling: usize, narrow: bool) !usize {
    if (n < 1 or n > 20) return error.InvalidRetrievalAgentRequest;
    const count: usize = @intCast(n);
    return if (narrow) @min(count, ceiling) else count;
}

fn contains(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| if (std.mem.eql(u8, value, needle)) return true;
    return false;
}

pub fn requestBody(arena: std.mem.Allocator, config: Config, query: []const u8) ![]const u8 {
    if (std.mem.trim(u8, query, " \t\r\n").len == 0 or query.len > 8192) return error.InvalidRetrievalAgentRequest;
    if (config.provider == .tavily) return std.json.Stringify.valueAlloc(arena, tavily.SearchRequest{
        .query = query,
        .search_depth = config.search_depth,
        .max_results = @intCast(config.max_results),
        .safe_search = config.safe_search,
        .include_answer = .{ .bool = config.include_answer },
        .include_raw_content = .{ .bool = config.include_raw_content },
        .include_domains = if (config.include_domains.len > 0) config.include_domains else null,
        .exclude_domains = if (config.exclude_domains.len > 0) config.exclude_domains else null,
    }, .{ .emit_null_optional_fields = false });
    var limit = std.json.ObjectMap.empty;
    try limit.put(arena, "maxCharacters", .{ .integer = max_text_bytes });
    const content_limit: std.json.Value = .{ .object = limit };
    return std.json.Stringify.valueAlloc(arena, exa.SearchRequest{
        .query = query,
        .type = config.search_type,
        .num_results = @intCast(config.max_results),
        .moderation = config.safe_search,
        .user_location = config.region,
        .start_published_date = config.start_published_date,
        .end_published_date = config.end_published_date,
        .include_domains = if (config.include_domains.len > 0) config.include_domains else null,
        .exclude_domains = if (config.exclude_domains.len > 0) config.exclude_domains else null,
        .contents = .{
            .text = if (config.include_content) content_limit else std.json.Value{ .bool = false },
            .highlights = if (config.include_highlights) content_limit else std.json.Value{ .bool = false },
        },
    }, .{ .emit_null_optional_fields = false });
}

pub fn search(arena: std.mem.Allocator, client: *httpx.Client, store: ?*secrets.FileStore, config: Config, query: []const u8, deadline_ns: u64, cancellation: ?httpx.CancellationToken) ![]const metadata.QueryHit {
    const now = time.monotonicNs();
    if (now >= deadline_ns) return error.Timeout;
    var credential = try secrets.SecretValue.initConfigOrEnv(arena, config.api_key, if (config.provider == .tavily) "TAVILY_API_KEY" else "EXA_API_KEY");
    defer credential.deinit(arena);
    const key = (try credential.resolveOwned(arena, store)) orelse return error.MissingWebSearchCredentials;
    defer arena.free(key);
    if (key.len == 0 or std.mem.indexOfAny(u8, key, "\r\n") != null) return error.MissingWebSearchCredentials;
    const body = try requestBody(arena, config, query);
    defer arena.free(body);
    const auth = if (config.provider == .tavily) try std.fmt.allocPrint(arena, "Bearer {s}", .{key}) else key;
    defer if (config.provider == .tavily) arena.free(auth);
    var response = try client.post(config.endpoint, .{
        .json = body,
        .cancellation = cancellation,
        .headers = &.{.{ if (config.provider == .tavily) "Authorization" else "x-api-key", auth }},
        .follow_redirects = false,
        .cookies_enabled = false,
        .max_response_size = max_response_bytes,
        .timeout_ms = @max(1, @min(config.timeout_ms, (deadline_ns - now) / std.time.ns_per_ms)),
    });
    defer response.deinit();
    switch (response.status.code) {
        200 => {},
        401, 403 => return error.WebSearchAuthenticationFailed,
        429 => return error.WebSearchRateLimited,
        else => return error.WebSearchProviderFailed,
    }
    // Provider error bodies may echo credentials; only typed errors escape.
    return parseResults(arena, config, response.body orelse return error.InvalidWebSearchResponse);
}

fn bounded(text: []const u8, max: usize) []const u8 {
    var end = @min(text.len, max);
    while (end > 0 and !std.unicode.utf8ValidateSlice(text[0..end])) : (end -= 1) {}
    return text[0..end];
}

// Compare URL authority, not its percent-encoded spelling. Reject non-ASCII
// hostnames (use their IDNA ASCII form) and decoded delimiters rather than
// letting a downstream URL parser reinterpret them after the policy check.
fn canonicalHost(arena: std.mem.Allocator, encoded: []const u8) !?[]const u8 {
    const decoded = std.Uri.percentDecodeInPlace(try arena.dupe(u8, encoded));
    const host = std.mem.trimEnd(u8, decoded, ".");
    if (host.len == 0) return null;
    for (host) |c| {
        if (!std.ascii.isAlphanumeric(c) and std.mem.indexOfScalar(u8, ".-_:[]", c) == null) return null;
    }
    return host;
}

fn domainMatches(host: []const u8, configured_domain: []const u8) bool {
    const domain = std.mem.trimEnd(u8, configured_domain, ".");
    return std.ascii.eqlIgnoreCase(host, domain) or (host.len > domain.len and host[host.len - domain.len - 1] == '.' and std.ascii.eqlIgnoreCase(host[host.len - domain.len ..], domain));
}

const Result = struct {
    url: ?[]const u8,
    title: ?[]const u8,
    text: ?[]const u8,
    highlights: ?[]const []const u8 = null,
    score: ?f64,
    published_date: ?[]const u8,
    author: ?[]const u8 = null,
};

pub fn parseResults(arena: std.mem.Allocator, config: Config, body: []const u8) ![]const metadata.QueryHit {
    if (body.len > max_response_bytes) return error.InvalidWebSearchResponse;
    const results = if (config.provider == .tavily) blk: {
        // Only source results are evidence. Upstream marks answer as required,
        // though it is omitted unless requested; do not parse unused metadata.
        const response = std.json.parseFromSliceLeaky(std.json.Value, arena, body, .{ .allocate = .alloc_always }) catch return error.InvalidWebSearchResponse;
        if (response != .object) return error.InvalidWebSearchResponse;
        const items = response.object.get("results") orelse return error.InvalidWebSearchResponse;
        if (items != .array) return error.InvalidWebSearchResponse;
        const normalized = try arena.alloc(Result, items.array.items.len);
        for (items.array.items, normalized) |*value, *result| {
            if (value.* != .object) return error.InvalidWebSearchResponse;
            // Upstream documents null for these optional fields but omits
            // nullable in its schema. Treat those documented nulls as absent
            // before decoding the unchanged generated wire type.
            for ([_][]const u8{ "raw_content", "published_date" }) |field| {
                if (value.object.get(field)) |v| if (v == .null) {
                    _ = value.object.swapRemove(field);
                };
            }
            const item = std.json.parseFromValueLeaky(tavily.SearchResult, arena, value.*, .{ .ignore_unknown_fields = true }) catch return error.InvalidWebSearchResponse;
            result.* = .{
                .url = item.url,
                .title = item.title,
                .text = if (config.include_raw_content) item.raw_content orelse item.content else item.content,
                .score = if (item.score) |score| @as(f64, score) else null,
                .published_date = item.published_date,
            };
        }
        break :blk normalized;
    } else blk: {
        const response = std.json.parseFromSliceLeaky(exa.SearchResponse, arena, body, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }) catch return error.InvalidWebSearchResponse;
        const items = response.results orelse return error.InvalidWebSearchResponse;
        const normalized = try arena.alloc(Result, items.len);
        for (items, normalized) |item, *result| result.* = .{
            .url = item.url,
            .title = item.title,
            .text = item.text,
            .highlights = item.highlights,
            .score = item.score.valueOrNull(),
            .published_date = item.published_date.valueOrNull(),
            .author = item.author.valueOrNull(),
        };
        break :blk normalized;
    };
    var hits = std.ArrayListUnmanaged(metadata.QueryHit).empty;
    var seen = std.StringHashMapUnmanaged(void).empty;
    for (results) |item| {
        if (item.url == null) return error.InvalidWebSearchResponse;
    }
    for (results) |item| {
        const url = item.url.?;
        if (hits.items.len >= config.max_results) break;
        if (url.len == 0 or url.len > 8192 or seen.contains(url)) continue;
        const uri = std.Uri.parse(url) catch continue;
        if ((!std.mem.eql(u8, uri.scheme, "https") and !std.mem.eql(u8, uri.scheme, "http")) or uri.host == null or uri.user != null or uri.password != null) continue;
        const host = (try canonicalHost(arena, uri.host.?.percent_encoded)) orelse continue;
        var allowed = config.include_domains.len == 0;
        for (config.include_domains) |domain| if (domainMatches(host, domain)) {
            allowed = true;
        };
        for (config.exclude_domains) |domain| if (domainMatches(host, domain)) {
            allowed = false;
        };
        if (!allowed) continue;
        try seen.put(arena, url, {});
        var source = std.json.ArrayHashMap(std.json.Value){};
        try source.map.put(arena, "provider", .{ .string = @tagName(config.provider) });
        try source.map.put(arena, "url", .{ .string = url });
        if (item.title) |title| try source.map.put(arena, "title", .{ .string = bounded(title, 1024) });
        if (config.include_content or config.include_raw_content) if (item.text) |value| try source.map.put(arena, "text", .{ .string = bounded(value, max_text_bytes) });
        if (config.include_highlights) if (item.highlights) |values| {
            var highlights = std.json.Array.init(arena);
            var remaining: usize = max_text_bytes;
            for (values) |value| {
                if (remaining == 0 or highlights.items.len >= 10) break;
                const text = bounded(value, remaining);
                try highlights.append(.{ .string = text });
                remaining -= text.len;
            }
            try source.map.put(arena, "highlights", .{ .array = highlights });
        };
        if (item.published_date) |date| try source.map.put(arena, "published_at", .{ .string = bounded(date, 64) });
        if (item.author) |author| try source.map.put(arena, "author", .{ .string = bounded(author, 512) });
        const score: f32 = if (item.score) |value| @floatCast(value) else 1 / @as(f32, @floatFromInt(hits.items.len + 1));
        try hits.append(arena, .{
            ._id = try std.fmt.allocPrint(arena, "web:{s}", .{url}),
            ._score = if (std.math.isFinite(score)) score else 0,
            ._source = source,
        });
    }
    return hits.toOwnedSlice(arena);
}

test "Exa connection policy only permits narrowing and protects credentials" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var node = try common.Config.parseFromSlice(std.testing.allocator,
        \\{"connections":{"web":{"kind":"web_search","provider":"exa","capabilities":["web.search","agents.use"],"web_search":{"api_key":"test-config-key","max_results":3,"timeout_ms":5000,"safe_search":true,"include_content":true,"include_domains":["example.com"],"exclude_domains":["blocked.example.com"]}},"denied":{"kind":"web_search","provider":"exa","capabilities":["web.search"],"web_search":{}}}}
    );
    defer node.deinit();
    const narrowed = try resolve(a, &node, .{ .connection = "web", .inline_config = .{ .provider = .exa, .max_results = 20, .timeout_ms = 9000, .include_content = false, .exclude_domains = &.{"other.example.com"} } });
    try std.testing.expectEqual(@as(usize, 3), narrowed.max_results);
    try std.testing.expectEqual(@as(u64, 5000), narrowed.timeout_ms);
    try std.testing.expect(!narrowed.include_content);
    try std.testing.expectEqual(@as(usize, 2), narrowed.exclude_domains.len);
    try std.testing.expectEqualStrings("test-config-key", narrowed.api_key.?);
    const overrides = [_]api.ExaSearchConfig{
        .{ .provider = .exa, .endpoint = "http://localhost/search" },
        .{ .provider = .exa, .api_key = "replacement" },
        .{ .provider = .exa, .safe_search = false },
        .{ .provider = .exa, .include_highlights = true },
        .{ .provider = .exa, .include_domains = &.{} },
        .{ .provider = .exa, .include_domains = &.{"untrusted.example"} },
    };
    for (overrides) |value| try std.testing.expectError(error.Forbidden, resolve(a, &node, .{ .connection = "web", .inline_config = value }));
    try std.testing.expectError(error.Forbidden, resolve(a, &node, .{ .connection = "denied" }));
    try std.testing.expectError(error.InvalidRetrievalAgentRequest, resolve(a, &node, .{ .connection = "missing" }));
    try std.testing.expectError(error.Forbidden, resolve(a, null, .{ .inline_config = .{ .provider = .exa, .endpoint = "http://127.0.0.1/search" } }));
}

test "Exa provider subtype and request encoding preserve filters and content controls" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const raw = try std.json.parseFromSliceLeaky(metadata.RetrievalAgentRequest, a,
        \\{"query":"evidence","queries":[],"steps":{"retrieval":{"tools":{"web_search_config":{"provider":"exa","api_key":"private","num_results":2,"include_content":true,"include_highlights":true,"search_type":"neural","start_published_date":"2026-01-01T00:00:00Z","include_domains":["example.com"]}}}}}
    , .{});
    const config = try resolve(a, null, (try parseOptions(raw.tools, raw.steps.?.retrieval.?.tools)).?);
    const body = try requestBody(a, config, "evidence");
    try std.testing.expect(std.mem.indexOf(u8, body, "private") == null);
    const wire = (try std.json.parseFromSliceLeaky(std.json.Value, a, body, .{})).object;
    try std.testing.expectEqual(@as(i64, 2), wire.get("numResults").?.integer);
    try std.testing.expectEqualStrings("neural", wire.get("type").?.string);
    try std.testing.expectEqualStrings("example.com", wire.get("includeDomains").?.array.items[0].string);
    try std.testing.expect(wire.get("moderation").?.bool);
    try std.testing.expect(wire.get("contents").?.object.contains("text"));
    try std.testing.expect(wire.get("contents").?.object.contains("highlights"));
    try std.testing.expectError(error.InvalidRetrievalAgentRequest, requestBody(a, config, " \n "));
    try std.testing.expectError(error.InvalidRetrievalAgentRequest, resolve(a, null, .{ .inline_config = .{ .provider = .exa, .num_results = 0 } }));
    try std.testing.expectError(error.InvalidRetrievalAgentRequest, resolve(a, null, .{ .inline_config = .{ .provider = .tavily } }));
}

test "Exa response normalizes citations and bounds content while enforcing domain policy" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const config = Config{ .include_content = true, .include_highlights = true, .include_domains = &.{"example.com"}, .exclude_domains = &.{"blocked.example.com"} };
    const body =
        \\{"results":[{"url":"https://example.com/doc","title":"Evidence","text":"canary","highlights":["excerpt"],"score":0.7},{"url":"https://example.com/doc"},{"url":"https://example.com.evil/doc"},{"url":"https://blocked.example.com/doc"},{"url":"javascript:alert(1)"},{"url":"https://docs.example.com/page"}]}
    ;
    const hits = try parseResults(a, config, body);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("web:https://example.com/doc", hits[0]._id);
    try std.testing.expectEqualStrings("https://example.com/doc", hits[0]._source.?.map.get("url").?.string);
    try std.testing.expectEqualStrings("canary", hits[0]._source.?.map.get("text").?.string);
    const no_content = try parseResults(a, .{ .max_results = 1 }, body);
    try std.testing.expectEqual(@as(usize, 1), no_content.len);
    try std.testing.expect(!no_content[0]._source.?.map.contains("text"));
    try std.testing.expect(!no_content[0]._source.?.map.contains("highlights"));
    try std.testing.expectError(error.InvalidWebSearchResponse, parseResults(a, config, "{\"error\":\"secret\"}"));
    try std.testing.expectEqualStrings("a", bounded("aé", 2));
}

test "Exa domain policy checks decoded hostnames and DNS root dots" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const body =
        \\{"results":[{"url":"https://%65xample.com/encoded"},{"url":"https://EXAMPLE.com./root-dot"},{"url":"https://sub%2eexample.com/subdomain"},{"url":"https://outside.example/ok"},{"url":"https://example.com%2f.evil/invalid"},{"url":"https://%zz.example/invalid"},{"url":"https://%EF%BD%85xample.com/unicode"}]}
    ;
    const excluded = try parseResults(a, .{ .exclude_domains = &.{"example.com"} }, body);
    try std.testing.expectEqual(@as(usize, 1), excluded.len);
    try std.testing.expectEqualStrings("https://outside.example/ok", excluded[0]._source.?.map.get("url").?.string);
    const included = try parseResults(a, .{ .include_domains = &.{"EXAMPLE.com."} }, body);
    try std.testing.expectEqual(@as(usize, 3), included.len);
}

test "Exa generated wire types preserve optional metadata and required evidence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const body =
        \\{"requestId":"test","searchType":"neural","results":[{"url":"https://example.com/one","publishedDate":null,"author":null,"score":null},{"url":"https://example.com/two","publishedDate":"2026-01-01","author":"Author","score":0.75},{"url":"https://example.com/three","score":1e100}],"costDollars":{"total":0.005},"futureField":true}
    ;
    const hits = try parseResults(a, .{}, body);
    try std.testing.expectEqual(@as(usize, 3), hits.len);
    try std.testing.expectEqual(@as(f32, 1), hits[0]._score);
    try std.testing.expect(!hits[0]._source.?.map.contains("published_at"));
    try std.testing.expect(!hits[0]._source.?.map.contains("author"));
    try std.testing.expectEqualStrings("2026-01-01", hits[1]._source.?.map.get("published_at").?.string);
    try std.testing.expectEqualStrings("Author", hits[1]._source.?.map.get("author").?.string);
    try std.testing.expectEqual(@as(f32, 0.75), hits[1]._score);
    try std.testing.expectEqual(@as(f32, 0), hits[2]._score);
    try std.testing.expectEqual(@as(usize, 0), (try parseResults(a, .{}, "{\"results\":[]}")).len);
    for ([_][]const u8{ "{}", "{\"results\":null}", "{\"results\":[{}]}", "{\"results\":[{\"url\":null}]}" }) |invalid| {
        try std.testing.expectError(error.InvalidWebSearchResponse, parseResults(a, .{}, invalid));
    }

    const request = (try std.json.parseFromSliceLeaky(std.json.Value, a, try requestBody(a, .{ .region = "US", .safe_search = false }, "evidence"), .{})).object;
    try std.testing.expectEqualStrings("US", request.get("userLocation").?.string);
    try std.testing.expectEqual(@as(i64, 5), request.get("numResults").?.integer);
    try std.testing.expect(!request.get("moderation").?.bool);
    try std.testing.expect(!request.get("contents").?.object.get("text").?.bool);
    try std.testing.expect(!request.get("contents").?.object.get("highlights").?.bool);
    try std.testing.expect(!request.contains("startPublishedDate"));
    try std.testing.expect(!request.contains("includeDomains"));
    try std.testing.expect(!request.contains("user_location"));
}

test "Tavily typed options produce upstream wire fields and preserve false values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const tools = try std.json.parseFromSliceLeaky(generating_api.ChatToolsConfig, a,
        \\{"web_search_config":{"provider":"tavily","max_results":2,"search_depth":"advanced","include_content":true,"include_answer":false,"include_raw_content":true,"include_domains":["example.com"]}}
    , .{});
    const config = try resolve(a, null, (try parseOptions(tools, null)).?);
    try std.testing.expectEqual(Provider.tavily, config.provider);
    try std.testing.expectEqualStrings(tavily_endpoint, config.endpoint);
    const body = try requestBody(a, config, "evidence");
    const wire = (try std.json.parseFromSliceLeaky(std.json.Value, a, body, .{})).object;
    try std.testing.expectEqualStrings("advanced", wire.get("search_depth").?.string);
    try std.testing.expectEqual(@as(i64, 2), wire.get("max_results").?.integer);
    try std.testing.expect(!wire.get("include_answer").?.bool);
    try std.testing.expect(wire.get("include_raw_content").?.bool);
    try std.testing.expect(wire.get("safe_search").?.bool);
    try std.testing.expectEqualStrings("example.com", wire.get("include_domains").?.array.items[0].string);
    try std.testing.expect(wire.get("api_key") == null and wire.get("numResults") == null);
    try std.testing.expectError(error.Forbidden, resolve(a, null, .{ .tavily_config = .{ .provider = .tavily, .endpoint = default_endpoint } }));
}

test "Tavily results use source content with nullable raw fallback and shared domain policy" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const body =
        \\{"answer":"UNSOURCED-ANSWER","results":[{"url":"https://example.com/one","title":"One","content":"snippet","raw_content":null,"published_date":null,"score":0.75},{"url":"https://example.com/two","content":"summary","raw_content":"full source"},{"url":"https://example.com/two"},{"url":"https://outside.example/doc"},{"url":"https://blocked.example.com/doc"}],"response_time":"1.5"}
    ;
    const config = Config{ .provider = .tavily, .include_content = true, .include_raw_content = true, .include_domains = &.{"example.com"}, .exclude_domains = &.{"blocked.example.com"} };
    const hits = try parseResults(a, config, body);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("tavily", hits[0]._source.?.map.get("provider").?.string);
    try std.testing.expectEqualStrings("snippet", hits[0]._source.?.map.get("text").?.string);
    try std.testing.expectEqualStrings("full source", hits[1]._source.?.map.get("text").?.string);
    try std.testing.expectEqual(@as(f32, 0.75), hits[0]._score);
    const hidden = try parseResults(a, .{ .provider = .tavily }, body);
    try std.testing.expect(hidden[0]._source.?.map.get("text") == null);
    try std.testing.expectError(error.InvalidWebSearchResponse, parseResults(a, config, "{\"answer\":\"not evidence\"}"));
    try std.testing.expectError(error.InvalidWebSearchResponse, parseResults(a, config, "{\"results\":[{\"content\":\"missing URL\"}]}"));
}

test "Tavily named connections prevent provider substitution and content policy expansion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var node = try common.Config.parseFromSlice(std.testing.allocator,
        \\{"connections":{"web":{"kind":"web_search","provider":"tavily","capabilities":["web.search","agents.use"],"web_search":{"max_results":2,"include_content":false,"include_domains":["example.com"]}}}}
    );
    defer node.deinit();
    const config = try resolve(a, &node, .{ .connection = "web" });
    try std.testing.expectEqualStrings(tavily_endpoint, config.endpoint);
    try std.testing.expectError(error.InvalidRetrievalAgentRequest, resolve(a, &node, .{ .connection = "web", .inline_config = .{ .provider = .exa } }));
    for ([_]api.TavilySearchConfig{
        .{ .provider = .tavily, .api_key = "replacement" },
        .{ .provider = .tavily, .endpoint = default_endpoint },
        .{ .provider = .tavily, .include_content = true },
        .{ .provider = .tavily, .include_raw_content = true },
        .{ .provider = .tavily, .safe_search = false },
        .{ .provider = .tavily, .include_domains = &.{} },
    }) |override| try std.testing.expectError(error.Forbidden, resolve(a, &node, .{ .connection = "web", .tavily_config = override }));
    const narrowed = try resolve(a, &node, .{ .connection = "web", .tavily_config = .{ .provider = .tavily, .max_results = 20, .exclude_domains = &.{"blocked.example.com"} } });
    try std.testing.expectEqual(@as(usize, 2), narrowed.max_results);
    try std.testing.expectEqualStrings("blocked.example.com", narrowed.exclude_domains[0]);
}
