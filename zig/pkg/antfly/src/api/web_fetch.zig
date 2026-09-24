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

//! Agent `fetch` tool: policy resolution, URL admission and readable-text
//! extraction. Downloads go through the runner, which applies the shared
//! remote-content SSRF controls (private address blocking at connect time,
//! per-hop redirect validation, size and time ceilings).
//!
//! A model may only fetch a URL that is either under a caller-declared
//! `allowed_hosts` entry or was returned verbatim by an earlier `web_search`
//! result in the same run. Arbitrary model-invented URLs are rejected so that
//! injected document text cannot exfiltrate retrieved data through a query
//! string.

const std = @import("std");
const generating_api = @import("antfly_generating_api_openapi");
const websearch_api = @import("antfly_websearch_openapi");
const metadata = @import("antfly_metadata_openapi");

/// Characters of readable text returned to the model per fetched page.
pub const default_max_content_chars: usize = 12000;
pub const max_content_chars_ceiling: usize = 50000;
pub const default_max_download_bytes: u64 = 5 * 1024 * 1024;
pub const max_download_bytes_ceiling: u64 = 20 * 1024 * 1024;
pub const default_timeout_ms: u64 = 15000;
pub const max_timeout_ms: u64 = 60000;
pub const max_url_bytes: usize = 8192;

/// Request-arena owned. Never enters model history or step details.
pub const Config = struct {
    max_content_chars: usize = default_max_content_chars,
    max_download_bytes: u64 = default_max_download_bytes,
    timeout_ms: u64 = default_timeout_ms,
    /// Hosts the model may fetch directly. Subdomains match.
    allowed_hosts: []const []const u8 = &.{},
};

/// Downloaded bytes before extraction. Arena owned by the caller.
pub const Download = struct {
    content_type: []const u8,
    data: []const u8,
};

pub const Page = struct {
    title: ?[]const u8 = null,
    text: []const u8,
    truncated: bool,
};

fn listsFetch(tools: ?generating_api.ChatToolsConfig) bool {
    const config = tools orelse return false;
    const enabled = config.enabled_tools orelse return false;
    for (enabled) |tool| if (tool == .fetch) return true;
    return false;
}

/// Fetch is an explicit opt-in: a scope must list `fetch` in enabled_tools or
/// carry fetch_config. Retrieval-scope settings narrow the request scope.
/// Returns null when fetch is not requested.
pub fn resolve(global: ?generating_api.ChatToolsConfig, retrieval: ?generating_api.ChatToolsConfig) !?Config {
    const global_cfg = if (global) |tools| tools.fetch_config else null;
    const local_cfg = if (retrieval) |tools| tools.fetch_config else null;
    if (!listsFetch(global) and !listsFetch(retrieval) and global_cfg == null and local_cfg == null) return null;
    var config = Config{};
    for ([_]?websearch_api.FetchConfig{ global_cfg, local_cfg }, 0..) |maybe, scope| {
        const cfg = maybe orelse continue;
        // The agent tool never carries object-store authority and cannot
        // disable private-address blocking.
        if (cfg.s3_credentials != null) return error.Forbidden;
        if (cfg.block_private_ips) |block| if (!block) return error.Forbidden;
        // The request scope may set any value up to the server ceiling; the
        // retrieval scope may only narrow it.
        if (cfg.max_content_length) |n| {
            if (n < 1) return error.InvalidRetrievalAgentRequest;
            const value = @min(@as(usize, @intCast(n)), max_content_chars_ceiling);
            config.max_content_chars = if (scope == 0) value else @min(config.max_content_chars, value);
        }
        if (cfg.max_download_size_bytes) |n| {
            if (n < 1) return error.InvalidRetrievalAgentRequest;
            const value = @min(@as(u64, @intCast(n)), max_download_bytes_ceiling);
            config.max_download_bytes = if (scope == 0) value else @min(config.max_download_bytes, value);
        }
        if (cfg.timeout_seconds) |seconds| {
            if (seconds < 1) return error.InvalidRetrievalAgentRequest;
            const value = @min(@as(u64, @intCast(seconds)) *| std.time.ms_per_s, max_timeout_ms);
            config.timeout_ms = if (scope == 0) value else @min(config.timeout_ms, value);
        }
        if (cfg.allowed_hosts) |hosts| {
            if (hosts.len > 100) return error.InvalidRetrievalAgentRequest;
            for (hosts) |host| {
                if (host.len == 0 or host.len > 253 or std.mem.indexOfAny(u8, host, "/:*?#@ \t\r\n") != null) return error.InvalidRetrievalAgentRequest;
            }
            if (scope == 1 and config.allowed_hosts.len > 0) {
                // A retrieval-scope list may only narrow the request list.
                for (hosts) |host| {
                    var covered = false;
                    for (config.allowed_hosts) |outer| if (domainMatches(host, outer)) {
                        covered = true;
                    };
                    if (!covered) return error.Forbidden;
                }
            }
            config.allowed_hosts = hosts;
        }
    }
    return config;
}

pub const Admission = enum { allowed_host, search_result };

/// Validate a model-supplied URL. `known_urls` are URLs returned by web search
/// in this run. Returns a canonical copy owned by `arena`.
pub fn admitUrl(arena: std.mem.Allocator, config: Config, url: []const u8, known_urls: *const std.StringHashMapUnmanaged(void)) !struct { url: []const u8, admission: Admission } {
    const trimmed = std.mem.trim(u8, url, " \t\r\n");
    if (trimmed.len == 0 or trimmed.len > max_url_bytes) return error.InvalidFetchUrl;
    const uri = std.Uri.parse(trimmed) catch return error.InvalidFetchUrl;
    if (!std.ascii.eqlIgnoreCase(uri.scheme, "https") and !std.ascii.eqlIgnoreCase(uri.scheme, "http")) return error.InvalidFetchUrl;
    if (uri.user != null or uri.password != null) return error.InvalidFetchUrl;
    const raw_host = uri.host orelse return error.InvalidFetchUrl;
    const host = (try canonicalHost(arena, switch (raw_host) {
        .raw => |value| value,
        .percent_encoded => |value| value,
    })) orelse return error.InvalidFetchUrl;
    const owned = try arena.dupe(u8, trimmed);
    if (known_urls.contains(trimmed)) return .{ .url = owned, .admission = .search_result };
    for (config.allowed_hosts) |allowed| {
        if (domainMatches(host, allowed)) return .{ .url = owned, .admission = .allowed_host };
    }
    return error.FetchUrlNotAllowed;
}

fn canonicalHost(arena: std.mem.Allocator, encoded: []const u8) !?[]const u8 {
    const decoded = std.Uri.percentDecodeInPlace(try arena.dupe(u8, encoded));
    const host = std.mem.trimEnd(u8, decoded, ".");
    if (host.len == 0) return null;
    for (host) |c| {
        if (!std.ascii.isAlphanumeric(c) and std.mem.indexOfScalar(u8, ".-_:[]", c) == null) return null;
    }
    return host;
}

fn domainMatches(host: []const u8, configured: []const u8) bool {
    const domain = std.mem.trimEnd(u8, configured, ".");
    return std.ascii.eqlIgnoreCase(host, domain) or (host.len > domain.len and host[host.len - domain.len - 1] == '.' and std.ascii.eqlIgnoreCase(host[host.len - domain.len ..], domain));
}

fn mimeIs(content_type: []const u8, expected: []const u8) bool {
    const end = std.mem.indexOfScalar(u8, content_type, ';') orelse content_type.len;
    return std.ascii.eqlIgnoreCase(std.mem.trim(u8, content_type[0..end], " \t"), expected);
}

fn looksLikeHtml(bytes: []const u8) bool {
    const head = std.mem.trimStart(u8, bytes[0..@min(bytes.len, 512)], " \t\r\n\xef\xbb\xbf");
    return std.ascii.startsWithIgnoreCase(head, "<!doctype html") or std.ascii.startsWithIgnoreCase(head, "<html") or std.ascii.startsWithIgnoreCase(head, "<head");
}

/// Convert a downloaded body into bounded readable text. HTML is reduced to
/// visible text with block structure preserved as line breaks; plain text,
/// markdown, JSON and XML pass through. Binary formats are rejected rather
/// than returned as undecodable bytes.
pub fn extract(arena: std.mem.Allocator, download: Download, max_chars: usize) !Page {
    const ct = download.content_type;
    if (mimeIs(ct, "text/html") or mimeIs(ct, "application/xhtml+xml") or ((ct.len == 0 or mimeIs(ct, "application/octet-stream")) and looksLikeHtml(download.data))) {
        const page = try htmlToText(arena, download.data);
        return bound(page.title, page.text, max_chars);
    }
    const textual = std.ascii.startsWithIgnoreCase(ct, "text/") or mimeIs(ct, "application/json") or mimeIs(ct, "application/xml") or mimeIs(ct, "application/markdown");
    if (!textual) return error.UnsupportedFetchContent;
    if (!std.unicode.utf8ValidateSlice(download.data)) return error.UnsupportedFetchContent;
    return bound(null, download.data, max_chars);
}

pub fn bound(title: ?[]const u8, text: []const u8, max_chars: usize) Page {
    var end: usize = 0;
    var chars: usize = 0;
    while (end < text.len and chars < max_chars) : (chars += 1) {
        end += @min(std.unicode.utf8ByteSequenceLength(text[end]) catch 1, text.len - end);
    }
    return .{ .title = title, .text = text[0..end], .truncated = end < text.len };
}

const skipped_elements = [_][]const u8{ "script", "style", "noscript", "template", "svg", "iframe", "object", "canvas" };
const block_elements = [_][]const u8{ "p", "div", "br", "li", "ul", "ol", "tr", "table", "section", "article", "header", "footer", "nav", "aside", "main", "h1", "h2", "h3", "h4", "h5", "h6", "pre", "blockquote", "hr", "dt", "dd", "figcaption", "title" };

fn tagName(tag: []const u8) []const u8 {
    var start: usize = 0;
    if (start < tag.len and tag[start] == '/') start += 1;
    var end = start;
    while (end < tag.len and (std.ascii.isAlphanumeric(tag[end]) or tag[end] == '-')) end += 1;
    return tag[start..end];
}

fn isOneOf(name: []const u8, set: []const []const u8) bool {
    for (set) |candidate| if (std.ascii.eqlIgnoreCase(name, candidate)) return true;
    return false;
}

const HtmlText = struct { title: ?[]const u8, text: []const u8 };

/// Small, allocation-bounded HTML visible-text extractor. It is not a full
/// HTML5 parser; it is tuned for articles and documentation pages.
pub fn htmlToText(arena: std.mem.Allocator, html: []const u8) !HtmlText {
    var out = std.ArrayListUnmanaged(u8).empty;
    var title = std.ArrayListUnmanaged(u8).empty;
    var in_title = false;
    var skip_until: ?[]const u8 = null;
    var pending_space = false;
    var i: usize = 0;
    while (i < html.len) {
        const c = html[i];
        if (c == '<') {
            if (std.mem.startsWith(u8, html[i..], "<!--")) {
                const end = std.mem.indexOfPos(u8, html, i + 4, "-->") orelse html.len;
                i = @min(html.len, end + 3);
                continue;
            }
            const close = std.mem.indexOfScalarPos(u8, html, i + 1, '>') orelse html.len;
            const tag = html[i + 1 .. close];
            i = @min(html.len, close + 1);
            const name = tagName(tag);
            const closing = tag.len > 0 and tag[0] == '/';
            if (skip_until) |until| {
                if (closing and std.ascii.eqlIgnoreCase(name, until)) skip_until = null;
                continue;
            }
            if (std.ascii.eqlIgnoreCase(name, "title")) {
                in_title = !closing;
                continue;
            }
            if (!closing and isOneOf(name, &skipped_elements) and !(tag.len > 0 and tag[tag.len - 1] == '/')) {
                skip_until = name;
                continue;
            }
            // Inline tags do not separate words; block tags start a line.
            if (isOneOf(name, &block_elements)) {
                if (out.items.len > 0 and out.items[out.items.len - 1] != '\n') try out.append(arena, '\n');
                pending_space = false;
            }
            continue;
        }
        if (skip_until != null) {
            i += 1;
            continue;
        }
        const target = if (in_title) &title else &out;
        if (c == '&') {
            const decoded = decodeEntity(html[i..]);
            if (decoded.len > 0) {
                i += decoded.len;
                if (decoded.codepoint == 0xa0 or decoded.codepoint == ' ') {
                    pending_space = target.items.len > 0;
                    continue;
                }
                if (pending_space and target.items.len > 0 and target.items[target.items.len - 1] != '\n') try target.append(arena, ' ');
                pending_space = false;
                var buf: [4]u8 = undefined;
                const n = std.unicode.utf8Encode(decoded.codepoint, &buf) catch 0;
                try target.appendSlice(arena, buf[0..n]);
                continue;
            }
        }
        if (std.ascii.isWhitespace(c)) {
            pending_space = target.items.len > 0;
            i += 1;
            continue;
        }
        if (pending_space and target.items.len > 0 and target.items[target.items.len - 1] != '\n') try target.append(arena, ' ');
        pending_space = false;
        try target.append(arena, c);
        i += 1;
    }
    const text = try collapseBlankLines(arena, out.items);
    const trimmed_title = std.mem.trim(u8, title.items, " \t\r\n");
    return .{ .title = if (trimmed_title.len > 0) trimmed_title else null, .text = text };
}

fn collapseBlankLines(arena: std.mem.Allocator, text: []const u8) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;
        if (out.items.len > 0) try out.append(arena, '\n');
        try out.appendSlice(arena, trimmed);
    }
    return out.items;
}

const Entity = struct { codepoint: u21 = 0, len: usize = 0 };

fn decodeEntity(text: []const u8) Entity {
    const end = std.mem.indexOfScalar(u8, text[0..@min(text.len, 12)], ';') orelse return .{};
    const body = text[1..end];
    const named = [_]struct { []const u8, u21 }{
        .{ "amp", '&' },       .{ "lt", '<' },       .{ "gt", '>' },       .{ "quot", '"' },
        .{ "apos", '\'' },     .{ "nbsp", 0xa0 },    .{ "mdash", 0x2014 }, .{ "ndash", 0x2013 },
        .{ "hellip", 0x2026 }, .{ "rsquo", 0x2019 }, .{ "lsquo", 0x2018 }, .{ "rdquo", 0x201d },
        .{ "ldquo", 0x201c },  .{ "copy", 0xa9 },
    };
    for (named) |entry| if (std.mem.eql(u8, body, entry[0])) return .{ .codepoint = entry[1], .len = end + 1 };
    if (body.len > 1 and body[0] == '#') {
        const value = if (body[1] == 'x' or body[1] == 'X')
            std.fmt.parseInt(u21, body[2..], 16) catch return .{}
        else
            std.fmt.parseInt(u21, body[1..], 10) catch return .{};
        if (value == 0 or value > 0x10ffff or (value >= 0xd800 and value <= 0xdfff)) return .{};
        return .{ .codepoint = value, .len = end + 1 };
    }
    return .{};
}

/// Evidence hit for a fetched page. The `fetch:` prefix keeps fetched pages
/// distinct from the same URL's web-search snippet.
pub fn toHit(arena: std.mem.Allocator, url: []const u8, content_type: []const u8, page: Page) !metadata.QueryHit {
    var source = std.json.ArrayHashMap(std.json.Value){};
    try source.map.put(arena, "url", .{ .string = url });
    if (page.title) |title| try source.map.put(arena, "title", .{ .string = page.title.?[0..@min(title.len, 1024)] });
    try source.map.put(arena, "text", .{ .string = page.text });
    try source.map.put(arena, "content_type", .{ .string = content_type });
    try source.map.put(arena, "truncated", .{ .bool = page.truncated });
    return .{
        ._id = try std.fmt.allocPrint(arena, "fetch:{s}", .{url}),
        ._score = 1,
        ._source = source,
    };
}

test "fetch policy is opt-in and never disables private address blocking" {
    try std.testing.expect((try resolve(null, null)) == null);
    try std.testing.expect((try resolve(.{ .enabled_tools = &.{.web_search} }, null)) == null);
    const enabled = (try resolve(.{ .enabled_tools = &.{.fetch} }, null)).?;
    try std.testing.expectEqual(default_max_content_chars, enabled.max_content_chars);
    try std.testing.expectError(error.Forbidden, resolve(.{ .fetch_config = .{ .block_private_ips = false } }, null));
    const narrowed = (try resolve(
        .{ .fetch_config = .{ .allowed_hosts = &.{"example.com"}, .max_content_length = 100000, .timeout_seconds = 600 } },
        .{ .fetch_config = .{ .allowed_hosts = &.{"docs.example.com"}, .max_content_length = 500 } },
    )).?;
    try std.testing.expectEqual(@as(usize, 500), narrowed.max_content_chars);
    try std.testing.expectEqual(max_timeout_ms, narrowed.timeout_ms);
    try std.testing.expectEqualStrings("docs.example.com", narrowed.allowed_hosts[0]);
    try std.testing.expectError(error.Forbidden, resolve(
        .{ .fetch_config = .{ .allowed_hosts = &.{"example.com"} } },
        .{ .fetch_config = .{ .allowed_hosts = &.{"evil.test"} } },
    ));
}

test "fetch admits only allowed hosts or earlier search results" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var known = std.StringHashMapUnmanaged(void).empty;
    try known.put(a, "https://news.test/a?id=1", {});
    const config = Config{ .allowed_hosts = &.{"example.com"} };
    try std.testing.expectEqual(Admission.allowed_host, (try admitUrl(a, config, "https://docs.example.com/x", &known)).admission);
    try std.testing.expectEqual(Admission.search_result, (try admitUrl(a, config, "https://news.test/a?id=1", &known)).admission);
    try std.testing.expectError(error.FetchUrlNotAllowed, admitUrl(a, config, "https://news.test/a?id=2&secret=leak", &known));
    try std.testing.expectError(error.FetchUrlNotAllowed, admitUrl(a, config, "https://notexample.com/", &known));
    try std.testing.expectError(error.InvalidFetchUrl, admitUrl(a, config, "file:///etc/passwd", &known));
    try std.testing.expectError(error.InvalidFetchUrl, admitUrl(a, config, "https://user:pw@example.com/", &known));
    try std.testing.expectError(error.InvalidFetchUrl, admitUrl(a, config, "s3://bucket/key", &known));
}

test "html extraction keeps visible text and drops scripts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const page = try extract(arena.allocator(), .{
        .content_type = "text/html; charset=utf-8",
        .data = "<html><head><title>Hybrid &amp; Rerank</title><style>p{}</style></head><body><nav>Home</nav><h1>Tuning</h1><p>Use <b>RRF</b>&nbsp;first.</p><script>steal()</script><!-- note --><p>Then&#x20;rerank &#8212; carefully.</p></body></html>",
    }, 1000);
    try std.testing.expectEqualStrings("Hybrid & Rerank", page.title.?);
    try std.testing.expectEqualStrings("Home\nTuning\nUse RRF first.\nThen rerank \u{2014} carefully.", page.text);
    try std.testing.expect(!page.truncated);
    const bounded = try extract(arena.allocator(), .{ .content_type = "text/plain", .data = "\u{4e2d}\u{6587}abc" }, 3);
    try std.testing.expectEqualStrings("\u{4e2d}\u{6587}a", bounded.text);
    try std.testing.expect(bounded.truncated);
    try std.testing.expectError(error.UnsupportedFetchContent, extract(arena.allocator(), .{ .content_type = "image/png", .data = "\x89PNG" }, 10));
}
