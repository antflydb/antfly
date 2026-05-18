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

//! Template rendering for the enrichment pipeline.
//!
//! Wraps handlebars-zig to render document fields through Handlebars templates,
//! producing text for embedding. Matches Go antfly's lib/template/ package.
//!
//! Built-in helpers:
//!   - scrubHtml: strip HTML tags, return plain text
//!   - eq: equality comparison for use in {{#if (eq a b)}}
//!   - media: GenKit dotprompt media directive {{media url=...}}
//!
//! Go reference:
//!   lib/template/template.go — Render, helpers (scrubHtml, eq, media, encodeToon)
//!   lib/template/parts.go — DocumentToParts
//!   lib/template/extractor.go — ExtractFieldPaths

const std = @import("std");
const Allocator = std.mem.Allocator;
const hbs = @import("handlebars");
const transcribing = @import("antfly_transcribing");
const Value = hbs.Value;
const Helper = hbs.Helper;
const HelperContext = hbs.HelperContext;
const HelperMap = hbs.HelperMap;

/// Render a Handlebars template against a JSON document.
/// The document is parsed as JSON and its top-level fields become template variables.
/// Internal fields starting with "_" (like _embeddings) are excluded.
///
/// Returns the rendered string, allocated with `alloc`.
pub fn renderDocument(
    alloc: Allocator,
    template_source: []const u8,
    doc_json: []const u8,
) ![]const u8 {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // Parse document JSON
    const parsed = try std.json.parseFromSlice(std.json.Value, arena, doc_json, .{});

    // Convert JSON into a handlebars root value. Object roots expose top-level
    // fields as variables; scalar roots are still valid and make `this` usable
    // for query-template rendering like `{{remoteText url=this}}`.
    const context = try jsonToValue(arena, parsed.value);

    // Register helpers
    var helpers: HelperMap = .{};
    try helpers.put(arena, "scrubHtml", Helper.from(&scrubHtmlHelper));
    try helpers.put(arena, "eq", Helper.from(&eqHelper));
    try helpers.put(arena, "media", Helper.from(&mediaHelper));
    try helpers.put(arena, "remoteMedia", Helper.from(&remoteMediaHelper));
    try helpers.put(arena, "remotePDF", Helper.from(&remotePDFHelper));
    try helpers.put(arena, "remoteText", Helper.from(&remoteTextHelper));
    try helpers.put(arena, "transcribeAudio", Helper.from(&transcribeAudioHelper));

    const partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};

    // Render
    const result = try hbs.render(arena, template_source, context, &helpers, &partials);

    return try alloc.dupe(u8, result);
}

/// Render with additional caller-supplied helpers.
pub fn renderDocumentWithHelpers(
    alloc: Allocator,
    template_source: []const u8,
    doc_json: []const u8,
    extra_helpers: *const HelperMap,
) ![]const u8 {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const parsed = try std.json.parseFromSlice(std.json.Value, arena, doc_json, .{});
    const context = try jsonToValue(arena, parsed.value);

    // Merge built-in + caller helpers
    var helpers: HelperMap = .{};
    try helpers.put(arena, "scrubHtml", Helper.from(&scrubHtmlHelper));
    try helpers.put(arena, "eq", Helper.from(&eqHelper));
    try helpers.put(arena, "media", Helper.from(&mediaHelper));
    try helpers.put(arena, "remoteMedia", Helper.from(&remoteMediaHelper));
    try helpers.put(arena, "remotePDF", Helper.from(&remotePDFHelper));
    try helpers.put(arena, "remoteText", Helper.from(&remoteTextHelper));
    try helpers.put(arena, "transcribeAudio", Helper.from(&transcribeAudioHelper));
    // Caller helpers override built-ins (allows wiring real HTTP-backed implementations)
    for (extra_helpers.keys(), extra_helpers.values()) |k, v| {
        try helpers.put(arena, k, v);
    }

    const partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    const result = try hbs.render(arena, template_source, context, &helpers, &partials);
    return try alloc.dupe(u8, result);
}

/// Extract field paths referenced in a Handlebars template.
/// Returns deduplicated field path arrays (e.g., [["title"], ["author", "name"]]).
/// Matches Go's template.ExtractFieldPaths.
pub fn extractFieldPaths(
    alloc: Allocator,
    template_source: []const u8,
) ![]const []const []const u8 {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const paths = try hbs.extractFieldPaths(arena, template_source);

    // Copy to caller's allocator
    const result = try alloc.alloc([]const []const u8, paths.len);
    for (paths, 0..) |path, i| {
        const parts = try alloc.alloc([]const u8, path.len);
        for (path, 0..) |part, j| {
            parts[j] = try alloc.dupe(u8, part);
        }
        result[i] = parts;
    }
    return result;
}

/// Free field paths returned by extractFieldPaths.
pub fn freeFieldPaths(alloc: Allocator, paths: []const []const []const u8) void {
    for (paths) |path| {
        for (path) |part| alloc.free(@constCast(part));
        alloc.free(path);
    }
    alloc.free(paths);
}

/// Extract top-level field names referenced in a template (flattened).
/// Returns just the first part of each path, deduplicated.
pub fn extractTopLevelFields(
    alloc: Allocator,
    template_source: []const u8,
) ![]const []const u8 {
    const paths = try extractFieldPaths(alloc, template_source);
    defer freeFieldPaths(alloc, paths);

    var seen = std.StringArrayHashMapUnmanaged(void){};
    defer seen.deinit(alloc);

    var result = std.ArrayListUnmanaged([]const u8).empty;
    defer result.deinit(alloc);

    for (paths) |path| {
        if (path.len == 0) continue;
        if (seen.contains(path[0])) continue;
        const field = try alloc.dupe(u8, path[0]);
        try seen.put(alloc, field, {});
        try result.append(alloc, field);
    }

    return try result.toOwnedSlice(alloc);
}

// ============================================================================
// Built-in helpers
// ============================================================================

/// Strip HTML tags and return plain text.
fn scrubHtmlHelper(ctx: HelperContext) anyerror!Value {
    if (ctx.params.len == 0) return .{ .string = "" };
    const html = switch (ctx.params[0]) {
        .string => |s| s,
        else => return .{ .string = "" },
    };

    var result = std.ArrayListUnmanaged(u8).empty;
    var in_tag = false;
    var in_script = false;
    var idx: usize = 0;
    while (idx < html.len) {
        if (html[idx] == '<') {
            if (!in_script) {
                // Check for script/style open tags
                if (idx + 7 <= html.len and std.ascii.eqlIgnoreCase(html[idx..][0..7], "<script")) {
                    in_script = true;
                } else if (idx + 6 <= html.len and std.ascii.eqlIgnoreCase(html[idx..][0..6], "<style")) {
                    in_script = true;
                }
            }
            // Check for close tags
            if (in_script) {
                if (idx + 9 <= html.len and std.ascii.eqlIgnoreCase(html[idx..][0..9], "</script>")) {
                    in_script = false;
                    idx += 9;
                    continue;
                }
                if (idx + 8 <= html.len and std.ascii.eqlIgnoreCase(html[idx..][0..8], "</style>")) {
                    in_script = false;
                    idx += 8;
                    continue;
                }
            }
            in_tag = true;
            idx += 1;
            continue;
        }
        if (html[idx] == '>') {
            in_tag = false;
            idx += 1;
            continue;
        }
        if (!in_tag and !in_script) {
            try result.append(ctx.arena, html[idx]);
        }
        idx += 1;
    }

    const trimmed = std.mem.trim(u8, result.items, &std.ascii.whitespace);
    return .{ .string = try ctx.arena.dupe(u8, trimmed) };
}

/// Equality comparison: {{#if (eq a b)}}
fn eqHelper(ctx: HelperContext) anyerror!Value {
    if (ctx.params.len < 2) return .{ .boolean = false };
    const a = ctx.params[0];
    const b = ctx.params[1];

    const equal = switch (a) {
        .string => |s| switch (b) {
            .string => |s2| std.mem.eql(u8, s, s2),
            else => false,
        },
        .integer => |int_a| switch (b) {
            .integer => |int_b| int_a == int_b,
            .float => |f| @as(f64, @floatFromInt(int_a)) == f,
            else => false,
        },
        .float => |f| switch (b) {
            .float => |f2| f == f2,
            .integer => |int_b| f == @as(f64, @floatFromInt(int_b)),
            else => false,
        },
        .boolean => |bl| switch (b) {
            .boolean => |bl2| bl == bl2,
            else => false,
        },
        .null => switch (b) {
            .null => true,
            else => false,
        },
        else => false,
    };

    return .{ .boolean = equal };
}

/// GenKit dotprompt media directive: {{media url=imageDataURI}}
/// Returns: <<<dotprompt:media:url {url}>>>
fn mediaHelper(ctx: HelperContext) anyerror!Value {
    const url = ctx.hash.get("url") orelse return .{ .string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .string = "" },
    };
    if (url_str.len == 0) return .{ .string = "" };

    const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url {s}>>>", .{url_str});
    return .{ .safe_string = result };
}

// ============================================================================
// Error Directives
// ============================================================================
// Structured markers embedded in rendered template output by helpers (e.g.,
// remoteMedia) when they encounter errors. Format follows the existing
// <<<...>>> directive pattern used by dotprompt media markers.
//
// Format:
//   <<<error:status=404 message=Not Found>>>
//   <<<error:message=connection refused>>>

pub const ErrorDirective = struct {
    status: u16, // HTTP status code, or 0 for non-HTTP errors
    message: []const u8,

    /// Returns true if the status indicates the resource is permanently
    /// unavailable and should not be retried.
    pub fn isPermanent(self: ErrorDirective) bool {
        return self.status == 401 or self.status == 403 or
            self.status == 404 or self.status == 410;
    }
};

/// Format an error directive for embedding in rendered output.
/// If status is 0, the status field is omitted.
pub fn formatErrorDirective(alloc: Allocator, status: u16, message: []const u8) ![]const u8 {
    const sanitized = try sanitizeDirectiveMessage(alloc, message);
    defer alloc.free(sanitized);

    if (status > 0) {
        return try std.fmt.allocPrint(alloc, "<<<error:status={d} message={s}>>>", .{ status, sanitized });
    }
    return try std.fmt.allocPrint(alloc, "<<<error:message={s}>>>", .{sanitized});
}

fn sanitizeDirectiveMessage(alloc: Allocator, msg: []const u8) ![]const u8 {
    var result = std.ArrayListUnmanaged(u8).empty;
    var i: usize = 0;
    while (i < msg.len) {
        if (i + 3 <= msg.len and std.mem.eql(u8, msg[i..][0..3], ">>>")) {
            try result.appendSlice(alloc, ">>\\>");
            i += 3;
        } else {
            try result.append(alloc, msg[i]);
            i += 1;
        }
    }
    return try result.toOwnedSlice(alloc);
}

/// Parse all error directives from a string.
/// Caller owns the returned slice and each message within.
pub fn parseErrorDirectives(alloc: Allocator, text: []const u8) ![]ErrorDirective {
    var directives = std.ArrayListUnmanaged(ErrorDirective).empty;
    const prefix_with_status = "<<<error:status=";
    const prefix_no_status = "<<<error:message=";
    const suffix = ">>>";

    var pos: usize = 0;
    while (pos < text.len) {
        if (pos + prefix_with_status.len <= text.len and
            std.mem.eql(u8, text[pos..][0..prefix_with_status.len], prefix_with_status))
        {
            // Parse: <<<error:status=NNN message=...>>>
            const after_prefix = pos + prefix_with_status.len;
            const space_idx = std.mem.indexOfScalarPos(u8, text, after_prefix, ' ') orelse {
                pos += 1;
                continue;
            };
            const status_str = text[after_prefix..space_idx];
            const status = std.fmt.parseInt(u16, status_str, 10) catch {
                pos += 1;
                continue;
            };

            const msg_prefix = "message=";
            if (space_idx + 1 + msg_prefix.len > text.len or
                !std.mem.eql(u8, text[space_idx + 1 ..][0..msg_prefix.len], msg_prefix))
            {
                pos += 1;
                continue;
            }
            const msg_start = space_idx + 1 + msg_prefix.len;
            const end_idx = std.mem.indexOfPos(u8, text, msg_start, suffix) orelse {
                pos += 1;
                continue;
            };
            const message = try alloc.dupe(u8, text[msg_start..end_idx]);
            try directives.append(alloc, .{ .status = status, .message = message });
            pos = end_idx + suffix.len;
        } else if (pos + prefix_no_status.len <= text.len and
            std.mem.eql(u8, text[pos..][0..prefix_no_status.len], prefix_no_status))
        {
            // Parse: <<<error:message=...>>>
            const msg_start = pos + prefix_no_status.len;
            const end_idx = std.mem.indexOfPos(u8, text, msg_start, suffix) orelse {
                pos += 1;
                continue;
            };
            const message = try alloc.dupe(u8, text[msg_start..end_idx]);
            try directives.append(alloc, .{ .status = 0, .message = message });
            pos = end_idx + suffix.len;
        } else {
            pos += 1;
        }
    }

    return try directives.toOwnedSlice(alloc);
}

/// Free directives returned by parseErrorDirectives.
pub fn freeErrorDirectives(alloc: Allocator, directives: []const ErrorDirective) void {
    for (directives) |d| alloc.free(@constCast(d.message));
    alloc.free(directives);
}

/// Returns true if the string contains any error directive.
pub fn containsErrorDirective(text: []const u8) bool {
    return std.mem.indexOf(u8, text, "<<<error:") != null;
}

/// Remove all error directives from a string.
/// Caller owns the returned slice.
pub fn stripErrorDirectives(alloc: Allocator, text: []const u8) ![]const u8 {
    var result = std.ArrayListUnmanaged(u8).empty;
    const prefix_with_status = "<<<error:status=";
    const prefix_no_status = "<<<error:message=";
    const suffix = ">>>";

    var pos: usize = 0;
    while (pos < text.len) {
        if ((pos + prefix_with_status.len <= text.len and
            std.mem.eql(u8, text[pos..][0..prefix_with_status.len], prefix_with_status)) or
            (pos + prefix_no_status.len <= text.len and
                std.mem.eql(u8, text[pos..][0..prefix_no_status.len], prefix_no_status)))
        {
            // Find the closing >>>
            const end_idx = std.mem.indexOfPos(u8, text, pos + 9, suffix) orelse {
                try result.append(alloc, text[pos]);
                pos += 1;
                continue;
            };
            pos = end_idx + suffix.len;
        } else {
            try result.append(alloc, text[pos]);
            pos += 1;
        }
    }

    return try result.toOwnedSlice(alloc);
}

// ============================================================================
// Content Parts (DocumentToParts)
// ============================================================================
// Splits rendered template output into structured content parts by parsing
// <<<dotprompt:media:url ...>>> and <<<error:...>>> directives.
// Matches Go ai.TextToParts from lib/ai/utils.go.

pub const ContentPart = union(enum) {
    text: []const u8,
    media_url: []const u8, // URL or data URI
    binary: BinaryContent, // parsed data URI

    pub const BinaryContent = struct {
        mime_type: []const u8,
        data: []const u8, // raw bytes (decoded from base64)
    };
};

/// Parse a rendered template string into content parts.
/// Splits on <<<dotprompt:media:url ...>>> directives.
/// Error directives are stripped. Text segments between markers become text parts.
/// Data URIs in media markers are parsed into binary parts.
/// Caller owns all returned slices.
pub fn textToParts(alloc: Allocator, text: []const u8) ![]ContentPart {
    // First strip error directives
    const cleaned = try stripErrorDirectives(alloc, text);
    defer alloc.free(cleaned);

    const media_prefix = "<<<dotprompt:media:url ";
    const media_suffix = ">>>";

    var parts = std.ArrayListUnmanaged(ContentPart).empty;
    var pos: usize = 0;

    while (pos < cleaned.len) {
        const marker_start = std.mem.indexOfPos(u8, cleaned, pos, media_prefix) orelse {
            // No more markers — rest is text
            const segment = std.mem.trim(u8, cleaned[pos..], &std.ascii.whitespace);
            if (segment.len > 0) {
                try parts.append(alloc, .{ .text = try alloc.dupe(u8, segment) });
            }
            break;
        };

        // Text before marker
        if (marker_start > pos) {
            const segment = std.mem.trim(u8, cleaned[pos..marker_start], &std.ascii.whitespace);
            if (segment.len > 0) {
                try parts.append(alloc, .{ .text = try alloc.dupe(u8, segment) });
            }
        }

        const url_start = marker_start + media_prefix.len;
        const marker_end = std.mem.indexOfPos(u8, cleaned, url_start, media_suffix) orelse {
            // Unterminated marker — treat rest as text
            const segment = std.mem.trim(u8, cleaned[pos..], &std.ascii.whitespace);
            if (segment.len > 0) {
                try parts.append(alloc, .{ .text = try alloc.dupe(u8, segment) });
            }
            break;
        };

        const url = cleaned[url_start..marker_end];

        if (std.mem.startsWith(u8, url, "data:")) {
            // Parse data URI into binary content
            if (parseDataURI(alloc, url)) |binary| {
                try parts.append(alloc, .{ .binary = binary });
            } else |_| {
                // Failed to parse — keep as media_url
                try parts.append(alloc, .{ .media_url = try alloc.dupe(u8, url) });
            }
        } else {
            try parts.append(alloc, .{ .media_url = try alloc.dupe(u8, url) });
        }

        pos = marker_end + media_suffix.len;
    }

    return try parts.toOwnedSlice(alloc);
}

/// Free content parts returned by textToParts.
pub fn freeContentParts(alloc: Allocator, parts: []const ContentPart) void {
    for (parts) |part| {
        switch (part) {
            .text => |t| alloc.free(@constCast(t)),
            .media_url => |u| alloc.free(@constCast(u)),
            .binary => |b| {
                alloc.free(@constCast(b.mime_type));
                alloc.free(@constCast(b.data));
            },
        }
    }
    alloc.free(parts);
}

/// Parse a data URI (data:mime/type;base64,DATA) into mime type and decoded bytes.
pub fn parseDataURI(alloc: Allocator, uri: []const u8) !ContentPart.BinaryContent {
    if (!std.mem.startsWith(u8, uri, "data:")) return error.InvalidDataURI;

    const after_data = uri[5..];
    // Find ;base64, separator
    const base64_marker = ";base64,";
    const sep_idx = std.mem.indexOf(u8, after_data, base64_marker) orelse return error.InvalidDataURI;

    const mime_type = try alloc.dupe(u8, after_data[0..sep_idx]);
    errdefer alloc.free(mime_type);

    const encoded = after_data[sep_idx + base64_marker.len ..];

    // Decode base64
    const decoder = std.base64.standard;
    const decoded_len = decoder.Decoder.calcSizeForSlice(encoded) catch return error.InvalidDataURI;
    const decoded = try alloc.alloc(u8, decoded_len);
    errdefer alloc.free(decoded);
    decoder.Decoder.decode(decoded, encoded) catch return error.InvalidDataURI;

    return .{
        .mime_type = mime_type,
        .data = decoded,
    };
}

// ============================================================================
// Remote content helper stubs
// ============================================================================
// These are stub helpers that emit error directives since they need HTTP client
// integration (httpx.zig) to actually download content. They can be wired to
// real implementations by passing them as extra_helpers to renderDocumentWithHelpers.

/// remoteMedia stub: emits error directive (needs HTTP client to download)
fn remoteMediaHelper(ctx: HelperContext) anyerror!Value {
    const url = ctx.hash.get("url") orelse return .{ .safe_string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .safe_string = "" },
    };
    if (url_str.len == 0) return .{ .safe_string = "" };

    // If it's already a data URI, emit media directive directly
    if (std.mem.startsWith(u8, url_str, "data:")) {
        const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url {s}>>>", .{url_str});
        return .{ .safe_string = result };
    }

    // For remote URLs, emit error directive (no HTTP client available in Zig yet)
    const result = try std.fmt.allocPrint(ctx.arena, "<<<error:message=remoteMedia requires HTTP client for {s}>>>", .{url_str});
    return .{ .safe_string = result };
}

/// remotePDF stub: emits error directive (needs HTTP client + PDF extraction)
fn remotePDFHelper(ctx: HelperContext) anyerror!Value {
    const url = ctx.hash.get("url") orelse return .{ .safe_string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .safe_string = "" },
    };
    if (url_str.len == 0) return .{ .safe_string = "" };

    const result = try std.fmt.allocPrint(ctx.arena, "<<<error:message=remotePDF requires HTTP client for {s}>>>", .{url_str});
    return .{ .safe_string = result };
}

/// remoteText stub: emits error directive (needs HTTP client)
fn remoteTextHelper(ctx: HelperContext) anyerror!Value {
    const url = ctx.hash.get("url") orelse return .{ .safe_string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .safe_string = "" },
    };
    if (url_str.len == 0) return .{ .safe_string = "" };

    const result = try std.fmt.allocPrint(ctx.arena, "<<<error:message=remoteText requires HTTP client for {s}>>>", .{url_str});
    return .{ .safe_string = result };
}

/// transcribeAudio helper. Uses the active transcribing runtime when configured,
/// otherwise emits an error directive.
fn transcribeAudioHelper(ctx: HelperContext) anyerror!Value {
    const url = ctx.hash.get("url") orelse return .{ .safe_string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .safe_string = "" },
    };
    if (url_str.len == 0) return .{ .safe_string = "" };

    const runtime = transcribing.getActiveRuntime() orelse {
        const result = try std.fmt.allocPrint(ctx.arena, "<<<error:message=transcribeAudio requires STT provider for {s}>>>", .{url_str});
        return .{ .safe_string = result };
    };

    const provider_name = if (ctx.hash.get("provider")) |provider| switch (provider) {
        .string => |s| s,
        else => null,
    } else null;
    const language = if (ctx.hash.get("language")) |lang| switch (lang) {
        .string => |s| s,
        else => null,
    } else null;
    const timestamps = if (ctx.hash.get("timestamps")) |flag| switch (flag) {
        .boolean => |b| b,
        else => null,
    } else null;
    const diarization = if (ctx.hash.get("diarization")) |flag| switch (flag) {
        .boolean => |b| b,
        else => null,
    } else null;

    const transcriber = runtime.get(provider_name) catch |err| {
        const result = try formatErrorDirective(ctx.arena, 0, @errorName(err));
        return .{ .safe_string = result };
    };

    var response = transcriber.transcribe(ctx.arena, .{
        .url = url_str,
        .language = language,
        .timestamps = timestamps,
        .diarization = diarization,
    }) catch |err| {
        const result = try formatErrorDirective(ctx.arena, 0, @errorName(err));
        return .{ .safe_string = result };
    };
    defer transcribing.deinitResponse(ctx.arena, &response);

    return .{ .string = try ctx.arena.dupe(u8, response.text orelse "") };
}

// ============================================================================
// JSON → Value conversion
// ============================================================================

/// Convert a std.json.Value to a handlebars Value.
fn jsonToValue(arena: Allocator, json: std.json.Value) Allocator.Error!Value {
    return switch (json) {
        .null => .null,
        .bool => |b| .{ .boolean = b },
        .integer => |val| .{ .integer = val },
        .float => |f| .{ .float = f },
        .number_string => |s| .{ .string = s },
        .string => |s| .{ .string = s },
        .array => |arr| blk: {
            const items = try arena.alloc(Value, arr.items.len);
            for (arr.items, 0..) |item, j| {
                items[j] = try jsonToValue(arena, item);
            }
            break :blk .{ .array = items };
        },
        .object => |obj| blk: {
            var map: hbs.ValueMap = .{};
            var it = obj.iterator();
            while (it.next()) |entry| {
                // Skip internal fields
                if (entry.key_ptr.*.len > 0 and entry.key_ptr.*[0] == '_') continue;
                try map.put(arena, entry.key_ptr.*, try jsonToValue(arena, entry.value_ptr.*));
            }
            break :blk .{ .map = map };
        },
    };
}

// ============================================================================
// Tests
// ============================================================================

test "renderDocument basic field substitution" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "Title: {{title}}", "{\"title\":\"Hello World\"}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("Title: Hello World", result);
}

test "renderDocument skips _embeddings" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{title}}{{_embeddings}}", "{\"title\":\"Hello\",\"_embeddings\":[1,2,3]}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("Hello", result);
}

test "renderDocument with if block" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{#if photoUrl}}Has photo{{/if}}", "{\"photoUrl\":\"http://example.com/img.jpg\"}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("Has photo", result);
}

test "renderDocument with each block" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{#each tags}}{{this}} {{/each}}", "{\"tags\":[\"a\",\"b\",\"c\"]}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("a b c ", result);
}

test "renderDocument with nested fields" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{author.name}}", "{\"author\":{\"name\":\"Alice\"}}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("Alice", result);
}

test "renderDocument supports scalar root values via this" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{this}}", "\"https://example.com/doc.txt\"");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("https://example.com/doc.txt", result);
}

test "scrubHtml helper" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{scrubHtml body}}", "{\"body\":\"<p>Hello</p><script>evil()</script><p>World</p>\"}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("HelloWorld", result);
}

test "eq helper true" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{#if (eq status \"active\")}}yes{{else}}no{{/if}}", "{\"status\":\"active\"}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("yes", result);
}

test "eq helper false" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{#if (eq status \"active\")}}yes{{else}}no{{/if}}", "{\"status\":\"inactive\"}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("no", result);
}

test "media helper" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{media url=photoUrl}}", "{\"photoUrl\":\"data:image/png;base64,abc\"}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("<<<dotprompt:media:url data:image/png;base64,abc>>>", result);
}

test "extractFieldPaths" {
    const alloc = std.testing.allocator;
    const fields = try extractFieldPaths(alloc, "{{title}} {{author.name}} {{body}}");
    defer freeFieldPaths(alloc, fields);
    try std.testing.expectEqual(@as(usize, 3), fields.len);
}

test "extractTopLevelFields" {
    const alloc = std.testing.allocator;
    const fields = try extractTopLevelFields(alloc, "{{title}} {{author.name}} {{body}}");
    defer {
        for (fields) |f| alloc.free(@constCast(f));
        alloc.free(fields);
    }
    try std.testing.expectEqual(@as(usize, 3), fields.len);
}

// ============================================================================
// Error Directive Tests
// ============================================================================

test "formatErrorDirective with status" {
    const alloc = std.testing.allocator;
    const result = try formatErrorDirective(alloc, 404, "Not Found");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("<<<error:status=404 message=Not Found>>>", result);
}

test "formatErrorDirective without status" {
    const alloc = std.testing.allocator;
    const result = try formatErrorDirective(alloc, 0, "connection refused");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("<<<error:message=connection refused>>>", result);
}

test "formatErrorDirective sanitizes >>>" {
    const alloc = std.testing.allocator;
    const result = try formatErrorDirective(alloc, 500, "bad response>>>injected");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("<<<error:status=500 message=bad response>>\\>injected>>>", result);
}

test "parseErrorDirectives single with status" {
    const alloc = std.testing.allocator;
    const directives = try parseErrorDirectives(alloc, "<<<error:status=404 message=Not Found>>>");
    defer freeErrorDirectives(alloc, directives);
    try std.testing.expectEqual(@as(usize, 1), directives.len);
    try std.testing.expectEqual(@as(u16, 404), directives[0].status);
    try std.testing.expectEqualStrings("Not Found", directives[0].message);
}

test "parseErrorDirectives single without status" {
    const alloc = std.testing.allocator;
    const directives = try parseErrorDirectives(alloc, "<<<error:message=connection refused>>>");
    defer freeErrorDirectives(alloc, directives);
    try std.testing.expectEqual(@as(usize, 1), directives.len);
    try std.testing.expectEqual(@as(u16, 0), directives[0].status);
    try std.testing.expectEqualStrings("connection refused", directives[0].message);
}

test "parseErrorDirectives multiple" {
    const alloc = std.testing.allocator;
    const directives = try parseErrorDirectives(alloc, "<<<error:status=404 message=Not Found>>> some text <<<error:status=503 message=Service Unavailable>>>");
    defer freeErrorDirectives(alloc, directives);
    try std.testing.expectEqual(@as(usize, 2), directives.len);
    try std.testing.expectEqual(@as(u16, 404), directives[0].status);
    try std.testing.expectEqual(@as(u16, 503), directives[1].status);
}

test "parseErrorDirectives no directives" {
    const alloc = std.testing.allocator;
    const directives = try parseErrorDirectives(alloc, "just some normal text");
    defer freeErrorDirectives(alloc, directives);
    try std.testing.expectEqual(@as(usize, 0), directives.len);
}

test "parseErrorDirectives does not match dotprompt media" {
    const alloc = std.testing.allocator;
    const directives = try parseErrorDirectives(alloc, "<<<dotprompt:media:url https://example.com/img.png>>>");
    defer freeErrorDirectives(alloc, directives);
    try std.testing.expectEqual(@as(usize, 0), directives.len);
}

test "format and parse round trip" {
    const alloc = std.testing.allocator;
    const cases = [_]struct { status: u16, message: []const u8 }{
        .{ .status = 404, .message = "Not Found" },
        .{ .status = 0, .message = "connection refused" },
        .{ .status = 503, .message = "Service Unavailable" },
    };
    for (cases) |c| {
        const formatted = try formatErrorDirective(alloc, c.status, c.message);
        defer alloc.free(formatted);
        const directives = try parseErrorDirectives(alloc, formatted);
        defer freeErrorDirectives(alloc, directives);
        try std.testing.expectEqual(@as(usize, 1), directives.len);
        try std.testing.expectEqual(c.status, directives[0].status);
        try std.testing.expectEqualStrings(c.message, directives[0].message);
    }
}

test "containsErrorDirective" {
    try std.testing.expect(containsErrorDirective("<<<error:status=404 message=Not Found>>>"));
    try std.testing.expect(containsErrorDirective("prefix <<<error:message=timeout>>> suffix"));
    try std.testing.expect(!containsErrorDirective("clean text"));
    try std.testing.expect(!containsErrorDirective(""));
    try std.testing.expect(!containsErrorDirective("<<<dotprompt:media:url https://example.com>>>"));
}

test "ErrorDirective isPermanent" {
    const permanent = [_]u16{ 401, 403, 404, 410 };
    for (permanent) |code| {
        const d = ErrorDirective{ .status = code, .message = "test" };
        try std.testing.expect(d.isPermanent());
    }
    const transient = [_]u16{ 0, 200, 301, 400, 429, 500, 503 };
    for (transient) |code| {
        const d = ErrorDirective{ .status = code, .message = "test" };
        try std.testing.expect(!d.isPermanent());
    }
}

test "stripErrorDirectives removes directives" {
    const alloc = std.testing.allocator;
    const result = try stripErrorDirectives(alloc, "before <<<error:status=404 message=Not Found>>> after");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("before  after", result);
}

test "stripErrorDirectives preserves dotprompt media" {
    const alloc = std.testing.allocator;
    const result = try stripErrorDirectives(alloc, "<<<dotprompt:media:url https://example.com>>> <<<error:message=fail>>>");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("<<<dotprompt:media:url https://example.com>>> ", result);
}

// ============================================================================
// Content Parts Tests
// ============================================================================

test "textToParts plain text" {
    const alloc = std.testing.allocator;
    const parts = try textToParts(alloc, "Hello world");
    defer freeContentParts(alloc, parts);
    try std.testing.expectEqual(@as(usize, 1), parts.len);
    try std.testing.expectEqualStrings("Hello world", parts[0].text);
}

test "textToParts single media directive" {
    const alloc = std.testing.allocator;
    const parts = try textToParts(alloc, "<<<dotprompt:media:url https://example.com/img.png>>>");
    defer freeContentParts(alloc, parts);
    try std.testing.expectEqual(@as(usize, 1), parts.len);
    try std.testing.expectEqualStrings("https://example.com/img.png", parts[0].media_url);
}

test "textToParts text with media" {
    const alloc = std.testing.allocator;
    const parts = try textToParts(alloc, "Title: Hello <<<dotprompt:media:url https://example.com/img.png>>> Content: World");
    defer freeContentParts(alloc, parts);
    try std.testing.expectEqual(@as(usize, 3), parts.len);
    try std.testing.expectEqualStrings("Title: Hello", parts[0].text);
    try std.testing.expectEqualStrings("https://example.com/img.png", parts[1].media_url);
    try std.testing.expectEqualStrings("Content: World", parts[2].text);
}

test "textToParts data URI parsed as binary" {
    const alloc = std.testing.allocator;
    const parts = try textToParts(alloc, "<<<dotprompt:media:url data:image/png;base64,AQID>>>");
    defer freeContentParts(alloc, parts);
    try std.testing.expectEqual(@as(usize, 1), parts.len);
    try std.testing.expectEqualStrings("image/png", parts[0].binary.mime_type);
    try std.testing.expectEqualStrings(&[_]u8{ 1, 2, 3 }, parts[0].binary.data);
}

test "textToParts strips error directives" {
    const alloc = std.testing.allocator;
    const parts = try textToParts(alloc, "Title: Hello <<<error:status=404 message=Not Found>>> Content: World");
    defer freeContentParts(alloc, parts);
    try std.testing.expectEqual(@as(usize, 1), parts.len);
    try std.testing.expectEqualStrings("Title: Hello  Content: World", parts[0].text);
}

test "textToParts mixed media and error directives" {
    const alloc = std.testing.allocator;
    const parts = try textToParts(alloc, "<<<dotprompt:media:url https://img.com/a.jpg>>> <<<error:message=fail>>> text");
    defer freeContentParts(alloc, parts);
    try std.testing.expectEqual(@as(usize, 2), parts.len);
    try std.testing.expectEqualStrings("https://img.com/a.jpg", parts[0].media_url);
    try std.testing.expectEqualStrings("text", parts[1].text);
}

test "parseDataURI valid" {
    const alloc = std.testing.allocator;
    const result = try parseDataURI(alloc, "data:image/jpeg;base64,AQID");
    defer alloc.free(@constCast(result.mime_type));
    defer alloc.free(@constCast(result.data));
    try std.testing.expectEqualStrings("image/jpeg", result.mime_type);
    try std.testing.expectEqualStrings(&[_]u8{ 1, 2, 3 }, result.data);
}

test "parseDataURI invalid prefix" {
    const alloc = std.testing.allocator;
    const result = parseDataURI(alloc, "http://example.com");
    try std.testing.expectError(error.InvalidDataURI, result);
}

test "parseDataURI missing base64 marker" {
    const alloc = std.testing.allocator;
    const result = parseDataURI(alloc, "data:image/png,rawdata");
    try std.testing.expectError(error.InvalidDataURI, result);
}

// ============================================================================
// Remote helper stub tests
// ============================================================================

test "remoteMedia helper with data URI passes through" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{remoteMedia url=photoUrl}}", "{\"photoUrl\":\"data:image/png;base64,abc\"}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("<<<dotprompt:media:url data:image/png;base64,abc>>>", result);
}

test "remoteMedia helper with remote URL emits error directive" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{remoteMedia url=photoUrl}}", "{\"photoUrl\":\"https://example.com/img.jpg\"}");
    defer alloc.free(result);
    try std.testing.expect(containsErrorDirective(result));
    try std.testing.expect(std.mem.indexOf(u8, result, "remoteMedia requires HTTP client") != null);
}

test "remotePDF helper emits error directive" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{remotePDF url=pdfUrl}}", "{\"pdfUrl\":\"https://example.com/doc.pdf\"}");
    defer alloc.free(result);
    try std.testing.expect(containsErrorDirective(result));
}

test "remoteText helper emits error directive" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{remoteText url=textUrl}}", "{\"textUrl\":\"https://example.com/page.html\"}");
    defer alloc.free(result);
    try std.testing.expect(containsErrorDirective(result));
}

test "transcribeAudio helper emits error directive" {
    const alloc = std.testing.allocator;
    const result = try renderDocument(alloc, "{{transcribeAudio url=audioUrl}}", "{\"audioUrl\":\"https://example.com/audio.mp3\"}");
    defer alloc.free(result);
    try std.testing.expect(containsErrorDirective(result));
}

test "transcribeAudio helper uses active transcribing runtime" {
    const alloc = std.testing.allocator;

    const FakeTranscriber = struct {
        fn transcribe(_: *anyopaque, arena: Allocator, req: transcribing.Request) anyerror!transcribing.Response {
            return .{
                .text = try std.fmt.allocPrint(arena, "transcribed:{s}", .{req.url}),
            };
        }

        fn deinit(_: *anyopaque) void {}
    };

    var runtime = transcribing.Runtime.init(alloc);
    defer runtime.deinit();
    try runtime.registerOwnedTranscriber("whisper-local", .{
        .ptr = undefined,
        .vtable = &.{
            .transcribe = FakeTranscriber.transcribe,
            .deinit = FakeTranscriber.deinit,
        },
    });

    const prev = transcribing.getActiveRuntime();
    transcribing.setActiveRuntime(&runtime);
    defer transcribing.setActiveRuntime(prev);

    const result = try renderDocument(alloc, "{{transcribeAudio url=audioUrl provider=\"whisper-local\"}}", "{\"audioUrl\":\"https://example.com/audio.mp3\"}");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("transcribed:https://example.com/audio.mp3", result);
}

test "renderDocument multimodal template" {
    const alloc = std.testing.allocator;
    const tmpl =
        \\{{#if photoUrl}}
        \\{{media url=photoUrl}}
        \\{{/if}}
        \\Title: {{title}}
        \\Content: {{body}}
    ;
    const doc =
        \\{"photoUrl":"data:image/png;base64,abc","title":"Test","body":"Hello world"}
    ;
    const result = try renderDocument(alloc, tmpl, doc);
    defer alloc.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "<<<dotprompt:media:url data:image/png;base64,abc>>>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Title: Test") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Content: Hello world") != null);
}
