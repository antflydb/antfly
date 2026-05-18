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
const builtin = @import("builtin");
const build_options = @import("build_options");
const host_template = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps) struct {} else @import("../../template.zig");

const Allocator = std.mem.Allocator;

pub const ContentPart = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps) union(enum) {
    text: []const u8,
    media_url: []const u8,
    binary: BinaryContent,

    pub const BinaryContent = struct {
        mime_type: []const u8,
        data: []const u8,
    };
} else host_template.ContentPart;

pub fn renderDocument(
    alloc: Allocator,
    template_source: []const u8,
    doc_json: []const u8,
) ![]const u8 {
    if (builtin.os.tag != .freestanding and !build_options.bench_minimal_deps) {
        return try host_template.renderDocument(alloc, template_source, doc_json);
    }

    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const parsed = try std.json.parseFromSlice(std.json.Value, arena, doc_json, .{});

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    var pos: usize = 0;
    while (pos < template_source.len) {
        const expr_start = std.mem.indexOfPos(u8, template_source, pos, "{{") orelse {
            try out.appendSlice(alloc, template_source[pos..]);
            break;
        };
        try out.appendSlice(alloc, template_source[pos..expr_start]);

        const body_start = expr_start + 2;
        const expr_end = std.mem.indexOfPos(u8, template_source, body_start, "}}") orelse {
            try out.appendSlice(alloc, template_source[expr_start..]);
            break;
        };

        const expr = std.mem.trim(u8, template_source[body_start..expr_end], &std.ascii.whitespace);
        const rendered = try renderExpression(arena, parsed.value, expr);
        try out.appendSlice(alloc, rendered);
        pos = expr_end + 2;
    }

    return try out.toOwnedSlice(alloc);
}

pub fn renderDocumentWithHelpers(
    alloc: Allocator,
    template_source: []const u8,
    doc_json: []const u8,
    extra_helpers: anytype,
) ![]const u8 {
    if (builtin.os.tag != .freestanding and !build_options.bench_minimal_deps) {
        return try host_template.renderDocumentWithHelpers(alloc, template_source, doc_json, extra_helpers);
    }

    return try renderDocument(alloc, template_source, doc_json);
}

pub fn textToParts(alloc: Allocator, text: []const u8) ![]ContentPart {
    if (builtin.os.tag != .freestanding and !build_options.bench_minimal_deps) {
        return try host_template.textToParts(alloc, text);
    }

    const cleaned = try stripErrorDirectives(alloc, text);
    defer alloc.free(cleaned);

    const media_prefix = "<<<dotprompt:media:url ";
    const media_suffix = ">>>";

    var parts = std.ArrayListUnmanaged(ContentPart).empty;
    errdefer freeContentParts(alloc, parts.items);
    var pos: usize = 0;

    while (pos < cleaned.len) {
        const marker_start = std.mem.indexOfPos(u8, cleaned, pos, media_prefix) orelse {
            const segment = std.mem.trim(u8, cleaned[pos..], &std.ascii.whitespace);
            if (segment.len > 0) {
                try parts.append(alloc, .{ .text = try alloc.dupe(u8, segment) });
            }
            break;
        };

        if (marker_start > pos) {
            const segment = std.mem.trim(u8, cleaned[pos..marker_start], &std.ascii.whitespace);
            if (segment.len > 0) {
                try parts.append(alloc, .{ .text = try alloc.dupe(u8, segment) });
            }
        }

        const url_start = marker_start + media_prefix.len;
        const marker_end = std.mem.indexOfPos(u8, cleaned, url_start, media_suffix) orelse {
            const segment = std.mem.trim(u8, cleaned[pos..], &std.ascii.whitespace);
            if (segment.len > 0) {
                try parts.append(alloc, .{ .text = try alloc.dupe(u8, segment) });
            }
            break;
        };

        const url = cleaned[url_start..marker_end];
        if (std.mem.startsWith(u8, url, "data:")) {
            if (parseDataURI(alloc, url)) |binary| {
                try parts.append(alloc, .{ .binary = binary });
            } else |_| {
                try parts.append(alloc, .{ .media_url = try alloc.dupe(u8, url) });
            }
        } else {
            try parts.append(alloc, .{ .media_url = try alloc.dupe(u8, url) });
        }
        pos = marker_end + media_suffix.len;
    }

    return try parts.toOwnedSlice(alloc);
}

pub fn freeContentParts(alloc: Allocator, parts: []const ContentPart) void {
    if (builtin.os.tag != .freestanding and !build_options.bench_minimal_deps) {
        host_template.freeContentParts(alloc, parts);
        return;
    }

    for (parts) |part| {
        switch (part) {
            .text => |text| alloc.free(@constCast(text)),
            .media_url => |url| alloc.free(@constCast(url)),
            .binary => |binary| {
                alloc.free(@constCast(binary.mime_type));
                alloc.free(@constCast(binary.data));
            },
        }
    }
    alloc.free(parts);
}

fn renderExpression(
    arena: Allocator,
    root: std.json.Value,
    expr: []const u8,
) ![]const u8 {
    if (expr.len == 0) return "";

    if (std.mem.startsWith(u8, expr, "scrubHtml ")) {
        const path = std.mem.trim(u8, expr["scrubHtml ".len..], &std.ascii.whitespace);
        const value = resolvePath(root, path) orelse return "";
        return try scrubHtmlAlloc(arena, valueAsText(arena, value));
    }

    const value = resolvePath(root, expr) orelse return "";
    return valueAsText(arena, value);
}

fn resolvePath(root: std.json.Value, path: []const u8) ?std.json.Value {
    if (std.mem.eql(u8, path, "this")) return root;

    var value = root;
    var parts = std.mem.splitScalar(u8, path, '.');
    var is_first = true;
    while (parts.next()) |part| {
        if (part.len == 0) return null;
        if (is_first and part[0] == '_') return null;
        is_first = false;

        switch (value) {
            .object => |obj| value = obj.get(part) orelse return null,
            else => return null,
        }
    }
    return value;
}

fn valueAsText(arena: Allocator, value: std.json.Value) []const u8 {
    return switch (value) {
        .null => "",
        .bool => |b| if (b) "true" else "false",
        .integer => |n| std.fmt.allocPrint(arena, "{d}", .{n}) catch "",
        .float => |n| std.fmt.allocPrint(arena, "{d}", .{n}) catch "",
        .number_string => |s| s,
        .string => |s| s,
        .array, .object => std.json.Stringify.valueAlloc(arena, value, .{}) catch "",
    };
}

fn scrubHtmlAlloc(alloc: Allocator, html: []const u8) ![]const u8 {
    var result = std.ArrayListUnmanaged(u8).empty;
    defer result.deinit(alloc);

    var in_tag = false;
    var in_script = false;
    var idx: usize = 0;
    while (idx < html.len) {
        if (html[idx] == '<') {
            if (!in_script) {
                if (idx + 7 <= html.len and std.ascii.eqlIgnoreCase(html[idx..][0..7], "<script")) {
                    in_script = true;
                } else if (idx + 6 <= html.len and std.ascii.eqlIgnoreCase(html[idx..][0..6], "<style")) {
                    in_script = true;
                }
            }
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
            try result.append(alloc, html[idx]);
        }
        idx += 1;
    }

    return try alloc.dupe(u8, std.mem.trim(u8, result.items, &std.ascii.whitespace));
}

fn stripErrorDirectives(alloc: Allocator, text: []const u8) ![]u8 {
    const prefix = "<<<error:";
    const suffix = ">>>";

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    var pos: usize = 0;
    while (pos < text.len) {
        const start = std.mem.indexOfPos(u8, text, pos, prefix) orelse {
            try out.appendSlice(alloc, text[pos..]);
            break;
        };
        try out.appendSlice(alloc, text[pos..start]);
        pos = std.mem.indexOfPos(u8, text, start + prefix.len, suffix) orelse break;
        pos += suffix.len;
    }

    return try out.toOwnedSlice(alloc);
}

pub const ErrorDirective = struct {
    status: u16,
    message: []const u8,

    pub fn isPermanent(self: ErrorDirective) bool {
        return self.status == 401 or self.status == 403 or
            self.status == 404 or self.status == 410;
    }
};

pub fn formatErrorDirective(alloc: Allocator, status: u16, message: []const u8) ![]const u8 {
    if (builtin.os.tag != .freestanding and !build_options.bench_minimal_deps) {
        return try host_template.formatErrorDirective(alloc, status, message);
    }

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

pub fn freeErrorDirectives(alloc: Allocator, directives: []const ErrorDirective) void {
    for (directives) |d| alloc.free(@constCast(d.message));
    alloc.free(directives);
}

pub fn containsErrorDirective(text: []const u8) bool {
    return std.mem.indexOf(u8, text, "<<<error:") != null;
}

fn parseDataURI(alloc: Allocator, uri: []const u8) !ContentPart.BinaryContent {
    if (!std.mem.startsWith(u8, uri, "data:")) return error.InvalidDataURI;

    const after_data = uri[5..];
    const base64_marker = ";base64,";
    const sep_idx = std.mem.indexOf(u8, after_data, base64_marker) orelse return error.InvalidDataURI;

    const mime_type = try alloc.dupe(u8, after_data[0..sep_idx]);
    errdefer alloc.free(mime_type);

    const encoded = after_data[sep_idx + base64_marker.len ..];
    const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InvalidDataURI;
    const decoded = try alloc.alloc(u8, decoded_len);
    errdefer alloc.free(decoded);
    try std.base64.standard.Decoder.decode(decoded, encoded);

    return .{
        .mime_type = mime_type,
        .data = decoded,
    };
}

test "template stub renders local templates" {
    const alloc = std.testing.allocator;

    const rendered = try renderDocument(alloc, "{{title}} {{body}}",
        \\{"title":"Hello","body":"world"}
    );
    defer alloc.free(@constCast(rendered));

    try std.testing.expectEqualStrings("Hello world", rendered);
}

test "template stub skips internal top-level fields" {
    const alloc = std.testing.allocator;

    const rendered = try renderDocument(alloc, "{{title}}{{_embeddings}}",
        \\{"title":"Hello","_embeddings":[1,2,3]}
    );
    defer alloc.free(@constCast(rendered));

    try std.testing.expectEqualStrings("Hello", rendered);
}

test "template stub supports scrubHtml helper" {
    const alloc = std.testing.allocator;

    const rendered = try renderDocument(alloc, "{{scrubHtml body}}",
        \\{"body":"<p>Hello</p><script>evil()</script><p>World</p>"}
    );
    defer alloc.free(@constCast(rendered));

    try std.testing.expectEqualStrings("HelloWorld", rendered);
}
