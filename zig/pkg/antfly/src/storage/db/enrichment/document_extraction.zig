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
const pdf = if (builtin.os.tag == .freestanding or builtin.is_test)
    struct {
        pub const reader = struct {
            pub const Reader = struct {
                pub fn init(_: Allocator, _: []const u8) !Reader {
                    return error.PdfExtractionUnavailable;
                }

                pub fn deinit(_: *Reader) void {}

                pub fn pageCount(_: *Reader) !usize {
                    return 0;
                }

                pub fn extractPageTextAlloc(_: *Reader, _: usize) ![]u8 {
                    return error.PdfExtractionUnavailable;
                }

                pub fn extractPageBox(_: *Reader, _: usize) !struct { min_x: f64, min_y: f64, max_x: f64, max_y: f64 } {
                    return error.PdfExtractionUnavailable;
                }
            };
        };
    }
else
    @import("antfly_pdf");

const Allocator = std.mem.Allocator;

pub const Unit = struct {
    unit_id: []u8,
    unit_type: []u8,
    text: []u8,
    method: []u8,
    page_number: ?u32 = null,
    page_label: ?[]u8 = null,
    page_bbox: ?[4]f64 = null,
    page_rotation: ?i32 = null,
    char_start: ?u32 = null,
    char_end: ?u32 = null,

    pub fn deinit(self: *Unit, alloc: Allocator) void {
        alloc.free(self.unit_id);
        alloc.free(self.unit_type);
        alloc.free(self.text);
        alloc.free(self.method);
        if (self.page_label) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const Result = struct {
    content_type: []u8,
    route_type: []u8,
    unsupported_reason: []u8 = "",
    units: []Unit,

    pub fn deinit(self: *Result, alloc: Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.route_type);
        if (self.unsupported_reason.len > 0) alloc.free(self.unsupported_reason);
        for (self.units) |*unit| unit.deinit(alloc);
        if (self.units.len > 0) alloc.free(self.units);
        self.* = undefined;
    }
};

pub const Config = struct {
    filename: []const u8 = "",
    content_type: []const u8 = "",
    credentials: []const u8 = "",
    filename_field: []const u8 = "",
    content_type_field: []const u8 = "",
    html_strip_tags: bool = true,
    routes: []Route = &.{},

    pub fn deinit(self: *Config, alloc: Allocator) void {
        if (self.filename.len > 0) alloc.free(@constCast(self.filename));
        if (self.content_type.len > 0) alloc.free(@constCast(self.content_type));
        if (self.credentials.len > 0) alloc.free(@constCast(self.credentials));
        if (self.filename_field.len > 0) alloc.free(@constCast(self.filename_field));
        if (self.content_type_field.len > 0) alloc.free(@constCast(self.content_type_field));
        for (self.routes) |*route| route.deinit(alloc);
        if (self.routes.len > 0) alloc.free(self.routes);
        self.* = undefined;
    }
};

const ExtractorType = enum {
    pdf,
    html,
    text,
    email,
    unsupported,

    fn parse(value: []const u8) ?ExtractorType {
        if (std.mem.eql(u8, value, "pdf")) return .pdf;
        if (std.mem.eql(u8, value, "html")) return .html;
        if (std.mem.eql(u8, value, "text")) return .text;
        if (std.mem.eql(u8, value, "email")) return .email;
        if (std.mem.eql(u8, value, "unsupported")) return .unsupported;
        return null;
    }
};

const RouteMatch = struct {
    content_type: []const u8 = "",
    content_type_prefix: []const u8 = "",
    extensions: []const []const u8 = &.{},
    magic_prefixes: []const []const u8 = &.{},

    fn deinit(self: *const RouteMatch, alloc: Allocator) void {
        if (self.content_type.len > 0) alloc.free(@constCast(self.content_type));
        if (self.content_type_prefix.len > 0) alloc.free(@constCast(self.content_type_prefix));
        for (self.extensions) |extension| alloc.free(@constCast(extension));
        if (self.extensions.len > 0) alloc.free(@constCast(self.extensions));
        for (self.magic_prefixes) |prefix| alloc.free(@constCast(prefix));
        if (self.magic_prefixes.len > 0) alloc.free(@constCast(self.magic_prefixes));
    }
};

const Route = struct {
    match: RouteMatch = .{},
    extractor_type: ExtractorType,
    unit: []const u8 = "",

    fn deinit(self: *const Route, alloc: Allocator) void {
        self.match.deinit(alloc);
        if (self.unit.len > 0) alloc.free(@constCast(self.unit));
    }
};

pub fn parseConfig(alloc: Allocator, raw: []const u8) !Config {
    if (raw.len == 0) return .{};
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidDocumentExtractionConfig;

    const object = parsed.value.object;
    var config = Config{};
    errdefer config.deinit(alloc);
    config.filename = try dupeStringField(alloc, object, "filename");
    config.content_type = try dupeStringField(alloc, object, "content_type");
    config.credentials = try dupeStringField(alloc, object, "credentials");
    config.filename_field = try dupeSourceStringField(alloc, object, "filename_field");
    config.content_type_field = try dupeSourceStringField(alloc, object, "content_type_field");
    config.html_strip_tags = boolField(object, "html_strip_tags") orelse true;
    config.routes = try parseRoutesAlloc(alloc, object);
    return config;
}

fn dupeStringField(alloc: Allocator, object: std.json.ObjectMap, field: []const u8) ![]const u8 {
    const value = object.get(field) orelse return "";
    if (value != .string) return error.InvalidDocumentExtractionConfig;
    return try alloc.dupe(u8, value.string);
}

fn dupeSourceStringField(alloc: Allocator, object: std.json.ObjectMap, field: []const u8) ![]const u8 {
    if (object.get("source")) |source| {
        if (source != .object) return error.InvalidDocumentExtractionConfig;
        if (source.object.get(field)) |value| {
            if (value != .string) return error.InvalidDocumentExtractionConfig;
            return try alloc.dupe(u8, value.string);
        }
    }
    return try dupeStringField(alloc, object, field);
}

fn boolField(object: std.json.ObjectMap, field: []const u8) ?bool {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .bool => |v| v,
        else => null,
    };
}

fn parseRoutesAlloc(alloc: Allocator, object: std.json.ObjectMap) ![]Route {
    const value = object.get("routes") orelse return &.{};
    if (value != .array) return error.InvalidDocumentExtractionConfig;
    if (value.array.items.len == 0) return &.{};

    var routes = try alloc.alloc(Route, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (routes[0..initialized]) |*route| route.deinit(alloc);
        alloc.free(routes);
    }

    for (value.array.items) |item| {
        if (item != .object) return error.InvalidDocumentExtractionConfig;
        const route_object = item.object;
        const extractor_value = route_object.get("extractor") orelse return error.InvalidDocumentExtractionConfig;
        if (extractor_value != .object) return error.InvalidDocumentExtractionConfig;
        const extractor_object = extractor_value.object;
        const type_value = extractor_object.get("type") orelse return error.InvalidDocumentExtractionConfig;
        if (type_value != .string) return error.InvalidDocumentExtractionConfig;
        const extractor_type = ExtractorType.parse(type_value.string) orelse return error.InvalidDocumentExtractionConfig;

        routes[initialized] = blk: {
            var route = Route{ .extractor_type = extractor_type };
            errdefer route.deinit(alloc);
            route.match = if (route_object.get("match")) |match_value| route_match: {
                if (match_value != .object) return error.InvalidDocumentExtractionConfig;
                break :route_match try parseRouteMatchAlloc(alloc, match_value.object);
            } else RouteMatch{};
            route.unit = try dupeStringField(alloc, extractor_object, "unit");
            break :blk route;
        };
        initialized += 1;
    }

    return routes;
}

fn parseRouteMatchAlloc(alloc: Allocator, object: std.json.ObjectMap) !RouteMatch {
    return .{
        .content_type = try dupeStringField(alloc, object, "content_type"),
        .content_type_prefix = try dupeStringField(alloc, object, "content_type_prefix"),
        .extensions = try dupeStringArrayOrStringField(alloc, object, "extension"),
        .magic_prefixes = try dupeStringArrayOrStringField(alloc, object, "magic_prefix"),
    };
}

fn dupeStringArrayOrStringField(alloc: Allocator, object: std.json.ObjectMap, field: []const u8) ![]const []const u8 {
    const value = object.get(field) orelse return &.{};
    switch (value) {
        .string => |string| {
            const out = try alloc.alloc([]const u8, 1);
            errdefer alloc.free(out);
            out[0] = try alloc.dupe(u8, string);
            return out;
        },
        .array => |array| {
            if (array.items.len == 0) return &.{};
            var out = try alloc.alloc([]const u8, array.items.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |item| alloc.free(@constCast(item));
                alloc.free(out);
            }
            for (array.items) |item| {
                if (item != .string) return error.InvalidDocumentExtractionConfig;
                out[initialized] = try alloc.dupe(u8, item.string);
                initialized += 1;
            }
            return out;
        },
        else => return error.InvalidDocumentExtractionConfig,
    }
}

pub fn applySourceMetadataFromJson(alloc: Allocator, config: *Config, doc_value: []const u8) !void {
    if (config.filename_field.len == 0 and config.content_type_field.len == 0) return;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc_value, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;

    if (config.filename_field.len > 0) {
        if (try dupeOptionalStringField(alloc, parsed.value.object, config.filename_field)) |value| {
            if (config.filename.len > 0) alloc.free(@constCast(config.filename));
            config.filename = value;
        }
    }
    if (config.content_type_field.len > 0) {
        if (try dupeOptionalStringField(alloc, parsed.value.object, config.content_type_field)) |value| {
            if (config.content_type.len > 0) alloc.free(@constCast(config.content_type));
            config.content_type = value;
        }
    }
}

fn dupeOptionalStringField(alloc: Allocator, object: std.json.ObjectMap, field: []const u8) !?[]const u8 {
    const value = object.get(field) orelse return null;
    if (value != .string) return null;
    return try alloc.dupe(u8, value.string);
}

pub fn extractDownloadedAlloc(
    alloc: Allocator,
    downloaded: anytype,
    source_url: []const u8,
    config: Config,
) !Result {
    const content_type = if (config.content_type.len > 0) config.content_type else downloaded.content_type;
    for (config.routes) |route| {
        if (!routeMatches(route.match, content_type, config.filename, source_url, downloaded.data)) continue;
        return try extractWithRouteAlloc(alloc, downloaded.data, content_type, route, config.html_strip_tags);
    }
    if (isPdfContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractPdfAlloc(alloc, downloaded.data, content_type);
    }
    if (isHtmlContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractSingleTextUnitAlloc(alloc, downloaded.data, content_type, "article:000001", "article", "html_text", config.html_strip_tags);
    }
    if (isEmailContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractEmailAlloc(alloc, downloaded.data, content_type, "email");
    }
    if (isTextContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractSingleTextUnitAlloc(alloc, downloaded.data, content_type, "document:000001", "document", "text", false);
    }
    return try unsupportedResultAlloc(alloc, content_type, "unsupported_content_type");
}

fn routeMatches(match: RouteMatch, content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (match.content_type.len == 0 and match.content_type_prefix.len == 0 and match.extensions.len == 0 and match.magic_prefixes.len == 0) return true;
    if (match.content_type.len > 0 and contentTypeEquals(content_type, match.content_type)) return true;
    if (match.content_type_prefix.len > 0 and contentTypeStartsWith(content_type, match.content_type_prefix)) return true;
    for (match.extensions) |extension| {
        if (hasConfiguredExtension(filename, extension) or hasConfiguredExtension(source_url, extension)) return true;
    }
    for (match.magic_prefixes) |prefix| {
        if (std.mem.startsWith(u8, bytes, prefix)) return true;
    }
    return false;
}

fn extractWithRouteAlloc(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    route: Route,
    html_strip_tags: bool,
) !Result {
    return switch (route.extractor_type) {
        .pdf => try extractPdfAlloc(alloc, bytes, content_type),
        .html => try extractSingleConfiguredUnitAlloc(alloc, bytes, content_type, route.unit, "article", "html_text", html_strip_tags),
        .text => try extractSingleConfiguredUnitAlloc(alloc, bytes, content_type, route.unit, "document", "text", false),
        .email => try extractEmailAlloc(alloc, bytes, content_type, if (route.unit.len > 0) route.unit else "email"),
        .unsupported => try unsupportedResultAlloc(alloc, content_type, "matched_unsupported_route"),
    };
}

fn extractSingleConfiguredUnitAlloc(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    configured_unit: []const u8,
    default_unit: []const u8,
    method: []const u8,
    strip_html: bool,
) !Result {
    const unit_type = if (configured_unit.len > 0) configured_unit else default_unit;
    const unit_id = try std.fmt.allocPrint(alloc, "{s}:000001", .{unit_type});
    defer alloc.free(unit_id);
    return try extractSingleTextUnitAlloc(alloc, bytes, content_type, unit_id, unit_type, method, strip_html);
}

fn unsupportedResultAlloc(alloc: Allocator, content_type: []const u8, reason: []const u8) !Result {
    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, "unsupported"),
        .unsupported_reason = try alloc.dupe(u8, reason),
        .units = try alloc.alloc(Unit, 0),
    };
}

fn extractPdfAlloc(alloc: Allocator, bytes: []const u8, content_type: []const u8) !Result {
    var parsed = try pdf.reader.Reader.init(alloc, bytes);
    defer parsed.deinit();

    const page_count = try parsed.pageCount();
    var units = try alloc.alloc(Unit, page_count);
    var initialized: usize = 0;
    errdefer {
        for (units[0..initialized]) |*unit| unit.deinit(alloc);
        alloc.free(units);
    }

    var page_num: usize = 1;
    var cursor: usize = 0;
    while (page_num <= page_count) : (page_num += 1) {
        const text = try parsed.extractPageTextAlloc(page_num);
        errdefer alloc.free(text);
        const page_box = parsed.extractPageBox(page_num) catch null;
        const char_start = std.math.cast(u32, cursor);
        const char_end = std.math.cast(u32, cursor + text.len);
        var unit_id: ?[]u8 = try std.fmt.allocPrint(alloc, "page:{d:0>6}", .{page_num});
        errdefer if (unit_id) |value| alloc.free(value);
        var unit_type: ?[]u8 = try alloc.dupe(u8, "page");
        errdefer if (unit_type) |value| alloc.free(value);
        var method: ?[]u8 = try alloc.dupe(u8, "pdf_text");
        errdefer if (method) |value| alloc.free(value);
        var page_label: ?[]u8 = try std.fmt.allocPrint(alloc, "{d}", .{page_num});
        errdefer if (page_label) |value| alloc.free(value);
        units[initialized] = .{
            .unit_id = unit_id.?,
            .unit_type = unit_type.?,
            .text = text,
            .method = method.?,
            .page_number = @intCast(page_num),
            .page_label = page_label.?,
            .page_bbox = if (page_box) |box| .{ box.min_x, box.min_y, box.max_x, box.max_y } else null,
            .char_start = char_start,
            .char_end = char_end,
        };
        unit_id = null;
        unit_type = null;
        method = null;
        page_label = null;
        cursor += text.len;
        initialized += 1;
    }

    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, "pdf"),
        .units = units,
    };
}

fn extractSingleTextUnitAlloc(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    unit_id: []const u8,
    unit_type: []const u8,
    method: []const u8,
    strip_html: bool,
) !Result {
    var units = try alloc.alloc(Unit, 1);
    errdefer alloc.free(units);
    const text = if (strip_html)
        try htmlToTextAlloc(alloc, bytes)
    else
        try alloc.dupe(u8, bytes);
    errdefer alloc.free(text);
    units[0] = .{
        .unit_id = try alloc.dupe(u8, unit_id),
        .unit_type = try alloc.dupe(u8, unit_type),
        .text = text,
        .method = try alloc.dupe(u8, method),
        .char_start = 0,
        .char_end = std.math.cast(u32, text.len),
    };
    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, if (strip_html) "html" else "text"),
        .units = units,
    };
}

fn extractEmailAlloc(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    unit_prefix: []const u8,
) !Result {
    const split = emailHeaderBodySplit(bytes) orelse return try unsupportedResultAlloc(alloc, content_type, "invalid_rfc822_message");
    const headers = bytes[0..split.header_end];
    const body = bytes[split.body_start..];

    var units = std.ArrayListUnmanaged(Unit).empty;
    errdefer {
        for (units.items) |*unit| unit.deinit(alloc);
        units.deinit(alloc);
    }

    if (headers.len > 0) {
        const header_text = try normalizeEmailHeaderTextAlloc(alloc, headers);
        errdefer alloc.free(header_text);
        if (header_text.len > 0) {
            const unit_id = try std.fmt.allocPrint(alloc, "{s}:headers", .{unit_prefix});
            errdefer alloc.free(unit_id);
            try units.append(alloc, .{
                .unit_id = unit_id,
                .unit_type = try alloc.dupe(u8, "email_headers"),
                .text = header_text,
                .method = try alloc.dupe(u8, "email_rfc822"),
                .char_start = 0,
                .char_end = std.math.cast(u32, split.header_end),
            });
        } else {
            alloc.free(header_text);
        }
    }

    try appendEmailBodyUnits(alloc, &units, headers, body, unit_prefix, split.body_start, bytes.len);

    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, "email"),
        .units = try units.toOwnedSlice(alloc),
    };
}

const EmailHeaderBodySplit = struct {
    header_end: usize,
    body_start: usize,
};

fn emailHeaderBodySplit(bytes: []const u8) ?EmailHeaderBodySplit {
    if (std.mem.indexOf(u8, bytes, "\r\n\r\n")) |pos| {
        if (!looksLikeEmailHeaders(bytes[0..pos])) return null;
        return .{ .header_end = pos, .body_start = pos + 4 };
    }
    if (std.mem.indexOf(u8, bytes, "\n\n")) |pos| {
        if (!looksLikeEmailHeaders(bytes[0..pos])) return null;
        return .{ .header_end = pos, .body_start = pos + 2 };
    }
    return null;
}

fn looksLikeEmailHeaders(headers: []const u8) bool {
    var lines = std.mem.splitScalar(u8, headers, '\n');
    while (lines.next()) |raw_line| {
        const line = trimTrailingCarriageReturn(raw_line);
        if (line.len == 0) continue;
        if (line[0] == ' ' or line[0] == '\t') continue;
        if (std.mem.indexOfScalar(u8, line, ':') != null) return true;
    }
    return false;
}

fn normalizeEmailHeaderTextAlloc(alloc: Allocator, headers: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var lines = std.mem.splitScalar(u8, headers, '\n');
    var first = true;
    while (lines.next()) |raw_line| {
        const line = trimTrailingCarriageReturn(raw_line);
        if (line.len == 0) continue;
        if (line[0] == ' ' or line[0] == '\t') {
            try out.append(alloc, ' ');
            try out.appendSlice(alloc, std.mem.trim(u8, line, " \t"));
            continue;
        }
        if (!first) try out.append(alloc, '\n');
        first = false;
        try out.appendSlice(alloc, line);
    }
    return try out.toOwnedSlice(alloc);
}

fn appendEmailBodyUnits(
    alloc: Allocator,
    units: *std.ArrayListUnmanaged(Unit),
    headers: []const u8,
    body: []const u8,
    unit_prefix: []const u8,
    body_start: usize,
    message_len: usize,
) !void {
    if (try emailMultipartBoundaryAlloc(alloc, headers)) |boundary| {
        defer alloc.free(boundary);
        const appended = try appendEmailMultipartTextUnits(alloc, units, body, unit_prefix, body_start, boundary);
        if (appended > 0) return;
    }

    const body_text = try emailBodyTextAlloc(alloc, headers, body, false);
    errdefer alloc.free(body_text);
    if (body_text.len > 0) {
        const unit_id = try std.fmt.allocPrint(alloc, "{s}:body", .{unit_prefix});
        errdefer alloc.free(unit_id);
        try units.append(alloc, .{
            .unit_id = unit_id,
            .unit_type = try alloc.dupe(u8, "email_body"),
            .text = body_text,
            .method = try alloc.dupe(u8, "email_rfc822"),
            .char_start = std.math.cast(u32, body_start),
            .char_end = std.math.cast(u32, message_len),
        });
    } else {
        alloc.free(body_text);
    }
}

fn appendEmailMultipartTextUnits(
    alloc: Allocator,
    units: *std.ArrayListUnmanaged(Unit),
    body: []const u8,
    unit_prefix: []const u8,
    body_start: usize,
    boundary: []const u8,
) !usize {
    if (boundary.len == 0) return 0;
    var appended: usize = 0;
    var in_part = false;
    var part_start: usize = 0;
    var cursor: usize = 0;
    while (nextEmailLine(body, cursor)) |line| {
        if (emailBoundaryLineKind(line.text, boundary)) |kind| {
            if (in_part and line.start >= part_start) {
                if (try appendEmailMultipartTextPartUnit(alloc, units, body[part_start..line.start], unit_prefix, body_start + part_start, appended + 1)) {
                    appended += 1;
                }
            }
            if (kind == .closing) break;
            in_part = true;
            part_start = line.next;
        }
        cursor = line.next;
    }
    return appended;
}

const EmailLine = struct {
    start: usize,
    next: usize,
    text: []const u8,
};

fn nextEmailLine(bytes: []const u8, start: usize) ?EmailLine {
    if (start >= bytes.len) return null;
    const newline_offset = std.mem.indexOfScalar(u8, bytes[start..], '\n');
    const raw_end = if (newline_offset) |offset| start + offset else bytes.len;
    const next = if (newline_offset != null) raw_end + 1 else bytes.len;
    return .{
        .start = start,
        .next = next,
        .text = trimTrailingCarriageReturn(bytes[start..raw_end]),
    };
}

const EmailBoundaryLineKind = enum {
    part,
    closing,
};

fn emailBoundaryLineKind(line: []const u8, boundary: []const u8) ?EmailBoundaryLineKind {
    if (line.len < boundary.len + 2) return null;
    if (line[0] != '-' or line[1] != '-') return null;
    if (!std.mem.eql(u8, line[2 .. 2 + boundary.len], boundary)) return null;
    const suffix = line[2 + boundary.len ..];
    if (suffix.len == 0) return .part;
    if (std.mem.eql(u8, suffix, "--")) return .closing;
    return null;
}

fn appendEmailMultipartTextPartUnit(
    alloc: Allocator,
    units: *std.ArrayListUnmanaged(Unit),
    part: []const u8,
    unit_prefix: []const u8,
    part_message_start: usize,
    part_index: usize,
) !bool {
    const split = emailHeaderBodySplit(part) orelse return false;
    const part_headers = part[0..split.header_end];
    const part_body = part[split.body_start..];
    const part_content_type = emailHeaderValue(part_headers, "content-type") orelse "text/plain";
    const part_content_base = contentTypeBase(part_content_type);
    const strip_html = std.ascii.eqlIgnoreCase(part_content_base, "text/html");
    if (!strip_html and !std.ascii.eqlIgnoreCase(part_content_base, "text/plain")) return false;

    const part_text = try emailBodyTextAlloc(alloc, part_headers, part_body, strip_html);
    errdefer alloc.free(part_text);
    if (part_text.len == 0) {
        alloc.free(part_text);
        return false;
    }

    const unit_id = try std.fmt.allocPrint(alloc, "{s}:part:{d:0>6}", .{ unit_prefix, part_index });
    errdefer alloc.free(unit_id);
    try units.append(alloc, .{
        .unit_id = unit_id,
        .unit_type = try alloc.dupe(u8, "email_part"),
        .text = part_text,
        .method = try alloc.dupe(u8, if (strip_html) "email_html" else "email_text"),
        .char_start = std.math.cast(u32, part_message_start + split.body_start),
        .char_end = std.math.cast(u32, part_message_start + part.len),
    });
    return true;
}

fn emailBodyTextAlloc(alloc: Allocator, headers: []const u8, body: []const u8, strip_html: bool) ![]u8 {
    const encoding = emailHeaderValue(headers, "content-transfer-encoding") orelse "";
    const decoded = if (std.ascii.eqlIgnoreCase(encoding, "base64"))
        decodeBase64BodyAlloc(alloc, body) catch try alloc.dupe(u8, body)
    else if (std.ascii.eqlIgnoreCase(encoding, "quoted-printable"))
        try decodeQuotedPrintableAlloc(alloc, body)
    else
        try alloc.dupe(u8, body);
    errdefer alloc.free(decoded);
    if (!strip_html) return decoded;
    const text = try htmlToTextAlloc(alloc, decoded);
    alloc.free(decoded);
    return text;
}

fn emailHeaderValue(headers: []const u8, name_lower: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, headers, '\n');
    while (lines.next()) |raw_line| {
        const line = trimTrailingCarriageReturn(raw_line);
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = std.mem.trim(u8, line[0..colon], " \t");
        if (!std.ascii.eqlIgnoreCase(name, name_lower)) continue;
        return std.mem.trim(u8, line[colon + 1 ..], " \t");
    }
    return null;
}

fn emailMultipartBoundaryAlloc(alloc: Allocator, headers: []const u8) !?[]u8 {
    const content_type = emailHeaderValue(headers, "content-type") orelse return null;
    if (!contentTypeStartsWith(content_type, "multipart/")) return null;
    const boundary = contentTypeParameter(content_type, "boundary") orelse return null;
    return try alloc.dupe(u8, boundary);
}

fn contentTypeParameter(content_type: []const u8, name_lower: []const u8) ?[]const u8 {
    var parts = std.mem.splitScalar(u8, content_type, ';');
    _ = parts.next();
    while (parts.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        const eq = std.mem.indexOfScalar(u8, trimmed, '=') orelse continue;
        const name = std.mem.trim(u8, trimmed[0..eq], " \t");
        if (!std.ascii.eqlIgnoreCase(name, name_lower)) continue;
        var value = std.mem.trim(u8, trimmed[eq + 1 ..], " \t");
        if (value.len >= 2 and value[0] == '"' and value[value.len - 1] == '"') {
            value = value[1 .. value.len - 1];
        }
        return value;
    }
    return null;
}

fn trimTrailingCarriageReturn(line: []const u8) []const u8 {
    if (line.len > 0 and line[line.len - 1] == '\r') return line[0 .. line.len - 1];
    return line;
}

fn decodeBase64BodyAlloc(alloc: Allocator, body: []const u8) ![]u8 {
    var compact = std.ArrayListUnmanaged(u8).empty;
    defer compact.deinit(alloc);
    for (body) |byte| {
        if (std.ascii.isWhitespace(byte)) continue;
        try compact.append(alloc, byte);
    }
    const decoded_len = try std.base64.standard.Decoder.calcSizeForSlice(compact.items);
    const decoded = try alloc.alloc(u8, decoded_len);
    errdefer alloc.free(decoded);
    try std.base64.standard.Decoder.decode(decoded, compact.items);
    return decoded;
}

fn decodeQuotedPrintableAlloc(alloc: Allocator, body: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var i: usize = 0;
    while (i < body.len) {
        if (body[i] != '=') {
            try out.append(alloc, body[i]);
            i += 1;
            continue;
        }
        if (i + 1 < body.len and body[i + 1] == '\n') {
            i += 2;
            continue;
        }
        if (i + 2 < body.len and body[i + 1] == '\r' and body[i + 2] == '\n') {
            i += 3;
            continue;
        }
        if (i + 2 < body.len) {
            const hi = std.fmt.charToDigit(body[i + 1], 16) catch null;
            const lo = std.fmt.charToDigit(body[i + 2], 16) catch null;
            if (hi != null and lo != null) {
                try out.append(alloc, @as(u8, @intCast(hi.? * 16 + lo.?)));
                i += 3;
                continue;
            }
        }
        try out.append(alloc, body[i]);
        i += 1;
    }
    return try out.toOwnedSlice(alloc);
}

fn htmlToTextAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var in_tag = false;
    var last_was_space = true;
    for (bytes) |c| {
        if (c == '<') {
            in_tag = true;
            if (!last_was_space) {
                try out.append(alloc, ' ');
                last_was_space = true;
            }
            continue;
        }
        if (c == '>') {
            in_tag = false;
            continue;
        }
        if (in_tag) continue;
        if (std.ascii.isWhitespace(c)) {
            if (!last_was_space) {
                try out.append(alloc, ' ');
                last_was_space = true;
            }
            continue;
        }
        try out.append(alloc, c);
        last_was_space = false;
    }
    while (out.items.len > 0 and std.ascii.isWhitespace(out.items[out.items.len - 1])) {
        _ = out.pop();
    }
    return try out.toOwnedSlice(alloc);
}

fn isPdfContent(content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (contentTypeEquals(content_type, "application/pdf")) return true;
    if (hasExtension(filename, ".pdf") or hasExtension(source_url, ".pdf")) return true;
    return std.mem.startsWith(u8, bytes, "%PDF-");
}

fn isHtmlContent(content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (contentTypeEquals(content_type, "text/html")) return true;
    if (hasExtension(filename, ".html") or hasExtension(filename, ".htm") or
        hasExtension(source_url, ".html") or hasExtension(source_url, ".htm"))
    {
        return true;
    }
    return looksLikeHtml(bytes);
}

fn isEmailContent(content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (contentTypeEquals(content_type, "message/rfc822")) return true;
    if (hasExtension(filename, ".eml") or hasExtension(source_url, ".eml")) return true;
    return contentTypeAllowsTextSniff(content_type) and emailHeaderBodySplit(bytes) != null;
}

fn isTextContent(content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (contentTypeStartsWith(content_type, "text/")) return true;
    if (contentTypeEquals(content_type, "application/json")) return true;
    if (hasExtension(filename, ".txt") or hasExtension(source_url, ".txt") or
        hasExtension(filename, ".json") or hasExtension(source_url, ".json") or
        hasExtension(filename, ".csv") or hasExtension(source_url, ".csv"))
    {
        return true;
    }
    return contentTypeAllowsTextSniff(content_type) and looksLikePlainText(bytes);
}

fn looksLikeHtml(bytes: []const u8) bool {
    const trimmed = std.mem.trim(u8, bytes, " \t\r\n");
    if (trimmed.len == 0 or trimmed[0] != '<') return false;
    return startsWithIgnoreCase(trimmed, "<!doctype html") or
        startsWithIgnoreCase(trimmed, "<html") or
        startsWithIgnoreCase(trimmed, "<head") or
        startsWithIgnoreCase(trimmed, "<body") or
        startsWithIgnoreCase(trimmed, "<title") or
        startsWithIgnoreCase(trimmed, "<meta");
}

fn contentTypeAllowsTextSniff(content_type: []const u8) bool {
    const base = contentTypeBase(content_type);
    return base.len == 0 or
        std.ascii.eqlIgnoreCase(base, "application/octet-stream") or
        std.ascii.eqlIgnoreCase(base, "binary/octet-stream") or
        std.ascii.eqlIgnoreCase(base, "application/x-unknown-content-type");
}

fn looksLikePlainText(bytes: []const u8) bool {
    if (bytes.len == 0) return false;
    _ = std.unicode.utf8CountCodepoints(bytes) catch return false;
    var printable: usize = 0;
    for (bytes) |byte| {
        if (byte == 0) return false;
        if (byte == '\n' or byte == '\r' or byte == '\t') continue;
        if (byte < 0x20) return false;
        printable += 1;
    }
    return printable > 0;
}

fn startsWithIgnoreCase(value: []const u8, prefix: []const u8) bool {
    if (value.len < prefix.len) return false;
    return std.ascii.eqlIgnoreCase(value[0..prefix.len], prefix);
}

fn hasExtension(value: []const u8, extension: []const u8) bool {
    const cleaned = trimUrlSuffix(value);
    if (cleaned.len < extension.len) return false;
    return std.ascii.eqlIgnoreCase(cleaned[cleaned.len - extension.len ..], extension);
}

fn hasConfiguredExtension(value: []const u8, extension: []const u8) bool {
    if (extension.len == 0) return false;
    if (extension[0] == '.') return hasExtension(value, extension);
    const cleaned = trimUrlSuffix(value);
    if (cleaned.len < extension.len) return false;
    if (!std.ascii.eqlIgnoreCase(cleaned[cleaned.len - extension.len ..], extension)) return false;
    if (cleaned.len == extension.len) return true;
    return cleaned[cleaned.len - extension.len - 1] == '.';
}

fn trimUrlSuffix(value: []const u8) []const u8 {
    var end = value.len;
    if (std.mem.indexOfScalar(u8, value, '?')) |pos| end = @min(end, pos);
    if (std.mem.indexOfScalar(u8, value, '#')) |pos| end = @min(end, pos);
    return value[0..end];
}

fn contentTypeBase(content_type: []const u8) []const u8 {
    const end = std.mem.indexOfScalar(u8, content_type, ';') orelse content_type.len;
    return std.mem.trim(u8, content_type[0..end], " \t\r\n");
}

fn contentTypeEquals(content_type: []const u8, expected: []const u8) bool {
    return std.ascii.eqlIgnoreCase(contentTypeBase(content_type), expected);
}

fn contentTypeStartsWith(content_type: []const u8, prefix: []const u8) bool {
    const base = contentTypeBase(content_type);
    if (base.len < prefix.len) return false;
    return std.ascii.eqlIgnoreCase(base[0..prefix.len], prefix);
}

const TestDownloadedContent = struct {
    content_type: []u8,
    data: []u8,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.data);
    }
};

test "document extraction extracts text data uri content as single document unit" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "text/plain"),
        .data = try alloc.dupe(u8, "alpha beta"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "data:text/plain,alpha%20beta", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.units.len);
    try std.testing.expectEqualStrings("document:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("alpha beta", result.units[0].text);
    try std.testing.expectEqual(@as(?u32, 0), result.units[0].char_start);
    try std.testing.expectEqual(@as(?u32, 10), result.units[0].char_end);
}

test "document extraction routes configured extensions into text units" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "alpha beta"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"filename":"notes.md","routes":[{"match":{"extension":["md"]},"extractor":{"type":"text","unit":"note"}}]}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/download?id=1", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("text", result.route_type);
    try std.testing.expectEqualStrings("note:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("note", result.units[0].unit_type);
    try std.testing.expectEqualStrings("alpha beta", result.units[0].text);
}

test "document extraction routes configured magic prefixes into text units" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "# title\nbody"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"routes":[{"match":{"magic_prefix":"# "},"extractor":{"type":"text","unit":"markdown"}}]}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/download", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("text", result.route_type);
    try std.testing.expectEqualStrings("markdown:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("markdown", result.units[0].unit_type);
    try std.testing.expectEqualStrings("# title\nbody", result.units[0].text);
}

test "document extraction sniffs html bytes without metadata" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "  <!doctype html><html><body><h1>Alpha</h1></body></html>"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/download", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("html", result.route_type);
    try std.testing.expectEqualStrings("article:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("article", result.units[0].unit_type);
    try std.testing.expectEqualStrings("Alpha", result.units[0].text);
}

test "document extraction sniffs utf8 text bytes without metadata" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "plain text\nsecond line"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/download", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("text", result.route_type);
    try std.testing.expectEqualStrings("document:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("document", result.units[0].unit_type);
    try std.testing.expectEqualStrings("plain text\nsecond line", result.units[0].text);
}

test "document extraction routes rfc822 email into header and body units" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "message/rfc822"),
        .data = try alloc.dupe(u8, "Subject: Alpha\r\nFrom: a@example.test\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\nHello=20world=\r\n!"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/message.eml", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("email", result.route_type);
    try std.testing.expectEqual(@as(usize, 2), result.units.len);
    try std.testing.expectEqualStrings("email:headers", result.units[0].unit_id);
    try std.testing.expectEqualStrings("email_headers", result.units[0].unit_type);
    try std.testing.expect(std.mem.indexOf(u8, result.units[0].text, "Subject: Alpha") != null);
    try std.testing.expectEqualStrings("email:body", result.units[1].unit_id);
    try std.testing.expectEqualStrings("email_body", result.units[1].unit_type);
    try std.testing.expectEqualStrings("Hello world!", result.units[1].text);
    try std.testing.expectEqualStrings("email_rfc822", result.units[1].method);
}

test "document extraction routes configured email extractor with unit prefix" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "Subject: Alpha\n\nBody"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"routes":[{"match":{"extension":"mail"},"extractor":{"type":"email","unit":"message"}}],"filename":"thread.mail"}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/download", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("email", result.route_type);
    try std.testing.expectEqual(@as(usize, 2), result.units.len);
    try std.testing.expectEqualStrings("message:headers", result.units[0].unit_id);
    try std.testing.expectEqualStrings("message:body", result.units[1].unit_id);
    try std.testing.expectEqualStrings("Body", result.units[1].text);
}

test "document extraction extracts multipart rfc822 text parts" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "message/rfc822"),
        .data = try alloc.dupe(
            u8,
            "Subject: Alpha\r\nContent-Type: multipart/alternative; boundary=\"b1\"\r\n\r\n" ++
                "--b1\r\nContent-Type: text/plain\r\n\r\nPlain body\r\n" ++
                "--b1\r\nContent-Type: text/html\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n<p>HTML=20body</p>\r\n" ++
                "--b1\r\nContent-Type: application/octet-stream\r\n\r\nopaque\r\n" ++
                "--b1--\r\n",
        ),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/message.eml", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("email", result.route_type);
    try std.testing.expectEqual(@as(usize, 3), result.units.len);
    try std.testing.expectEqualStrings("email:headers", result.units[0].unit_id);
    try std.testing.expectEqualStrings("email:part:000001", result.units[1].unit_id);
    try std.testing.expectEqualStrings("email_part", result.units[1].unit_type);
    try std.testing.expectEqualStrings("email_text", result.units[1].method);
    try std.testing.expectEqualStrings("Plain body\r\n", result.units[1].text);
    try std.testing.expectEqualStrings("email:part:000002", result.units[2].unit_id);
    try std.testing.expectEqualStrings("email_html", result.units[2].method);
    try std.testing.expectEqualStrings("HTML body", result.units[2].text);
}

test "document extraction applies source metadata fields from row json" {
    const alloc = std.testing.allocator;
    var config = try parseConfig(alloc,
        \\{"source":{"filename_field":"filename","content_type_field":"mime_type"}}
    );
    defer config.deinit(alloc);

    try applySourceMetadataFromJson(alloc, &config,
        \\{"filename":"contract.pdf","mime_type":"application/pdf"}
    );
    try std.testing.expectEqualStrings("contract.pdf", config.filename);
    try std.testing.expectEqualStrings("application/pdf", config.content_type);
}

test "document extraction route matching normalizes content type parameters" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "text/html; charset=utf-8"),
        .data = try alloc.dupe(u8, "<h1>Alpha</h1><p>Beta</p>"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"routes":[{"match":{"content_type":"text/html"},"extractor":{"type":"html","unit":"article"}}]}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/doc", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("html", result.route_type);
    try std.testing.expectEqualStrings("article:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("Alpha Beta", result.units[0].text);
}

test "document extraction can route configured unsupported files without searchable units" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/x-custom"),
        .data = try alloc.dupe(u8, "opaque"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"routes":[{"match":{"content_type_prefix":"application/x-"},"extractor":{"type":"unsupported"}}]}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/file.bin", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("unsupported", result.route_type);
    try std.testing.expectEqualStrings("matched_unsupported_route", result.unsupported_reason);
    try std.testing.expectEqual(@as(usize, 0), result.units.len);
}

test "document extraction strips simple html tags" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "text/html"),
        .data = try alloc.dupe(u8, "<h1>Alpha</h1><p>Beta</p>"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/doc.html", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.units.len);
    try std.testing.expectEqualStrings("article", result.units[0].unit_type);
    try std.testing.expectEqualStrings("Alpha Beta", result.units[0].text);
}

test "document extraction classifies unsupported content without units" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "\x00\x01\x02"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "data:application/octet-stream;base64,AAEC", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("unsupported", result.route_type);
    try std.testing.expectEqualStrings("unsupported_content_type", result.unsupported_reason);
    try std.testing.expectEqual(@as(usize, 0), result.units.len);
}
