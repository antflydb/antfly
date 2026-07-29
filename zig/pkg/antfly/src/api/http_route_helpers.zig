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
const metadata_openapi = @import("antfly_metadata_openapi");
const query_contract = @import("query_contract.zig");
const http_common = @import("../raft/transport/http_common.zig");

pub fn jsonResponse(alloc: std.mem.Allocator, value: anytype) !http_common.HttpResponse {
    return .{
        .status = 200,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})}),
    };
}

pub fn jsonResponseWithStatus(alloc: std.mem.Allocator, status: u16, value: anytype) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})}),
    };
}

pub fn textResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/plain"),
        .body = try alloc.dupe(u8, body),
    };
}

pub fn jsonWithHeadersResponse(
    alloc: std.mem.Allocator,
    status: u16,
    body: []const u8,
    headers_in: []const struct { name: []const u8, value: []const u8 },
) !http_common.HttpResponse {
    const headers = try alloc.alloc(http_common.Header, headers_in.len);
    var header_index: usize = 0;
    errdefer {
        for (headers[0..header_index]) |*header| header.deinit(alloc);
        alloc.free(headers);
    }
    for (headers_in, 0..) |header, i| {
        const name = try alloc.dupe(u8, header.name);
        errdefer alloc.free(name);
        headers[i] = .{
            .name = name,
            .value = try alloc.dupe(u8, header.value),
        };
        header_index += 1;
    }
    const content_type = try alloc.dupe(u8, "application/json");
    errdefer alloc.free(content_type);
    const owned_body = try alloc.dupe(u8, body);
    errdefer alloc.free(owned_body);
    return .{
        .status = status,
        .content_type = content_type,
        .headers = headers,
        .body = owned_body,
    };
}

pub fn ndjsonResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "application/x-ndjson"),
        .body = try alloc.dupe(u8, body),
    };
}

pub const OwnedLookupOptions = struct {
    fields: [][]const u8 = &.{},
    opts: @import("../storage/db/types.zig").LookupOptions = .{},

    pub fn deinit(self: *OwnedLookupOptions, alloc: std.mem.Allocator) void {
        for (self.fields) |field| alloc.free(field);
        if (self.fields.len > 0) alloc.free(self.fields);
        self.* = undefined;
    }
};

pub const OwnedScanKeysRequest = struct {
    from: []const u8 = "",
    to: []const u8 = "",
    fields: [][]const u8 = &.{},
    filter_query_json: []const u8 = "",
    opts: @import("../storage/db/types.zig").ScanOptions = .{},

    pub fn deinit(self: *OwnedScanKeysRequest, alloc: std.mem.Allocator) void {
        if (self.from.len > 0) alloc.free(self.from);
        if (self.to.len > 0) alloc.free(self.to);
        for (self.fields) |field| alloc.free(field);
        if (self.fields.len > 0) alloc.free(self.fields);
        if (self.filter_query_json.len > 0) alloc.free(@constCast(self.filter_query_json));
        self.* = undefined;
    }
};

pub fn parseLookupOptions(alloc: std.mem.Allocator, query: []const u8) !OwnedLookupOptions {
    if (query.len == 0) return .{};
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |part| {
        if (!std.mem.startsWith(u8, part, "fields=")) continue;
        const raw_fields = part["fields=".len..];
        if (raw_fields.len == 0) return .{};
        const decoded_fields = try decodePercentEncodedPathComponentAlloc(alloc, raw_fields);
        defer alloc.free(decoded_fields);
        var field_count: usize = 1;
        for (decoded_fields) |ch| {
            if (ch == ',') field_count += 1;
        }
        const fields = try alloc.alloc([]const u8, field_count);
        var field_index: usize = 0;
        errdefer {
            for (fields[0..field_index]) |field| alloc.free(field);
            alloc.free(fields);
        }
        var field_it = std.mem.splitScalar(u8, decoded_fields, ',');
        while (field_it.next()) |field| {
            fields[field_index] = try alloc.dupe(u8, field);
            field_index += 1;
        }
        return .{
            .fields = fields,
            .opts = .{
                .fields = fields,
                .include_all_fields = false,
            },
        };
    }
    return .{};
}

pub fn decodePercentEncodedPathComponentAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    if (std.mem.indexOfScalar(u8, raw, '%') == null) return try alloc.dupe(u8, raw);

    var out = try alloc.alloc(u8, raw.len);
    errdefer alloc.free(out);

    var in_index: usize = 0;
    var out_index: usize = 0;
    while (in_index < raw.len) {
        const ch = raw[in_index];
        if (ch != '%') {
            out[out_index] = ch;
            in_index += 1;
            out_index += 1;
            continue;
        }

        if (in_index + 2 >= raw.len) return error.InvalidArgument;
        const hi = std.fmt.charToDigit(raw[in_index + 1], 16) catch return error.InvalidArgument;
        const lo = std.fmt.charToDigit(raw[in_index + 2], 16) catch return error.InvalidArgument;
        out[out_index] = @as(u8, @intCast((hi << 4) | lo));
        in_index += 3;
        out_index += 1;
    }

    return try alloc.realloc(out, out_index);
}

test "lookup options decode generated SDK query values before splitting fields" {
    const alloc = std.testing.allocator;
    var opts = try parseLookupOptions(alloc, "fields=title%2Cbody%2Cauthor");
    defer opts.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), opts.fields.len);
    try std.testing.expectEqualStrings("title", opts.fields[0]);
    try std.testing.expectEqualStrings("body", opts.fields[1]);
    try std.testing.expectEqualStrings("author", opts.fields[2]);
}

pub fn parseScanKeysRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedScanKeysRequest {
    if (body.len == 0) return .{};

    var parsed = metadata_openapi.server.parseScanKeysBody(alloc, body) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidQueryRequest,
    };
    defer parsed.deinit();

    const fields: [][]const u8 = if (parsed.value.fields) |raw_fields|
        try cloneFieldList(alloc, raw_fields)
    else
        &.{};
    errdefer freeFieldList(alloc, fields);

    const from = if (parsed.value.from) |value| try alloc.dupe(u8, value) else "";
    errdefer if (from.len > 0) alloc.free(from);
    const to = if (parsed.value.to) |value| try alloc.dupe(u8, value) else "";
    errdefer if (to.len > 0) alloc.free(to);
    const filter_query_json = if (parsed.value.filter_query) |filter_query|
        try query_contract.normalizePublicStoredFilterQueryAlloc(alloc, filter_query)
    else
        "";
    errdefer if (filter_query_json.len > 0) alloc.free(filter_query_json);

    return .{
        .from = from,
        .to = to,
        .fields = fields,
        .filter_query_json = filter_query_json,
        .opts = .{
            .inclusive_from = parsed.value.inclusive_from orelse false,
            .exclusive_to = parsed.value.exclusive_to orelse false,
            .include_documents = fields.len > 0,
            .limit = if (parsed.value.limit) |limit|
                std.math.cast(u32, limit) orelse return error.InvalidQueryRequest
            else
                0,
            .fields = fields,
            .include_all_fields = false,
            .filter_query_json = filter_query_json,
        },
    };
}

pub fn scanRequestErrorResponse(
    alloc: std.mem.Allocator,
    err: anyerror,
) !?http_common.HttpResponse {
    return switch (err) {
        error.InvalidQueryRequest => try textResponse(alloc, 400, "invalid scan request"),
        // scanKeys declares BadRequest, not an operation-specific 422, in the
        // public OpenAPI contract. Keep every server and generated SDK aligned.
        error.UnsupportedQueryRequest => try textResponse(alloc, 400, "unsupported scan filter query"),
        else => null,
    };
}

fn cloneFieldList(alloc: std.mem.Allocator, raw_fields: []const []const u8) ![][]const u8 {
    const fields = try alloc.alloc([]const u8, raw_fields.len);
    var field_index: usize = 0;
    errdefer {
        for (fields[0..field_index]) |field| alloc.free(field);
        alloc.free(fields);
    }
    for (raw_fields) |field| {
        fields[field_index] = try alloc.dupe(u8, field);
        field_index += 1;
    }
    return fields;
}

fn freeFieldList(alloc: std.mem.Allocator, fields: [][]const u8) void {
    for (fields) |field| alloc.free(field);
    if (fields.len > 0) alloc.free(fields);
}

test "parse scan request normalizes filter query" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "filter_query": {"term": {"tenant": "t1"}}
        \\}
    ;

    var parsed = try parseScanKeysRequest(alloc, body);
    defer parsed.deinit(alloc);

    try std.testing.expect(!parsed.opts.include_documents);
    try std.testing.expectEqualStrings(parsed.filter_query_json, parsed.opts.filter_query_json);
    try std.testing.expect(std.mem.indexOf(u8, parsed.filter_query_json, "\"tenant\"") != null);
}

test "parse scan request rejects text-index-only filter clauses" {
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        parseScanKeysRequest(
            std.testing.allocator,
            \\{"filter_query":{"match_phrase":"paid receipt","field":"body"}}
            ,
        ),
    );
}

test "scan request errors map to stable client responses" {
    const alloc = std.testing.allocator;
    var invalid = (try scanRequestErrorResponse(alloc, error.InvalidQueryRequest)).?;
    defer invalid.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid.status);
    try std.testing.expectEqualStrings("invalid scan request", invalid.body);

    var unsupported = (try scanRequestErrorResponse(alloc, error.UnsupportedQueryRequest)).?;
    defer unsupported.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), unsupported.status);
    try std.testing.expectEqualStrings("unsupported scan filter query", unsupported.body);

    try std.testing.expect((try scanRequestErrorResponse(alloc, error.OutOfMemory)) == null);
}
