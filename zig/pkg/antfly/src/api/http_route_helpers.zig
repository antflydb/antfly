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

pub fn textResponseWithHeaders(
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
    const content_type = try alloc.dupe(u8, "text/plain");
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
    relational_integrity_jobs_json: []const u8 = "",
    relational_activation_json: []const u8 = "",
    relational_index_status_json: []const u8 = "",
    relational_topology_json: []const u8 = "",
    opts: @import("../storage/db/types.zig").LookupOptions = .{},

    pub fn deinit(self: *OwnedLookupOptions, alloc: std.mem.Allocator) void {
        if (self.relational_integrity_jobs_json.len != 0) alloc.free(self.relational_integrity_jobs_json);
        if (self.relational_activation_json.len != 0) alloc.free(self.relational_activation_json);
        if (self.relational_index_status_json.len != 0) alloc.free(self.relational_index_status_json);
        if (self.relational_topology_json.len != 0) alloc.free(self.relational_topology_json);
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
    relational_query_json: []const u8 = "",
    opts: @import("../storage/db/types.zig").ScanOptions = .{},

    pub fn deinit(self: *OwnedScanKeysRequest, alloc: std.mem.Allocator) void {
        if (self.from.len > 0) alloc.free(self.from);
        if (self.to.len > 0) alloc.free(self.to);
        for (self.fields) |field| alloc.free(field);
        if (self.fields.len > 0) alloc.free(self.fields);
        if (self.filter_query_json.len > 0) alloc.free(@constCast(self.filter_query_json));
        if (self.relational_query_json.len > 0) alloc.free(self.relational_query_json);
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

pub fn parseInternalLookupOptions(alloc: std.mem.Allocator, query: []const u8) !OwnedLookupOptions {
    var result = try parseLookupOptions(alloc, query);
    errdefer result.deinit(alloc);
    var parts = std.mem.splitScalar(u8, query, '&');
    var seen = false;
    while (parts.next()) |part| {
        if (std.mem.startsWith(u8, part, "_relational_integrity_catalog=")) {
            if (seen or !std.mem.eql(u8, part, "_relational_integrity_catalog=true")) return error.InvalidQueryRequest;
            seen = true;
            result.opts.relational_integrity_catalog = true;
        } else if (std.mem.startsWith(u8, part, "_primary_digest=")) {
            if (result.opts.include_primary_digest or !std.mem.eql(u8, part, "_primary_digest=true")) return error.InvalidQueryRequest;
            result.opts.include_primary_digest = true;
        } else if (std.mem.startsWith(u8, part, "_relational_integrity_action=")) {
            if (seen or !std.mem.eql(u8, part, "_relational_integrity_action=true")) return error.InvalidQueryRequest;
            seen = true;
            result.opts.relational_integrity_action = true;
        } else if (std.mem.startsWith(u8, part, "_relational_integrity_jobs=")) {
            if (seen) return error.InvalidQueryRequest;
            seen = true;
            result.relational_integrity_jobs_json = try decodePercentEncodedPathComponentAlloc(alloc, part["_relational_integrity_jobs=".len..]);
            if (result.relational_integrity_jobs_json.len == 0 or result.relational_integrity_jobs_json.len > 4096) return error.InvalidQueryRequest;
            result.opts.relational_integrity_jobs_json = result.relational_integrity_jobs_json;
        } else if (std.mem.startsWith(u8, part, "_relational_index_status=")) {
            if (seen) return error.InvalidQueryRequest;
            seen = true;
            result.relational_index_status_json = try decodePercentEncodedPathComponentAlloc(alloc, part["_relational_index_status=".len..]);
            if (result.relational_index_status_json.len == 0 or result.relational_index_status_json.len > 4096) return error.InvalidQueryRequest;
            result.opts.relational_index_status_json = result.relational_index_status_json;
        } else if (std.mem.startsWith(u8, part, "_relational_activation=")) {
            if (seen) return error.InvalidQueryRequest;
            seen = true;
            result.relational_activation_json = try decodePercentEncodedPathComponentAlloc(alloc, part["_relational_activation=".len..]);
            if (result.relational_activation_json.len == 0 or result.relational_activation_json.len > 4096) return error.InvalidQueryRequest;
            result.opts.relational_activation_json = result.relational_activation_json;
        } else if (std.mem.startsWith(u8, part, "_relational_topology=")) {
            if (seen) return error.InvalidQueryRequest;
            seen = true;
            result.relational_topology_json = try decodePercentEncodedPathComponentAlloc(alloc, part["_relational_topology=".len..]);
            if (result.relational_topology_json.len == 0 or result.relational_topology_json.len > 4096) return error.InvalidQueryRequest;
            result.opts.relational_topology_json = result.relational_topology_json;
        } else if (std.mem.startsWith(u8, part, "_restore_staging_scope=")) {
            if (result.opts.restore_staging_scope != null) return error.InvalidQueryRequest;
            const hex = part["_restore_staging_scope=".len..];
            if (hex.len != 64) return error.InvalidQueryRequest;
            var scope: [32]u8 = undefined;
            _ = std.fmt.hexToBytes(&scope, hex) catch return error.InvalidQueryRequest;
            result.opts.restore_staging_scope = scope;
        } else if (std.mem.startsWith(u8, part, "_restore_staging_plan_id=")) {
            if (result.opts.restore_staging_plan_id != null) return error.InvalidQueryRequest;
            const hex = part["_restore_staging_plan_id=".len..];
            if (hex.len != 32) return error.InvalidQueryRequest;
            var plan: [16]u8 = undefined;
            _ = std.fmt.hexToBytes(&plan, hex) catch return error.InvalidQueryRequest;
            if (std.mem.allEqual(u8, &plan, 0)) return error.InvalidQueryRequest;
            result.opts.restore_staging_plan_id = plan;
        }
    }
    if (result.opts.restore_staging_plan_id != null and result.opts.restore_staging_scope == null) return error.InvalidQueryRequest;
    return result;
}

test "relational row query control modes require authenticated internal parsing" {
    const alloc = std.testing.allocator;
    var primary = try parseInternalLookupOptions(alloc, "_primary_digest=true");
    defer primary.deinit(alloc);
    try std.testing.expect(primary.opts.include_primary_digest);
    var public_primary = try parseLookupOptions(alloc, "_primary_digest=true");
    defer public_primary.deinit(alloc);
    try std.testing.expect(!public_primary.opts.include_primary_digest);
    try std.testing.expectError(error.InvalidQueryRequest, parseInternalLookupOptions(alloc, "_primary_digest=true&_primary_digest=true"));
    const query = "_relational_activation=%7B%22mode%22%3A%22status%22%7D";
    var internal = try parseInternalLookupOptions(alloc, query);
    defer internal.deinit(alloc);
    try std.testing.expectEqualStrings("{\"mode\":\"status\"}", internal.opts.relational_activation_json);
    var public = try parseLookupOptions(alloc, query);
    defer public.deinit(alloc);
    try std.testing.expectEqualStrings("", public.opts.relational_activation_json);
    const index_query = "_relational_index_status=%7B%22name%22%3A%22by_id%22%2C%22schema_version%22%3A2%7D";
    var index_internal = try parseInternalLookupOptions(alloc, index_query);
    defer index_internal.deinit(alloc);
    try std.testing.expectEqualStrings("{\"name\":\"by_id\",\"schema_version\":2}", index_internal.opts.relational_index_status_json);
    var index_public = try parseLookupOptions(alloc, index_query);
    defer index_public.deinit(alloc);
    try std.testing.expectEqualStrings("", index_public.opts.relational_index_status_json);
    try std.testing.expectError(error.InvalidQueryRequest, parseInternalLookupOptions(alloc, "_relational_integrity_catalog=true&_relational_integrity_action=true"));
    const topology_query = "_relational_topology=%7B%22mode%22%3A%22identity%22%7D";
    var topology_internal = try parseInternalLookupOptions(alloc, topology_query);
    defer topology_internal.deinit(alloc);
    try std.testing.expectEqualStrings("{\"mode\":\"identity\"}", topology_internal.opts.relational_topology_json);
    var topology_public = try parseLookupOptions(alloc, topology_query);
    defer topology_public.deinit(alloc);
    try std.testing.expectEqualStrings("", topology_public.opts.relational_topology_json);
    try std.testing.expectError(error.InvalidQueryRequest, parseInternalLookupOptions(alloc, topology_query ++ "&_relational_integrity_catalog=true"));
}

test "private restore lookup plan identity never leaks into public admission" {
    const alloc = std.testing.allocator;
    const plan = "_restore_staging_plan_id=ffffffffffffffffffffffffffffffff";
    const scope = "_restore_staging_scope=" ++ "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    var parsed = try parseInternalLookupOptions(alloc, plan ++ "&" ++ scope);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as([16]u8, @splat(0xff)), parsed.opts.restore_staging_plan_id.?);
    try std.testing.expectEqual(@as([32]u8, @splat(0xee)), parsed.opts.restore_staging_scope.?);
    var public = try parseLookupOptions(alloc, plan ++ "&" ++ scope);
    defer public.deinit(alloc);
    try std.testing.expect(public.opts.restore_staging_plan_id == null and public.opts.restore_staging_scope == null);
    try std.testing.expectError(error.InvalidQueryRequest, parseInternalLookupOptions(alloc, plan));
    try std.testing.expectError(error.InvalidQueryRequest, parseInternalLookupOptions(alloc, plan ++ "&" ++ scope ++ "&" ++ plan));
    try std.testing.expectError(error.InvalidQueryRequest, parseInternalLookupOptions(alloc, scope ++ "&_restore_staging_plan_id=00000000000000000000000000000000"));
}

test "relational mutation endpoints are not aliased to serverless ingestion" {
    const serverless = @import("../serverless/api/http_routes.zig");
    try std.testing.expect(serverless.match(.post, "/db/v1/tables/rows/rows/mutate") == null);
    try std.testing.expect(serverless.match(.post, "/db/v1/tables/rows/rows/query") == null);
    try std.testing.expect(serverless.match(.get, "/db/v1/tables/rows/constraints/status") == null);
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
    return parseScanKeysRequestImpl(alloc, body, false);
}

pub fn parseInternalScanKeysRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedScanKeysRequest {
    return parseScanKeysRequestImpl(alloc, body, true);
}

pub fn parseRelationalRowQueryRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedScanKeysRequest {
    var parsed = std.json.parseFromSlice(metadata_openapi.types.RelationalRowQueryRequest, alloc, body, .{ .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidQueryRequest,
    };
    defer parsed.deinit();
    const req = parsed.value;
    const limit = std.math.cast(u32, req.limit orelse 128) orelse return error.InvalidQueryRequest;
    if (req.index) |name| {
        if (name.len == 0 or req.schema_version == null or req.from != null or req.to != null) return error.InvalidQueryRequest;
    } else if (req.after != null or req.lower != null or req.upper != null) return error.InvalidQueryRequest;
    if (req.after) |cursor| @import("../storage/db/relational_row_cursor.zig").validate(cursor) catch return error.InvalidQueryRequest;
    inline for (.{ "lower", "upper" }) |field| if (@field(req, field)) |bound| {
        if (bound.values.len == 0 or bound.values.len > 256) return error.InvalidQueryRequest;
    };
    // Newly created tables use epoch zero. Presence provides the fence;
    // rejecting zero makes those tables impossible to query by index.
    if (req.schema_version) |version| if (std.math.cast(u32, version) == null) return error.InvalidQueryRequest;
    const conditions: []const metadata_openapi.types.RelationalRowCondition = req.conditions orelse &.{};
    if (req.fields.len > 256 or conditions.len > 256 or limit == 0 or limit > 4096) return error.InvalidQueryRequest;
    for (req.fields, 0..) |field, i| {
        if (field.len == 0) return error.InvalidQueryRequest;
        for (req.fields[0..i]) |prior| if (std.mem.eql(u8, prior, field)) return error.InvalidQueryRequest;
    }
    for (conditions) |condition| if (condition.column.len == 0) return error.InvalidQueryRequest;
    var result: OwnedScanKeysRequest = .{};
    errdefer result.deinit(alloc);
    result.from = if (req.from) |value| try alloc.dupe(u8, value) else "";
    result.to = if (req.to) |value| try alloc.dupe(u8, value) else "";
    result.fields = try cloneFieldList(alloc, req.fields);
    result.relational_query_json = try alloc.dupe(u8, body);
    result.opts = .{
        .exclusive_to = true,
        .include_documents = true,
        .limit = limit,
        .fields = result.fields,
        .include_all_fields = false,
        .relational_query_json = result.relational_query_json,
    };
    return result;
}

test "relational row query preserves exact operands and empty projection" {
    const alloc = std.testing.allocator;
    var req = try parseRelationalRowQueryRequest(alloc,
        \\{"fields":[],"conditions":[{"column":"id","op":"eq","value":9007199254740993}],"schema_version":3,"from":"a","limit":5}
    );
    defer req.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 5), req.opts.limit);
    try std.testing.expect(req.opts.include_documents and !req.opts.include_all_fields);
    try std.testing.expect(std.mem.indexOf(u8, req.opts.relational_query_json, "9007199254740993") != null);
    try std.testing.expectError(error.InvalidQueryRequest, parseRelationalRowQueryRequest(alloc, "{\"fields\":[],\"limit\":-1}"));
    try std.testing.expectError(error.InvalidQueryRequest, parseRelationalRowQueryRequest(alloc, "{\"fields\":[\"x\",\"x\"]}"));
}

test "relational row query index selection requires schema fencing and exclusive cursor vocabulary" {
    const alloc = std.testing.allocator;
    var request = try parseRelationalRowQueryRequest(alloc,
        \\{"schema_version":7,"index":"tenant_id","fields":[],"lower":{"values":["9007199254740993"],"inclusive":false},"limit":4}
    );
    defer request.deinit(alloc);
    try std.testing.expectEqualStrings("", request.from);
    try std.testing.expectEqualStrings("", request.to);
    try std.testing.expectEqual(@as(u32, 4), request.opts.limit);
    for ([_][]const u8{
        "{\"index\":\"tenant_id\",\"fields\":[]}",
        "{\"schema_version\":7,\"index\":\"tenant_id\",\"from\":\"a\",\"fields\":[]}",
        "{\"fields\":[],\"lower\":{\"values\":[1]}}",
        "{\"schema_version\":7,\"index\":\"tenant_id\",\"fields\":[],\"lower\":{\"values\":[]}}",
        "{\"schema_version\":7,\"index\":\"tenant_id\",\"fields\":[],\"after\":\"invalid\"}",
    }) |invalid| try std.testing.expectError(error.InvalidQueryRequest, parseRelationalRowQueryRequest(alloc, invalid));
    try std.testing.expectEqual(@as(u16, 409), scanRequestError(error.RelationalIndexNotReady).?.status);
    try std.testing.expectEqual(@as(u16, 404), scanRequestError(error.IndexNotFound).?.status);
}

test "relational row query accepts the initial zero epoch without weakening index fencing" {
    const alloc = std.testing.allocator;
    var request = try parseRelationalRowQueryRequest(alloc,
        \\{"schema_version":0,"index":"active_price","fields":["id","price"],"conditions":[{"column":"active","op":"eq","value":true}],"lower":{"values":[1]},"upper":{"values":[1]},"limit":4096}
    );
    defer request.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, request.relational_query_json, "\"schema_version\":0") != null);
    for ([_][]const u8{
        "{\"index\":\"active_price\",\"fields\":[]}",
        "{\"schema_version\":null,\"index\":\"active_price\",\"fields\":[]}",
        "{\"schema_version\":-1,\"index\":\"active_price\",\"fields\":[]}",
        "{\"schema_version\":4294967296,\"index\":\"active_price\",\"fields\":[]}",
    }) |invalid| try std.testing.expectError(error.InvalidQueryRequest, parseRelationalRowQueryRequest(alloc, invalid));
}

fn parseScanKeysRequestImpl(alloc: std.mem.Allocator, body: []const u8, allow_internal_options: bool) !OwnedScanKeysRequest {
    if (body.len == 0) return .{};

    const relational_query_json: []const u8 = if (allow_internal_options) blk: {
        var raw = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .parse_numbers = false });
        defer raw.deinit();
        if (raw.value != .object) return error.InvalidQueryRequest;
        const query = raw.value.object.get("_relational_query") orelse break :blk "";
        if (query != .object) return error.InvalidQueryRequest;
        break :blk try std.json.Stringify.valueAlloc(alloc, query, .{});
    } else "";
    errdefer if (relational_query_json.len != 0) alloc.free(relational_query_json);

    const include_content_hashes = if (allow_internal_options) blk: {
        var raw = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidQueryRequest,
        };
        defer raw.deinit();
        if (raw.value != .object) return error.InvalidQueryRequest;
        break :blk if (raw.value.object.get("_include_content_hashes")) |value| switch (value) {
            .bool => |flag| flag,
            else => return error.InvalidQueryRequest,
        } else false;
    } else false;

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
        .relational_query_json = relational_query_json,
        .opts = .{
            .inclusive_from = parsed.value.inclusive_from orelse false,
            .exclusive_to = parsed.value.exclusive_to orelse false,
            .include_documents = fields.len > 0 or relational_query_json.len != 0,
            .limit = if (parsed.value.limit) |limit|
                std.math.cast(u32, limit) orelse return error.InvalidQueryRequest
            else
                0,
            .fields = fields,
            .include_all_fields = false,
            .filter_query_json = filter_query_json,
            .include_content_hashes = include_content_hashes,
            .relational_query_json = relational_query_json,
        },
    };
}

pub const ScanRequestError = struct {
    status: u16,
    message: []const u8,
};

/// Public responses deliberately hide internal read failures. Preserve the
/// cause operationally without allowing a failing read loop to flood logs.
pub const RelationalReadDiagnosticGate = @import("bounded_diagnostic_gate.zig").Gate;

test "relational row query diagnostics bound repeated unavailable responses" {
    var gate: RelationalReadDiagnosticGate = .{};
    try std.testing.expect(gate.admit(1));
    for (0..1000) |_| try std.testing.expect(!gate.admit(2));
    try std.testing.expect(gate.admit(1 + 30 * std.time.ns_per_s));
    try std.testing.expect(!gate.admit(1 + 30 * std.time.ns_per_s));
}

pub fn scanRequestError(err: anyerror) ?ScanRequestError {
    return switch (err) {
        error.InvalidQueryRequest,
        error.InvalidRelationalRowsRequest,
        error.RelationalTableRequired,
        error.RelationalIndexColumnNotFound,
        error.UnsupportedRelationalIndexColumn,
        error.InvalidRelationalIndexBound,
        error.InvalidBatchRequest,
        => .{ .status = 400, .message = "invalid scan request" },
        error.PreparedSchemaChanged,
        error.PreparedGenerationChanged,
        error.SchemaVersionChanged,
        error.RelationalIndexColumnTypeMismatch,
        => .{ .status = 409, .message = "relational schema epoch changed" },
        error.RelationalIndexNotReady => .{ .status = 409, .message = "relational index is not ready on every owning shard" },
        error.IndexNotFound => .{ .status = 404, .message = "relational index not found" },
        error.RelationalRowsOutputBudgetExceeded, error.RelationalRowResultTooLarge => .{ .status = 413, .message = "projected row exceeds output budget" },
        // scanKeys declares BadRequest, not an operation-specific 422, in the
        // public OpenAPI contract. Keep every server and generated SDK aligned.
        error.UnsupportedQueryRequest => .{ .status = 400, .message = "unsupported scan filter query" },
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
    const invalid = scanRequestError(error.InvalidQueryRequest).?;
    try std.testing.expectEqual(@as(u16, 400), invalid.status);
    try std.testing.expectEqualStrings("invalid scan request", invalid.message);

    const unsupported = scanRequestError(error.UnsupportedQueryRequest).?;
    try std.testing.expectEqual(@as(u16, 400), unsupported.status);
    try std.testing.expectEqualStrings("unsupported scan filter query", unsupported.message);

    try std.testing.expect(scanRequestError(error.OutOfMemory) == null);
}
