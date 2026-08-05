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

pub const SearchRequest = struct {
    pub const max_facets = 8;
    const default_page_size = 10;
    pub const max_page_size = 100;

    parsed: ?std.json.Parsed(std.json.Value) = null,
    text: ?[]const u8 = null,
    filter: ?std.json.Value = null,
    federation: FederationMode = .auto,
    page_start: usize = 0,
    page_size: usize = default_page_size,
    facet_fields: [max_facets][]const u8 = undefined,
    facet_field_count: usize = 0,

    pub fn deinit(self: SearchRequest) void {
        if (self.parsed) |parsed| parsed.deinit();
    }

    pub fn facetFields(self: *const SearchRequest) []const []const u8 {
        return self.facet_fields[0..self.facet_field_count];
    }
};

pub const FederationMode = enum {
    none,
    referrals,
    auto,

    fn parse(value: std.json.Value) !FederationMode {
        return switch (value) {
            .string => |mode| {
                if (std.mem.eql(u8, mode, "none")) return .none;
                if (std.mem.eql(u8, mode, "referrals")) return .referrals;
                if (std.mem.eql(u8, mode, "auto")) return .auto;
                return error.InvalidArdSearchRequest;
            },
            .null => .none,
            else => error.InvalidArdSearchRequest,
        };
    }

    pub fn name(self: FederationMode) []const u8 {
        return switch (self) {
            .none => "none",
            .referrals => "referrals",
            .auto => "auto",
        };
    }

    pub fn includesReferrals(self: FederationMode) bool {
        return self == .referrals or self == .auto;
    }
};

pub const AgentsRequest = struct {
    const default_page_size = 100;
    pub const max_page_size = 100;

    parsed_filter: ?std.json.Parsed(std.json.Value) = null,
    filter: ?std.json.Value = null,
    order_by: AgentOrder = .{},
    page_start: usize = 0,
    page_size: usize = default_page_size,

    pub fn deinit(self: AgentsRequest) void {
        if (self.parsed_filter) |parsed| parsed.deinit();
    }
};

pub const AgentOrder = struct {
    pub const Field = enum { natural, identifier, displayName, type };

    field: Field = .natural,
    desc: bool = false,
};

pub fn parseSearchRequest(alloc: std.mem.Allocator, body: []const u8) !SearchRequest {
    if (body.len == 0) return .{};
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidArdSearchRequest;
    errdefer parsed.deinit();
    if (parsed.value != .object) return error.InvalidArdSearchRequest;
    var request: SearchRequest = .{ .parsed = parsed };
    if (parsed.value.object.get("federation")) |value| request.federation = try FederationMode.parse(value);
    if (parsed.value.object.get("pageSize")) |value| request.page_size = try parsePageSize(value);
    if (parsed.value.object.get("pageToken")) |value| request.page_start = try parsePageToken(value);
    try parseFacetFields(&request, parsed.value);
    const query = parsed.value.object.get("query") orelse return request;
    if (query != .object) return error.InvalidArdSearchRequest;
    request.text = if (query.object.get("text")) |value| switch (value) {
        .string => |text_value| text_value,
        .null => null,
        else => return error.InvalidArdSearchRequest,
    } else null;
    request.filter = query.object.get("filter");
    if (request.filter) |value| {
        if (value != .object) return error.InvalidArdSearchRequest;
    }
    return request;
}

pub fn parseAgentsRequest(alloc: std.mem.Allocator, query: []const u8) !AgentsRequest {
    var request: AgentsRequest = .{};
    errdefer request.deinit();
    if (query.len == 0) return request;

    if (try decodedQueryParamAlloc(alloc, query, "filter")) |filter_json| {
        defer alloc.free(filter_json);
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{ .allocate = .alloc_always }) catch return error.InvalidArdAgentsRequest;
        errdefer parsed.deinit();
        if (parsed.value != .object) return error.InvalidArdAgentsRequest;
        request.filter = parsed.value;
        request.parsed_filter = parsed;
    }
    if (try decodedQueryParamAlloc(alloc, query, "orderBy")) |order_by| {
        defer alloc.free(order_by);
        request.order_by = try parseAgentOrder(order_by);
    }
    if (queryParam(query, "pageSize")) |page_size| {
        const parsed = std.fmt.parseUnsigned(usize, page_size, 10) catch return error.InvalidArdAgentsRequest;
        if (parsed < 1 or parsed > AgentsRequest.max_page_size) return error.InvalidArdAgentsRequest;
        request.page_size = parsed;
    }
    if (queryParam(query, "pageToken")) |page_token| {
        const parsed = std.fmt.parseUnsigned(usize, page_token, 10) catch return error.InvalidArdAgentsRequest;
        if (parsed > std.math.maxInt(usize) - AgentsRequest.max_page_size) return error.InvalidArdAgentsRequest;
        request.page_start = parsed;
    }
    return request;
}

fn parsePageToken(value: std.json.Value) !usize {
    return switch (value) {
        .string => |token| {
            if (token.len == 0) return error.InvalidArdSearchRequest;
            const parsed = std.fmt.parseUnsigned(usize, token, 10) catch return error.InvalidArdSearchRequest;
            if (parsed > std.math.maxInt(usize) - SearchRequest.max_page_size) return error.InvalidArdSearchRequest;
            return parsed;
        },
        .null => 0,
        else => error.InvalidArdSearchRequest,
    };
}

fn parsePageSize(value: std.json.Value) !usize {
    return switch (value) {
        .integer => |actual| {
            if (actual < 1 or actual > SearchRequest.max_page_size) return error.InvalidArdSearchRequest;
            return @intCast(actual);
        },
        .null => SearchRequest.default_page_size,
        else => error.InvalidArdSearchRequest,
    };
}

fn parseFacetFields(request: *SearchRequest, root: std.json.Value) !void {
    const result_type = root.object.get("resultType") orelse return;
    if (result_type != .object) return error.InvalidArdSearchRequest;
    const facets = result_type.object.get("facets") orelse return;
    if (facets != .array) return error.InvalidArdSearchRequest;
    for (facets.array.items) |facet| {
        if (request.facet_field_count >= SearchRequest.max_facets) break;
        switch (facet) {
            .string => |field| {
                request.facet_fields[request.facet_field_count] = field;
                request.facet_field_count += 1;
            },
            .object => |object| {
                const field = object.get("field") orelse return error.InvalidArdSearchRequest;
                if (field != .string) return error.InvalidArdSearchRequest;
                request.facet_fields[request.facet_field_count] = field.string;
                request.facet_field_count += 1;
            },
            else => return error.InvalidArdSearchRequest,
        }
    }
}

fn parseAgentOrder(raw: []const u8) !AgentOrder {
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    if (trimmed.len == 0) return .{};
    var field_text = trimmed;
    var desc = false;
    if (std.mem.endsWith(u8, trimmed, " desc")) {
        field_text = std.mem.trim(u8, trimmed[0 .. trimmed.len - " desc".len], " \t\r\n");
        desc = true;
    } else if (std.mem.endsWith(u8, trimmed, " asc")) {
        field_text = std.mem.trim(u8, trimmed[0 .. trimmed.len - " asc".len], " \t\r\n");
    } else if (trimmed[0] == '-') {
        field_text = std.mem.trim(u8, trimmed[1..], " \t\r\n");
        desc = true;
    }

    const field: AgentOrder.Field = if (std.mem.eql(u8, field_text, "identifier"))
        .identifier
    else if (std.mem.eql(u8, field_text, "displayName"))
        .displayName
    else if (std.mem.eql(u8, field_text, "type"))
        .type
    else
        return error.InvalidArdAgentsRequest;
    return .{ .field = field, .desc = desc };
}

fn queryParam(query: []const u8, key: []const u8) ?[]const u8 {
    if (query.len == 0) return null;
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |part| {
        if (!std.mem.startsWith(u8, part, key)) continue;
        if (part.len <= key.len or part[key.len] != '=') continue;
        return part[key.len + 1 ..];
    }
    return null;
}

fn decodedQueryParamAlloc(alloc: std.mem.Allocator, query: []const u8, key: []const u8) !?[]u8 {
    const raw = queryParam(query, key) orelse return null;
    return try decodePercentEncodedAlloc(alloc, raw);
}

fn decodePercentEncodedAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    var needs_decode = false;
    for (raw) |ch| {
        if (ch == '%' or ch == '+') {
            needs_decode = true;
            break;
        }
    }
    if (!needs_decode) return try alloc.dupe(u8, raw);

    var out = try alloc.alloc(u8, raw.len);
    errdefer alloc.free(out);

    var in_index: usize = 0;
    var out_index: usize = 0;
    while (in_index < raw.len) {
        const ch = raw[in_index];
        if (ch == '+') {
            out[out_index] = ' ';
            in_index += 1;
            out_index += 1;
            continue;
        }
        if (ch != '%') {
            out[out_index] = ch;
            in_index += 1;
            out_index += 1;
            continue;
        }
        if (in_index + 2 >= raw.len) return error.InvalidArdAgentsRequest;
        const hi = std.fmt.charToDigit(raw[in_index + 1], 16) catch return error.InvalidArdAgentsRequest;
        const lo = std.fmt.charToDigit(raw[in_index + 2], 16) catch return error.InvalidArdAgentsRequest;
        out[out_index] = @intCast((hi << 4) | lo);
        in_index += 3;
        out_index += 1;
    }
    return try alloc.realloc(out, out_index);
}
