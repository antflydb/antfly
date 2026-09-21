// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Client code generation: OpenAPI operations → Zig client methods.

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const naming = @import("naming.zig");
const SourceWriter = @import("writer.zig").SourceWriter;
const Resolver = @import("resolver.zig").Resolver;
const TypeGenerator = @import("codegen_types.zig").TypeGenerator;
const shared = @import("codegen_shared.zig");

pub const ClientGenerator = struct {
    arena: Allocator,
    w: *SourceWriter,
    resolver: *Resolver,
    type_gen: *TypeGenerator,

    pub fn init(arena: Allocator, w: *SourceWriter, resolver: *Resolver, type_gen: *TypeGenerator) ClientGenerator {
        return .{ .arena = arena, .w = w, .resolver = resolver, .type_gen = type_gen };
    }

    /// Generate the full client module.
    pub fn generate(self: *ClientGenerator, doc: *const types.OpenApiDoc) !void {
        // ApiResponse wrapper
        try self.generateApiResponse();
        try self.w.blank();

        // Single pass: detect streaming responses and generate query param structs
        var needs_raw = false;
        const paths = try shared.sortedStringKeys(self.arena, doc.paths.keys());
        for (paths) |path| {
            const path_item = doc.paths.get(path) orelse continue;
            for (shared.methodOps(path_item)) |mo| {
                const op = mo.op orelse continue;
                if (!needs_raw and shared.isStreamingOrBinaryResponse(op)) {
                    needs_raw = true;
                }
                const op_id = op.operation_id orelse continue;
                if (!shared.isStreamingOrBinaryResponse(op)) {
                    const responses = try shared.successSchemas(self.arena, self.resolver, op);
                    if (responses.len > 1) try self.generateSuccessUnion(op_id, responses);
                }
                const params = try shared.collectParameters(self.arena, self.resolver, path_item.parameters, op.parameters);
                if (params.query.len > 0) {
                    try shared.generateQueryParamsStruct(self.arena, self.w, op_id, params.query);
                    try self.w.blank();
                }
            }
        }
        if (needs_raw) {
            try self.generateRawResponse();
            try self.w.blank();
        }

        // Client struct
        try self.w.line("pub const Client = struct {{", .{});
        self.w.indent();

        try self.w.line("http: *httpx.Client,", .{});
        try self.w.line("base_url: []const u8,", .{});
        try self.w.line("allocator: std.mem.Allocator,", .{});
        try self.w.line("auth_header: ?[2][]const u8 = null,", .{});
        try self.w.blank();

        try self.w.line("pub fn init(allocator: std.mem.Allocator, http: *httpx.Client, base_url: []const u8) @This() {{", .{});
        self.w.indent();
        try self.w.line("return .{{ .http = http, .base_url = base_url, .allocator = allocator }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();

        try self.w.line("pub fn setBearer(self: *@This(), token: []const u8) !void {{", .{});
        self.w.indent();
        try self.w.line("self.freeAuth();", .{});
        try self.w.line("self.auth_header = .{{ \"Authorization\", try std.fmt.allocPrint(self.allocator, \"Bearer {{s}}\", .{{token}}) }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();

        try self.w.line("pub fn deinit(self: *@This()) void {{", .{});
        self.w.indent();
        try self.w.line("self.freeAuth();", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();

        // Generate a method for each operation
        for (paths) |path| {
            const path_item = doc.paths.get(path) orelse continue;
            for (shared.methodOps(path_item)) |mo| {
                const op = mo.op orelse continue;
                const op_id = op.operation_id orelse continue;
                try self.generateMethod(path, mo.method, op_id, op, path_item.parameters);
                try self.w.blank();
            }
        }

        // authHeaders helper
        try self.generateAuthHeaders();

        self.w.dedent();
        try self.w.line("}};", .{});
    }

    fn generateApiResponse(self: *ClientGenerator) !void {
        try self.w.line("pub fn ApiResponse(comptime T: type) type {{", .{});
        self.w.indent();
        try self.w.line("return struct {{", .{});
        self.w.indent();
        try self.w.line("status_code: u16,", .{});
        try self.w.line("data: ?std.json.Parsed(T) = null,", .{});
        try self.w.line("/// Set instead of `data` when the server answered a negotiated", .{});
        try self.w.line("/// request in a format other than JSON; `content_type` says which.", .{});
        try self.w.line("bytes: ?[]const u8 = null,", .{});
        try self.w.line("content_type: ?[]const u8 = null,", .{});
        try self.w.line("err_body: ?[]const u8 = null,", .{});
        try self.w.line("allocator: std.mem.Allocator,", .{});
        try self.w.blank();

        try self.w.line("pub fn deinit(self: *@This()) void {{", .{});
        self.w.indent();
        try self.w.line("if (self.data) |*d| d.deinit();", .{});
        try self.w.line("if (self.bytes) |b| self.allocator.free(b);", .{});
        try self.w.line("if (self.content_type) |ct| self.allocator.free(ct);", .{});
        try self.w.line("if (self.err_body) |b| self.allocator.free(b);", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        try self.w.blank();

        try self.w.line("pub fn fromResponse(allocator: std.mem.Allocator, resp: *httpx.Response) !@This() {{", .{});
        self.w.indent();
        try self.w.line("defer resp.deinit();", .{});
        try self.w.line("if (resp.ok()) {{", .{});
        self.w.indent();
        try self.w.line("if (resp.status.code == 204 or resp.status.code == 205) return .{{ .status_code = resp.status.code, .allocator = allocator }};", .{});
        try self.w.line("if (resp.body) |body| {{", .{});
        self.w.indent();
        try self.w.line("const parse_result = if (comptime @typeInfo(T) == .@\"union\" and @hasDecl(T, \"parseResponse\")) T.parseResponse(allocator, resp.status.code, body) else std.json.parseFromSlice(T, allocator, body, .{{ .allocate = .alloc_always, .ignore_unknown_fields = true }});", .{});
        try self.w.line("const parsed = parse_result catch |err| {{", .{});
        self.w.indent();
        try self.w.line("return if (err == error.OutOfMemory) error.OutOfMemory else error.InvalidApiResponse;", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.line("return .{{ .status_code = resp.status.code, .data = parsed, .allocator = allocator }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.line("return error.InvalidApiResponse;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.line("return .{{ .status_code = resp.status.code, .err_body = if (resp.body) |b| try allocator.dupe(u8, b) else null, .allocator = allocator }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        try self.w.blank();

        try self.w.docComment("Same as `fromResponse`, for an operation whose success response the");
        try self.w.docComment("caller can negotiate: a JSON body is parsed into `data`, and any other");
        try self.w.docComment("media type is kept verbatim in `bytes` with its `content_type`.");
        try self.w.line("pub fn fromNegotiatedResponse(allocator: std.mem.Allocator, resp: *httpx.Response) !@This() {{", .{});
        self.w.indent();
        try self.w.line("const negotiated = resp.contentType();", .{});
        try self.w.line("// An empty or JSON body, and every failure, stay on the typed path.", .{});
        try self.w.line("if (!resp.ok() or resp.body == null or negotiated == null or isJsonContentType(negotiated.?)) return fromResponse(allocator, resp);", .{});
        try self.w.line("defer resp.deinit();", .{});
        try self.w.line("const owned = try allocator.dupe(u8, resp.body.?);", .{});
        try self.w.line("errdefer allocator.free(owned);", .{});
        try self.w.line("const owned_type = try allocator.dupe(u8, negotiated.?);", .{});
        try self.w.line("return .{{ .status_code = resp.status.code, .bytes = owned, .content_type = owned_type, .allocator = allocator }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        self.w.dedent();
        try self.w.line("}};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        try self.w.blank();
        try self.w.docComment("Whether a response media type carries a JSON body.");
        try self.w.line("fn isJsonContentType(content_type: []const u8) bool {{", .{});
        self.w.indent();
        try self.w.line("const essence = std.mem.trim(u8, std.mem.sliceTo(content_type, ';'), \" \\t\");", .{});
        try self.w.line("if (std.ascii.eqlIgnoreCase(essence, \"application/json\")) return true;", .{});
        try self.w.line("return std.ascii.endsWithIgnoreCase(essence, \"+json\");", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
    }

    fn generateRawResponse(self: *ClientGenerator) !void {
        try self.w.docComment("Raw HTTP response for streaming/binary endpoints.");
        try self.w.line("pub const RawResponse = struct {{", .{});
        self.w.indent();
        try self.w.line("status_code: u16,", .{});
        try self.w.line("body: ?[]const u8 = null,", .{});
        try self.w.line("content_type: ?[]const u8 = null,", .{});
        try self.w.line("allocator: std.mem.Allocator,", .{});
        try self.w.blank();
        try self.w.line("pub fn deinit(self: *@This()) void {{", .{});
        self.w.indent();
        try self.w.line("if (self.body) |b| self.allocator.free(b);", .{});
        try self.w.line("if (self.content_type) |ct| self.allocator.free(ct);", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
    }

    fn generateMethod(
        self: *ClientGenerator,
        path: []const u8,
        http_method: []const u8,
        op_id: []const u8,
        op: types.Operation,
        path_level_params: []const types.ParameterOrRef,
    ) !void {
        const method_name = try naming.toMethodName(self.arena, op_id);

        // Collect all parameters (path-level + operation-level), resolving $refs
        const params = try shared.collectParameters(self.arena, self.resolver, path_level_params, op.parameters);

        // Detect streaming/binary response
        const is_raw = shared.isStreamingOrBinaryResponse(op);
        // A response the caller can negotiate keeps its typed JSON form and
        // gains the bytes when the server answers in the other format.
        const is_negotiated = shared.hasNegotiatedResponse(op);

        // Determine response type
        const response_type = if (is_raw) null else if ((try shared.successSchemas(self.arena, self.resolver, op)).len > 1)
            try std.fmt.allocPrint(self.arena, "{s}Response", .{try naming.toTypeName(self.arena, op_id)})
        else
            try shared.getSuccessResponseType(self.arena, self.resolver, self.type_gen, op);

        // Doc comment
        if (op.summary) |summary| try self.w.docComment(summary);
        const upper_method = try std.ascii.allocUpperString(self.arena, http_method);
        try self.w.docComment(try std.fmt.allocPrint(self.arena, "{s} {s}", .{ upper_method, path }));

        // Resolve request body type once (used for both signature and body serialization)
        const body_type: ?[]const u8 = if (op.request_body) |rb_or|
            try shared.getRequestBodyType(self.arena, self.resolver, self.type_gen, rb_or)
        else
            null;
        const is_binary_request = if (op.request_body) |rb_or|
            shared.hasOctetStreamRequestBody(self.resolver, rb_or)
        else
            false;
        const is_body_required = if (op.request_body) |rb_or|
            shared.isRequestBodyRequired(self.resolver, rb_or)
        else
            false;

        // Method signature
        {
            var sig = std.ArrayListUnmanaged(u8).empty;
            try sig.print(self.arena, "pub fn {s}(self: *@This()", .{method_name});

            for (params.path) |p| {
                const pname = try naming.zigFieldName(self.arena, p.name);
                try sig.print(self.arena, ", {s}: []const u8", .{pname});
            }

            // Request body parameter
            if (is_binary_request) {
                try sig.print(self.arena, ", body: {s}[]const u8", .{if (is_body_required) "" else "?"});
            } else if (body_type) |bt| {
                try sig.print(self.arena, ", body: {s}{s}", .{ if (is_body_required) "" else "?", bt });
            }

            for (params.header) |p| {
                const pname = try naming.zigFieldName(self.arena, p.name);
                try sig.print(self.arena, ", {s}: {s}[]const u8", .{ pname, if (p.required) "" else "?" });
            }

            // Query params struct
            if (params.query.len > 0) {
                try sig.print(self.arena, ", params: {s}", .{try shared.queryParamsTypeName(self.arena, op_id)});
            }

            if (is_raw) {
                try sig.appendSlice(self.arena, ") !RawResponse {");
            } else {
                try sig.print(self.arena, ") !ApiResponse({s}) {{", .{response_type.?});
            }
            try self.w.line("{s}", .{sig.items});
        }

        self.w.indent();

        // Build URL with path parameter interpolation and query params
        const has_query = params.query.len > 0;
        try self.generateUrlConstruction(path, params.path, has_query);

        // Append query parameters to URL
        if (has_query) {
            try self.generateQueryParamAppend(params.query);
        }

        // Build request body
        if (!is_binary_request and body_type != null) {
            if (is_body_required) {
                try self.w.line("const json_body = try httpx.json.Json.stringifyRequest(self.allocator, body);", .{});
                try self.w.line("defer self.allocator.free(json_body);", .{});
            } else {
                try self.w.line("const json_body = if (body) |value| try httpx.json.Json.stringifyRequest(self.allocator, value) else null;", .{});
                try self.w.line("defer if (json_body) |value| self.allocator.free(value);", .{});
            }
        }

        if (params.header.len > 0) {
            try self.w.line("var request_headers = std.ArrayListUnmanaged([2][]const u8).empty;", .{});
            try self.w.line("defer request_headers.deinit(self.allocator);", .{});
            try self.w.line("if (self.auth_header) |header| try request_headers.append(self.allocator, header);", .{});
            for (params.header) |p| {
                const pname = try naming.zigFieldName(self.arena, p.name);
                if (p.required) {
                    try self.w.line("try request_headers.append(self.allocator, .{{ \"{s}\", {s} }});", .{ p.name, pname });
                } else {
                    try self.w.line("if ({s}) |value| try request_headers.append(self.allocator, .{{ \"{s}\", value }});", .{ pname, p.name });
                }
            }
        }

        // Make request
        const headers_expr = if (params.header.len > 0) "request_headers.items" else "self.authHeaders()";
        if (is_binary_request) {
            if (is_body_required) {
                try self.w.line("var resp = try self.http.{s}(url, .{{ .body = body, .headers = {s} }});", .{ http_method, headers_expr });
            } else {
                try self.w.line("var resp = if (body) |value|", .{});
                self.w.indent();
                try self.w.line("try self.http.{s}(url, .{{ .body = value, .headers = {s} }})", .{ http_method, headers_expr });
                self.w.dedent();
                try self.w.line("else", .{});
                self.w.indent();
                try self.w.line("try self.http.{s}(url, .{{ .headers = {s} }});", .{ http_method, headers_expr });
                self.w.dedent();
            }
        } else if (body_type != null) {
            if (is_body_required) {
                try self.w.line("var resp = try self.http.{s}(url, .{{ .json = json_body, .headers = {s} }});", .{ http_method, headers_expr });
            } else {
                try self.w.line("var resp = if (json_body) |value|", .{});
                self.w.indent();
                try self.w.line("try self.http.{s}(url, .{{ .json = value, .headers = {s} }})", .{ http_method, headers_expr });
                self.w.dedent();
                try self.w.line("else", .{});
                self.w.indent();
                try self.w.line("try self.http.{s}(url, .{{ .headers = {s} }});", .{ http_method, headers_expr });
                self.w.dedent();
            }
        } else {
            try self.w.line("var resp = try self.http.{s}(url, .{{ .headers = {s} }});", .{ http_method, headers_expr });
        }

        // Return response
        if (is_raw) {
            try self.w.line("defer resp.deinit();", .{});
            try self.w.line("return .{{ .status_code = resp.status.code, .body = if (resp.body) |b| (self.allocator.dupe(u8, b) catch null) else null, .content_type = if (resp.contentType()) |ct| (self.allocator.dupe(u8, ct) catch null) else null, .allocator = self.allocator }};", .{});
        } else {
            try self.w.line("return ApiResponse({s}).{s}(self.allocator, &resp);", .{
                response_type.?,
                if (is_negotiated) "fromNegotiatedResponse" else "fromResponse",
            });
        }

        self.w.dedent();
        try self.w.line("}}", .{});
    }

    /// Generate query parameter append code.
    fn generateSuccessUnion(self: *ClientGenerator, op_id: []const u8, responses: []const shared.SuccessSchema) !void {
        try self.w.line("/// Success payload selected by HTTP status, never by trial-decoding another status's schema.", .{});
        try self.w.line("pub const {s}Response = union(enum) {{", .{try naming.toTypeName(self.arena, op_id)});
        self.w.indent();
        for (responses) |response| {
            try self.w.line("status_{s}: {s},", .{ response.code, try shared.successSchemaType(self.arena, self.type_gen, op_id, response) });
        }
        try self.w.line("pub fn parseResponse(allocator: std.mem.Allocator, status: u16, body: []const u8) !std.json.Parsed(@This()) {{", .{});
        self.w.indent();
        try self.w.line("const arena = try allocator.create(std.heap.ArenaAllocator);", .{});
        try self.w.line("errdefer allocator.destroy(arena);", .{});
        try self.w.line("arena.* = std.heap.ArenaAllocator.init(allocator);", .{});
        try self.w.line("errdefer arena.deinit();", .{});
        try self.w.line("const value: @This() = switch (status) {{", .{});
        self.w.indent();
        var wildcard: ?shared.SuccessSchema = null;
        for (responses) |response| {
            if (std.mem.eql(u8, response.code, "2XX")) {
                wildcard = response;
                continue;
            }
            try self.w.line("{s} => .{{ .status_{s} = try std.json.parseFromSliceLeaky({s}, arena.allocator(), body, .{{ .allocate = .alloc_always, .ignore_unknown_fields = true }}) }},", .{ response.code, response.code, try shared.successSchemaType(self.arena, self.type_gen, op_id, response) });
        }
        if (wildcard) |response| {
            try self.w.line("else => if (status >= 200 and status < 300) .{{ .status_2XX = try std.json.parseFromSliceLeaky({s}, arena.allocator(), body, .{{ .allocate = .alloc_always, .ignore_unknown_fields = true }}) }} else return error.InvalidApiResponse,", .{try shared.successSchemaType(self.arena, self.type_gen, op_id, response)});
        } else try self.w.line("else => return error.InvalidApiResponse,", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.line("return .{{ .arena = arena, .value = value }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.blank();
    }

    fn generateQueryParamAppend(self: *ClientGenerator, query_params: []const types.Parameter) !void {
        try self.w.line("var query_buf = std.ArrayListUnmanaged(u8).empty;", .{});
        try self.w.line("defer query_buf.deinit(self.allocator);", .{});
        try self.w.line("var sep: u8 = '?';", .{});

        for (query_params) |p| {
            const field_name = try naming.zigFieldName(self.arena, p.name);
            if (p.required) {
                const encoded_name = try encodedQueryParamValueName(self.arena, field_name);
                try self.w.line("const {s} = try httpx.PercentEncoding.encode(self.allocator, params.{s});", .{ encoded_name, field_name });
                try self.w.line("defer self.allocator.free({s});", .{encoded_name});
                try self.w.line("try query_buf.appendSlice(self.allocator, &.{{sep}});", .{});
                try self.w.line("try query_buf.appendSlice(self.allocator, \"{s}=\");", .{p.name});
                try self.w.line("try query_buf.appendSlice(self.allocator, {s});", .{encoded_name});
                try self.w.line("sep = '&';", .{});
            } else {
                try self.w.line("if (params.{s}) |v| {{", .{field_name});
                self.w.indent();
                try self.w.line("const encoded_query_value = try httpx.PercentEncoding.encode(self.allocator, v);", .{});
                try self.w.line("defer self.allocator.free(encoded_query_value);", .{});
                try self.w.line("try query_buf.appendSlice(self.allocator, &.{{sep}});", .{});
                try self.w.line("try query_buf.appendSlice(self.allocator, \"{s}=\");", .{p.name});
                try self.w.line("try query_buf.appendSlice(self.allocator, encoded_query_value);", .{});
                try self.w.line("sep = '&';", .{});
                self.w.dedent();
                try self.w.line("}}", .{});
            }
        }

        try self.w.line("if (query_buf.items.len > 0) {{", .{});
        self.w.indent();
        try self.w.line("const new_url = try std.fmt.allocPrint(self.allocator, \"{{s}}{{s}}\", .{{ url, query_buf.items }});", .{});
        try self.w.line("self.allocator.free(url);", .{});
        try self.w.line("url = new_url;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
    }

    fn generateUrlConstruction(self: *ClientGenerator, path: []const u8, path_params: []const types.Parameter, has_query: bool) !void {
        const decl = if (has_query) "var" else "const";
        if (path_params.len == 0) {
            try self.w.line("{s} url = try std.fmt.allocPrint(self.allocator, \"{{s}}{s}\", .{{self.base_url}});", .{ decl, path });
            try self.w.line("defer self.allocator.free(url);", .{});
            return;
        }

        // Build format string: replace {param} with {s}
        var fmt_str = std.ArrayListUnmanaged(u8).empty;
        var param_args = std.ArrayListUnmanaged(u8).empty;

        for (path_params) |p| {
            const pname = try naming.zigFieldName(self.arena, p.name);
            const encoded_name = try encodedPathParamName(self.arena, p.name);
            try self.w.line("const {s} = try httpx.PercentEncoding.encode(self.allocator, {s});", .{ encoded_name, pname });
            try self.w.line("defer self.allocator.free({s});", .{encoded_name});
        }

        try fmt_str.appendSlice(self.arena, "{s}");
        try param_args.appendSlice(self.arena, "self.base_url");

        var it = shared.iterPathTemplate(path);
        while (it.next()) |seg| {
            switch (seg) {
                .literal => |c| try fmt_str.append(self.arena, c),
                .param => |name| {
                    try fmt_str.appendSlice(self.arena, "{s}");
                    const encoded_name = try encodedPathParamName(self.arena, name);
                    try param_args.print(self.arena, ", {s}", .{encoded_name});
                },
            }
        }

        try self.w.line("{s} url = try std.fmt.allocPrint(self.allocator, \"{s}\", .{{{s}}});", .{
            decl,
            fmt_str.items,
            param_args.items,
        });
        try self.w.line("defer self.allocator.free(url);", .{});
    }

    fn generateAuthHeaders(self: *ClientGenerator) !void {
        try self.w.line("fn authHeaders(self: *const @This()) ?[]const [2][]const u8 {{", .{});
        self.w.indent();
        try self.w.line("if (self.auth_header) |*h| return @as(*const [1][2][]const u8, h);", .{});
        try self.w.line("return null;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();

        // Free the allocated bearer token string if present.
        try self.w.line("fn freeAuth(self: *@This()) void {{", .{});
        self.w.indent();
        try self.w.line("if (self.auth_header) |h| {{", .{});
        self.w.indent();
        // h[0] is "Authorization" (static), h[1] is the allocPrint'd "Bearer ..." string
        try self.w.line("self.allocator.free(h[1]);", .{});
        try self.w.line("self.auth_header = null;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
    }
};

/// The generated source of one method, from its signature to the next one.
fn methodBody(generated: []const u8, signature: []const u8) []const u8 {
    const start = std.mem.indexOf(u8, generated, signature) orelse return "";
    const next = std.mem.indexOfPos(u8, generated, start + signature.len, "pub fn ") orelse generated.len;
    return generated[start..next];
}

fn encodedPathParamName(allocator: Allocator, name: []const u8) ![]u8 {
    const prefixed = try std.fmt.allocPrint(allocator, "encoded_{s}", .{name});
    return naming.zigFieldName(allocator, prefixed);
}

fn encodedQueryParamValueName(allocator: Allocator, field_name: []const u8) ![]u8 {
    const prefixed = try std.fmt.allocPrint(allocator, "encoded_query_value_{s}", .{field_name});
    return naming.zigFieldName(allocator, prefixed);
}

test "client generator smoke" {
    // Just verify the module compiles
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var type_gen = TypeGenerator.init(arena, &w, &resolver);
    _ = ClientGenerator.init(arena, &w, &resolver, &type_gen);
}

test "client generator preserves optional request body semantics" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var optional_content = std.StringArrayHashMapUnmanaged(types.MediaType){};
    try optional_content.put(arena, "application/json", .{
        .schema = .{ .schema = .{ .schema_type = .{ .single = "string" } } },
    });
    var required_content = std.StringArrayHashMapUnmanaged(types.MediaType){};
    try required_content.put(arena, "application/json", .{
        .schema = .{ .schema = .{ .schema_type = .{ .single = "string" } } },
    });
    var optional_binary_content = std.StringArrayHashMapUnmanaged(types.MediaType){};
    try optional_binary_content.put(arena, "application/octet-stream", .{});

    var doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
    };
    try doc.paths.put(arena, "/optional", .{
        .post = .{
            .operation_id = "optionalBody",
            .request_body = .{ .request_body = .{
                .required = false,
                .content = optional_content,
            } },
        },
    });
    try doc.paths.put(arena, "/required", .{
        .post = .{
            .operation_id = "requiredBody",
            .request_body = .{ .request_body = .{
                .required = true,
                .content = required_content,
            } },
        },
    });
    try doc.paths.put(arena, "/optional-binary", .{
        .post = .{
            .operation_id = "optionalBinaryBody",
            .request_body = .{ .request_body = .{
                .required = false,
                .content = optional_binary_content,
            } },
        },
    });

    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var type_gen = TypeGenerator.init(arena, &w, &resolver);
    var generator = ClientGenerator.init(arena, &w, &resolver, &type_gen);
    try generator.generate(&doc);

    const generated = w.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, generated, "pub fn optionalBody(self: *@This(), body: ?[]const u8)") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "const json_body = if (body) |value| try httpx.json.Json.stringifyRequest(self.allocator, value) else null;") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "pub fn requiredBody(self: *@This(), body: []const u8)") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "const json_body = try httpx.json.Json.stringifyRequest(self.allocator, body);") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "pub fn optionalBinaryBody(self: *@This(), body: ?[]const u8)") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, ".body = value") != null);
}

test "client generator keeps json typed and bytes available for a negotiated response" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    // A response the caller negotiates: JSON by default, a packed frame when
    // the request asks for one. Both forms have to reach the caller, so the
    // operation keeps its typed JSON and gains the bytes.
    var negotiated_content = std.StringArrayHashMapUnmanaged(types.MediaType){};
    try negotiated_content.put(arena, "application/json", .{
        .schema = .{ .schema = .{ .schema_type = .{ .single = "string" } } },
    });
    try negotiated_content.put(arena, "application/vnd.antfly.numeric.v1", .{
        .schema = .{ .schema = .{ .schema_type = .{ .single = "string" } } },
    });
    var negotiated_responses = std.StringArrayHashMapUnmanaged(types.ResponseOrRef){};
    try negotiated_responses.put(arena, "200", .{ .response = .{
        .description = "Values",
        .content = negotiated_content,
    } });

    var json_content = std.StringArrayHashMapUnmanaged(types.MediaType){};
    try json_content.put(arena, "application/json", .{
        .schema = .{ .schema = .{ .schema_type = .{ .single = "string" } } },
    });
    var json_responses = std.StringArrayHashMapUnmanaged(types.ResponseOrRef){};
    try json_responses.put(arena, "200", .{ .response = .{
        .description = "Values",
        .content = json_content,
    } });

    // A streaming response has no JSON form to keep, so it stays raw.
    var stream_content = std.StringArrayHashMapUnmanaged(types.MediaType){};
    try stream_content.put(arena, "text/event-stream", .{});
    var stream_responses = std.StringArrayHashMapUnmanaged(types.ResponseOrRef){};
    try stream_responses.put(arena, "200", .{ .response = .{
        .description = "Events",
        .content = stream_content,
    } });

    var doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
    };
    try doc.paths.put(arena, "/negotiated", .{
        .post = .{ .operation_id = "negotiatedValues", .responses = negotiated_responses },
    });
    try doc.paths.put(arena, "/json", .{
        .post = .{ .operation_id = "jsonValues", .responses = json_responses },
    });
    try doc.paths.put(arena, "/stream", .{
        .post = .{ .operation_id = "streamValues", .responses = stream_responses },
    });

    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var type_gen = TypeGenerator.init(arena, &w, &resolver);
    var generator = ClientGenerator.init(arena, &w, &resolver, &type_gen);
    try generator.generate(&doc);

    const generated = w.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, generated, "bytes: ?[]const u8 = null,") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "pub fn fromNegotiatedResponse(") != null);
    // The negotiated operation uses it; a JSON-only one is untouched.
    const negotiated_body = methodBody(generated, "pub fn negotiatedValues(");
    const json_body = methodBody(generated, "pub fn jsonValues(");
    const stream_body = methodBody(generated, "pub fn streamValues(");
    try std.testing.expect(std.mem.indexOf(u8, negotiated_body, "fromNegotiatedResponse(self.allocator") != null);
    // A JSON-only operation is untouched: same call as before this existed.
    try std.testing.expect(std.mem.indexOf(u8, json_body, "fromResponse(self.allocator") != null);
    try std.testing.expect(std.mem.indexOf(u8, json_body, "fromNegotiatedResponse") == null);
    // Streaming stays raw: there is no JSON form to keep.
    try std.testing.expect(std.mem.indexOf(u8, stream_body, "!RawResponse") != null);
}

test "client generator percent-encodes path parameters" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var path_params = [_]types.ParameterOrRef{
        .{ .parameter = .{ .name = "tableName", .in = .path, .required = true } },
        .{ .parameter = .{ .name = "key", .in = .path, .required = true } },
        .{ .parameter = .{ .name = "artifactName", .in = .path, .required = true } },
    };
    var doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
    };
    try doc.paths.put(arena, "/db/v1/tables/{tableName}/documents/{key}/artifacts/{artifactName}", .{
        .get = .{
            .operation_id = "getArtifact",
            .parameters = &path_params,
        },
    });

    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var type_gen = TypeGenerator.init(arena, &w, &resolver);
    var generator = ClientGenerator.init(arena, &w, &resolver, &type_gen);
    try generator.generate(&doc);

    const generated = w.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, generated, "const encoded_table_name = try httpx.PercentEncoding.encode(self.allocator, table_name);") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "const encoded_key = try httpx.PercentEncoding.encode(self.allocator, key);") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "const encoded_artifact_name = try httpx.PercentEncoding.encode(self.allocator, artifact_name);") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "self.base_url, encoded_table_name, encoded_key, encoded_artifact_name") != null);
}

test "client generator percent-encodes query parameters" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var params = [_]types.ParameterOrRef{
        .{ .parameter = .{ .name = "tableName", .in = .path, .required = true } },
        .{ .parameter = .{ .name = "resource", .in = .query, .required = true } },
        .{ .parameter = .{ .name = "index", .in = .query, .required = false } },
        .{ .parameter = .{ .name = "cursor", .in = .query, .required = false } },
    };
    var doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
    };
    try doc.paths.put(arena, "/db/v1/tables/{tableName}/repair/issues", .{
        .get = .{
            .operation_id = "listRepairIssues",
            .parameters = &params,
        },
    });

    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var type_gen = TypeGenerator.init(arena, &w, &resolver);
    var generator = ClientGenerator.init(arena, &w, &resolver, &type_gen);
    try generator.generate(&doc);

    const generated = w.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, generated, "const encoded_query_value_resource = try httpx.PercentEncoding.encode(self.allocator, params.resource);") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "try query_buf.appendSlice(self.allocator, encoded_query_value_resource);") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "const encoded_query_value = try httpx.PercentEncoding.encode(self.allocator, v);") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "try query_buf.appendSlice(self.allocator, encoded_query_value);") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "resource=") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "index=") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "cursor=") != null);
}
