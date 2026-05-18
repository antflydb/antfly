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
const routes = @import("http_routes.zig");
const http_common = @import("../raft/transport/http_common.zig");
const mcp = @import("antfly_mcp");
const a2a = @import("antfly_a2a");

const McpToolKind = enum {
    create_table,
    drop_table,
    list_tables,
    create_index,
    drop_index,
    list_indexes,
    query,
    backup,
    restore,
    batch,
};

const McpToolSpec = struct {
    kind: McpToolKind,
    name: []const u8,
    description: []const u8,
    fields: []const McpToolFieldSpec = &.{},
};

const McpToolFieldType = enum {
    string,
    integer,
    array,
    object,
};

const McpToolFieldSpec = struct {
    name: []const u8,
    schema_type: McpToolFieldType,
    required: bool = false,
    description: ?[]const u8 = null,
    default_json: ?[]const u8 = null,
    items_json: ?[]const u8 = null,
};

const mcp_tool_specs = [_]McpToolSpec{
    .{
        .kind = .create_table,
        .name = "create_table",
        .description = "Create an Antfly table",
        .fields = &.{
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "numShards", .schema_type = .integer, .default_json = "3" },
            .{ .name = "key", .schema_type = .string },
            .{ .name = "fields", .schema_type = .string, .description = "JSON object defining field types" },
        },
    },
    .{
        .kind = .drop_table,
        .name = "drop_table",
        .description = "Drop an Antfly table",
        .fields = &.{.{ .name = "tableName", .schema_type = .string, .required = true }},
    },
    .{ .kind = .list_tables, .name = "list_tables", .description = "List Antfly tables" },
    .{
        .kind = .create_index,
        .name = "create_index",
        .description = "Create an Antfly index",
        .fields = &.{
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "indexName", .schema_type = .string, .required = true },
            .{ .name = "field", .schema_type = .string },
            .{ .name = "template", .schema_type = .string },
            .{ .name = "dimension", .schema_type = .integer },
            .{ .name = "embedder", .schema_type = .string },
            .{ .name = "summarizer", .schema_type = .string },
        },
    },
    .{
        .kind = .drop_index,
        .name = "drop_index",
        .description = "Drop an Antfly index",
        .fields = &.{
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "indexName", .schema_type = .string, .required = true },
        },
    },
    .{
        .kind = .list_indexes,
        .name = "list_indexes",
        .description = "List indexes for an Antfly table",
        .fields = &.{.{ .name = "tableName", .schema_type = .string, .required = true }},
    },
    .{
        .kind = .query,
        .name = "query",
        .description = "Run an Antfly table query",
        .fields = &.{
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "fullTextSearch", .schema_type = .string },
            .{ .name = "semanticSearch", .schema_type = .string },
            .{ .name = "fields", .schema_type = .array, .items_json = "{\"type\":\"string\"}" },
            .{ .name = "limit", .schema_type = .integer, .default_json = "10" },
            .{ .name = "orderBy", .schema_type = .array },
            .{ .name = "indexes", .schema_type = .array, .items_json = "{\"type\":\"string\"}" },
            .{ .name = "filterPrefix", .schema_type = .string },
        },
    },
    .{
        .kind = .backup,
        .name = "backup",
        .description = "Backup an Antfly table",
        .fields = &.{
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "backupId", .schema_type = .string, .required = true },
            .{ .name = "location", .schema_type = .string, .required = true },
        },
    },
    .{
        .kind = .restore,
        .name = "restore",
        .description = "Restore an Antfly table",
        .fields = &.{
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "backupId", .schema_type = .string, .required = true },
            .{ .name = "location", .schema_type = .string, .required = true },
        },
    },
    .{
        .kind = .batch,
        .name = "batch",
        .description = "Insert and delete documents in an Antfly table",
        .fields = &.{
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "writes", .schema_type = .object },
            .{ .name = "deletes", .schema_type = .array, .items_json = "{\"type\":\"string\"}" },
        },
    },
};

pub fn handleMcpRequest(server_ptr: anytype, req: http_common.HttpRequest) !http_common.HttpResponse {
    const Server = @TypeOf(server_ptr);
    const ToolContext = struct {
        server: Server,
        authorization: ?[]const u8,
        kind: McpToolKind,

        fn handler(ctx: *@This()) mcp.ToolHandler {
            return .{ .ptr = ctx, .call_fn = call };
        }

        fn call(ptr: *anyopaque, alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            return switch (ctx.kind) {
                .create_table => try ctx.createTable(alloc, args),
                .drop_table => try ctx.tableRoute(alloc, args, .DELETE, "tableName", null, ""),
                .list_tables => try ctx.simpleRoute(alloc, .GET, routes.Routes.tables, ""),
                .create_index => try ctx.createIndex(alloc, args),
                .drop_index => try ctx.indexRoute(alloc, args, .DELETE, ""),
                .list_indexes => try ctx.listIndexes(alloc, args),
                .query => try ctx.query(alloc, args),
                .backup => try ctx.backupRestore(alloc, args, "backup"),
                .restore => try ctx.backupRestore(alloc, args, "restore"),
                .batch => try ctx.batch(alloc, args),
            };
        }

        fn createTable(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const table_name = jsonStringArg(args, "tableName") orelse return mcpError(alloc, "missing tableName");
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "num_shards", .{ .integer = jsonIntArg(args, "numShards") orelse 3 });
            if (jsonStringArg(args, "fields")) |fields_json| {
                if (fields_json.len != 0) {
                    const fields = std.json.parseFromSliceLeaky(std.json.Value, alloc, fields_json, .{}) catch return mcpError(alloc, "invalid fields JSON");
                    try body.put(alloc, "schema", fields);
                }
            }
            if (jsonStringArg(args, "key")) |key| {
                if (key.len != 0) try body.put(alloc, "key", .{ .string = key });
            }
            const body_json = try stringifyJsonValue(alloc, .{ .object = body });
            const uri = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ routes.Routes.tables, table_name });
            return try ctx.simpleRoute(alloc, .POST, uri, body_json);
        }

        fn createIndex(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const table_name = jsonStringArg(args, "tableName") orelse return mcpError(alloc, "missing tableName");
            const index_name = jsonStringArg(args, "indexName") orelse return mcpError(alloc, "missing indexName");
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "name", .{ .string = index_name });
            try body.put(alloc, "type", .{ .string = "embeddings" });
            if (jsonIntArg(args, "dimension")) |dimension| try body.put(alloc, "dimension", .{ .integer = dimension });
            if (jsonStringArg(args, "field")) |field| if (field.len != 0) try body.put(alloc, "field", .{ .string = field });
            if (jsonStringArg(args, "template")) |template| if (template.len != 0) try body.put(alloc, "template", .{ .string = template });
            if (jsonStringArg(args, "embedder")) |embedder_json| {
                if (embedder_json.len != 0) try body.put(alloc, "embedder", std.json.parseFromSliceLeaky(std.json.Value, alloc, embedder_json, .{}) catch return mcpError(alloc, "invalid embedder JSON"));
            }
            if (jsonStringArg(args, "summarizer")) |summarizer_json| {
                if (summarizer_json.len != 0) try body.put(alloc, "summarizer", std.json.parseFromSliceLeaky(std.json.Value, alloc, summarizer_json, .{}) catch return mcpError(alloc, "invalid summarizer JSON"));
            }
            const uri = try std.fmt.allocPrint(alloc, "{s}/{s}/indexes/{s}", .{ routes.Routes.tables, table_name, index_name });
            return try ctx.simpleRoute(alloc, .POST, uri, try stringifyJsonValue(alloc, .{ .object = body }));
        }

        fn listIndexes(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const table_name = jsonStringArg(args, "tableName") orelse return mcpError(alloc, "missing tableName");
            const uri = try std.fmt.allocPrint(alloc, "{s}/{s}/indexes", .{ routes.Routes.tables, table_name });
            return try ctx.simpleRoute(alloc, .GET, uri, "");
        }

        fn indexRoute(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value, method: http_common.Method, body: []const u8) !mcp.CallToolResult {
            const table_name = jsonStringArg(args, "tableName") orelse return mcpError(alloc, "missing tableName");
            const index_name = jsonStringArg(args, "indexName") orelse return mcpError(alloc, "missing indexName");
            const uri = try std.fmt.allocPrint(alloc, "{s}/{s}/indexes/{s}", .{ routes.Routes.tables, table_name, index_name });
            return try ctx.simpleRoute(alloc, method, uri, body);
        }

        fn query(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const table_name = jsonStringArg(args, "tableName") orelse return mcpError(alloc, "missing tableName");
            var body = std.json.ObjectMap.empty;
            if (jsonStringArg(args, "fullTextSearch")) |full_text| {
                if (full_text.len != 0) {
                    var full_text_obj = std.json.ObjectMap.empty;
                    try full_text_obj.put(alloc, "query", .{ .string = full_text });
                    try body.put(alloc, "full_text_search", .{ .object = full_text_obj });
                }
            }
            if (jsonStringArg(args, "semanticSearch")) |semantic| if (semantic.len != 0) try body.put(alloc, "semantic_search", .{ .string = semantic });
            if (jsonValueArg(args, "fields")) |fields| try body.put(alloc, "fields", fields);
            if (jsonValueArg(args, "orderBy")) |order_by| try body.put(alloc, "order_by", order_by);
            if (jsonValueArg(args, "indexes")) |indexes| try body.put(alloc, "indexes", indexes);
            if (jsonStringArg(args, "filterPrefix")) |prefix| if (prefix.len != 0) try body.put(alloc, "filter_prefix", .{ .string = prefix });
            try body.put(alloc, "limit", .{ .integer = jsonIntArg(args, "limit") orelse 10 });
            const uri = try std.fmt.allocPrint(alloc, "{s}/{s}/query", .{ routes.Routes.tables, table_name });
            return try ctx.simpleRoute(alloc, .POST, uri, try stringifyJsonValue(alloc, .{ .object = body }));
        }

        fn backupRestore(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value, operation: []const u8) !mcp.CallToolResult {
            const table_name = jsonStringArg(args, "tableName") orelse return mcpError(alloc, "missing tableName");
            const backup_id = jsonStringArg(args, "backupId") orelse return mcpError(alloc, "missing backupId");
            const location = jsonStringArg(args, "location") orelse return mcpError(alloc, "missing location");
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "backup_id", .{ .string = backup_id });
            try body.put(alloc, "location", .{ .string = location });
            const uri = try std.fmt.allocPrint(alloc, "{s}/{s}/{s}", .{ routes.Routes.tables, table_name, operation });
            return try ctx.simpleRoute(alloc, .POST, uri, try stringifyJsonValue(alloc, .{ .object = body }));
        }

        fn batch(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const table_name = jsonStringArg(args, "tableName") orelse return mcpError(alloc, "missing tableName");
            var body = std.json.ObjectMap.empty;
            if (jsonValueArg(args, "writes")) |writes| try body.put(alloc, "inserts", writes);
            if (jsonValueArg(args, "deletes")) |deletes| try body.put(alloc, "deletes", deletes);
            const uri = try std.fmt.allocPrint(alloc, "{s}/{s}/batch", .{ routes.Routes.tables, table_name });
            return try ctx.simpleRoute(alloc, .POST, uri, try stringifyJsonValue(alloc, .{ .object = body }));
        }

        fn tableRoute(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value, method: http_common.Method, table_arg: []const u8, suffix: ?[]const u8, body: []const u8) !mcp.CallToolResult {
            const table_name = jsonStringArg(args, table_arg) orelse return mcpError(alloc, "missing tableName");
            const uri = if (suffix) |route_suffix|
                try std.fmt.allocPrint(alloc, "{s}/{s}/{s}", .{ routes.Routes.tables, table_name, route_suffix })
            else
                try std.fmt.allocPrint(alloc, "{s}/{s}", .{ routes.Routes.tables, table_name });
            return try ctx.simpleRoute(alloc, method, uri, body);
        }

        fn simpleRoute(ctx: *@This(), alloc: std.mem.Allocator, method: http_common.Method, uri: []const u8, body: []const u8) !mcp.CallToolResult {
            var resp = try ctx.server.handle(.{
                .method = method,
                .uri = uri,
                .authorization = ctx.authorization,
                .content_type = if (body.len == 0) null else "application/json",
                .body = body,
            });
            defer resp.deinit(ctx.server.alloc);
            return try mcpResultFromHttpResponse(alloc, resp);
        }
    };

    var contexts: [mcp_tool_specs.len]ToolContext = undefined;
    for (&contexts, mcp_tool_specs) |*ctx, spec| {
        ctx.* = .{ .server = server_ptr, .authorization = req.authorization, .kind = spec.kind };
    }

    var input_schemas: [mcp_tool_specs.len][]u8 = undefined;
    var input_schema_count: usize = 0;
    defer {
        for (input_schemas[0..input_schema_count]) |schema| server_ptr.alloc.free(schema);
    }
    for (mcp_tool_specs, 0..) |spec, i| {
        input_schemas[i] = try buildMcpInputSchema(server_ptr.alloc, spec);
        input_schema_count += 1;
    }

    var protocol_server = mcp.Server{
        .implementation = .{ .name = "antfly", .version = "1.0.0" },
        .session_store = server_ptr.mcp_sessions.iface(),
    };
    defer protocol_server.deinit(server_ptr.alloc);
    for (&contexts, mcp_tool_specs, 0..) |*ctx, spec, i| {
        try protocol_server.addTool(server_ptr.alloc, .{
            .name = spec.name,
            .description = spec.description,
            .input_schema_json = input_schemas[i],
            .handler = ctx.handler(),
        });
    }

    const is_initialize = req.method == .POST and isJsonRpcMethod(server_ptr.alloc, req.body, "initialize");
    if (!is_initialize) {
        if (try validateMcpSession(server_ptr, req)) |err_resp| return err_resp;
    }

    var transport = switch (req.method) {
        .GET => try protocol_server.handleStreamableHttpGetWithSession(
            server_ptr.alloc,
            routes.Routes.mcp_v1,
            req.header(mcp.session_id_header),
            req.header(mcp.last_event_id_header),
        ),
        .POST => try protocol_server.handleStreamableHttpPost(server_ptr.alloc, req.body),
        .DELETE => try protocol_server.handleStreamableHttpDelete(server_ptr.alloc, req.header(mcp.session_id_header)),
        else => return try textResponse(server_ptr.alloc, 405, "method not allowed"),
    };
    defer transport.deinit(server_ptr.alloc);
    return try mcpBodyResponseWithStatus(server_ptr.alloc, transport);
}

fn validateMcpSession(server_ptr: anytype, req: http_common.HttpRequest) !?http_common.HttpResponse {
    const session_id = req.header(mcp.session_id_header) orelse return try textResponse(server_ptr.alloc, 400, "missing MCP session");
    if (!server_ptr.mcp_sessions.iface().exists(session_id)) return try textResponse(server_ptr.alloc, 404, "unknown MCP session");
    return null;
}

fn buildMcpInputSchema(alloc: std.mem.Allocator, spec: McpToolSpec) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"type\":\"object\"");

    var required_count: usize = 0;
    for (spec.fields) |field| {
        if (field.required) required_count += 1;
    }
    if (required_count > 0) {
        try out.appendSlice(alloc, ",\"required\":[");
        var emitted: usize = 0;
        for (spec.fields) |field| {
            if (!field.required) continue;
            if (emitted != 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, field.name);
            emitted += 1;
        }
        try out.append(alloc, ']');
    }

    try out.appendSlice(alloc, ",\"properties\":{");
    for (spec.fields, 0..) |field, i| {
        if (i != 0) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, field.name);
        try out.appendSlice(alloc, ":{\"type\":");
        try appendJsonString(alloc, &out, @tagName(field.schema_type));
        if (field.items_json) |items_json| {
            try out.appendSlice(alloc, ",\"items\":");
            try out.appendSlice(alloc, items_json);
        }
        if (field.description) |description| {
            try out.appendSlice(alloc, ",\"description\":");
            try appendJsonString(alloc, &out, description);
        }
        if (field.default_json) |default_json| {
            try out.appendSlice(alloc, ",\"default\":");
            try out.appendSlice(alloc, default_json);
        }
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "}}");
    return try out.toOwnedSlice(alloc);
}

pub fn handleA2aRequest(server_ptr: anytype, req: http_common.HttpRequest) !http_common.HttpResponse {
    var arena_impl = std.heap.ArenaAllocator.init(server_ptr.alloc);
    defer arena_impl.deinit();
    var dispatcher = try buildA2aDispatcher(server_ptr, arena_impl.allocator(), req.authorization);
    if (isJsonRpcMethod(arena_impl.allocator(), req.body, "message/stream")) {
        var sink = A2aSseSink{};
        defer sink.out.deinit(server_ptr.alloc);
        try dispatcher.handleJsonRpcStream(server_ptr.alloc, req.body, sink.iface());
        try sink.out.appendSlice(server_ptr.alloc, "event: done\ndata: {}\n\n");
        return try eventStreamResponse(server_ptr.alloc, 200, sink.out.items);
    }
    const response_body = try dispatcher.handleJsonRpc(server_ptr.alloc, req.body);
    defer server_ptr.alloc.free(response_body);
    return try jsonBodyResponseWithStatus(server_ptr.alloc, 200, response_body);
}

pub fn isA2aStreamingRequest(alloc: std.mem.Allocator, req: http_common.HttpRequest) bool {
    return req.method == .POST and isJsonRpcMethod(alloc, req.body, "message/stream");
}

pub fn handleA2aStreamingRequest(server_ptr: anytype, req: http_common.HttpRequest, writer: http_common.StreamWriter) !bool {
    var arena_impl = std.heap.ArenaAllocator.init(server_ptr.alloc);
    defer arena_impl.deinit();
    if (!isJsonRpcMethod(arena_impl.allocator(), req.body, "message/stream")) return false;

    try writer.start(server_ptr.alloc, .{
        .status = 200,
        .content_type = "text/event-stream",
    });

    var dispatcher = try buildA2aDispatcher(server_ptr, arena_impl.allocator(), req.authorization);
    var sink = A2aLiveSseSink{ .writer = writer };
    try dispatcher.handleJsonRpcStream(server_ptr.alloc, req.body, sink.iface());
    try writer.writeAll("event: done\ndata: {}\n\n");
    try writer.flush();
    return true;
}

const A2aSseSink = struct {
    out: std.ArrayListUnmanaged(u8) = .empty,

    fn iface(self: *@This()) a2a.StreamSink {
        return .{ .ptr = self, .emit_fn = emit };
    }

    fn emit(ptr: *anyopaque, alloc: std.mem.Allocator, event: std.json.Value) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try appendSseJsonEvent(alloc, &self.out, "message", event);
    }
};

const A2aLiveSseSink = struct {
    writer: http_common.StreamWriter,

    fn iface(self: *@This()) a2a.StreamSink {
        return .{ .ptr = self, .emit_fn = emit };
    }

    fn emit(ptr: *anyopaque, alloc: std.mem.Allocator, event: std.json.Value) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try writeSseJsonEvent(alloc, self.writer, "message", event);
        try self.writer.flush();
    }
};

pub fn handleA2aCard(server_ptr: anytype) !http_common.HttpResponse {
    var arena_impl = std.heap.ArenaAllocator.init(server_ptr.alloc);
    defer arena_impl.deinit();
    var dispatcher = try buildA2aDispatcher(server_ptr, arena_impl.allocator(), null);
    const card = try dispatcher.agentCard(arena_impl.allocator());
    const body = try stringifyJsonValue(server_ptr.alloc, card);
    defer server_ptr.alloc.free(body);
    return try jsonBodyResponseWithStatus(server_ptr.alloc, 200, body);
}

fn buildA2aDispatcher(server_ptr: anytype, dispatcher_alloc: std.mem.Allocator, authorization: ?[]const u8) !a2a.Dispatcher {
    const Server = @TypeOf(server_ptr);
    const HandlerKind = enum { query_builder, retrieval };
    const HandlerContext = struct {
        server: Server,
        authorization: ?[]const u8,
        kind: HandlerKind,

        fn iface(ctx: *@This()) a2a.AgentHandler {
            return .{
                .ptr = ctx,
                .skill_id_fn = skillId,
                .skill_fn = skill,
                .execute_fn = @This().execute,
            };
        }

        fn skillId(ptr: *anyopaque) []const u8 {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            return switch (ctx.kind) {
                .query_builder => "query-builder",
                .retrieval => "retrieval",
            };
        }

        fn skill(ptr: *anyopaque, _: std.mem.Allocator) !a2a.Skill {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            return switch (ctx.kind) {
                .query_builder => .{ .id = "query-builder", .name = "Query Builder", .description = "Translate natural language into Antfly query requests", .tags = &.{ "antfly", "query" } },
                .retrieval => .{ .id = "retrieval", .name = "Retrieval", .description = "Run Antfly retrieval and generation workflows", .tags = &.{ "antfly", "retrieval" } },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, request_ctx: a2a.RequestContext, queue: *a2a.EventQueue) !void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            return switch (ctx.kind) {
                .query_builder => try ctx.executeQueryBuilder(alloc, request_ctx, queue),
                .retrieval => try ctx.executeRetrieval(alloc, request_ctx, queue),
            };
        }

        fn executeQueryBuilder(ctx: *@This(), alloc: std.mem.Allocator, request_ctx: a2a.RequestContext, queue: *a2a.EventQueue) !void {
            const text = try a2a.messageText(alloc, request_ctx.message);
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "intent", .{ .string = text });
            if (a2a.firstDataPart(request_ctx.message)) |data| {
                if (data == .object) {
                    if (jsonStringObjectField(data.object, "table")) |table| try body.put(alloc, "table", .{ .string = table });
                    if (data.object.get("context")) |context| try body.put(alloc, "context", context);
                }
            }
            const body_json = try stringifyJsonValue(alloc, .{ .object = body });
            var resp = try ctx.server.handle(.{
                .method = .POST,
                .uri = routes.Routes.agents_query_builder,
                .authorization = ctx.authorization,
                .content_type = "application/json",
                .body = body_json,
            });
            defer resp.deinit(ctx.server.alloc);
            if (resp.status < 200 or resp.status >= 300) {
                try queue.status(alloc, request_ctx.task_id, request_ctx.context_id, "failed", resp.body);
                return;
            }
            const parsed: std.json.Value = std.json.parseFromSliceLeaky(std.json.Value, alloc, resp.body, .{}) catch .{ .string = resp.body };
            try queue.artifact(alloc, request_ctx.task_id, request_ctx.context_id, "query", try a2a.dataPart(alloc, parsed));
            try queue.status(alloc, request_ctx.task_id, request_ctx.context_id, "completed", "query built");
        }

        fn executeRetrieval(ctx: *@This(), alloc: std.mem.Allocator, request_ctx: a2a.RequestContext, queue: *a2a.EventQueue) !void {
            const text = try a2a.messageText(alloc, request_ctx.message);
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "query", .{ .string = text });
            try body.put(alloc, "stream", .{ .bool = false });
            if (a2a.firstDataPart(request_ctx.message)) |data| {
                if (data == .object) {
                    if (data.object.get("queries")) |queries| {
                        try body.put(alloc, "queries", queries);
                    } else if (jsonStringObjectField(data.object, "table")) |table| {
                        var query_obj = std.json.ObjectMap.empty;
                        try query_obj.put(alloc, "table", .{ .string = table });
                        var full_text = std.json.ObjectMap.empty;
                        try full_text.put(alloc, "query", .{ .string = text });
                        try query_obj.put(alloc, "full_text_search", .{ .object = full_text });
                        try query_obj.put(alloc, "limit", .{ .integer = jsonIntObjectField(data.object, "limit") orelse 5 });
                        var queries = std.json.Array.init(alloc);
                        try queries.append(.{ .object = query_obj });
                        try body.put(alloc, "queries", .{ .array = queries });
                    }
                    if (data.object.get("steps")) |steps| try body.put(alloc, "steps", steps);
                    if (jsonIntObjectField(data.object, "max_internal_iterations")) |max_iterations| try body.put(alloc, "max_internal_iterations", .{ .integer = max_iterations });
                }
            }
            const body_json = try stringifyJsonValue(alloc, .{ .object = body });
            try ctx.server.executeA2aRetrieval(alloc, body_json, request_ctx.task_id, request_ctx.context_id, queue);
        }
    };

    var dispatcher = a2a.Dispatcher{
        .name = "Antfly",
        .version = "1.0.0",
        .base_url = routes.Routes.a2a,
        .task_store = server_ptr.a2a_tasks.iface(),
    };
    const contexts = try dispatcher_alloc.alloc(HandlerContext, 2);
    contexts[0] = .{ .server = server_ptr, .authorization = authorization, .kind = .query_builder };
    contexts[1] = .{ .server = server_ptr, .authorization = authorization, .kind = .retrieval };
    try dispatcher.addHandler(dispatcher_alloc, contexts[0].iface());
    try dispatcher.addHandler(dispatcher_alloc, contexts[1].iface());
    return dispatcher;
}

fn jsonBodyResponseWithStatus(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return try bodyResponseWithStatus(alloc, status, "application/json", body);
}

fn bodyResponseWithStatus(alloc: std.mem.Allocator, status: u16, content_type: []const u8, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, content_type),
        .body = try alloc.dupe(u8, body),
    };
}

fn mcpBodyResponseWithStatus(alloc: std.mem.Allocator, result: mcp.HttpResult) !http_common.HttpResponse {
    var headers = try alloc.alloc(http_common.Header, result.headers.len);
    var initialized: usize = 0;
    errdefer {
        for (headers[0..initialized]) |*header| header.deinit(alloc);
        alloc.free(headers);
    }
    for (headers, result.headers) |*out, header| {
        const name = try alloc.dupe(u8, header.name);
        const value = alloc.dupe(u8, header.value) catch |err| {
            alloc.free(name);
            return err;
        };
        out.* = .{
            .name = name,
            .value = value,
        };
        initialized += 1;
    }
    const content_type = try alloc.dupe(u8, result.content_type);
    errdefer alloc.free(content_type);
    const body = try alloc.dupe(u8, result.body);
    return .{
        .status = result.status,
        .content_type = content_type,
        .headers = headers,
        .body = body,
    };
}

fn textResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/plain"),
        .body = try alloc.dupe(u8, body),
    };
}

fn eventStreamResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/event-stream"),
        .body = try alloc.dupe(u8, body),
    };
}

fn isJsonRpcMethod(alloc: std.mem.Allocator, body: []const u8, method_name: []const u8) bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return false;
    defer parsed.deinit();
    const request = parsed.value;
    if (request != .object) return false;
    const method = jsonStringObjectField(request.object, "method") orelse return false;
    return std.mem.eql(u8, method, method_name);
}

fn appendSseJsonEvent(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), event_name: []const u8, value: std.json.Value) !void {
    const body = try stringifyJsonValue(alloc, value);
    defer alloc.free(body);
    try out.appendSlice(alloc, "event: ");
    try out.appendSlice(alloc, event_name);
    try out.appendSlice(alloc, "\n");
    try out.appendSlice(alloc, "data: ");
    try out.appendSlice(alloc, body);
    try out.appendSlice(alloc, "\n\n");
}

fn writeSseJsonEvent(alloc: std.mem.Allocator, writer: http_common.StreamWriter, event_name: []const u8, value: std.json.Value) !void {
    const body = try stringifyJsonValue(alloc, value);
    defer alloc.free(body);
    try writer.writeAll("event: ");
    try writer.writeAll(event_name);
    try writer.writeAll("\n");
    try writer.writeAll("data: ");
    try writer.writeAll(body);
    try writer.writeAll("\n\n");
}

fn jsonStringArg(value: std.json.Value, key: []const u8) ?[]const u8 {
    if (value != .object) return null;
    return jsonStringObjectField(value.object, key);
}

fn jsonStringObjectField(object: anytype, key: []const u8) ?[]const u8 {
    const value = object.get(key) orelse return null;
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

fn jsonIntArg(value: std.json.Value, key: []const u8) ?i64 {
    if (value != .object) return null;
    return jsonIntObjectField(value.object, key);
}

fn jsonIntObjectField(object: anytype, key: []const u8) ?i64 {
    const value = object.get(key) orelse return null;
    return switch (value) {
        .integer => |n| n,
        .float => |n| @intFromFloat(n),
        else => null,
    };
}

fn jsonValueArg(value: std.json.Value, key: []const u8) ?std.json.Value {
    if (value != .object) return null;
    return value.object.get(key);
}

fn stringifyJsonValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    const encoded = try stringifyJsonValue(alloc, .{ .string = text });
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn mcpError(alloc: std.mem.Allocator, text: []const u8) !mcp.CallToolResult {
    return .{
        .is_error = true,
        .text = try alloc.dupe(u8, text),
    };
}

fn mcpResultFromHttpResponse(alloc: std.mem.Allocator, resp: http_common.HttpResponse) !mcp.CallToolResult {
    if (resp.status < 200 or resp.status >= 300) {
        return .{
            .is_error = true,
            .text = try alloc.dupe(u8, resp.body),
        };
    }
    var structured: ?std.json.Value = null;
    if (resp.content_type) |content_type| {
        if (std.mem.indexOf(u8, content_type, "json") != null and resp.body.len != 0) {
            structured = std.json.parseFromSliceLeaky(std.json.Value, alloc, resp.body, .{}) catch null;
        }
    }
    return .{
        .text = try alloc.dupe(u8, if (resp.body.len == 0) "ok" else resp.body),
        .structured = structured,
    };
}
