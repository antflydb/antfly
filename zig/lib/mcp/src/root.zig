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

const std = @import("std");

pub const protocol_version = "2025-06-18";
pub const session_id_header = "Mcp-Session-Id";
pub const protocol_version_header = "Mcp-Protocol-Version";
pub const last_event_id_header = "Last-Event-ID";

pub const Implementation = struct {
    name: []const u8 = "antfly",
    version: []const u8 = "0.0.0",
};

pub const CallToolResult = struct {
    is_error: bool = false,
    text: []const u8 = "",
    structured: ?std.json.Value = null,
};

pub const ToolHandler = struct {
    ptr: *anyopaque,
    call_fn: *const fn (*anyopaque, std.mem.Allocator, std.json.Value) anyerror!CallToolResult,

    pub fn call(self: ToolHandler, alloc: std.mem.Allocator, args: std.json.Value) !CallToolResult {
        return try self.call_fn(self.ptr, alloc, args);
    }
};

pub const Tool = struct {
    name: []const u8,
    description: []const u8,
    input_schema_json: []const u8 = "{\"type\":\"object\"}",
    handler: ToolHandler,
};

pub const HttpResult = struct {
    status: u16,
    content_type: []const u8,
    headers: []const HttpHeader = &.{},
    body: []u8,

    pub fn deinit(self: *HttpResult, alloc: std.mem.Allocator) void {
        if (self.headers.len > 0) alloc.free(self.headers);
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const HttpHeader = struct {
    name: []const u8,
    value: []const u8,
};

pub const SessionStore = struct {
    ptr: *anyopaque,
    create_fn: *const fn (*anyopaque) anyerror![]const u8,
    exists_fn: *const fn (*anyopaque, []const u8) bool,
    close_fn: *const fn (*anyopaque, []const u8) bool,
    next_event_id_fn: *const fn (*anyopaque, []const u8, ?[]const u8) anyerror!?u64,

    pub fn create(self: SessionStore) ![]const u8 {
        return try self.create_fn(self.ptr);
    }

    pub fn exists(self: SessionStore, session_id: []const u8) bool {
        return self.exists_fn(self.ptr, session_id);
    }

    pub fn close(self: SessionStore, session_id: []const u8) bool {
        return self.close_fn(self.ptr, session_id);
    }

    pub fn nextEventId(self: SessionStore, session_id: []const u8, last_event_id: ?[]const u8) !?u64 {
        return try self.next_event_id_fn(self.ptr, session_id, last_event_id);
    }
};

pub const InMemorySessionStore = struct {
    const SessionState = struct {
        next_event_id: u64 = 1,
    };

    alloc: ?std.mem.Allocator = null,
    next_id: u64 = 1,
    sessions: std.StringHashMapUnmanaged(SessionState) = .empty,

    pub fn init(alloc: std.mem.Allocator) InMemorySessionStore {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *InMemorySessionStore, alloc: std.mem.Allocator) void {
        var iter = self.sessions.keyIterator();
        while (iter.next()) |key| alloc.free(key.*);
        self.sessions.deinit(alloc);
        self.* = undefined;
    }

    pub fn iface(self: *InMemorySessionStore) SessionStore {
        return .{
            .ptr = self,
            .create_fn = create,
            .exists_fn = exists,
            .close_fn = close,
            .next_event_id_fn = nextEventId,
        };
    }

    fn create(ptr: *anyopaque) ![]const u8 {
        const self: *InMemorySessionStore = @ptrCast(@alignCast(ptr));
        const alloc = self.alloc orelse return error.MissingAllocator;
        const session_id = try std.fmt.allocPrint(alloc, "mcp-session-{d}", .{self.next_id});
        errdefer alloc.free(session_id);
        self.next_id += 1;
        try self.sessions.put(alloc, session_id, .{});
        return session_id;
    }

    fn exists(ptr: *anyopaque, session_id: []const u8) bool {
        const self: *InMemorySessionStore = @ptrCast(@alignCast(ptr));
        return self.sessions.contains(session_id);
    }

    fn close(ptr: *anyopaque, session_id: []const u8) bool {
        const self: *InMemorySessionStore = @ptrCast(@alignCast(ptr));
        const alloc = self.alloc orelse return false;
        const removed = self.sessions.fetchRemove(session_id) orelse return false;
        alloc.free(removed.key);
        return true;
    }

    fn nextEventId(ptr: *anyopaque, session_id: []const u8, last_event_id: ?[]const u8) !?u64 {
        const self: *InMemorySessionStore = @ptrCast(@alignCast(ptr));
        const entry = self.sessions.getEntry(session_id) orelse return null;
        if (last_event_id) |raw| {
            const parsed = std.fmt.parseUnsigned(u64, raw, 10) catch null;
            if (parsed) |seen| {
                if (entry.value_ptr.next_event_id <= seen) entry.value_ptr.next_event_id = seen +| 1;
            }
        }
        const event_id = entry.value_ptr.next_event_id;
        entry.value_ptr.next_event_id +|= 1;
        return event_id;
    }
};

pub const Server = struct {
    implementation: Implementation = .{},
    tools: std.ArrayListUnmanaged(Tool) = .empty,
    session_store: ?SessionStore = null,

    pub fn deinit(self: *Server, alloc: std.mem.Allocator) void {
        self.tools.deinit(alloc);
    }

    pub fn addTool(self: *Server, alloc: std.mem.Allocator, tool: Tool) !void {
        try self.tools.append(alloc, tool);
    }

    pub fn handleStreamableHttpPost(self: *Server, alloc: std.mem.Allocator, body: []const u8) !HttpResult {
        if (try self.handleJsonRpc(alloc, body)) |response_body| {
            const headers = if (try self.sessionHeadersForRequest(alloc, body)) |session_headers|
                session_headers
            else
                &.{};
            return .{
                .status = 200,
                .content_type = "application/json",
                .headers = headers,
                .body = response_body,
            };
        }
        return .{
            .status = 202,
            .content_type = "text/plain",
            .body = try alloc.dupe(u8, ""),
        };
    }

    pub fn handleStreamableHttpGet(self: *Server, alloc: std.mem.Allocator, endpoint: []const u8) !HttpResult {
        return try self.handleStreamableHttpGetWithSession(alloc, endpoint, null, null);
    }

    pub fn handleStreamableHttpGetWithSession(self: *Server, alloc: std.mem.Allocator, endpoint: []const u8, session_id: ?[]const u8, last_event_id: ?[]const u8) !HttpResult {
        const event_id = if (session_id) |id|
            if (self.session_store) |store| try store.nextEventId(id, last_event_id) else null
        else
            null;
        const body = if (event_id) |id|
            try std.fmt.allocPrint(alloc, "id: {d}\nevent: endpoint\ndata: {s}\n\n", .{ id, endpoint })
        else
            try std.fmt.allocPrint(alloc, "event: endpoint\ndata: {s}\n\n", .{endpoint});
        return .{
            .status = 200,
            .content_type = "text/event-stream",
            .body = body,
        };
    }

    pub fn handleStreamableHttpDelete(self: *Server, alloc: std.mem.Allocator, session_id: ?[]const u8) !HttpResult {
        const closed = if (session_id) |id|
            if (self.session_store) |store| store.close(id) else false
        else
            false;
        if (!closed) {
            return .{
                .status = 404,
                .content_type = "text/plain",
                .body = try alloc.dupe(u8, "session not found"),
            };
        }
        return .{
            .status = 202,
            .content_type = "text/plain",
            .body = try alloc.dupe(u8, ""),
        };
    }

    pub fn handleStdioLine(self: *Server, alloc: std.mem.Allocator, line: []const u8) !?[]u8 {
        const trimmed = std.mem.trim(u8, line, "\r\n");
        const response = (try self.handleJsonRpc(alloc, trimmed)) orelse return null;
        errdefer alloc.free(response);
        const framed = try std.fmt.allocPrint(alloc, "{s}\n", .{response});
        alloc.free(response);
        return framed;
    }

    pub fn handleJsonRpc(self: *Server, alloc: std.mem.Allocator, body: []const u8) !?[]u8 {
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const temp_alloc = arena_impl.allocator();

        const request = std.json.parseFromSliceLeaky(std.json.Value, temp_alloc, body, .{}) catch {
            return try stringifyValue(alloc, try errorResponse(temp_alloc, .null, -32700, "parse error"));
        };
        if (request != .object) {
            return try stringifyValue(alloc, try errorResponse(temp_alloc, .null, -32600, "invalid request"));
        }

        const root = request.object;
        const method = stringField(root, "method") orelse {
            return try stringifyValue(alloc, try errorResponse(temp_alloc, idField(root), -32600, "invalid request"));
        };
        const id = idField(root);

        if (std.mem.eql(u8, method, "notifications/initialized")) return null;
        if (std.mem.eql(u8, method, "initialize")) {
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, try self.initializeResult(temp_alloc)));
        }
        if (std.mem.eql(u8, method, "tools/list")) {
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, try self.toolsListResult(temp_alloc)));
        }
        if (std.mem.eql(u8, method, "tools/call")) {
            const params = root.get("params") orelse .null;
            const result = self.toolsCallResult(temp_alloc, params) catch |err| switch (err) {
                error.UnknownTool => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "unknown tool")),
                error.InvalidParams => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "invalid params")),
                else => return err,
            };
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, result));
        }
        return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32601, "method not found"));
    }

    fn initializeResult(self: *const Server, alloc: std.mem.Allocator) !std.json.Value {
        var capabilities_tools = std.json.ObjectMap.empty;
        try capabilities_tools.put(alloc, "listChanged", .{ .bool = false });

        var capabilities = std.json.ObjectMap.empty;
        try capabilities.put(alloc, "tools", .{ .object = capabilities_tools });

        var server_info = std.json.ObjectMap.empty;
        try server_info.put(alloc, "name", .{ .string = self.implementation.name });
        try server_info.put(alloc, "version", .{ .string = self.implementation.version });

        var result = std.json.ObjectMap.empty;
        try result.put(alloc, "protocolVersion", .{ .string = protocol_version });
        try result.put(alloc, "capabilities", .{ .object = capabilities });
        try result.put(alloc, "serverInfo", .{ .object = server_info });
        return .{ .object = result };
    }

    fn sessionHeadersForRequest(self: *Server, alloc: std.mem.Allocator, body: []const u8) !?[]const HttpHeader {
        if (self.session_store == null) return null;
        if (!isJsonRpcMethod(alloc, body, "initialize")) return null;
        const session_id = try self.session_store.?.create();
        const headers = try alloc.alloc(HttpHeader, 2);
        headers[0] = .{ .name = session_id_header, .value = session_id };
        headers[1] = .{ .name = protocol_version_header, .value = protocol_version };
        return headers;
    }

    fn toolsListResult(self: *const Server, alloc: std.mem.Allocator) !std.json.Value {
        var tools = std.json.Array.init(alloc);
        for (self.tools.items) |tool| {
            const parsed_schema = try std.json.parseFromSliceLeaky(std.json.Value, alloc, tool.input_schema_json, .{});
            var entry = std.json.ObjectMap.empty;
            try entry.put(alloc, "name", .{ .string = tool.name });
            try entry.put(alloc, "description", .{ .string = tool.description });
            try entry.put(alloc, "inputSchema", parsed_schema);
            try tools.append(.{ .object = entry });
        }

        var result = std.json.ObjectMap.empty;
        try result.put(alloc, "tools", .{ .array = tools });
        return .{ .object = result };
    }

    fn toolsCallResult(self: *Server, alloc: std.mem.Allocator, params: std.json.Value) !std.json.Value {
        if (params != .object) return error.InvalidParams;
        const name = stringField(params.object, "name") orelse return error.InvalidParams;
        const args = params.object.get("arguments") orelse emptyObject();

        for (self.tools.items) |tool| {
            if (!std.mem.eql(u8, tool.name, name)) continue;
            const called = try tool.handler.call(alloc, args);
            var text_part = std.json.ObjectMap.empty;
            try text_part.put(alloc, "type", .{ .string = "text" });
            try text_part.put(alloc, "text", .{ .string = called.text });
            var content = std.json.Array.init(alloc);
            try content.append(.{ .object = text_part });

            var result = std.json.ObjectMap.empty;
            try result.put(alloc, "content", .{ .array = content });
            try result.put(alloc, "isError", .{ .bool = called.is_error });
            if (called.structured) |structured| {
                try result.put(alloc, "structuredContent", structured);
            }
            return .{ .object = result };
        }
        return error.UnknownTool;
    }
};

fn isJsonRpcMethod(alloc: std.mem.Allocator, body: []const u8, method_name: []const u8) bool {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const request = std.json.parseFromSliceLeaky(std.json.Value, arena_impl.allocator(), body, .{}) catch return false;
    if (request != .object) return false;
    const method = stringField(request.object, "method") orelse return false;
    return std.mem.eql(u8, method, method_name);
}

fn emptyObject() std.json.Value {
    return .{ .object = .empty };
}

fn stringField(object: anytype, key: []const u8) ?[]const u8 {
    const value = object.get(key) orelse return null;
    return if (value == .string) value.string else null;
}

fn idField(object: anytype) std.json.Value {
    return object.get("id") orelse .null;
}

fn successResponse(alloc: std.mem.Allocator, id: std.json.Value, result: std.json.Value) !std.json.Value {
    var out = std.json.ObjectMap.empty;
    try out.put(alloc, "jsonrpc", .{ .string = "2.0" });
    try out.put(alloc, "id", id);
    try out.put(alloc, "result", result);
    return .{ .object = out };
}

fn errorResponse(alloc: std.mem.Allocator, id: std.json.Value, code: i64, message: []const u8) !std.json.Value {
    var err = std.json.ObjectMap.empty;
    try err.put(alloc, "code", .{ .integer = code });
    try err.put(alloc, "message", .{ .string = message });

    var out = std.json.ObjectMap.empty;
    try out.put(alloc, "jsonrpc", .{ .string = "2.0" });
    try out.put(alloc, "id", id);
    try out.put(alloc, "error", .{ .object = err });
    return .{ .object = out };
}

fn stringifyValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

test "mcp handles initialize and tool call" {
    const alloc = std.testing.allocator;

    const Echo = struct {
        fn call(_: *anyopaque, a: std.mem.Allocator, args: std.json.Value) !CallToolResult {
            const text = if (args == .object and args.object.get("text") != null and args.object.get("text").? == .string)
                args.object.get("text").?.string
            else
                "";
            return .{
                .text = text,
                .structured = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"ok\":true}", .{}),
            };
        }
    };

    var ctx: u8 = 0;
    var server = Server{ .implementation = .{ .name = "test", .version = "1" } };
    defer server.deinit(alloc);
    try server.addTool(alloc, .{
        .name = "echo",
        .description = "Echo text",
        .input_schema_json = "{\"type\":\"object\",\"properties\":{\"text\":{\"type\":\"string\"}}}",
        .handler = .{ .ptr = &ctx, .call_fn = Echo.call },
    });

    const init_body =
        \\{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
    ;
    const init_resp = (try server.handleJsonRpc(alloc, init_body)).?;
    defer alloc.free(init_resp);
    try std.testing.expect(std.mem.indexOf(u8, init_resp, "\"protocolVersion\":\"2025-06-18\"") != null);

    const list_body =
        \\{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
    ;
    const list_resp = (try server.handleJsonRpc(alloc, list_body)).?;
    defer alloc.free(list_resp);
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "\"name\":\"echo\"") != null);

    const call_body =
        \\{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"echo","arguments":{"text":"hello"}}}
    ;
    const call_resp = (try server.handleJsonRpc(alloc, call_body)).?;
    defer alloc.free(call_resp);
    try std.testing.expect(std.mem.indexOf(u8, call_resp, "\"text\":\"hello\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, call_resp, "\"structuredContent\":{\"ok\":true}") != null);
}

test "mcp initialized notification has no response" {
    var server = Server{};
    try std.testing.expectEqual(@as(?[]u8, null), try server.handleJsonRpc(std.testing.allocator,
        \\{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}
    ));
}

test "mcp maps malformed and unknown tool requests to JSON-RPC errors" {
    const alloc = std.testing.allocator;
    var server = Server{};

    const parse_resp = (try server.handleJsonRpc(alloc, "{")).?;
    defer alloc.free(parse_resp);
    try std.testing.expect(std.mem.indexOf(u8, parse_resp, "\"code\":-32700") != null);
    try std.testing.expect(std.mem.indexOf(u8, parse_resp, "\"message\":\"parse error\"") != null);

    const invalid_params = (try server.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"tools/call","params":[]}
    )).?;
    defer alloc.free(invalid_params);
    try std.testing.expect(std.mem.indexOf(u8, invalid_params, "\"code\":-32602") != null);
    try std.testing.expect(std.mem.indexOf(u8, invalid_params, "\"message\":\"invalid params\"") != null);

    const unknown_tool = (try server.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"missing","arguments":{}}}
    )).?;
    defer alloc.free(unknown_tool);
    try std.testing.expect(std.mem.indexOf(u8, unknown_tool, "\"code\":-32602") != null);
    try std.testing.expect(std.mem.indexOf(u8, unknown_tool, "\"message\":\"unknown tool\"") != null);
}

test "mcp streamable http helpers map responses" {
    const alloc = std.testing.allocator;
    var server = Server{};

    var post = try server.handleStreamableHttpPost(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
    );
    defer post.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), post.status);
    try std.testing.expectEqualStrings("application/json", post.content_type);

    var notification = try server.handleStreamableHttpPost(alloc,
        \\{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}
    );
    defer notification.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), notification.status);

    var get = try server.handleStreamableHttpGet(alloc, "/mcp/v1");
    defer get.deinit(alloc);
    try std.testing.expectEqualStrings("text/event-stream", get.content_type);
    try std.testing.expect(std.mem.indexOf(u8, get.body, "event: endpoint") != null);
}

test "mcp streamable http creates and closes sessions" {
    const alloc = std.testing.allocator;
    var sessions = InMemorySessionStore.init(alloc);
    defer sessions.deinit(alloc);
    var server = Server{ .session_store = sessions.iface() };

    var post = try server.handleStreamableHttpPost(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
    );
    defer post.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), post.headers.len);
    try std.testing.expectEqualStrings(session_id_header, post.headers[0].name);
    try std.testing.expect(sessions.iface().exists(post.headers[0].value));
    try std.testing.expectEqualStrings(protocol_version_header, post.headers[1].name);
    try std.testing.expectEqualStrings(protocol_version, post.headers[1].value);

    const session_id = try alloc.dupe(u8, post.headers[0].value);
    defer alloc.free(session_id);
    var deleted = try server.handleStreamableHttpDelete(alloc, session_id);
    defer deleted.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), deleted.status);
    try std.testing.expect(!sessions.iface().exists(session_id));
}

test "mcp streamable http get emits session event ids and honors resume cursor" {
    const alloc = std.testing.allocator;
    var sessions = InMemorySessionStore.init(alloc);
    defer sessions.deinit(alloc);
    var server = Server{ .session_store = sessions.iface() };

    var post = try server.handleStreamableHttpPost(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
    );
    defer post.deinit(alloc);
    const session_id = post.headers[0].value;

    var first = try server.handleStreamableHttpGetWithSession(alloc, "/mcp/v1", session_id, null);
    defer first.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, first.body, "id: 1\n") != null);

    var resumed = try server.handleStreamableHttpGetWithSession(alloc, "/mcp/v1", session_id, "7");
    defer resumed.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, resumed.body, "id: 8\n") != null);
}

test "mcp stdio line dispatch frames responses" {
    const alloc = std.testing.allocator;
    var server = Server{};

    const response = (try server.handleStdioLine(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
    )).?;
    defer alloc.free(response);
    try std.testing.expect(std.mem.endsWith(u8, response, "\n"));
    try std.testing.expect(std.mem.indexOf(u8, response, "\"protocolVersion\":\"2025-06-18\"") != null);

    try std.testing.expectEqual(@as(?[]u8, null), try server.handleStdioLine(alloc,
        \\{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}
    ));
}
