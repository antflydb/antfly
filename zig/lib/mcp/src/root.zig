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

pub const testing = @import("testing.zig");

test {
    _ = testing;
}

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
        if (self.headers.len > 0) {
            for (self.headers) |header| {
                if (header.owns_value) alloc.free(@constCast(header.value));
            }
            alloc.free(self.headers);
        }
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const HttpHeader = struct {
    name: []const u8,
    value: []const u8,
    owns_value: bool = false,
};

pub const SessionStore = struct {
    ptr: *anyopaque,
    create_fn: *const fn (*anyopaque, std.mem.Allocator) anyerror![]u8,
    exists_fn: *const fn (*anyopaque, []const u8) bool,
    close_fn: *const fn (*anyopaque, []const u8) bool,
    next_event_id_fn: *const fn (*anyopaque, []const u8, ?[]const u8) anyerror!?u64,

    pub fn create(self: SessionStore, alloc: std.mem.Allocator) ![]u8 {
        return try self.create_fn(self.ptr, alloc);
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
    pub const Options = struct {
        max_sessions: usize = 1024,
        idle_ttl_ns: u64 = std.time.ns_per_hour,
        cleanup_interval_ns: u64 = std.time.ns_per_min,
        now_ns_fn: ?*const fn () u64 = null,
    };

    const SessionState = struct {
        next_event_id: u64 = 1,
        last_activity_ns: u64,
    };

    alloc: ?std.mem.Allocator = null,
    io: ?std.Io = null,
    options: Options = .{},
    mutex: std.Io.Mutex = .init,
    sessions: std.StringHashMapUnmanaged(SessionState) = .empty,
    next_cleanup_ns: u64 = 0,

    pub fn init(alloc: std.mem.Allocator) InMemorySessionStore {
        return .{
            .alloc = alloc,
            .io = std.Io.Threaded.global_single_threaded.io(),
        };
    }

    pub fn initWithOptions(alloc: std.mem.Allocator, io: std.Io, options: Options) InMemorySessionStore {
        return .{ .alloc = alloc, .io = io, .options = options };
    }

    pub fn deinit(self: *InMemorySessionStore, alloc: std.mem.Allocator) void {
        const store_alloc = self.alloc orelse alloc;
        const io = self.io orelse std.Io.Threaded.global_single_threaded.io();
        self.mutex.lockUncancelable(io);
        var iter = self.sessions.keyIterator();
        while (iter.next()) |key| store_alloc.free(key.*);
        self.sessions.deinit(store_alloc);
        self.mutex.unlock(io);
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

    fn create(ptr: *anyopaque, response_alloc: std.mem.Allocator) ![]u8 {
        const self: *InMemorySessionStore = @ptrCast(@alignCast(ptr));
        const store_alloc = self.alloc orelse return error.MissingAllocator;
        const io = self.io orelse return error.MissingSecureRandomSource;

        for (0..8) |_| {
            var entropy: [16]u8 = undefined;
            try io.randomSecure(&entropy);
            const encoded = std.fmt.bytesToHex(entropy, .lower);
            const response_id = try std.fmt.allocPrint(response_alloc, "mcp-session-{s}", .{&encoded});
            errdefer response_alloc.free(response_id);
            const stored_id = try store_alloc.dupe(u8, response_id);
            errdefer store_alloc.free(stored_id);

            const now_ns = self.nowNs();
            self.mutex.lockUncancelable(io);
            self.cleanupExpiredLocked(store_alloc, now_ns, false);
            if (self.sessions.count() >= self.options.max_sessions) {
                self.cleanupExpiredLocked(store_alloc, now_ns, true);
            }
            if (self.sessions.count() >= self.options.max_sessions) {
                self.mutex.unlock(io);
                return error.McpSessionCapacityExceeded;
            }
            if (self.sessions.contains(stored_id)) {
                self.mutex.unlock(io);
                store_alloc.free(stored_id);
                response_alloc.free(response_id);
                continue;
            }
            self.sessions.put(store_alloc, stored_id, .{ .last_activity_ns = now_ns }) catch |err| {
                self.mutex.unlock(io);
                return err;
            };
            self.mutex.unlock(io);
            return response_id;
        }
        return error.McpSessionIdCollision;
    }

    fn exists(ptr: *anyopaque, session_id: []const u8) bool {
        const self: *InMemorySessionStore = @ptrCast(@alignCast(ptr));
        const alloc = self.alloc orelse return false;
        const io = self.io orelse return false;
        const now_ns = self.nowNs();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.cleanupExpiredLocked(alloc, now_ns, false);
        const state = self.sessions.getPtr(session_id) orelse return false;
        if (self.isExpired(state.*, now_ns)) {
            _ = self.removeLocked(alloc, session_id);
            return false;
        }
        state.last_activity_ns = now_ns;
        return true;
    }

    fn close(ptr: *anyopaque, session_id: []const u8) bool {
        const self: *InMemorySessionStore = @ptrCast(@alignCast(ptr));
        const alloc = self.alloc orelse return false;
        const io = self.io orelse return false;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.removeLocked(alloc, session_id);
    }

    fn nextEventId(ptr: *anyopaque, session_id: []const u8, last_event_id: ?[]const u8) !?u64 {
        const self: *InMemorySessionStore = @ptrCast(@alignCast(ptr));
        const alloc = self.alloc orelse return error.MissingAllocator;
        const io = self.io orelse return error.MissingSecureRandomSource;
        const now_ns = self.nowNs();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.cleanupExpiredLocked(alloc, now_ns, false);
        const entry = self.sessions.getEntry(session_id) orelse return null;
        if (self.isExpired(entry.value_ptr.*, now_ns)) {
            _ = self.removeLocked(alloc, session_id);
            return null;
        }
        if (last_event_id) |raw| {
            const seen = std.fmt.parseUnsigned(u64, raw, 10) catch return error.InvalidLastEventId;
            if (seen == std.math.maxInt(u64)) return error.McpEventIdExhausted;
            if (entry.value_ptr.next_event_id <= seen) entry.value_ptr.next_event_id = seen + 1;
        }
        if (entry.value_ptr.next_event_id == std.math.maxInt(u64)) return error.McpEventIdExhausted;
        const event_id = entry.value_ptr.next_event_id;
        entry.value_ptr.next_event_id += 1;
        entry.value_ptr.last_activity_ns = now_ns;
        return event_id;
    }

    fn nowNs(self: *const InMemorySessionStore) u64 {
        if (self.options.now_ns_fn) |now_ns_fn| return now_ns_fn();
        const io = self.io orelse return 0;
        return @intCast(std.Io.Timestamp.now(io, .awake).toNanoseconds());
    }

    fn cleanupExpiredLocked(self: *InMemorySessionStore, alloc: std.mem.Allocator, now_ns: u64, force: bool) void {
        if (self.options.idle_ttl_ns == 0) return;
        if (!force and self.next_cleanup_ns != 0 and now_ns < self.next_cleanup_ns) return;
        self.next_cleanup_ns = now_ns +| @min(
            self.options.cleanup_interval_ns,
            self.options.idle_ttl_ns,
        );
        var iter = self.sessions.iterator();
        while (iter.next()) |entry| {
            if (!self.isExpired(entry.value_ptr.*, now_ns)) continue;
            alloc.free(@constCast(entry.key_ptr.*));
            self.sessions.removeByPtr(entry.key_ptr);
        }
    }

    fn isExpired(self: *const InMemorySessionStore, state: SessionState, now_ns: u64) bool {
        return self.options.idle_ttl_ns != 0 and
            now_ns >= state.last_activity_ns and
            now_ns - state.last_activity_ns >= self.options.idle_ttl_ns;
    }

    fn removeLocked(self: *InMemorySessionStore, alloc: std.mem.Allocator, session_id: []const u8) bool {
        const removed = self.sessions.fetchRemove(session_id) orelse return false;
        alloc.free(removed.key);
        return true;
    }
};

pub const Server = struct {
    implementation: Implementation = .{},
    tools: std.ArrayListUnmanaged(Tool) = .empty,
    session_store: ?SessionStore = null,
    /// Maximum serialized `tools/call` result size. Zero disables the guard.
    /// This includes both TextContent and structuredContent when both are present.
    max_tool_result_bytes: usize = 0,
    tool_result_too_large_text: []const u8 = "Tool result exceeds the configured MCP response budget. Reduce the result limit or requested fields and retry.",

    pub fn deinit(self: *Server, alloc: std.mem.Allocator) void {
        self.tools.deinit(alloc);
    }

    pub fn addTool(self: *Server, alloc: std.mem.Allocator, tool: Tool) !void {
        try self.tools.append(alloc, tool);
    }

    pub fn handleStreamableHttpPost(self: *Server, alloc: std.mem.Allocator, body: []const u8) !HttpResult {
        return try self.handleStreamableHttpPostWithSession(alloc, body, null);
    }

    pub fn handleStreamableHttpPostWithSession(self: *Server, alloc: std.mem.Allocator, body: []const u8, session_id: ?[]const u8) !HttpResult {
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const temp_alloc = arena_impl.allocator();

        const request = std.json.parseFromSliceLeaky(std.json.Value, temp_alloc, body, .{}) catch {
            return .{
                .status = 200,
                .content_type = "application/json",
                .body = try stringifyValue(alloc, try errorResponse(temp_alloc, .null, -32700, "parse error")),
            };
        };
        const method = if (request == .object) stringField(request.object, "method") else null;
        if (self.session_store) |store| {
            if (method) |name| {
                if (!std.mem.eql(u8, name, "initialize")) {
                    const id = session_id orelse return .{
                        .status = 400,
                        .content_type = "text/plain",
                        .body = try alloc.dupe(u8, "missing MCP session"),
                    };
                    if (!store.exists(id)) {
                        return .{
                            .status = 404,
                            .content_type = "text/plain",
                            .body = try alloc.dupe(u8, "unknown MCP session"),
                        };
                    }
                }
            }
        }

        if (try self.handleJsonRpcRequest(temp_alloc, request)) |response| {
            const response_body = try stringifyValue(alloc, response);
            errdefer alloc.free(response_body);
            const headers = if (try self.sessionHeadersForMethod(alloc, method)) |session_headers|
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
        const event_id = if (self.session_store) |store| blk: {
            const id = session_id orelse return .{
                .status = 400,
                .content_type = "text/plain",
                .body = try alloc.dupe(u8, "missing MCP session"),
            };
            break :blk (try store.nextEventId(id, last_event_id)) orelse return .{
                .status = 404,
                .content_type = "text/plain",
                .body = try alloc.dupe(u8, "unknown MCP session"),
            };
        } else null;
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
        const store = self.session_store orelse return .{
            .status = 404,
            .content_type = "text/plain",
            .body = try alloc.dupe(u8, "session storage not configured"),
        };
        const id = session_id orelse return .{
            .status = 400,
            .content_type = "text/plain",
            .body = try alloc.dupe(u8, "missing MCP session"),
        };
        const closed = store.close(id);
        if (!closed) {
            return .{
                .status = 404,
                .content_type = "text/plain",
                .body = try alloc.dupe(u8, "unknown MCP session"),
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
        const response = (try self.handleJsonRpcRequest(temp_alloc, request)) orelse return null;
        return try stringifyValue(alloc, response);
    }

    fn handleJsonRpcRequest(self: *Server, alloc: std.mem.Allocator, request: std.json.Value) !?std.json.Value {
        if (request != .object) {
            return try errorResponse(alloc, .null, -32600, "invalid request");
        }

        const root = request.object;
        const method = stringField(root, "method") orelse {
            return try errorResponse(alloc, idField(root), -32600, "invalid request");
        };
        const id = idField(root);

        if (std.mem.eql(u8, method, "notifications/initialized")) return null;
        if (std.mem.eql(u8, method, "initialize")) {
            return try successResponse(alloc, id, try self.initializeResult(alloc));
        }
        if (std.mem.eql(u8, method, "tools/list")) {
            return try successResponse(alloc, id, try self.toolsListResult(alloc));
        }
        if (std.mem.eql(u8, method, "tools/call")) {
            const params = root.get("params") orelse .null;
            const result = self.toolsCallResult(alloc, params) catch |err| switch (err) {
                error.UnknownTool => return try errorResponse(alloc, id, -32602, "unknown tool"),
                error.InvalidParams => return try errorResponse(alloc, id, -32602, "invalid params"),
                else => return err,
            };
            return try successResponse(alloc, id, result);
        }
        return try errorResponse(alloc, id, -32601, "method not found");
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

    fn sessionHeadersForMethod(self: *Server, alloc: std.mem.Allocator, method: ?[]const u8) !?[]const HttpHeader {
        if (self.session_store == null) return null;
        if (method == null or !std.mem.eql(u8, method.?, "initialize")) return null;
        const session_id = try self.session_store.?.create(alloc);
        errdefer {
            _ = self.session_store.?.close(session_id);
            alloc.free(session_id);
        }
        const headers = try alloc.alloc(HttpHeader, 2);
        headers[0] = .{ .name = session_id_header, .value = session_id, .owns_value = true };
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
            const result = try callToolResultValue(alloc, called);
            if (self.max_tool_result_bytes > 0) {
                const encoded = try stringifyValue(alloc, result);
                if (encoded.len > self.max_tool_result_bytes) {
                    return try callToolResultValue(alloc, .{
                        .is_error = true,
                        .text = self.tool_result_too_large_text,
                    });
                }
            }
            return result;
        }
        return error.UnknownTool;
    }
};

fn callToolResultValue(alloc: std.mem.Allocator, called: CallToolResult) !std.json.Value {
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
    try testing.expectResultSubset(alloc, init_resp, "{\"protocolVersion\":\"2025-06-18\"}");

    const list_body =
        \\{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
    ;
    const list_resp = (try server.handleJsonRpc(alloc, list_body)).?;
    defer alloc.free(list_resp);
    var parsed_tools = try testing.parseToolsListResponse(alloc, list_resp);
    defer parsed_tools.deinit();
    const echo_tool = testing.findTool(parsed_tools.value.result.tools, "echo") orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("Echo text", echo_tool.description);
    try std.testing.expectEqualStrings("object", echo_tool.inputSchema.object.get("type").?.string);

    const call_body =
        \\{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"echo","arguments":{"text":"hello"}}}
    ;
    const call_resp = (try server.handleJsonRpc(alloc, call_body)).?;
    defer alloc.free(call_resp);
    var parsed_call = try testing.parseToolCallResponse(alloc, call_resp);
    defer parsed_call.deinit();
    try std.testing.expectEqualStrings("hello", testing.findTextContent(parsed_call.value.result.content).?);
    try std.testing.expect(parsed_call.value.result.structuredContent.? == .object);
    try std.testing.expectEqual(true, parsed_call.value.result.structuredContent.?.object.get("ok").?.bool);
}

test "mcp replaces oversized tool results with a text error" {
    const alloc = std.testing.allocator;

    const Large = struct {
        fn call(_: *anyopaque, a: std.mem.Allocator, _: std.json.Value) !CallToolResult {
            return .{
                .text = "a payload that is duplicated for compatibility",
                .structured = try std.json.parseFromSliceLeaky(
                    std.json.Value,
                    a,
                    "{\"hits\":[{\"_source\":{\"text\":\"a payload that is duplicated for compatibility\"}}]}",
                    .{},
                ),
            };
        }
    };

    var ctx: u8 = 0;
    var server = Server{
        .max_tool_result_bytes = 100,
        .tool_result_too_large_text = "narrow the query",
    };
    defer server.deinit(alloc);
    try server.addTool(alloc, .{
        .name = "large",
        .description = "Return a large result",
        .handler = .{ .ptr = &ctx, .call_fn = Large.call },
    });

    const response = (try server.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"large","arguments":{}}}
    )).?;
    defer alloc.free(response);
    var parsed = try testing.parseToolCallResponse(alloc, response);
    defer parsed.deinit();
    try std.testing.expect(parsed.value.result.isError);
    try std.testing.expectEqualStrings("narrow the query", testing.findTextContent(parsed.value.result.content).?);
    try std.testing.expect(parsed.value.result.structuredContent == null);
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
    try testing.expectError(alloc, parse_resp, -32700, "parse error");

    const invalid_params = (try server.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"tools/call","params":[]}
    )).?;
    defer alloc.free(invalid_params);
    try testing.expectError(alloc, invalid_params, -32602, "invalid params");

    const unknown_tool = (try server.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"missing","arguments":{}}}
    )).?;
    defer alloc.free(unknown_tool);
    try testing.expectError(alloc, unknown_tool, -32602, "unknown tool");
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

    const session_id = post.headers[0].value;
    var deleted = try server.handleStreamableHttpDelete(alloc, session_id);
    defer deleted.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), deleted.status);
    try std.testing.expect(!sessions.iface().exists(session_id));
    try std.testing.expect(std.mem.startsWith(u8, session_id, "mcp-session-"));
}

test "mcp streamable http enforces sessions inside the transport" {
    const alloc = std.testing.allocator;
    var sessions = InMemorySessionStore.init(alloc);
    defer sessions.deinit(alloc);
    var server = Server{ .session_store = sessions.iface() };

    var initialized = try server.handleStreamableHttpPost(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
    );
    defer initialized.deinit(alloc);
    const session_id = initialized.headers[0].value;

    var missing = try server.handleStreamableHttpPostWithSession(alloc,
        \\{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
    , null);
    defer missing.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), missing.status);

    var unknown = try server.handleStreamableHttpPostWithSession(alloc,
        \\{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}
    , "mcp-session-unknown");
    defer unknown.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), unknown.status);

    var valid = try server.handleStreamableHttpPostWithSession(alloc,
        \\{"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}
    , session_id);
    defer valid.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), valid.status);

    var malformed = try server.handleStreamableHttpPostWithSession(alloc, "{", null);
    defer malformed.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), malformed.status);
    try testing.expectError(alloc, malformed.body, -32700, "parse error");

    var missing_get = try server.handleStreamableHttpGetWithSession(alloc, "/mcp/v1", null, null);
    defer missing_get.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), missing_get.status);

    var missing_delete = try server.handleStreamableHttpDelete(alloc, null);
    defer missing_delete.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), missing_delete.status);
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

test "mcp session store enforces capacity and reclaims expired sessions" {
    const alloc = std.testing.allocator;
    const Clock = struct {
        var now_ns: std.atomic.Value(u64) = .init(100);

        fn read() u64 {
            return now_ns.load(.acquire);
        }
    };

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var sessions = InMemorySessionStore.initWithOptions(alloc, io_impl.io(), .{
        .max_sessions = 2,
        .idle_ttl_ns = 10,
        .cleanup_interval_ns = 100,
        .now_ns_fn = Clock.read,
    });
    defer sessions.deinit(alloc);

    const first = try sessions.iface().create(alloc);
    defer alloc.free(first);
    const second = try sessions.iface().create(alloc);
    defer alloc.free(second);
    try std.testing.expectError(
        error.McpSessionCapacityExceeded,
        sessions.iface().create(alloc),
    );

    Clock.now_ns.store(110, .release);
    const replacement = try sessions.iface().create(alloc);
    defer alloc.free(replacement);
    try std.testing.expect(!sessions.iface().exists(first));
    try std.testing.expect(!sessions.iface().exists(second));
    try std.testing.expect(sessions.iface().exists(replacement));
}

test "mcp session close is synchronized across callers" {
    const alloc = std.testing.allocator;
    const session_count = 32;
    const worker_count = 4;

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var sessions = InMemorySessionStore.initWithOptions(alloc, io_impl.io(), .{});
    defer sessions.deinit(alloc);

    var ids: [session_count][]u8 = undefined;
    for (&ids) |*id| id.* = try sessions.iface().create(alloc);
    defer for (ids) |id| alloc.free(id);

    const Worker = struct {
        fn run(store: *InMemorySessionStore, assigned: []const []u8) void {
            for (assigned) |id| _ = store.iface().close(id);
        }
    };
    var workers: [worker_count]std.Thread = undefined;
    const per_worker = session_count / worker_count;
    for (&workers, 0..) |*worker, i| {
        worker.* = try std.Thread.spawn(.{}, Worker.run, .{
            &sessions,
            ids[i * per_worker .. (i + 1) * per_worker],
        });
    }
    for (&workers) |*worker| worker.join();
    for (ids) |id| try std.testing.expect(!sessions.iface().exists(id));
}

test "mcp stdio line dispatch frames responses" {
    const alloc = std.testing.allocator;
    var server = Server{};

    const response = (try server.handleStdioLine(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
    )).?;
    defer alloc.free(response);
    try std.testing.expect(std.mem.endsWith(u8, response, "\n"));
    try testing.expectResultSubset(alloc, response, "{\"protocolVersion\":\"2025-06-18\"}");

    try std.testing.expectEqual(@as(?[]u8, null), try server.handleStdioLine(alloc,
        \\{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}
    ));
}
