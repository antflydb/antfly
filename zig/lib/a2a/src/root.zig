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

pub const protocol_version = "0.3.0";

pub const Skill = struct {
    id: []const u8,
    name: []const u8,
    description: []const u8,
    tags: []const []const u8 = &.{},
};

pub const RequestContext = struct {
    task_id: []const u8,
    context_id: []const u8,
    message: std.json.Value,
    metadata: ?std.json.Value = null,
};

pub const StreamSink = struct {
    ptr: *anyopaque,
    emit_fn: *const fn (*anyopaque, std.mem.Allocator, std.json.Value) anyerror!void,

    pub fn emit(self: StreamSink, alloc: std.mem.Allocator, event: std.json.Value) !void {
        try self.emit_fn(self.ptr, alloc, event);
    }
};

pub const EventQueue = struct {
    events: std.json.Array,
    sink: ?StreamSink = null,
    sink_alloc: ?std.mem.Allocator = null,

    pub fn init(alloc: std.mem.Allocator) EventQueue {
        return .{ .events = std.json.Array.init(alloc) };
    }

    pub fn initWithSink(alloc: std.mem.Allocator, sink: StreamSink, sink_alloc: std.mem.Allocator) EventQueue {
        return .{ .events = std.json.Array.init(alloc), .sink = sink, .sink_alloc = sink_alloc };
    }

    pub fn deinit(self: *EventQueue, alloc: std.mem.Allocator) void {
        _ = alloc;
        self.events.deinit();
    }

    pub fn status(self: *EventQueue, alloc: std.mem.Allocator, task_id: []const u8, context_id: []const u8, state: []const u8, message_text: ?[]const u8) !void {
        var status_obj = std.json.ObjectMap.empty;
        try status_obj.put(alloc, "state", .{ .string = state });
        if (message_text) |text| {
            try status_obj.put(alloc, "message", try textMessage(alloc, context_id, text));
        }

        var result = std.json.ObjectMap.empty;
        try result.put(alloc, "taskId", .{ .string = task_id });
        try result.put(alloc, "contextId", .{ .string = context_id });
        try result.put(alloc, "kind", .{ .string = "status-update" });
        try result.put(alloc, "status", .{ .object = status_obj });
        const event: std.json.Value = .{ .object = result };
        try self.events.append(event);
        if (self.sink) |sink| try sink.emit(self.sink_alloc orelse alloc, event);
    }

    pub fn artifact(self: *EventQueue, alloc: std.mem.Allocator, task_id: []const u8, context_id: []const u8, name: []const u8, parts: std.json.Value) !void {
        var artifact_obj = std.json.ObjectMap.empty;
        try artifact_obj.put(alloc, "name", .{ .string = name });
        try artifact_obj.put(alloc, "parts", parts);

        var result = std.json.ObjectMap.empty;
        try result.put(alloc, "taskId", .{ .string = task_id });
        try result.put(alloc, "contextId", .{ .string = context_id });
        try result.put(alloc, "kind", .{ .string = "artifact-update" });
        try result.put(alloc, "artifact", .{ .object = artifact_obj });
        const event: std.json.Value = .{ .object = result };
        try self.events.append(event);
        if (self.sink) |sink| try sink.emit(self.sink_alloc orelse alloc, event);
    }
};

pub const AgentHandler = struct {
    ptr: *anyopaque,
    skill_id_fn: *const fn (*anyopaque) []const u8,
    skill_fn: *const fn (*anyopaque, std.mem.Allocator) anyerror!Skill,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, RequestContext, *EventQueue) anyerror!void,
    cancel_fn: ?*const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!void = null,

    pub fn skillId(self: AgentHandler) []const u8 {
        return self.skill_id_fn(self.ptr);
    }

    pub fn skill(self: AgentHandler, alloc: std.mem.Allocator) !Skill {
        return try self.skill_fn(self.ptr, alloc);
    }

    pub fn execute(self: AgentHandler, alloc: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
        try self.execute_fn(self.ptr, alloc, ctx, queue);
    }

    pub fn cancel(self: AgentHandler, alloc: std.mem.Allocator, task_id: []const u8) !void {
        if (self.cancel_fn) |cancel_fn| try cancel_fn(self.ptr, alloc, task_id);
    }
};

pub const TaskStore = struct {
    ptr: *anyopaque,
    save_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, std.json.Value) anyerror!void,
    get_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!std.json.Value,
    cancel_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!std.json.Value,

    pub fn save(self: TaskStore, alloc: std.mem.Allocator, task_id: []const u8, task: std.json.Value) !void {
        try self.save_fn(self.ptr, alloc, task_id, task);
    }

    pub fn get(self: TaskStore, alloc: std.mem.Allocator, task_id: []const u8) !std.json.Value {
        return try self.get_fn(self.ptr, alloc, task_id);
    }

    pub fn cancel(self: TaskStore, alloc: std.mem.Allocator, task_id: []const u8) !std.json.Value {
        return try self.cancel_fn(self.ptr, alloc, task_id);
    }
};

pub const InMemoryTaskStore = struct {
    alloc: ?std.mem.Allocator = null,
    tasks: std.StringHashMapUnmanaged([]u8) = .{},

    pub fn init(alloc: std.mem.Allocator) InMemoryTaskStore {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *InMemoryTaskStore, fallback_alloc: std.mem.Allocator) void {
        const alloc = self.alloc orelse fallback_alloc;
        var it = self.tasks.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            alloc.free(entry.value_ptr.*);
        }
        self.tasks.deinit(alloc);
        self.* = .{};
    }

    pub fn iface(self: *InMemoryTaskStore) TaskStore {
        return .{
            .ptr = self,
            .save_fn = save,
            .get_fn = get,
            .cancel_fn = cancel,
        };
    }

    fn save(ptr: *anyopaque, alloc: std.mem.Allocator, task_id: []const u8, task: std.json.Value) !void {
        const self: *InMemoryTaskStore = @ptrCast(@alignCast(ptr));
        const store_alloc = self.alloc orelse alloc;
        const body = try stringifyValue(store_alloc, task);
        errdefer store_alloc.free(body);
        if (self.tasks.getEntry(task_id)) |entry| {
            store_alloc.free(entry.value_ptr.*);
            entry.value_ptr.* = body;
            return;
        }
        const owned_key = try store_alloc.dupe(u8, task_id);
        errdefer store_alloc.free(owned_key);
        try self.tasks.put(store_alloc, owned_key, body);
    }

    fn get(ptr: *anyopaque, alloc: std.mem.Allocator, task_id: []const u8) !std.json.Value {
        const self: *InMemoryTaskStore = @ptrCast(@alignCast(ptr));
        const body = self.tasks.get(task_id) orelse return error.TaskNotFound;
        return try std.json.parseFromSliceLeaky(std.json.Value, alloc, body, .{});
    }

    fn cancel(ptr: *anyopaque, alloc: std.mem.Allocator, task_id: []const u8) !std.json.Value {
        const self: *InMemoryTaskStore = @ptrCast(@alignCast(ptr));
        var task = try get(self, alloc, task_id);
        if (task != .object) return error.TaskNotFound;
        var status = std.json.ObjectMap.empty;
        try status.put(alloc, "state", .{ .string = "canceled" });
        try task.object.put(alloc, "status", .{ .object = status });
        try save(self, alloc, task_id, task);
        return task;
    }
};

pub const Dispatcher = struct {
    name: []const u8 = "Antfly",
    version: []const u8 = "1.0.0",
    base_url: []const u8 = "",
    handlers: std.ArrayListUnmanaged(AgentHandler) = .empty,
    task_store: ?TaskStore = null,

    pub fn deinit(self: *Dispatcher, alloc: std.mem.Allocator) void {
        self.handlers.deinit(alloc);
    }

    pub fn addHandler(self: *Dispatcher, alloc: std.mem.Allocator, handler: AgentHandler) !void {
        try self.handlers.append(alloc, handler);
    }

    pub fn handleJsonRpc(self: *Dispatcher, alloc: std.mem.Allocator, body: []const u8) ![]u8 {
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
        const id = root.get("id") orelse .null;
        const method = stringField(root, "method") orelse {
            return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32600, "invalid request"));
        };
        if (std.mem.eql(u8, method, "agent/getAuthenticatedExtendedCard")) {
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, try self.agentCard(temp_alloc)));
        }
        if (std.mem.eql(u8, method, "message/send") or std.mem.eql(u8, method, "message/stream")) {
            const params = root.get("params") orelse .null;
            const result = self.executeMessage(temp_alloc, params, std.mem.eql(u8, method, "message/stream")) catch |err| switch (err) {
                error.InvalidParams => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "invalid params")),
                error.UnknownSkill => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "unknown skill")),
                else => return err,
            };
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, result));
        }
        if (std.mem.eql(u8, method, "tasks/get")) {
            const params = root.get("params") orelse .null;
            const task_id = taskIdParam(params) orelse {
                return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "invalid params"));
            };
            const store = self.task_store orelse return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32001, "task storage not configured"));
            const task = store.get(temp_alloc, task_id) catch |err| switch (err) {
                error.TaskNotFound => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32004, "task not found")),
                else => return err,
            };
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, task));
        }
        if (std.mem.eql(u8, method, "tasks/cancel")) {
            const params = root.get("params") orelse .null;
            const task_id = taskIdParam(params) orelse {
                return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "invalid params"));
            };
            const store = self.task_store orelse return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32001, "task storage not configured"));
            const task = store.cancel(temp_alloc, task_id) catch |err| switch (err) {
                error.TaskNotFound => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32004, "task not found")),
                else => return err,
            };
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, task));
        }
        return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32601, "method not found"));
    }

    pub fn handleJsonRpcStream(self: *Dispatcher, alloc: std.mem.Allocator, body: []const u8, sink: StreamSink) !void {
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const temp_alloc = arena_impl.allocator();

        const request = std.json.parseFromSliceLeaky(std.json.Value, temp_alloc, body, .{}) catch {
            try sink.emit(alloc, try errorResponse(temp_alloc, .null, -32700, "parse error"));
            return;
        };
        if (request != .object) {
            try sink.emit(alloc, try errorResponse(temp_alloc, .null, -32600, "invalid request"));
            return;
        }

        const root = request.object;
        const id = root.get("id") orelse .null;
        const method = stringField(root, "method") orelse {
            try sink.emit(alloc, try errorResponse(temp_alloc, id, -32600, "invalid request"));
            return;
        };
        if (!std.mem.eql(u8, method, "message/stream")) {
            try sink.emit(alloc, try errorResponse(temp_alloc, id, -32601, "method not found"));
            return;
        }

        const params = root.get("params") orelse .null;
        self.executeMessageStream(temp_alloc, params, sink, alloc) catch |err| switch (err) {
            error.InvalidParams => try sink.emit(alloc, try errorResponse(temp_alloc, id, -32602, "invalid params")),
            error.UnknownSkill => try sink.emit(alloc, try errorResponse(temp_alloc, id, -32602, "unknown skill")),
            else => return err,
        };
    }

    pub fn agentCard(self: *Dispatcher, alloc: std.mem.Allocator) !std.json.Value {
        var skills = std.json.Array.init(alloc);
        for (self.handlers.items) |handler| {
            const skill_info = try handler.skill(alloc);
            try skills.append(try skillValue(alloc, skill_info));
        }

        var capabilities = std.json.ObjectMap.empty;
        try capabilities.put(alloc, "streaming", .{ .bool = true });
        try capabilities.put(alloc, "stateTransitionHistory", .{ .bool = true });

        var modes = std.json.Array.init(alloc);
        try modes.append(.{ .string = "text" });
        try modes.append(.{ .string = "data" });

        var card = std.json.ObjectMap.empty;
        try card.put(alloc, "name", .{ .string = self.name });
        try card.put(alloc, "url", .{ .string = self.base_url });
        try card.put(alloc, "version", .{ .string = self.version });
        try card.put(alloc, "protocolVersion", .{ .string = protocol_version });
        try card.put(alloc, "preferredTransport", .{ .string = "JSONRPC" });
        try card.put(alloc, "capabilities", .{ .object = capabilities });
        try card.put(alloc, "defaultInputModes", .{ .array = modes });
        try card.put(alloc, "defaultOutputModes", try textDataModes(alloc));
        try card.put(alloc, "skills", .{ .array = skills });
        return .{ .object = card };
    }

    fn executeMessage(self: *Dispatcher, alloc: std.mem.Allocator, params: std.json.Value, stream: bool) !std.json.Value {
        if (params != .object) return error.InvalidParams;
        const message = params.object.get("message") orelse return error.InvalidParams;
        const task_id = stringField(params.object, "taskId") orelse stringField(params.object, "id") orelse "task";
        const context_id = stringField(params.object, "contextId") orelse "default";
        const metadata = params.object.get("metadata");
        const handler = self.resolve(message, metadata) orelse return error.UnknownSkill;

        var queue = EventQueue.init(alloc);
        try handler.execute(alloc, .{
            .task_id = task_id,
            .context_id = context_id,
            .message = message,
            .metadata = metadata,
        }, &queue);

        const task = try taskFromEvents(alloc, task_id, context_id, queue.events.items);
        if (self.task_store) |store| try store.save(alloc, task_id, task);
        if (stream) {
            return .{ .array = queue.events };
        }
        return task;
    }

    fn executeMessageStream(self: *Dispatcher, alloc: std.mem.Allocator, params: std.json.Value, sink: StreamSink, sink_alloc: std.mem.Allocator) !void {
        if (params != .object) return error.InvalidParams;
        const message = params.object.get("message") orelse return error.InvalidParams;
        const task_id = stringField(params.object, "taskId") orelse stringField(params.object, "id") orelse "task";
        const context_id = stringField(params.object, "contextId") orelse "default";
        const metadata = params.object.get("metadata");
        const handler = self.resolve(message, metadata) orelse return error.UnknownSkill;

        var queue = EventQueue.initWithSink(alloc, sink, sink_alloc);
        try handler.execute(alloc, .{
            .task_id = task_id,
            .context_id = context_id,
            .message = message,
            .metadata = metadata,
        }, &queue);

        const task = try taskFromEvents(alloc, task_id, context_id, queue.events.items);
        if (self.task_store) |store| try store.save(alloc, task_id, task);
    }

    pub fn resolve(self: *const Dispatcher, message: std.json.Value, request_metadata: ?std.json.Value) ?AgentHandler {
        if (metadataSkill(message)) |skill| {
            if (self.findSkill(skill)) |handler| return handler;
        }
        if (request_metadata) |metadata| {
            if (metadata == .object) {
                if (stringField(metadata.object, "skill")) |skill| {
                    if (self.findSkill(skill)) |handler| return handler;
                }
            }
        }
        if (self.handlers.items.len == 1) return self.handlers.items[0];
        return null;
    }

    fn findSkill(self: *const Dispatcher, skill: []const u8) ?AgentHandler {
        for (self.handlers.items) |handler| {
            if (std.mem.eql(u8, handler.skillId(), skill)) return handler;
        }
        return null;
    }
};

pub fn messageText(alloc: std.mem.Allocator, message: std.json.Value) ![]const u8 {
    if (message != .object) return "";
    const parts = message.object.get("parts") orelse return "";
    if (parts != .array) return "";
    var out = std.ArrayListUnmanaged(u8).empty;
    for (parts.array.items) |part| {
        if (part != .object) continue;
        const kind = stringField(part.object, "kind") orelse stringField(part.object, "type") orelse "";
        if (!std.mem.eql(u8, kind, "text")) continue;
        const text = stringField(part.object, "text") orelse "";
        if (out.items.len != 0 and text.len != 0) try out.append(alloc, '\n');
        try out.appendSlice(alloc, text);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn firstDataPart(message: std.json.Value) ?std.json.Value {
    if (message != .object) return null;
    const parts = message.object.get("parts") orelse return null;
    if (parts != .array) return null;
    for (parts.array.items) |part| {
        if (part != .object) continue;
        const kind = stringField(part.object, "kind") orelse stringField(part.object, "type") orelse "";
        if (!std.mem.eql(u8, kind, "data")) continue;
        return part.object.get("data") orelse part.object.get("value");
    }
    return null;
}

pub fn textPart(alloc: std.mem.Allocator, text: []const u8) !std.json.Value {
    var part = std.json.ObjectMap.empty;
    try part.put(alloc, "kind", .{ .string = "text" });
    try part.put(alloc, "text", .{ .string = text });
    var parts = std.json.Array.init(alloc);
    try parts.append(.{ .object = part });
    return .{ .array = parts };
}

pub fn dataPart(alloc: std.mem.Allocator, data: std.json.Value) !std.json.Value {
    var part = std.json.ObjectMap.empty;
    try part.put(alloc, "kind", .{ .string = "data" });
    try part.put(alloc, "data", data);
    var parts = std.json.Array.init(alloc);
    try parts.append(.{ .object = part });
    return .{ .array = parts };
}

fn taskIdParam(params: std.json.Value) ?[]const u8 {
    if (params != .object) return null;
    return stringField(params.object, "taskId") orelse stringField(params.object, "id");
}

fn taskFromEvents(alloc: std.mem.Allocator, task_id: []const u8, context_id: []const u8, events: []const std.json.Value) !std.json.Value {
    var artifacts = std.json.Array.init(alloc);
    var status: ?std.json.Value = null;

    for (events) |event| {
        if (event != .object) continue;
        const kind = stringField(event.object, "kind") orelse "";
        if (std.mem.eql(u8, kind, "artifact-update")) {
            if (event.object.get("artifact")) |artifact| try artifacts.append(artifact);
        } else if (std.mem.eql(u8, kind, "status-update")) {
            status = event.object.get("status");
        }
    }

    if (status == null) {
        var submitted = std.json.ObjectMap.empty;
        try submitted.put(alloc, "state", .{ .string = "submitted" });
        status = .{ .object = submitted };
    }

    var task = std.json.ObjectMap.empty;
    try task.put(alloc, "id", .{ .string = task_id });
    try task.put(alloc, "contextId", .{ .string = context_id });
    try task.put(alloc, "status", status.?);
    try task.put(alloc, "artifacts", .{ .array = artifacts });
    return .{ .object = task };
}

fn metadataSkill(message: std.json.Value) ?[]const u8 {
    if (message != .object) return null;
    const metadata = message.object.get("metadata") orelse return null;
    if (metadata != .object) return null;
    return stringField(metadata.object, "skill");
}

fn textMessage(alloc: std.mem.Allocator, context_id: []const u8, text: []const u8) !std.json.Value {
    var message = std.json.ObjectMap.empty;
    try message.put(alloc, "kind", .{ .string = "message" });
    try message.put(alloc, "role", .{ .string = "agent" });
    try message.put(alloc, "contextId", .{ .string = context_id });
    try message.put(alloc, "parts", try textPart(alloc, text));
    return .{ .object = message };
}

fn skillValue(alloc: std.mem.Allocator, skill: Skill) !std.json.Value {
    var tags = std.json.Array.init(alloc);
    for (skill.tags) |tag| try tags.append(.{ .string = tag });

    var input_modes = std.json.Array.init(alloc);
    try input_modes.append(.{ .string = "text" });
    try input_modes.append(.{ .string = "data" });

    var out = std.json.ObjectMap.empty;
    try out.put(alloc, "id", .{ .string = skill.id });
    try out.put(alloc, "name", .{ .string = skill.name });
    try out.put(alloc, "description", .{ .string = skill.description });
    try out.put(alloc, "tags", .{ .array = tags });
    try out.put(alloc, "inputModes", .{ .array = input_modes });
    try out.put(alloc, "outputModes", try textDataModes(alloc));
    return .{ .object = out };
}

fn textDataModes(alloc: std.mem.Allocator) !std.json.Value {
    var modes = std.json.Array.init(alloc);
    try modes.append(.{ .string = "text" });
    try modes.append(.{ .string = "data" });
    return .{ .array = modes };
}

fn stringField(object: anytype, key: []const u8) ?[]const u8 {
    const value = object.get(key) orelse return null;
    return if (value == .string) value.string else null;
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

test "a2a dispatches message by metadata skill" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "echo";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "echo", .name = "Echo", .description = "Echo skill" };
        }
        fn execute(_: *anyopaque, a: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
            const text = try messageText(a, ctx.message);
            try queue.artifact(a, ctx.task_id, ctx.context_id, "echo", try textPart(a, text));
            try queue.status(a, ctx.task_id, ctx.context_id, "completed", text);
        }
    };

    var ctx: u8 = 0;
    var dispatcher = Dispatcher{ .base_url = "http://127.0.0.1/a2a" };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });

    const body =
        \\{"jsonrpc":"2.0","id":1,"method":"message/stream","params":{"taskId":"t1","contextId":"c1","message":{"kind":"message","role":"user","metadata":{"skill":"echo"},"parts":[{"kind":"text","text":"hello"}]}}}
    ;
    const resp = try dispatcher.handleJsonRpc(alloc, body);
    defer alloc.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "\"kind\":\"artifact-update\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "\"state\":\"completed\"") != null);
}

test "a2a message stream emits queue events through sink" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "echo";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "echo", .name = "Echo", .description = "Echo skill" };
        }
        fn execute(_: *anyopaque, a: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
            try queue.artifact(a, ctx.task_id, ctx.context_id, "echo", try textPart(a, "hello"));
            try queue.status(a, ctx.task_id, ctx.context_id, "completed", "done");
        }
    };
    const Sink = struct {
        out: std.ArrayListUnmanaged(u8) = .empty,

        fn iface(self: *@This()) StreamSink {
            return .{ .ptr = self, .emit_fn = emit };
        }

        fn emit(ptr: *anyopaque, a: std.mem.Allocator, event: std.json.Value) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const line = try stringifyValue(a, event);
            defer a.free(line);
            try self.out.appendSlice(a, line);
            try self.out.append(a, '\n');
        }
    };

    var store = InMemoryTaskStore.init(alloc);
    defer store.deinit(alloc);

    var ctx: u8 = 0;
    var dispatcher = Dispatcher{ .task_store = store.iface() };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });

    var sink = Sink{};
    defer sink.out.deinit(alloc);
    try dispatcher.handleJsonRpcStream(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"message/stream","params":{"taskId":"t1","contextId":"c1","message":{"kind":"message","role":"user","parts":[{"kind":"text","text":"hi"}]}}}
    , sink.iface());
    try std.testing.expect(std.mem.indexOf(u8, sink.out.items, "\"kind\":\"artifact-update\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, sink.out.items, "\"kind\":\"status-update\"") != null);

    const get_resp = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":2,"method":"tasks/get","params":{"id":"t1"}}
    );
    defer alloc.free(get_resp);
    try std.testing.expect(std.mem.indexOf(u8, get_resp, "\"state\":\"completed\"") != null);
}

test "a2a agent card lists skills" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "s";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "s", .name = "Skill", .description = "Desc" };
        }
        fn execute(_: *anyopaque, _: std.mem.Allocator, _: RequestContext, _: *EventQueue) !void {}
    };

    var ctx: u8 = 0;
    var dispatcher = Dispatcher{ .base_url = "http://example/a2a" };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });
    const resp = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":"card","method":"agent/getAuthenticatedExtendedCard","params":{}}
    );
    defer alloc.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "\"protocolVersion\":\"0.3.0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "\"id\":\"s\"") != null);
}

test "a2a maps malformed requests, unknown skills, and missing tasks to JSON-RPC errors" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "echo";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "echo", .name = "Echo", .description = "Echo skill" };
        }
        fn execute(_: *anyopaque, a: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
            try queue.status(a, ctx.task_id, ctx.context_id, "completed", "done");
        }
    };
    const OtherHandler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "other";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "other", .name = "Other", .description = "Other skill" };
        }
    };

    var store = InMemoryTaskStore.init(alloc);
    defer store.deinit(alloc);
    var ctx: u8 = 0;
    var dispatcher = Dispatcher{ .task_store = store.iface() };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = OtherHandler.skillId,
        .skill_fn = OtherHandler.skill,
        .execute_fn = Handler.execute,
    });

    const parse_resp = try dispatcher.handleJsonRpc(alloc, "{");
    defer alloc.free(parse_resp);
    try std.testing.expect(std.mem.indexOf(u8, parse_resp, "\"code\":-32700") != null);

    const invalid_params = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"message/send","params":[]}
    );
    defer alloc.free(invalid_params);
    try std.testing.expect(std.mem.indexOf(u8, invalid_params, "\"code\":-32602") != null);
    try std.testing.expect(std.mem.indexOf(u8, invalid_params, "\"message\":\"invalid params\"") != null);

    const unknown_skill = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":2,"method":"message/send","params":{"taskId":"t1","message":{"kind":"message","role":"user","metadata":{"skill":"missing"},"parts":[{"kind":"text","text":"hi"}]}}}
    );
    defer alloc.free(unknown_skill);
    try std.testing.expect(std.mem.indexOf(u8, unknown_skill, "\"code\":-32602") != null);
    try std.testing.expect(std.mem.indexOf(u8, unknown_skill, "\"message\":\"unknown skill\"") != null);

    const missing_task = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":3,"method":"tasks/get","params":{"id":"missing"}}
    );
    defer alloc.free(missing_task);
    try std.testing.expect(std.mem.indexOf(u8, missing_task, "\"code\":-32004") != null);
    try std.testing.expect(std.mem.indexOf(u8, missing_task, "\"message\":\"task not found\"") != null);
}

test "a2a task store supports get and cancel" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "echo";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "echo", .name = "Echo", .description = "Echo skill" };
        }
        fn execute(_: *anyopaque, a: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
            try queue.artifact(a, ctx.task_id, ctx.context_id, "echo", try textPart(a, "hello"));
            try queue.status(a, ctx.task_id, ctx.context_id, "completed", "done");
        }
    };

    var store = InMemoryTaskStore.init(alloc);
    defer store.deinit(alloc);

    var ctx: u8 = 0;
    var dispatcher = Dispatcher{
        .base_url = "http://127.0.0.1/a2a",
        .task_store = store.iface(),
    };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });

    const send_resp = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"message/send","params":{"taskId":"t1","contextId":"c1","message":{"kind":"message","role":"user","parts":[{"kind":"text","text":"hi"}]}}}
    );
    defer alloc.free(send_resp);
    try std.testing.expect(std.mem.indexOf(u8, send_resp, "\"id\":\"t1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, send_resp, "\"state\":\"completed\"") != null);

    const get_resp = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":2,"method":"tasks/get","params":{"id":"t1"}}
    );
    defer alloc.free(get_resp);
    try std.testing.expect(std.mem.indexOf(u8, get_resp, "\"name\":\"echo\"") != null);

    const cancel_resp = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":3,"method":"tasks/cancel","params":{"id":"t1"}}
    );
    defer alloc.free(cancel_resp);
    try std.testing.expect(std.mem.indexOf(u8, cancel_resp, "\"state\":\"canceled\"") != null);
}
