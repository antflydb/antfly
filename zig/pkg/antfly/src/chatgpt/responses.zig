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

//! ChatGPT Responses transport. Shared generation types remain provider-neutral.
const std = @import("std");
const httpx = @import("httpx");
const gen = @import("antfly_generating");
const manager = @import("manager.zig");
const protocol = @import("protocol.zig");
const RequestContext = @import("../inference/execution_context.zig").RequestContext;
const platform_time = @import("antfly_platform").time;

/// Request-owned diagnostic buffers survive transport cleanup. Only bounded
/// protocol identifiers are exposed; upstream messages and prompts stay private.
pub const Failure = struct {
    http_status: u16 = 0,
    code: [128]u8 = undefined,
    code_len: usize = 0,
    param: [128]u8 = undefined,
    param_len: usize = 0,
    request_id: [128]u8 = undefined,
    request_id_len: usize = 0,
    retry_after: [128]u8 = undefined,
    retry_after_len: usize = 0,
    fn identifier(buffer: []u8, value: ?[]const u8) usize {
        const input = value orelse return 0;
        if (input.len > buffer.len) return 0;
        for (input) |c| if (!std.ascii.isAlphanumeric(c) and std.mem.indexOfScalar(u8, "_-.[]", c) == null) return 0;
        @memcpy(buffer[0..input.len], input);
        return input.len;
    }
    pub fn summary(self: *const Failure) struct { http_status: u16, code: []const u8, param: []const u8, request_id: []const u8, retry_after: []const u8 } {
        return .{ .http_status = self.http_status, .code = self.code[0..self.code_len], .param = self.param[0..self.param_len], .request_id = self.request_id[0..self.request_id_len], .retry_after = self.retry_after[0..self.retry_after_len] };
    }
    fn capture(self: *Failure, value: ?std.json.Value) void {
        const object = value orelse return;
        if (object != .object) return;
        const detail = object.object.get("error") orelse object;
        if (detail != .object) return;
        if (detail.object.get("code")) |code| if (code == .string) {
            self.code_len = identifier(&self.code, code.string);
        };
        if (detail.object.get("param")) |param| if (param == .string) {
            self.param_len = identifier(&self.param, param.string);
        };
    }
};

pub const Provider = struct {
    failure: ?*Failure = null,
    pin: ?manager.Pin = null,
    http: *httpx.Client,
    registrations: *manager.Manager,
    owner: []const u8,
    connection_id: []const u8,
    timeout_ms: u64 = 120_000,
    cancellation: ?httpx.CancellationToken = null,
    request_context: ?RequestContext = null,
    tools_json: ?[]const u8 = null,
    tool_choice_json: ?[]const u8 = null,
    reasoning_effort: ?[]const u8 = null,
    max_response_bytes: usize = 16 * 1024 * 1024,
    pub fn generate(self: *Provider, a: std.mem.Allocator, model: []const u8, messages: []const gen.ChatMessage) !gen.GenerateResult {
        const ControlCancellation = struct {
            external: ?httpx.CancellationToken,
            context: ?RequestContext,
            fn cancelled(raw: *const anyopaque) bool {
                const c: *const @This() = @ptrCast(@alignCast(raw));
                if (c.external) |token| if (token.isCancelled()) return true;
                if (c.context) |context| if (context.cancellation) |token| return token.isCancelled();
                return false;
            }
        };
        const cancellation: ControlCancellation = .{ .external = self.cancellation, .context = self.request_context };
        var control: RequestContext = .{ .io = self.http.io, .deadline_ns = platform_time.monotonicNs() +| (self.timeout_ms *| std.time.ns_per_ms), .cancellation = .{ .ptr = &cancellation, .is_cancelled_fn = ControlCancellation.cancelled } };
        if (self.request_context) |context| if (context.deadline_ns) |deadline| {
            control.deadline_ns = @min(control.deadline_ns.?, deadline);
        };
        var lease = try self.registrations.leaseBoundWithContext(a, self.owner, self.connection_id, self.pin, control);
        defer lease.deinit();
        const Cancel = struct {
            lease: *manager.Lease,
            external: ?httpx.CancellationToken,
            fn cancelled(raw: *const anyopaque) bool {
                const c: *const @This() = @ptrCast(@alignCast(raw));
                return c.lease.cancelled() or if (c.external) |e| e.isCancelled() else false;
            }
        };
        var cancel: Cancel = .{ .lease = &lease, .external = httpx.CancellationToken.fromCallback(&cancellation, ControlCancellation.cancelled) };
        const body = try request(a, model, messages, self.tools_json, self.tool_choice_json, self.reasoning_effort);
        defer a.free(body);
        const bearer = try std.fmt.allocPrint(a, "Bearer {s}", .{lease.access_token});
        defer {
            std.crypto.secureZero(u8, bearer);
            a.free(bearer);
        }
        if (self.failure) |failure| failure.* = .{};
        var decoder: Decoder = .{ .alloc = a, .limit = self.max_response_bytes, .diagnostic = self.failure };
        defer decoder.deinit();
        var response = self.http.requestToWriter(.POST, protocol.resource ++ "/responses", .{
            .json = body,
            .headers = &.{.{ "Authorization", bearer }},
            .follow_redirects = false,
            .cookies_enabled = false,
            .max_retries = 0,
            .timeout_ms = try control.remainingTimeoutMs(),
            .max_response_size = self.max_response_bytes,
            .cancellation = httpx.CancellationToken.fromCallback(&cancel, Cancel.cancelled),
        }, &decoder, null, null) catch |err| {
            if (lease.cancelled()) return error.ChatGPTReconnectRequired;
            return err;
        };
        defer response.deinit();
        if (lease.cancelled()) return error.ChatGPTReconnectRequired;
        return decoder.result();
    }
};

fn quote(w: *std.Io.Writer, bytes: []const u8) !void {
    try w.print("{f}", .{std.json.fmt(bytes, .{})});
}
fn text(a: std.mem.Allocator, message: gen.ChatMessage) ![]u8 {
    var w: std.Io.Writer.Allocating = .init(a);
    errdefer w.deinit();
    if (message.content) |content| switch (content) {
        .text => |t| try w.writer.writeAll(t),
        .parts => |parts| for (parts) |part| switch (part) {
            .text => |t| try w.writer.writeAll(t),
            else => return error.ChatGPTUnsupportedCapability,
        },
    };
    return w.toOwnedSlice();
}
pub fn request(a: std.mem.Allocator, model: []const u8, messages: []const gen.ChatMessage, tools: ?[]const u8, choice: ?[]const u8, effort: ?[]const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(a);
    errdefer out.deinit();
    const w = &out.writer;
    try w.writeAll("{\"store\":false,\"stream\":true,\"include\":[\"reasoning.encrypted_content\"],\"model\":");
    try quote(w, model);
    try w.writeAll(",\"input\":[");
    var count: usize = 0;
    for (messages) |message| {
        if (message.responses_output_json) |raw| {
            if (message.role != .assistant) return error.InvalidToolMessage;
            var output = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
            defer output.deinit();
            if (output.value != .array) return error.InvalidToolMessage;
            for (output.value.array.items) |item| {
                if (item != .object) return error.InvalidToolMessage;
                const kind = item.object.get("type") orelse return error.InvalidToolMessage;
                if (kind != .string or !(std.mem.eql(u8, kind.string, "message") or std.mem.eql(u8, kind.string, "reasoning") or std.mem.eql(u8, kind.string, "function_call"))) return error.ChatGPTUnsupportedCapability;
                if (count != 0) try w.writeByte(',');
                count += 1;
                try w.print("{f}", .{std.json.fmt(item, .{})});
            }
            continue;
        }
        const content = try text(a, message);
        defer a.free(content);
        if (message.role == .tool) {
            if (count != 0) try w.writeByte(',');
            count += 1;
            try w.writeAll("{\"type\":\"function_call_output\",\"call_id\":");
            try quote(w, message.tool_call_id orelse return error.InvalidToolMessage);
            try w.writeAll(",\"output\":");
            try quote(w, content);
            try w.writeByte('}');
        } else if (content.len != 0) {
            if (count != 0) try w.writeByte(',');
            count += 1;
            try w.writeAll("{\"role\":");
            try quote(w, if (message.role == .system) "developer" else message.role.toSlice());
            try w.writeAll(",\"content\":");
            try quote(w, content);
            try w.writeByte('}');
        }
        if (message.tool_calls) |calls| for (calls) |call| {
            if (count != 0) try w.writeByte(',');
            count += 1;
            try w.writeAll("{\"type\":\"function_call\",\"namespace\":\"antfly\",\"call_id\":");
            try quote(w, call.id);
            try w.writeAll(",\"name\":");
            try quote(w, call.name);
            try w.writeAll(",\"arguments\":");
            try quote(w, call.arguments);
            try w.writeByte('}');
        };
    }
    try w.writeByte(']');
    if (tools) |raw| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
        defer parsed.deinit();
        if (parsed.value != .array) return error.ChatGPTUnsupportedCapability;
        try w.writeAll(",\"tools\":[{\"type\":\"namespace\",\"name\":\"antfly\",\"description\":\"Antfly database retrieval tools\",\"tools\":[");
        for (parsed.value.array.items, 0..) |tool, i| {
            if (tool != .object) return error.ChatGPTUnsupportedCapability;
            const kind = tool.object.get("type") orelse return error.ChatGPTUnsupportedCapability;
            if (kind != .string or !std.mem.eql(u8, kind.string, "function")) return error.ChatGPTUnsupportedCapability;
            const function = tool.object.get("function") orelse return error.ChatGPTUnsupportedCapability;
            if (function != .object) return error.ChatGPTUnsupportedCapability;
            if (i != 0) try w.writeByte(',');
            try w.writeAll("{\"type\":\"function\"");
            var it = function.object.iterator();
            while (it.next()) |entry| {
                try w.writeByte(',');
                try quote(w, entry.key_ptr.*);
                try w.writeByte(':');
                try w.print("{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
            }
            try w.writeByte('}');
        }
        try w.writeAll("]}]");
    }
    if (choice) |raw| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
        defer parsed.deinit();
        try w.writeAll(",\"tool_choice\":");
        if (parsed.value == .string) {
            try w.print("{f}", .{std.json.fmt(parsed.value, .{})});
        } else if (parsed.value == .object) {
            const function = parsed.value.object.get("function") orelse return error.ChatGPTUnsupportedCapability;
            if (function != .object) return error.ChatGPTUnsupportedCapability;
            const name = function.object.get("name") orelse return error.ChatGPTUnsupportedCapability;
            if (name != .string) return error.ChatGPTUnsupportedCapability;
            try w.writeAll("{\"type\":\"function\",\"namespace\":\"antfly\",\"name\":");
            try quote(w, name.string);
            try w.writeByte('}');
        } else return error.ChatGPTUnsupportedCapability;
    }
    if (effort) |e| {
        try w.writeAll(",\"reasoning\":{\"effort\":");
        try quote(w, e);
        try w.writeByte('}');
    }
    try w.writeByte('}');
    return out.toOwnedSlice();
}

/// Incremental SSE sink handles arbitrary transport chunking and CRLF. It keeps
/// only the current event and terminal response, bounded by the request ceiling.
pub const Decoder = struct {
    alloc: std.mem.Allocator,
    limit: usize = 16 * 1024 * 1024,
    received: usize = 0,
    status: u16 = 0,
    line: std.ArrayList(u8) = .empty,
    event: std.ArrayList(u8) = .empty,
    completed: ?[]u8 = null,
    failure: ?anyerror = null,
    diagnostic: ?*Failure = null,
    pub fn deinit(self: *Decoder) void {
        self.line.deinit(self.alloc);
        self.event.deinit(self.alloc);
        if (self.completed) |body| self.alloc.free(body);
    }
    pub fn startResponse(self: *Decoder, response: httpx.Response) !void {
        self.status = response.status.code;
        if (self.diagnostic) |d| {
            d.http_status = response.status.code;
            d.request_id_len = Failure.identifier(&d.request_id, response.header("x-request-id"));
            d.retry_after_len = Failure.identifier(&d.retry_after, response.header("retry-after"));
        }
    }
    pub fn writeAll(self: *Decoder, bytes: []const u8) !void {
        if (bytes.len > self.limit -| self.received) return error.ResponseTooLarge;
        self.received += bytes.len;
        for (bytes) |byte| {
            if (byte == '\n') {
                try self.finishLine();
            } else try self.line.append(self.alloc, byte);
        }
    }
    fn finishLine(self: *Decoder) !void {
        const line = std.mem.trimEnd(u8, self.line.items, "\r");
        if (self.status >= 400) {
            try self.event.appendSlice(self.alloc, self.line.items);
        } else if (line.len == 0) {
            if (self.event.items.len > 0) try self.finishEvent();
            self.event.clearRetainingCapacity();
        } else if (std.mem.startsWith(u8, line, "data:")) {
            if (self.event.items.len > 0) try self.event.append(self.alloc, '\n');
            try self.event.appendSlice(self.alloc, std.mem.trimStart(u8, line[5..], " "));
        }
        self.line.clearRetainingCapacity();
    }
    fn finishEvent(self: *Decoder) !void {
        const Event = struct { type: []const u8, response: ?std.json.Value = null, code: ?[]const u8 = null };
        var event = std.json.parseFromSlice(Event, self.alloc, self.event.items, .{ .ignore_unknown_fields = true }) catch return error.ChatGPTInvalidStream;
        defer event.deinit();
        if (self.completed != null or self.failure != null) return error.ChatGPTInvalidStream;
        if (std.mem.eql(u8, event.value.type, "response.completed")) {
            self.completed = try std.json.Stringify.valueAlloc(self.alloc, event.value.response orelse return error.ChatGPTInvalidStream, .{});
        } else if (std.mem.eql(u8, event.value.type, "response.failed")) {
            if (self.diagnostic) |d| d.capture(event.value.response);
            self.failure = responseFailure(event.value.response);
        } else if (std.mem.eql(u8, event.value.type, "response.incomplete")) self.failure = error.ChatGPTIncomplete else if (std.mem.eql(u8, event.value.type, "error")) self.failure = failureCode(event.value.code);
    }
    pub fn result(self: *Decoder) !gen.GenerateResult {
        if (self.status >= 400) {
            try self.event.appendSlice(self.alloc, self.line.items);
            var body = std.json.parseFromSlice(std.json.Value, self.alloc, self.event.items, .{}) catch return error.ChatGPTRequestFailed;
            defer body.deinit();
            if (self.diagnostic) |d| d.capture(body.value);
            if (self.status == 401) return error.ChatGPTReconnectRequired;
            return responseFailure(body.value);
        }
        // A terminal event must have a complete SSE frame. EOF cannot turn
        // a truncated event or stream into successful inference.
        if (self.failure) |err| return err;
        const raw = self.completed orelse return error.ChatGPTInterrupted;
        if (self.line.items.len != 0 or self.event.items.len != 0) return error.ChatGPTInvalidStream;
        const Item = struct { type: []const u8, call_id: ?[]const u8 = null, name: ?[]const u8 = null, namespace: ?[]const u8 = null, arguments: ?[]const u8 = null, content: ?[]const struct { type: []const u8, text: ?[]const u8 = null } = null };
        const Response = struct { status: []const u8, output: []const Item, usage: ?struct { input_tokens: u64 = 0, output_tokens: u64 = 0 } = null };
        var parsed = try std.json.parseFromSlice(Response, self.alloc, raw, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (!std.mem.eql(u8, parsed.value.status, "completed")) return error.ChatGPTInvalidStream;
        var content: std.Io.Writer.Allocating = .init(self.alloc);
        defer content.deinit();
        var calls: std.ArrayList(gen.ToolCall) = .empty;
        errdefer {
            for (calls.items) |*call| call.deinit(self.alloc);
            calls.deinit(self.alloc);
        }
        for (parsed.value.output) |item| {
            if (std.mem.eql(u8, item.type, "message")) {
                for (item.content orelse &.{}) |part| if (std.mem.eql(u8, part.type, "output_text")) {
                    try content.writer.writeAll(part.text orelse "");
                };
            } else if (std.mem.eql(u8, item.type, "function_call")) {
                if (item.namespace) |ns| if (!std.mem.eql(u8, ns, "antfly")) return error.ChatGPTUnsupportedCapability;
                const id = try self.alloc.dupe(u8, item.call_id orelse return error.ChatGPTInvalidStream);
                errdefer self.alloc.free(id);
                const name = try self.alloc.dupe(u8, item.name orelse return error.ChatGPTInvalidStream);
                errdefer self.alloc.free(name);
                const arguments = try self.alloc.dupe(u8, item.arguments orelse return error.ChatGPTInvalidStream);
                errdefer self.alloc.free(arguments);
                try calls.append(self.alloc, .{ .id = id, .name = name, .arguments = arguments });
            } else if (!std.mem.eql(u8, item.type, "reasoning")) return error.ChatGPTUnsupportedCapability;
        }
        var replay = try std.json.parseFromSlice(std.json.Value, self.alloc, raw, .{});
        defer replay.deinit();
        const output_json = try std.json.Stringify.valueAlloc(self.alloc, replay.value.object.get("output").?, .{});
        errdefer self.alloc.free(output_json);
        const answer = try content.toOwnedSlice();
        errdefer self.alloc.free(answer);
        return .{ .content = answer, .tool_calls = try calls.toOwnedSlice(self.alloc), .responses_output_json = output_json, .usage = if (parsed.value.usage) |u| .{ .input_tokens = u.input_tokens, .output_tokens = u.output_tokens } else null, .allocator = self.alloc };
    }
};
fn failureCode(code: ?[]const u8) anyerror {
    const c = code orelse return error.ChatGPTRequestFailed;
    if (std.mem.eql(u8, c, "subscription_sharing_usage_limit_exceeded")) return error.ChatGPTUsageLimitExceeded;
    if (std.mem.eql(u8, c, "subscription_sharing_user_not_eligible")) return error.ChatGPTNotEligible;
    if (std.mem.eql(u8, c, "subscription_sharing_usage_unavailable") or std.mem.eql(u8, c, "subscription_sharing_user_unavailable")) return error.ChatGPTUsageUnavailable;
    if (std.mem.eql(u8, c, "subscription_sharing_unsupported_capability")) return error.ChatGPTUnsupportedCapability;
    if (std.mem.eql(u8, c, "subscription_sharing_invalid_user")) return error.ChatGPTReconnectRequired;
    return error.ChatGPTRequestFailed;
}
fn responseFailure(value: ?std.json.Value) anyerror {
    const object = value orelse return error.ChatGPTRequestFailed;
    if (object != .object) return error.ChatGPTRequestFailed;
    const err = object.object.get("error") orelse return error.ChatGPTRequestFailed;
    if (err != .object) return error.ChatGPTRequestFailed;
    const code = err.object.get("code") orelse return error.ChatGPTRequestFailed;
    return failureCode(if (code == .string) code.string else null);
}

test "chatgpt responses rejects truncated and failed streams after deltas" {
    const a = std.testing.allocator;
    var decoder: Decoder = .{ .alloc = a, .status = 200 };
    defer decoder.deinit();
    const events = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\r\n\r\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"subscription_sharing_usage_limit_exceeded\"}}}\r\n\r\n";
    for (events) |byte| try decoder.writeAll(&.{byte});
    try std.testing.expectError(error.ChatGPTUsageLimitExceeded, decoder.result());
    var incomplete: Decoder = .{ .alloc = a, .status = 200 };
    defer incomplete.deinit();
    try incomplete.writeAll("data: {\"type\":\"response.completed\",\"response\":{}");
    try std.testing.expectError(error.ChatGPTInterrupted, incomplete.result());
}
test "chatgpt responses terminal output and tool history" {
    const a = std.testing.allocator;
    var decoder: Decoder = .{ .alloc = a, .status = 200 };
    defer decoder.deinit();
    try decoder.writeAll("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"function_call\",\"namespace\":\"antfly\",\"call_id\":\"c1\",\"name\":\"search\",\"arguments\":\"{}\"}],\"usage\":{\"input_tokens\":3,\"output_tokens\":4}}}\n\n");
    var result = try decoder.result();
    defer result.deinit();
    try std.testing.expectEqualStrings("c1", result.tool_calls[0].id);
    const body = try request(a, "model", &.{ .{ .role = .system, .content = .{ .text = "rules" } }, .{ .role = .assistant, .tool_calls = result.tool_calls }, .{ .role = .tool, .tool_call_id = "c1", .content = .{ .text = "rows" } } }, null, null, null);
    defer a.free(body);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("developer", parsed.value.object.get("input").?.array.items[0].object.get("role").?.string);
    try std.testing.expect(parsed.value.object.get("max_output_tokens") == null);
    try std.testing.expectEqualStrings("function_call_output", parsed.value.object.get("input").?.array.items[2].object.get("type").?.string);
}

test "chatgpt encrypted reasoning survives tool continuation and quota retains safe diagnostics" {
    const a = std.testing.allocator;
    var decoder: Decoder = .{ .alloc = a, .status = 200 };
    defer decoder.deinit();
    try decoder.writeAll("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"reasoning\",\"encrypted_content\":\"opaque-reasoning\",\"summary\":[]},{\"type\":\"function_call\",\"namespace\":\"antfly\",\"call_id\":\"c1\",\"name\":\"search\",\"arguments\":\"{}\"}]}}\n\n");
    var result = try decoder.result();
    defer result.deinit();
    const body = try request(a, "catalog-slug", &.{
        .{ .role = .assistant, .responses_output_json = result.responses_output_json, .tool_calls = result.tool_calls },
        .{ .role = .tool, .tool_call_id = "c1", .content = .{ .text = "retrieved" } },
    }, null, null, null);
    defer a.free(body);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, body, .{});
    defer parsed.deinit();
    const input = parsed.value.object.get("input").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), input.len);
    try std.testing.expectEqualStrings("opaque-reasoning", input[0].object.get("encrypted_content").?.string);
    try std.testing.expectEqualStrings("function_call_output", input[2].object.get("type").?.string);
    var diagnostic: Failure = .{};
    var failed: Decoder = .{ .alloc = a, .status = 200, .diagnostic = &diagnostic };
    defer failed.deinit();
    try failed.writeAll("data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"subscription_sharing_usage_limit_exceeded\",\"param\":\"model\",\"message\":\"private detail\"}}}\n\n");
    try std.testing.expectError(error.ChatGPTUsageLimitExceeded, failed.result());
    try std.testing.expectEqualStrings("subscription_sharing_usage_limit_exceeded", diagnostic.summary().code);
    try std.testing.expectEqualStrings("model", diagnostic.summary().param);
}
