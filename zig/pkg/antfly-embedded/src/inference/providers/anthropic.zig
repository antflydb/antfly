// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Native Anthropic Messages transport. Request-local state borrows the service
//! HTTP pool; secret resolution, quotas and cancellation stay with the factory.
const std = @import("std");
const httpx = @import("httpx");
const lib = @import("antfly_generating");
const inference = @import("antfly_inference_types");
const sse = @import("../../common/inference_sse.zig");
const Json = std.json.Value;
const Allocator = std.mem.Allocator;

pub const Provider = struct {
    http: *httpx.Client,
    base_url: []const u8,
    api_key: ?[]const u8 = null,
    cfg: lib.GeneratorConfig,
    timeout_ms: ?u64 = null,
    cancellation: ?httpx.CancellationToken = null,
    attempt_observer: ?httpx.AttemptObserver = null,
    max_response_bytes: ?usize = null,

    pub fn deinit(_: *Provider) void {}
    pub fn setRequestControl(self: *Provider, timeout: ?u64, cancellation: ?httpx.CancellationToken) void {
        self.timeout_ms = timeout;
        self.cancellation = cancellation;
    }
    fn request(self: *Provider, a: Allocator, messages: []const lib.ChatMessage, model: []const u8, streaming: bool, writer: anytype) !httpx.Response {
        const key = self.api_key orelse return error.MissingGeneratorCredentials;
        const body = try requestJsonAlloc(a, self.cfg, model, messages, streaming);
        defer a.free(body);
        const base = std.mem.trimEnd(u8, self.base_url, "/");
        const url = try std.fmt.allocPrint(a, "{s}/messages", .{base});
        defer a.free(url);
        var headers: [3][2][]const u8 = undefined;
        headers[0] = .{ "x-api-key", key };
        headers[1] = .{ "anthropic-version", @import("antfly_inference_provider_defaults").anthropic_api_version };
        var count: usize = 2;
        if (self.cfg.workspace_id orelse @import("antfly_platform").env.getenv("ANTHROPIC_WORKSPACE_ID")) |workspace| {
            headers[count] = .{ "anthropic-workspace-id", workspace };
            count += 1;
        }
        const options = httpx.RequestOptions{
            .headers = headers[0..count],
            .json = body,
            .timeout_ms = self.timeout_ms,
            .cancellation = self.cancellation,
            .attempt_observer = self.attempt_observer,
            .max_response_size = self.max_response_bytes,
            .max_retries = 0,
            .follow_redirects = false,
            .cookies_enabled = false,
        };
        if (comptime @TypeOf(writer) == @TypeOf(null)) return self.http.post(url, options);
        return self.http.requestToWriter(.POST, url, options, writer, null, null);
    }
    pub fn generate(self: *Provider, a: Allocator, model: []const u8, messages: []const lib.ChatMessage) !inference.GenerateResult {
        var response = try self.request(a, messages, model, false, null);
        defer response.deinit();
        if (!response.ok()) return statusError(response.status.code);
        return responseAlloc(a, response.body orelse return error.InvalidInferenceResponse);
    }
    pub fn chatResponse(self: *Provider, a: Allocator, request_: lib.ChatRequest) !httpx.Response {
        if (request_.stream != null) return self.chatStream(a, request_);
        var result = try self.generate(a, self.cfg.model, request_.messages);
        defer result.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const b = arena.allocator();
        const Call = struct { id: []const u8, type: []const u8 = "function", function: struct { name: []const u8, arguments: []const u8 } };
        const calls = try b.alloc(Call, result.tool_calls.len);
        for (result.tool_calls, calls) |call, *out| out.* = .{ .id = call.id, .function = .{ .name = call.name, .arguments = call.arguments } };
        var extra: ?Json = null;
        if (result.anthropic_content_json) |native| extra = try single(b, "anthropic", try single(b, "content", try std.json.parseFromSliceLeaky(Json, b, native, .{})));
        const id = try std.fmt.allocPrint(b, "chatcmpl-anthropic-{d}", .{std.Io.Clock.awake.now(self.http.io).nanoseconds});
        const body = try std.json.Stringify.valueAlloc(a, .{
            .id = id,
            .object = "chat.completion",
            .created = @as(u64, @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_s)),
            .model = self.cfg.model,
            .choices = &.{.{ .index = @as(u32, 0), .message = .{ .role = "assistant", .content = if (calls.len > 0 and result.content.len == 0) @as(?[]const u8, null) else result.content, .tool_calls = if (calls.len > 0) calls else @as(?[]Call, null), .extra_content = extra }, .finish_reason = @tagName(result.finish_reason.?) }},
            .usage = if (result.usage) |counts| .{ .prompt_tokens = counts.input_tokens, .completion_tokens = counts.output_tokens, .total_tokens = counts.total_tokens.? } else null,
        }, .{ .emit_null_optional_fields = false });
        errdefer a.free(body);
        var response = httpx.Response.init(a, 200);
        errdefer response.deinit();
        try response.headers.set("content-type", "application/json");
        response.body = body;
        response.body_owned = true;
        response.body_allocator = a;
        return response;
    }

    pub fn chatStream(self: *Provider, a: Allocator, request_: lib.ChatRequest) !httpx.Response {
        const sink = request_.stream orelse return error.UnsupportedInferenceFeature;
        var parsed = try std.json.parseFromSlice(Json, a, request_.body_json, .{});
        defer parsed.deinit();
        const include_usage = if (parsed.value.object.get("stream_options")) |options|
            if (options == .object) if (options.object.get("include_usage")) |value| value == .bool and value.bool else false else false
        else
            false;
        const id = try std.fmt.allocPrint(a, "chatcmpl-anthropic-{d}", .{std.Io.Clock.awake.now(self.http.io).nanoseconds});
        defer a.free(id);
        const Writer = struct {
            decoder: Decoder,
            pub fn startResponse(self_: *@This(), response: httpx.Response) !void {
                if (!response.ok()) return statusError(response.status.code);
                if (!sse.isEventStream(response.contentType() orelse "")) return error.InvalidInferenceResponse;
                self_.decoder.request_id = response.header("request-id");
            }
            pub fn writeAll(self_: *@This(), bytes: []const u8) !void {
                try self_.decoder.push(bytes);
            }
        };
        var writer = Writer{ .decoder = .{ .alloc = std.heap.smp_allocator, .sink = sink, .model = self.cfg.model, .id = id, .include_usage = include_usage, .created = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_s) } };
        defer writer.decoder.deinit();
        var response = try self.request(a, request_.messages, self.cfg.model, true, &writer);
        errdefer response.deinit();
        try response.headers.set("content-type", "text/event-stream");
        response.releaseBody();
        if (writer.decoder.usage) |value| {
            response.body = try std.json.Stringify.valueAlloc(a, .{ .usage = .{ .prompt_tokens = value.input_tokens, .completion_tokens = value.output_tokens, .total_tokens = value.total_tokens.? } }, .{});
            response.body_owned = true;
            response.body_allocator = a;
        }
        try writer.decoder.finish();
        return response;
    }
};

fn statusError(status: u16) anyerror {
    return switch (status) {
        400, 413, 422 => error.InvalidInferenceRequest,
        429 => error.InferenceUpstreamRateLimited,
        500, 502, 503, 529 => error.InferenceUpstreamUnavailable,
        504 => error.Timeout,
        else => error.GenerateRequestFailed,
    };
}
fn object() Json {
    return .{ .object = .empty };
}
fn array(a: Allocator) Json {
    return .{ .array = .init(a) };
}
fn str(value: []const u8) Json {
    return .{ .string = value };
}
fn put(a: Allocator, value: *Json, key: []const u8, field: Json) !void {
    try value.object.put(a, key, field);
}
fn single(a: Allocator, key: []const u8, value: Json) !Json {
    var out = object();
    try put(a, &out, key, value);
    return out;
}
fn get(value: Json, key: []const u8) !Json {
    if (value != .object) return error.InvalidInferenceResponse;
    return value.object.get(key) orelse error.InvalidInferenceResponse;
}
fn string(value: Json) ![]const u8 {
    if (value != .string) return error.InvalidInferenceResponse;
    return value.string;
}
fn requestGet(value: Json, key: []const u8) !Json {
    return get(value, key) catch return error.InvalidGeneratorConfig;
}
fn text(a: Allocator, value: []const u8) !Json {
    var result = try single(a, "type", str("text"));
    try put(a, &result, "text", str(value));
    return result;
}
fn appendText(a: Allocator, parts: *Json, value: []const u8) !void {
    if (std.mem.trim(u8, value, " \t\r\n").len != 0) try parts.array.append(try text(a, value));
}
fn image(a: Allocator, mime: []const u8, bytes: []const u8, remote: bool) !Json {
    var source = try single(a, "type", str(if (remote) "url" else "base64"));
    if (remote) {
        if (!std.mem.startsWith(u8, bytes, "https://") and !std.mem.startsWith(u8, bytes, "http://")) return error.InvalidInferenceRequest;
        try put(a, &source, "url", str(bytes));
    } else {
        if (!std.mem.eql(u8, mime, "image/jpeg") and !std.mem.eql(u8, mime, "image/png") and !std.mem.eql(u8, mime, "image/gif") and !std.mem.eql(u8, mime, "image/webp")) return error.UnsupportedInferenceFeature;
        if (bytes.len == 0) return error.InvalidInferenceRequest;
        try put(a, &source, "media_type", str(mime));
        try put(a, &source, "data", str(bytes));
    }
    var part = try single(a, "type", str("image"));
    try put(a, &part, "source", source);
    return part;
}
fn imageUrl(a: Allocator, url: []const u8) !Json {
    if (!std.mem.startsWith(u8, url, "data:")) return image(a, "", url, true);
    const separator = std.mem.indexOf(u8, url, ";base64,") orelse return error.InvalidInferenceRequest;
    return image(a, url[5..separator], url[separator + 8 ..], false);
}

pub fn requestJsonAlloc(alloc: Allocator, cfg: lib.GeneratorConfig, model: []const u8, messages: []const lib.ChatMessage, streaming: bool) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var root = try single(a, "model", str(model));
    try put(a, &root, "max_tokens", .{ .integer = cfg.max_tokens });
    try put(a, &root, "stream", .{ .bool = streaming });
    var system = array(a);
    var turns = array(a);
    for (messages) |message| {
        var parts = array(a);
        if (message.anthropic_content_json) |raw| {
            if (message.role != .assistant) return error.InvalidInferenceRequest;
            parts = std.json.parseFromSliceLeaky(Json, a, raw, .{}) catch return error.InvalidInferenceRequest;
            try validateNativeProjection(a, message, parts);
        } else {
            if (message.role == .tool) {
                var result = try single(a, "type", str("tool_result"));
                try put(a, &result, "tool_use_id", str(message.tool_call_id orelse return error.InvalidInferenceRequest));
                const content = if (message.content) |value| switch (value) {
                    .text => |value_| value_,
                    else => return error.UnsupportedInferenceFeature,
                } else "";
                try put(a, &result, "content", str(content));
                try parts.array.append(result);
            } else if (message.content) |content| switch (content) {
                .text => |value| try appendText(a, &parts, value),
                .parts => |values| for (values) |part| switch (part) {
                    .text => |value| try appendText(a, &parts, value),
                    .image_url => |value| {
                        if (message.role != .user) return error.UnsupportedInferenceFeature;
                        try parts.array.append(try imageUrl(a, value.url));
                    },
                    .media => |value| {
                        if (message.role != .user) return error.UnsupportedInferenceFeature;
                        try parts.array.append(if (value.url) |url| try imageUrl(a, url) else try image(a, value.mime_type, value.data, false));
                    },
                },
            };
            if (message.role == .system) {
                try system.array.appendSlice(parts.array.items);
                continue;
            }
            if (message.tool_calls) |calls| {
                if (message.role != .assistant) return error.InvalidInferenceRequest;
                for (calls) |call| {
                    var block = try single(a, "type", str("tool_use"));
                    try put(a, &block, "id", str(call.id));
                    try put(a, &block, "name", str(call.name));
                    const input = std.json.parseFromSliceLeaky(Json, a, call.arguments, .{}) catch return error.InvalidInferenceRequest;
                    if (input != .object) return error.InvalidInferenceRequest;
                    try put(a, &block, "input", input);
                    try parts.array.append(block);
                }
            }
        }
        if (parts.array.items.len == 0) return error.InvalidInferenceRequest;
        const role = if (message.role == .assistant) "assistant" else "user";
        if (turns.array.items.len > 0) {
            const previous = &turns.array.items[turns.array.items.len - 1];
            if (std.mem.eql(u8, (try get(previous.*, "role")).string, role)) {
                try previous.object.getPtr("content").?.array.appendSlice(parts.array.items);
                continue;
            }
        }
        var turn = try single(a, "role", str(role));
        try put(a, &turn, "content", parts);
        try turns.array.append(turn);
    }
    if (turns.array.items.len == 0) return error.InvalidInferenceRequest;
    try put(a, &root, "messages", turns);
    if (system.array.items.len > 0) try put(a, &root, "system", system);
    inline for (.{ "temperature", "top_p" }) |field| if (@field(cfg, field)) |value| try put(a, &root, field, .{ .float = value });
    if (cfg.top_k) |value| try put(a, &root, "top_k", .{ .integer = value });
    if (cfg.response_format_json) |raw| {
        const format = try std.json.parseFromSliceLeaky(Json, a, raw, .{});
        const kind = try requestGet(format, "type");
        if (kind != .string or !std.mem.eql(u8, kind.string, "json_schema")) return error.UnsupportedInferenceFeature;
        const schema = try requestGet(try requestGet(format, "json_schema"), "schema");
        if (schema != .object) return error.InvalidInferenceRequest;
        var format_ = try single(a, "type", str("json_schema"));
        try put(a, &format_, "schema", schema);
        try put(a, &root, "output_config", try single(a, "format", format_));
    }
    if (cfg.tools_json) |raw| {
        const tools = try std.json.parseFromSliceLeaky(Json, a, raw, .{});
        if (tools != .array or tools.array.items.len == 0) return error.InvalidInferenceRequest;
        var native = array(a);
        for (tools.array.items) |tool| {
            const function = try requestGet(tool, "function");
            var native_tool = try single(a, "name", try requestGet(function, "name"));
            if (function.object.get("description")) |value| try put(a, &native_tool, "description", value);
            const schema = function.object.get("parameters") orelse try lib.emptyToolParameters(a);
            if (schema != .object) return error.InvalidInferenceRequest;
            try put(a, &native_tool, "input_schema", schema);
            if (function.object.get("strict")) |value| try put(a, &native_tool, "strict", value);
            try native.array.append(native_tool);
        }
        try put(a, &root, "tools", native);
    }
    if (cfg.tool_choice_json) |raw| {
        const choice = try std.json.parseFromSliceLeaky(Json, a, raw, .{});
        const native = if (choice == .string) try single(a, "type", str(if (std.mem.eql(u8, choice.string, "required")) "any" else if (std.mem.eql(u8, choice.string, "none") or std.mem.eql(u8, choice.string, "auto")) choice.string else return error.InvalidInferenceRequest)) else blk: {
            var value = try single(a, "type", str("tool"));
            try put(a, &value, "name", try requestGet(try requestGet(choice, "function"), "name"));
            break :blk value;
        };
        try put(a, &root, "tool_choice", native);
    }
    return std.json.Stringify.valueAlloc(alloc, root, .{});
}

// Opaque replay cannot smuggle images, additional tools, or altered visible
// content around gateway admission. Compare the typed visible projection and
// retain signed native blocks verbatim only when it matches the public turn.
fn validateNativeProjection(a: Allocator, message: lib.ChatMessage, blocks: Json) !void {
    if (blocks != .array) return error.InvalidInferenceRequest;
    var visible = try lib.TextContentCursor.init(message.content);
    const calls = message.tool_calls orelse &.{};
    var call_offset: usize = 0;
    for (blocks.array.items) |block| {
        const kind = string(try requestGet(block, "type")) catch return error.InvalidInferenceRequest;
        if (std.mem.eql(u8, kind, "text")) {
            const value = string(try requestGet(block, "text")) catch return error.InvalidInferenceRequest;
            if (!visible.consume(value)) return error.InvalidInferenceRequest;
        } else if (std.mem.eql(u8, kind, "tool_use")) {
            if (call_offset >= calls.len) return error.InvalidInferenceRequest;
            const call = calls[call_offset];
            call_offset += 1;
            if (!std.mem.eql(u8, call.id, string(try requestGet(block, "id")) catch return error.InvalidInferenceRequest) or !std.mem.eql(u8, call.name, string(try requestGet(block, "name")) catch return error.InvalidInferenceRequest)) return error.InvalidInferenceRequest;
            const arguments = std.json.parseFromSliceLeaky(Json, a, call.arguments, .{}) catch return error.InvalidInferenceRequest;
            if (!lib.jsonValuesEqual(arguments, try requestGet(block, "input"))) return error.InvalidInferenceRequest;
        } else if (std.mem.eql(u8, kind, "thinking")) {
            _ = string(try requestGet(block, "thinking")) catch return error.InvalidInferenceRequest;
            _ = string(try requestGet(block, "signature")) catch return error.InvalidInferenceRequest;
        } else if (std.mem.eql(u8, kind, "redacted_thinking")) {
            _ = string(try requestGet(block, "data")) catch return error.InvalidInferenceRequest;
        } else return error.InvalidInferenceRequest;
    }
    if (!visible.done() or call_offset != calls.len) return error.InvalidInferenceRequest;
}
pub fn finishReason(reason: []const u8) !lib.GenerationFinishReason {
    if (std.mem.eql(u8, reason, "end_turn") or std.mem.eql(u8, reason, "stop_sequence")) return .stop;
    if (std.mem.eql(u8, reason, "max_tokens") or std.mem.eql(u8, reason, "model_context_window_exceeded") or std.mem.eql(u8, reason, "pause_turn")) return .length;
    if (std.mem.eql(u8, reason, "tool_use")) return .tool_calls;
    if (std.mem.eql(u8, reason, "refusal")) return .content_filter;
    return error.InvalidInferenceResponse;
}
/// Native usage fields are independent cumulative counters. A delta can omit
/// any input/cache field; absence (or null) retains its previous value. Keep
/// the counters separate until producing provider-neutral billing totals.
pub const UsageCounters = struct {
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64 = 0,
    cache_read_input_tokens: u64 = 0,

    fn optional(value: Json, name: []const u8) !?u64 {
        if (value != .object) return error.InvalidInferenceResponse;
        const field = value.object.get(name) orelse return null;
        if (field == .null) return null;
        return lib.GenerationUsage.count(value, name) orelse error.InvalidInferenceResponse;
    }

    pub fn init(value: Json) !UsageCounters {
        return .{
            .input_tokens = lib.GenerationUsage.count(value, "input_tokens") orelse return error.InvalidInferenceResponse,
            .output_tokens = lib.GenerationUsage.count(value, "output_tokens") orelse return error.InvalidInferenceResponse,
            .cache_creation_input_tokens = (try optional(value, "cache_creation_input_tokens")) orelse 0,
            .cache_read_input_tokens = (try optional(value, "cache_read_input_tokens")) orelse 0,
        };
    }

    pub fn apply(self: *UsageCounters, value: Json) !void {
        var next = self.*;
        next.output_tokens = lib.GenerationUsage.count(value, "output_tokens") orelse return error.InvalidInferenceResponse;
        inline for (.{ "input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens" }) |name| {
            if (try optional(value, name)) |count| @field(next, name) = count;
        }
        self.* = next;
    }

    pub fn normalized(self: UsageCounters) lib.GenerationUsage {
        const input = self.input_tokens +| self.cache_creation_input_tokens +| self.cache_read_input_tokens;
        return .{ .input_tokens = input, .output_tokens = self.output_tokens, .total_tokens = input +| self.output_tokens };
    }
};

fn usage(value: Json) !lib.GenerationUsage {
    return (try UsageCounters.init(value)).normalized();
}
pub fn responseAlloc(a: Allocator, body: []const u8) !inference.GenerateResult {
    var parsed = try std.json.parseFromSlice(Json, a, body, .{});
    defer parsed.deinit();
    if (!std.mem.eql(u8, try string(try get(parsed.value, "role")), "assistant")) return error.InvalidInferenceResponse;
    const blocks = try get(parsed.value, "content");
    if (blocks != .array) return error.InvalidInferenceResponse;
    var content: std.ArrayList(u8) = .empty;
    defer content.deinit(a);
    var calls: std.ArrayList(lib.ToolCall) = .empty;
    var has_opaque = false;
    errdefer {
        for (calls.items) |*call| call.deinit(a);
        calls.deinit(a);
    }
    for (blocks.array.items) |block| {
        const kind = try string(try get(block, "type"));
        if (std.mem.eql(u8, kind, "text")) try content.appendSlice(a, try string(try get(block, "text"))) else if (std.mem.eql(u8, kind, "tool_use")) {
            const id = try a.dupe(u8, try string(try get(block, "id")));
            errdefer a.free(id);
            const name = try a.dupe(u8, try string(try get(block, "name")));
            errdefer a.free(name);
            const arguments = try std.json.Stringify.valueAlloc(a, try get(block, "input"), .{});
            errdefer a.free(arguments);
            try calls.append(a, .{ .id = id, .name = name, .arguments = arguments });
        } else if (std.mem.eql(u8, kind, "thinking")) {
            _ = try string(try get(block, "thinking"));
            _ = try string(try get(block, "signature"));
            has_opaque = true;
        } else if (std.mem.eql(u8, kind, "redacted_thinking")) {
            _ = try string(try get(block, "data"));
            has_opaque = true;
        } else return error.UnsupportedInferenceFeature;
    }
    const reason = try finishReason(try string(try get(parsed.value, "stop_reason")));
    const counts = try usage(try get(parsed.value, "usage"));
    const text_ = try content.toOwnedSlice(a);
    errdefer a.free(text_);
    const native = if (has_opaque) try std.json.Stringify.valueAlloc(a, blocks, .{}) else null;
    errdefer if (native) |bytes| a.free(bytes);
    return .{ .content = text_, .anthropic_content_json = native, .finish_reason = reason, .tool_calls = try calls.toOwnedSlice(a), .usage = counts, .allocator = a };
}

const Decoder = @import("anthropic_stream.zig").Decoder;
pub const streamSse = sse;
pub const streamUsage = usage;
pub const streamGet = get;
pub const streamString = string;
