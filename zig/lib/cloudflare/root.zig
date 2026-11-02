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

//! Shared Workers AI transport and typed System One contracts.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const routing = @import("antfly_cloudflare_config");
pub const Config = struct {
    account_id: []const u8,
    model: []const u8,
    api_key: []const u8,
    gateway_id: ?[]const u8 = null,
    /// Optional deployment-owned full native endpoint (also used by fixtures).
    url: ?[]const u8 = null,
};

/// Reuses the caller's pooled HTTP client, secret resolution, deadline and
/// cancellation. One task call is exactly one upstream attempt.
pub fn invoke(client: anytype, a: A, config: Config, payload: V, options: anytype) !V {
    try validatePayload(a, payload);
    return unwrap(try invokeNative(client, a, config, payload, options));
}

/// Generic Workers AI transport. Role adapters validate their own wire shape.
pub fn invokeNative(client: anytype, a: A, config: Config, payload: V, options: anytype) !V {
    var response = try postNative(client, a, config, payload, options);
    defer response.deinit();
    return decodeNative(a, response.body orelse return error.InvalidCloudflareResponse);
}

/// Binary adapters use the same bounded JSON decoder when Workers AI returns
/// a JSON envelope instead of the native byte stream.
pub fn decodeNative(a: A, body: []const u8) !V {
    try responseDepth(body);
    const parsed = std.json.parseFromSliceLeaky(V, a, body, .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error", .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidCloudflareResponse,
    };
    if (field(parsed, "success")) |success| if (success != .bool or !success.bool) return error.CloudflareInferenceFailed;
    return field(parsed, "result") orelse parsed;
}

/// Native JSON and binary adapters share one bounded upstream attempt. The
/// response retains the HTTP client's buffer until the adapter releases it.
pub fn postNative(client: anytype, a: A, config: Config, payload: V, options: anytype) anyerror!@typeInfo(@TypeOf(client.post("", .{}))).error_union.payload {
    if (config.gateway_id) |id| {
        if (id.len == 0 or id.len > 256) return error.InvalidCloudflareConfig;
        for (id) |ch| if (ch < 0x21 or ch > 0x7e) return error.InvalidCloudflareConfig;
    }
    const canonical = try routing.nativeUrl(a, config.account_id, config.model);
    const url = config.url orelse canonical;
    const auth = try std.fmt.allocPrint(a, "Bearer {s}", .{config.api_key});
    var headers = std.ArrayList([2][]const u8).empty;
    try headers.append(a, .{ "Authorization", auth });
    if (config.gateway_id) |id| try headers.append(a, .{ "cf-aig-gateway-id", id });
    var response = try client.post(url, .{
        .json = try std.json.Stringify.valueAlloc(a, payload, .{}),
        .headers = headers.items,
        .max_retries = 0,
        .cookies_enabled = false,
        .follow_redirects = false,
        .max_response_size = options.max_response_bytes,
        .timeout_ms = options.timeout_ms,
        .cancellation = options.cancellation,
        .socket_control = if (@hasField(@TypeOf(options), "socket_control")) options.socket_control else null,
    });
    errdefer response.deinit();
    if (response.status.code == 429) return error.CloudflareRateLimited;
    if (response.status.code == 401 or response.status.code == 403) return error.CloudflareAuthenticationFailed;
    if (!response.ok()) return error.CloudflareInferenceFailed;
    return response;
}

fn responseDepth(bytes: []const u8) !void {
    var depth: usize = 0;
    var quoted = false;
    var escaped = false;
    for (bytes) |ch| {
        if (quoted) {
            if (escaped) {
                escaped = false;
            } else if (ch == '\\') {
                escaped = true;
            } else if (ch == '"') {
                quoted = false;
            }
        } else switch (ch) {
            '"' => quoted = true,
            '{', '[' => {
                depth += 1;
                if (depth > 64) return error.InvalidCloudflareResponse;
            },
            '}', ']' => {
                if (depth == 0) return error.InvalidCloudflareResponse;
                depth -= 1;
            },
            else => {},
        }
    }
    if (depth != 0 or quoted) return error.InvalidCloudflareResponse;
}

pub fn string(value: []const u8) V {
    return .{ .string = value };
}
pub fn object() V {
    return .{ .object = .{} };
}
pub fn put(a: A, value: *V, key: []const u8, item: V) !void {
    try value.object.put(a, key, item);
}
pub fn field(value: V, key: []const u8) ?V {
    return if (value == .object) value.object.get(key) else null;
}
pub fn text(value: V) ![]const u8 {
    return if (value == .string) value.string else error.InvalidCloudflareRequest;
}

pub const ChatReading = struct {
    text: []const u8,
    finish: enum { stop, length, other },
};

/// Vision transcription consumes one textual choice. Validate response values
/// here rather than using request-side coercions, so every host reports an
/// invalid successful upstream response consistently.
pub fn chatReading(response: V) !ChatReading {
    const choices = field(response, "choices") orelse return error.InvalidCloudflareResponse;
    if (choices != .array or choices.array.items.len != 1) return error.InvalidCloudflareResponse;
    const choice = choices.array.items[0];
    const message = field(choice, "message") orelse return error.InvalidCloudflareResponse;
    const content = field(message, "content") orelse return error.InvalidCloudflareResponse;
    if (content != .string) return error.InvalidCloudflareResponse;
    const reason = field(choice, "finish_reason") orelse .null;
    if (reason != .null and reason != .string) return error.InvalidCloudflareResponse;
    return .{
        .text = content.string,
        .finish = if (reason == .string and std.mem.eql(u8, reason.string, "stop")) .stop else if (reason == .string and std.mem.eql(u8, reason.string, "length")) .length else .other,
    };
}

test "cloudflare vision readings distinguish upstream shape errors and incomplete output" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    for ([_][]const u8{
        "{}",
        "{\"choices\":[]}",
        "{\"choices\":[{}]}",
        "{\"choices\":[{\"message\":{\"content\":null}}]}",
        "{\"choices\":[{\"message\":{\"content\":17}}]}",
        "{\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":17}]}",
    }) |bytes| try std.testing.expectError(error.InvalidCloudflareResponse, chatReading(try std.json.parseFromSliceLeaky(V, arena.allocator(), bytes, .{})));
    for ([_][]const u8{ "stop", "length", "content_filter" }, [_]@FieldType(ChatReading, "finish"){ .stop, .length, .other }) |reason, expected| {
        const bytes = try std.fmt.allocPrint(arena.allocator(), "{{\"choices\":[{{\"message\":{{\"content\":\"ok\"}},\"finish_reason\":\"{s}\"}}]}}", .{reason});
        const result = try chatReading(try std.json.parseFromSliceLeaky(V, arena.allocator(), bytes, .{}));
        try std.testing.expectEqualStrings("ok", result.text);
        try std.testing.expectEqual(expected, result.finish);
    }
    const unknown = try chatReading(try std.json.parseFromSliceLeaky(V, arena.allocator(), "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}", .{}));
    try std.testing.expectEqual(.other, unknown.finish);
}

pub fn endpoint(a: A, account: []const u8, model: []const u8) ![]const u8 {
    if (account.len == 0 or account.len > 64) return error.InvalidCloudflareConfig;
    for (account) |b| if (!std.ascii.isAlphanumeric(b) and b != '_' and b != '-') return error.InvalidCloudflareConfig;
    if (!std.mem.eql(u8, model, "@cf/cloudflare/clairic") and !std.mem.eql(u8, model, "@cf/cloudflare/clef") and !std.mem.eql(u8, model, "@cf/cloudflare/clef-flash")) return error.UnsupportedCloudflareModel;
    return std.fmt.allocPrint(a, "https://api.cloudflare.com/client/v4/accounts/{s}/ai/run/{s}", .{ account, model });
}
pub fn request(a: A, model: []const u8, state: []const u8, key: []const u8, schema: V) !V {
    const selector = if (std.mem.lastIndexOfScalar(u8, model, '/')) |i| model[i + 1 ..] else model;
    if (!std.unicode.utf8ValidateSlice(state) or schema != .object or schema.object.count() == 0 or schema.object.count() > 64 or state.len > 128 * 1024)
        return error.CloudflareInputLimitExceeded;
    var root = object();
    try put(a, &root, "model", string(selector));
    try put(a, &root, "state", string(state));
    try put(a, &root, key, schema);
    return root;
}
pub const Evidence = struct { state: []const u8, images: V };

/// Antfly content parts remain a typed input. Hosted models accept embedded
/// images only; remote source fetching belongs to Antfly's authorized adapters.
pub fn evidence(a: A, content: V, attachments: anytype, input_index: usize, inspect: *const fn ([]const u8) anyerror!void) !Evidence {
    var output = std.Io.Writer.Allocating.init(a);
    var images: std.array_list.Managed(V) = .init(a);
    var total: usize = 0;
    if (content == .string) {
        try output.writer.writeAll(content.string);
    } else if (content == .array) {
        for (content.array.items) |part| {
            const kind = try text(field(part, "type") orelse return error.UnsupportedCloudflareSchema);
            if (std.mem.eql(u8, kind, "text")) {
                if (output.written().len > 0) try output.writer.writeByte('\n');
                try output.writer.writeAll(try text(field(part, "text") orelse return error.UnsupportedCloudflareSchema));
            } else if (std.mem.eql(u8, kind, "image_url")) {
                const location = field(part, "image_url") orelse return error.UnsupportedCloudflareSchema;
                const uri = try text(field(location, "url") orelse return error.UnsupportedCloudflareSchema);
                try appendImage(a, &images, &total, uri, inspect);
            } else if (std.mem.eql(u8, kind, "media")) {
                const mime = try text(field(part, "mime_type") orelse return error.UnsupportedCloudflareSchema);
                const data = try text(field(part, "data") orelse return error.UnsupportedCloudflareSchema);
                try appendImage(a, &images, &total, try std.fmt.allocPrint(a, "data:{s};base64,{s}", .{ mime, data }), inspect);
            } else return error.UnsupportedCloudflareSchema;
        }
    } else return error.UnsupportedCloudflareSchema;
    for (attachments) |attachment| {
        if (attachment.input_index != input_index) continue;
        const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(attachment.bytes.len));
        _ = std.base64.standard.Encoder.encode(encoded, attachment.bytes);
        try appendImage(a, &images, &total, try std.fmt.allocPrint(a, "data:{s};base64,{s}", .{ attachment.mime_type, encoded }), inspect);
    }
    return .{ .state = try output.toOwnedSlice(), .images = .{ .array = images } };
}
fn appendImage(a: A, images: *std.array_list.Managed(V), total: *usize, uri: []const u8, inspect: *const fn ([]const u8) anyerror!void) !void {
    const prefix = if (std.mem.startsWith(u8, uri, "data:image/png;base64,")) "data:image/png;base64," else if (std.mem.startsWith(u8, uri, "data:image/jpeg;base64,")) "data:image/jpeg;base64," else if (std.mem.startsWith(u8, uri, "data:image/webp;base64,")) "data:image/webp;base64," else return error.UnsupportedCloudflareSchema;
    const encoded = uri[prefix.len..];
    const size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
    if (images.items.len >= 4 or size > 4 * 1024 * 1024 or size > 8 * 1024 * 1024 -| total.*) return error.CloudflareInputLimitExceeded;
    const bytes = try a.alloc(u8, size);
    try std.base64.standard.Decoder.decode(bytes, encoded);
    const signature: []const u8 = if (std.mem.startsWith(u8, prefix, "data:image/png")) "\x89PNG\r\n\x1a\n" else if (std.mem.startsWith(u8, prefix, "data:image/jpeg")) "\xff\xd8\xff" else "RIFF";
    if (!std.mem.startsWith(u8, bytes, signature)) return error.InvalidCloudflareRequest;
    try inspect(bytes);
    total.* += size;
    try images.append(string(uri));
}

pub fn evidenceRequest(a: A, model: []const u8, input: Evidence, fields: V) !V {
    var result = try request(a, model, input.state, "fields", fields);
    if (input.images.array.items.len > 0) try put(a, &result, "images", input.images);
    // Byte counts conservatively upper-bound text tokens. Reserve 1024 tokens
    // per image; reject a context we cannot guarantee rather than truncate it.
    const schema_bytes = (try std.json.Stringify.valueAlloc(a, fields, .{})).len;
    if (input.state.len + schema_bytes + input.images.array.items.len * 1024 > 60_000) return error.CloudflareInputLimitExceeded;
    if ((try std.json.Stringify.valueAlloc(a, result, .{})).len > 13 * 1024 * 1024) return error.CloudflareInputLimitExceeded;
    return result;
}
pub fn validatePayload(a: A, payload: V) !void {
    // Generated wire validation and task budgets are separate authorities.
    // Decode borrowed strings without copying embedded image payloads.
    if (field(payload, "fields") != null) {
        _ = std.json.parseFromValueLeaky(routing.wire.types.ClairicInput, a, payload, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidCloudflareRequest,
        };
    } else {
        _ = std.json.parseFromValueLeaky(routing.wire.types.ClefInput, a, payload, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidCloudflareRequest,
        };
    }

    const state = field(payload, "state") orelse return error.InvalidCloudflareRequest;
    const schema = field(payload, "fields") orelse field(payload, "questions") orelse return error.InvalidCloudflareRequest;
    if (schema != .object or schema.object.count() == 0 or schema.object.count() > 64) return error.CloudflareInputLimitExceeded;
    const images = field(payload, "images");
    const count = if (images) |v| blk: {
        if (v != .array or v.array.items.len > 4) return error.CloudflareInputLimitExceeded;
        break :blk v.array.items.len;
    } else 0;
    const state_bytes = if (state == .string) state.string.len else (try std.json.Stringify.valueAlloc(a, state, .{})).len;
    if (state_bytes + (try std.json.Stringify.valueAlloc(a, schema, .{})).len + count * 1024 > 60_000) return error.CloudflareInputLimitExceeded;
    if ((try std.json.Stringify.valueAlloc(a, payload, .{})).len > 13 * 1024 * 1024) return error.CloudflareInputLimitExceeded;
}
pub fn unwrap(response: V) !V {
    if (field(response, "success")) |success| if (success != .bool or !success.bool) return error.CloudflareInferenceFailed;
    const result = field(response, "result") orelse response;
    if (field(result, "truncated")) |truncated| if (truncated != .bool or truncated.bool) return error.CloudflareInputTruncated;
    const answers = field(result, "answers") orelse return error.InvalidCloudflareResponse;
    if (answers != .object) return error.InvalidCloudflareResponse;
    return result;
}

pub const ExtractionPlan = struct { structure: []const u8, fields: V };
pub fn extractionSchema(a: A, schema: V) !ExtractionPlan {
    if (schema != .object or schema.object.count() != 1) return error.UnsupportedCloudflareSchema;
    const structures = field(schema, "structures") orelse return error.UnsupportedCloudflareSchema;
    if (structures != .object or structures.object.count() != 1) return error.UnsupportedCloudflareSchema;
    const record = structures.object.values()[0];
    if (record != .object or record.object.count() != 1) return error.UnsupportedCloudflareSchema;
    const fields = field(record, "fields") orelse return error.UnsupportedCloudflareSchema;
    if (fields != .object or fields.object.count() == 0 or fields.object.count() > 64) return error.UnsupportedCloudflareSchema;
    var mapped = object();
    for (fields.object.keys(), fields.object.values()) |name, definition| {
        var value = object();
        const kind = if (definition == .string) definition.string else try text(field(definition, "type") orelse return error.UnsupportedCloudflareSchema);
        const native = if (std.mem.eql(u8, kind, "str") or std.mem.eql(u8, kind, "string") or std.mem.eql(u8, kind, "text")) "text" else if (std.mem.eql(u8, kind, "list") or std.mem.eql(u8, kind, "array")) "list" else if (std.mem.eql(u8, kind, "number") or std.mem.eql(u8, kind, "date") or std.mem.eql(u8, kind, "estimate")) kind else return error.UnsupportedCloudflareSchema;
        try put(a, &value, "type", string(native));
        if (std.mem.eql(u8, native, "list")) try put(a, &value, "items", string("text"));
        if (definition == .object) for (definition.object.keys(), definition.object.values()) |key, item| {
            if (std.mem.eql(u8, key, "type")) continue;
            if (std.mem.eql(u8, key, "description")) {
                if (item != .string) return error.UnsupportedCloudflareSchema;
                try put(a, &value, "instructions", item);
                continue;
            }
            if (std.mem.eql(u8, key, "decimals") or std.mem.eql(u8, key, "unit") or std.mem.eql(u8, key, "min") or std.mem.eql(u8, key, "max") or std.mem.eql(u8, key, "step")) {
                if (std.mem.eql(u8, key, "unit")) {
                    if (item != .string or (!std.mem.eql(u8, native, "number") and !std.mem.eql(u8, native, "estimate"))) return error.UnsupportedCloudflareSchema;
                } else if (std.mem.eql(u8, key, "decimals")) {
                    if (!std.mem.eql(u8, native, "number") or (unsigned(item) catch return error.UnsupportedCloudflareSchema) > 6) return error.UnsupportedCloudflareSchema;
                } else {
                    if (!std.mem.eql(u8, native, "estimate")) return error.UnsupportedCloudflareSchema;
                    const n = try numeric(item);
                    if (std.mem.eql(u8, key, "step") and n <= 0) return error.UnsupportedCloudflareSchema;
                }
                try put(a, &value, key, item);
                continue;
            }
            return error.UnsupportedCloudflareSchema;
        };
        if (std.mem.eql(u8, native, "estimate")) {
            const minimum = try numeric(field(value, "min") orelse return error.UnsupportedCloudflareSchema);
            const maximum = try numeric(field(value, "max") orelse return error.UnsupportedCloudflareSchema);
            _ = try numeric(field(value, "step") orelse return error.UnsupportedCloudflareSchema);
            if (minimum >= maximum) return error.UnsupportedCloudflareSchema;
        }
        try put(a, &mapped, name, value);
    }
    return .{ .structure = structures.object.keys()[0], .fields = mapped };
}

/// Keep native provenance. A null value must not inherit the model's guess or
/// confidence. Span support is checked against the exact submitted state.
pub fn extractionResult(a: A, plan: ExtractionPlan, result: V, state: []const u8) !V {
    const answers = field(try unwrap(result), "answers").?;
    if (answers.object.count() != plan.fields.object.count()) return error.InvalidCloudflareResponse;
    var record = object();
    for (plan.fields.object.keys()) |name| {
        const answer = field(answers, name) orelse return error.InvalidCloudflareResponse;
        const value = field(answer, "value") orelse return error.InvalidCloudflareResponse;
        const kind = try text(field(plan.fields.object.get(name).?, "type").?);
        if (value != .null) {
            if ((std.mem.eql(u8, kind, "text") or std.mem.eql(u8, kind, "date")) and value != .string) return error.InvalidCloudflareResponse;
            if ((std.mem.eql(u8, kind, "number") or std.mem.eql(u8, kind, "estimate")) and value != .integer and value != .float and value != .number_string) return error.InvalidCloudflareResponse;
            if (std.mem.eql(u8, kind, "list")) {
                if (value != .array) return error.InvalidCloudflareResponse;
                for (value.array.items) |item| if (item != .string) return error.InvalidCloudflareResponse;
            }
            if (value == .float and !std.math.isFinite(value.float)) return error.InvalidCloudflareResponse;
        }
        var entry = object();
        try put(a, &entry, "value", value);
        try put(a, &entry, "origin", string(if (std.mem.eql(u8, kind, "estimate")) "estimated" else "derived"));
        if (std.mem.eql(u8, kind, "estimate")) {
            var estimate = object();
            for ([_][]const u8{ "mean", "median", "p10", "p90", "p_below", "p_above" }) |key| {
                if (field(answer, key)) |v| {
                    // Validate at the provider boundary so every host receives
                    // schema-valid output. Preserve the original numeric value.
                    const n = numeric(v) catch return error.InvalidCloudflareResponse;
                    if ((std.mem.eql(u8, key, "p_below") or std.mem.eql(u8, key, "p_above")) and (n < 0 or n > 1)) return error.InvalidCloudflareResponse;
                    try put(a, &estimate, key, v);
                }
            }
            try put(a, &entry, "estimate", estimate);
        }
        if (value != .null) {
            if (field(answer, "confidence")) |confidence| {
                const p = numeric(confidence) catch return error.InvalidCloudflareResponse;
                if (!std.math.isFinite(p) or p < 0 or p > 1) return error.InvalidCloudflareResponse;
                try put(a, &entry, "score", confidence);
            }
            if (field(answer, "span")) |span| span: {
                if (span == .null) break :span;
                if (span != .array or span.array.items.len != 2 or value != .string) return error.InvalidCloudflareResponse;
                const start = try characterByteOffset(state, span.array.items[0]);
                const end = try characterByteOffset(state, span.array.items[1]);
                if (start > end or !std.mem.eql(u8, state[start..end], value.string)) return error.InvalidCloudflareResponse;
                try put(a, &entry, "start", .{ .integer = @intCast(start) });
                try put(a, &entry, "end", .{ .integer = @intCast(end) });
                try put(a, &entry, "origin", string("transcription"));
            }
        }
        if (value != .null and std.mem.eql(u8, kind, "list")) if (field(answer, "spans")) |spans| {
            if (spans != .null) {
                if (spans != .array or spans.array.items.len != value.array.items.len) return error.InvalidCloudflareResponse;
                var mapped: std.array_list.Managed(V) = .init(a);
                var all_grounded = true;
                for (value.array.items, spans.array.items) |item, span| {
                    if (span == .null) {
                        all_grounded = false;
                        try mapped.append(.null);
                        continue;
                    }
                    if (span != .array or span.array.items.len != 2) return error.InvalidCloudflareResponse;
                    const start = try characterByteOffset(state, span.array.items[0]);
                    const end = try characterByteOffset(state, span.array.items[1]);
                    if (start > end or !std.mem.eql(u8, state[start..end], item.string)) return error.InvalidCloudflareResponse;
                    var range: std.array_list.Managed(V) = .init(a);
                    try range.append(.{ .integer = @intCast(start) });
                    try range.append(.{ .integer = @intCast(end) });
                    try mapped.append(.{ .array = range });
                }
                try put(a, &entry, "spans", .{ .array = mapped });
                if (all_grounded) try put(a, &entry, "origin", string("transcription"));
            }
        };
        try put(a, &record, name, entry);
    }
    var rows: std.array_list.Managed(V) = .init(a);
    try rows.append(record);
    var structures = object();
    try put(a, &structures, plan.structure, .{ .array = rows });
    var out = object();
    try put(a, &out, "structures", structures);
    try put(a, &out, "offset_unit", string("utf8_bytes"));
    return out;
}
fn numeric(value: V) !f64 {
    const n: f64 = switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        .number_string => std.fmt.parseFloat(f64, value.number_string) catch return error.UnsupportedCloudflareSchema,
        else => return error.UnsupportedCloudflareSchema,
    };
    if (!std.math.isFinite(n)) return error.UnsupportedCloudflareSchema;
    return n;
}
pub const Usage = struct { input_tokens: u64, output_tokens: u64 };
pub fn nativeUsage(result: V) !Usage {
    const usage = field(result, "usage") orelse return error.InvalidCloudflareResponse;
    return .{
        .input_tokens = try unsigned(field(usage, "input_tokens") orelse return error.InvalidCloudflareResponse),
        .output_tokens = try unsigned(field(usage, "output_tokens") orelse return error.InvalidCloudflareResponse),
    };
}

pub fn unsigned(value: V) !u64 {
    return switch (value) {
        .integer => if (value.integer >= 0) @intCast(value.integer) else error.InvalidCloudflareResponse,
        .number_string => std.fmt.parseInt(u64, value.number_string, 10) catch error.InvalidCloudflareResponse,
        else => error.InvalidCloudflareResponse,
    };
}
fn characterByteOffset(state: []const u8, index: V) !usize {
    const wanted = try unsigned(index);
    var chars: u64 = 0;
    var offset: usize = 0;
    while (offset < state.len and chars < wanted) : (chars += 1) offset += std.unicode.utf8ByteSequenceLength(state[offset]) catch return error.InvalidCloudflareResponse;
    if (offset > state.len or chars != wanted) return error.InvalidCloudflareResponse;
    return offset;
}

test "cloudflare provider native transport keeps model path and rejects endpoint injection" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqualStrings("https://api.cloudflare.com/client/v4/accounts/abc/ai/run/@cf/cloudflare/clef", try endpoint(a, "abc", "@cf/cloudflare/clef"));
    try std.testing.expectError(error.InvalidCloudflareConfig, endpoint(a, "abc/../other", "@cf/cloudflare/clef"));
    try std.testing.expectError(error.CloudflareInputTruncated, unwrap(try std.json.parseFromSliceLeaky(V, a, "{\"answers\":{},\"truncated\":true}", .{})));
}
test "cloudflare provider extraction preserves null without promoting a guess" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = try std.json.parseFromSliceLeaky(V, a, "{\"structures\":{\"receipt\":{\"fields\":{\"total\":\"str\"}}}}", .{});
    const response = try std.json.parseFromSliceLeaky(V, a, "{\"answers\":{\"total\":{\"value\":null,\"guess\":\"100\",\"confidence\":1}}}", .{});
    const result = try extractionResult(a, try extractionSchema(a, schema), response, "no total");
    const value = result.object.get("structures").?.object.get("receipt").?.array.items[0].object.get("total").?;
    try std.testing.expectEqual(V.null, value.object.get("value").?);
    try std.testing.expect(value.object.get("score") == null);
}

test "cloudflare provider rebases unicode text and list spans to exact source bytes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = try std.json.parseFromSliceLeaky(V, a, "{\"structures\":{\"record\":{\"fields\":{\"name\":\"text\",\"names\":\"list\",\"date\":\"date\"}}}}", .{});
    const response = try std.json.parseFromSliceLeaky(V, a, "{\"answers\":{\"name\":{\"value\":\"Éva\",\"span\":[0,3]},\"names\":{\"value\":[\"Éva\",\"李\"],\"spans\":[[0,3],[4,5]]},\"date\":{\"value\":\"2026-10-23\",\"span\":null}}}", .{});
    const result = try extractionResult(a, try extractionSchema(a, schema), response, "Éva 李");
    const row = field(field(result, "structures").?, "record").?.array.items[0];
    try std.testing.expectEqual(@as(i64, 4), field(field(row, "name").?, "end").?.integer);
    const spans = field(field(row, "names").?, "spans").?.array.items;
    try std.testing.expectEqual(@as(i64, 5), spans[1].array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 8), spans[1].array.items[1].integer);
    try std.testing.expectEqualStrings("derived", try text(field(field(row, "date").?, "origin").?));
}

test "cloudflare provider rejects unsupported schema rather than coercing entities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = try std.json.parseFromSliceLeaky(V, a, "{\"entities\":[\"person\"]}", .{});
    try std.testing.expectError(error.UnsupportedCloudflareSchema, extractionSchema(a, schema));
    const fields = try std.json.parseFromSliceLeaky(V, a, "{\"total\":{\"type\":\"number\"}}", .{});
    const text_state = try a.alloc(u8, 60_001);
    @memset(text_state, 'x');
    try std.testing.expectError(error.CloudflareInputLimitExceeded, evidenceRequest(a, "@cf/cloudflare/clairic", .{ .state = text_state, .images = .{ .array = .init(a) } }, fields));
}

test "cloudflare provider validates every estimate metric before emitting typed output" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = try std.json.parseFromSliceLeaky(V, a,
        \\{"structures":{"r":{"fields":{"days":{"type":"estimate","min":0,"max":100,"step":1}}}}}
    , .{});
    const plan = try extractionSchema(a, schema);
    const response = try std.json.parseFromSliceLeaky(V, a,
        \\{"answers":{"days":{"value":42,"mean":42.5,"median":42,"p10":10,"p90":90,"p_below":0,"p_above":1}}}
    , .{});
    const result = try extractionResult(a, plan, response, "");
    const row = field(field(result, "structures").?, "r").?.array.items[0];
    const estimate = field(field(row, "days").?, "estimate").?;
    try std.testing.expectEqual(@as(f64, 42.5), field(estimate, "mean").?.float);
    try std.testing.expectEqual(@as(i64, 42), field(estimate, "median").?.integer);
    for ([_][]const u8{ "mean", "median", "p10", "p90", "p_below", "p_above" }) |key| {
        for ([_]V{ string("not-a-number"), .{ .bool = false }, object(), .{ .array = .init(a) }, .null, .{ .float = std.math.inf(f64) }, .{ .float = std.math.nan(f64) } }) |invalid| {
            var malformed = try std.json.parseFromSliceLeaky(V, a,
                \\{"answers":{"days":{"value":42}}}
            , .{});
            try malformed.object.getPtr("answers").?.object.getPtr("days").?.object.put(a, key, invalid);
            try std.testing.expectError(error.InvalidCloudflareResponse, extractionResult(a, plan, malformed, ""));
        }
    }
    for ([_][]const u8{ "p_below", "p_above" }) |key| {
        for ([_]f64{ -0.01, 1.01 }) |invalid| {
            var malformed = try std.json.parseFromSliceLeaky(V, a,
                \\{"answers":{"days":{"value":42}}}
            , .{});
            try malformed.object.getPtr("answers").?.object.getPtr("days").?.object.put(a, key, .{ .float = invalid });
            try std.testing.expectError(error.InvalidCloudflareResponse, extractionResult(a, plan, malformed, ""));
        }
    }
    // Optional metrics remain optional; abstention never becomes a guess.
    const abstained = try std.json.parseFromSliceLeaky(V, a,
        \\{"answers":{"days":{"value":null}}}
    , .{});
    const empty_result = try extractionResult(a, plan, abstained, "");
    const empty_row = field(field(empty_result, "structures").?, "r").?.array.items[0];
    try std.testing.expectEqual(V.null, field(field(empty_row, "days").?, "value").?);
    try std.testing.expectEqual(@as(usize, 0), field(field(empty_row, "days").?, "estimate").?.object.count());
}

test "cloudflare rejects malformed successful upstream JSON and preserves numeric lexemes" {
    const Fake = struct {
        body: []const u8,
        const Response = struct {
            status: struct { code: u16 = 200 } = .{},
            body: ?[]const u8,
            fn ok(_: @This()) bool {
                return true;
            }
            fn deinit(_: *@This()) void {}
        };
        fn post(self: *@This(), _: []const u8, _: anytype) !Response {
            return .{ .body = self.body };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fake = Fake{ .body = "{not-json" };
    const cfg = Config{ .account_id = "account", .model = "@cf/cloudflare/clairic", .api_key = "fixture" };
    const options = .{ .max_response_bytes = 4096, .timeout_ms = @as(?u64, 1000), .cancellation = @as(?u8, null) };
    try std.testing.expectError(error.InvalidCloudflareResponse, invokeNative(&fake, a, cfg, object(), options));
    fake.body = "{\"success\":true,\"result\":{\"value\":9007199254740993.1}}";
    const value = try invokeNative(&fake, a, cfg, object(), options);
    try std.testing.expectEqualStrings("9007199254740993.1", field(value, "value").?.number_string);
}

test "cloudflare Clairic decimals follow upstream zero through six bound" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "7", "15", "-1" }) |decimals| {
        const bytes = try std.mem.concat(a, u8, &.{ "{\"structures\":{\"record\":{\"fields\":{\"value\":{\"type\":\"number\",\"decimals\":", decimals, "}}}}}" });
        const schema = try std.json.parseFromSliceLeaky(V, a, bytes, .{ .parse_numbers = false });
        try std.testing.expectError(error.UnsupportedCloudflareSchema, extractionSchema(a, schema));
    }
}
