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

//! Immutable execution inputs shared by every model-family adapter.
//!
//! Model configuration describes what to run. This context describes where
//! Antfly-owned work runs and which tenant route selected it. Keeping these
//! concerns separate prevents document producers, embedders, rerankers, and
//! future task families from independently inventing localhost fallbacks.

const std = @import("std");
const httpx = @import("httpx");
const remote_capabilities = @import("antfly_inference_remote_capabilities");
const CancellationToken = @import("antfly_cancellation").CancellationToken;

pub const source_table_header = "X-Antfly-Source-Table";

pub const RoutingContext = struct {
    /// Internal, trusted table identity. Never populate this from a public
    /// model config: the inference proxy uses it only to select a provisioned
    /// route after authenticating the request.
    source_table: []const u8 = "",

    pub fn headerCount(self: RoutingContext) usize {
        return @intFromBool(self.source_table.len > 0);
    }

    pub fn appendHeaders(
        self: RoutingContext,
        storage: []([2][]const u8),
        start: usize,
    ) !usize {
        var count = start;
        if (self.source_table.len > 0) {
            if (count >= storage.len) return error.InferenceRoutingHeaderCapacityExceeded;
            storage[count] = .{ source_table_header, self.source_table };
            count += 1;
        }
        return count;
    }
};

const execution_control = @import("antfly_inference_execution_control");
pub const Phase = execution_control.Phase;
pub const Progress = execution_control.Progress;
pub const ProgressSink = execution_control.ProgressSink;
pub const RequestContext = execution_control.RequestContext;
pub const VerifiedMedia = execution_control.VerifiedMedia;

/// Durable, task-neutral execution environment. Provider owners may retain
/// this value; each invocation derives a short-lived `RequestContext` from it.
pub const ExecutionEnvironment = struct {
    /// Default distributed inference endpoint. Explicit per-model URLs retain
    /// precedence; an available linked callback retains precedence over this
    /// default for configs that did not explicitly request a remote endpoint.
    default_endpoint: ?[]const u8 = null,
    capability_cache: ?*remote_capabilities.Cache = null,
    io: ?std.Io = null,
    routing: RoutingContext = .{},
    /// Absolute monotonic deadline for discovery and execution. Task adapters
    /// may apply a stricter family-specific ceiling, but must never extend it.
    deadline_ns: ?u64 = null,
    cancellation: CancellationToken = .none,
    /// Optional caller-owned response ceiling. Task adapters intersect this
    /// with their own hard limit rather than treating it as permission to
    /// allocate more.
    max_response_bytes: ?usize = null,
    /// Optional provider-runtime transport. Keep new execution context fields
    /// append-only because this value crosses checked linked task boundaries.
    /// The context borrows this pointer; task adapters must not destroy it or
    /// retain it beyond their durable provider owner. Standalone invocations
    /// may create a call-scoped compatibility client when it is absent.
    http_client: ?*httpx.Client = null,

    pub fn resolveAntflyEndpoint(
        self: ExecutionEnvironment,
        explicit_endpoint: ?[]const u8,
        linked_callback_available: bool,
    ) ?[]const u8 {
        if (explicit_endpoint) |endpoint| {
            if (std.mem.trim(u8, endpoint, " \t\r\n").len > 0) return endpoint;
        }
        if (linked_callback_available) return null;
        if (self.default_endpoint) |endpoint| {
            if (std.mem.trim(u8, endpoint, " \t\r\n").len > 0) return endpoint;
        }
        return null;
    }

    pub fn waitContext(self: ExecutionEnvironment) remote_capabilities.WaitContext {
        return .{
            .deadline_ns = self.deadline_ns,
            .cancellation = self.cancellation,
        };
    }

    pub fn requestContext(self: ExecutionEnvironment, io: std.Io) RequestContext {
        return .{
            .io = io,
            .deadline_ns = self.deadline_ns,
            .cancellation = self.cancellation,
        };
    }

    pub fn check(self: ExecutionEnvironment, now_ns: u64) !void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| if (now_ns >= deadline) return error.Timeout;
    }

    pub fn boundedResponseBytes(self: ExecutionEnvironment, family_limit: usize) usize {
        return if (self.max_response_bytes) |requested|
            @min(requested, family_limit)
        else
            family_limit;
    }

    pub fn remainingTimeoutMs(
        self: ExecutionEnvironment,
        now_ns: u64,
        family_limit_ms: u64,
    ) !u64 {
        try self.check(now_ns);
        const deadline = self.deadline_ns orelse return family_limit_ms;
        const remaining_ns = deadline - now_ns;
        const rounded_ms = @max(
            @as(u64, 1),
            std.math.divCeil(u64, remaining_ns, std.time.ns_per_ms) catch 1,
        );
        return @min(rounded_ms, family_limit_ms);
    }
};

pub const InferenceExecutionContext = ExecutionEnvironment;
pub const Context = ExecutionEnvironment;

test "execution context gives explicit and linked routes precedence" {
    const context = Context{ .default_endpoint = "http://distributed" };
    try std.testing.expectEqualStrings(
        "http://explicit",
        context.resolveAntflyEndpoint("http://explicit", true).?,
    );
    try std.testing.expect(context.resolveAntflyEndpoint(null, true) == null);
    try std.testing.expectEqualStrings(
        "http://distributed",
        context.resolveAntflyEndpoint(null, false).?,
    );
    try std.testing.expect((Context{ .default_endpoint = " \t" }).resolveAntflyEndpoint(null, false) == null);
}

test "routing context appends trusted table header" {
    var storage: [2][2][]const u8 = undefined;
    storage[0] = .{ "Authorization", "Bearer token" };
    const count = try (RoutingContext{ .source_table = "docs" }).appendHeaders(&storage, 1);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqualStrings(source_table_header, storage[1][0]);
    try std.testing.expectEqualStrings("docs", storage[1][1]);
}

test "execution context preserves control and resource bounds" {
    var canceled = std.atomic.Value(bool).init(false);
    const context = Context{
        .deadline_ns = 200,
        .cancellation = CancellationToken.fromAtomic(&canceled),
        .max_response_bytes = 512,
    };
    try context.check(199);
    try std.testing.expectError(error.Timeout, context.check(200));
    try std.testing.expectEqual(@as(usize, 512), context.boundedResponseBytes(1024));
    try std.testing.expectEqual(@as(?u64, 200), context.waitContext().deadline_ns);
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, context.check(0));
}

test "request context preserves absolute deadlines and cancellation" {
    var cancelled = std.atomic.Value(bool).init(false);
    const context = RequestContext{
        .io = std.testing.io,
        .deadline_ns = null,
        .cancellation = CancellationToken.fromAtomic(&cancelled),
    };
    try context.check();
    cancelled.store(true, .release);
    try std.testing.expectError(error.Cancelled, context.check());

    var expired = context;
    expired.cancellation = null;
    expired.deadline_ns = 0;
    try std.testing.expectError(error.Timeout, expired.remainingTimeoutMs());
}

/// Native generation keeps verified media binary until the model consumes it.
/// Only canonical message media is adapted; tools, schemas and arbitrary text
/// remain opaque. Hosted providers keep their original data-URL representation.
pub const MediaEncoding = struct {
    bytes: []const u8,
    content_type: []const u8 = "application/json",
    owned: bool = false,
    segments: ?httpx.attachment_envelope.EncodedSegments = null,
    metadata: ?[]u8 = null,
    pub fn deinit(self: *@This(), a: std.mem.Allocator) void {
        if (self.segments) |*segments| segments.deinit();
        if (self.metadata) |metadata| a.free(metadata);
        if (self.owned) a.free(self.bytes);
        self.* = .{ .bytes = &.{} };
    }
};
pub fn encodeVerifiedGenerate(a: std.mem.Allocator, temporary: std.mem.Allocator, body: []const u8, existing: ?[]const httpx.attachment_envelope.Attachment, media: []const VerifiedMedia) !MediaEncoding {
    return encodeVerifiedGenerateMode(a, temporary, body, existing, media, false);
}
/// Replayable segments borrow immutable codec bytes until the synchronous
/// invocation completes. Only compact rewritten metadata and framing are owned.
pub fn encodeVerifiedGenerateSegments(a: std.mem.Allocator, temporary: std.mem.Allocator, body: []const u8, existing: ?[]const httpx.attachment_envelope.Attachment, media: []const VerifiedMedia) !MediaEncoding {
    return encodeVerifiedGenerateMode(a, temporary, body, existing, media, true);
}
fn encodeEnvelope(a: std.mem.Allocator, body: []const u8, items: []const httpx.attachment_envelope.Attachment, segmented: bool) !MediaEncoding {
    return if (segmented)
        .{ .bytes = &.{}, .segments = try httpx.attachment_envelope.encodeSegmentsAlloc(a, body, items), .content_type = httpx.attachment_envelope.content_type }
    else
        .{ .bytes = try httpx.attachment_envelope.encodeAlloc(a, body, items), .content_type = httpx.attachment_envelope.content_type, .owned = true };
}
fn encodeVerifiedGenerateMode(a: std.mem.Allocator, temporary: std.mem.Allocator, body: []const u8, existing: ?[]const httpx.attachment_envelope.Attachment, media: []const VerifiedMedia, segmented: bool) !MediaEncoding {
    if (media.len == 0) return if (existing) |items| try encodeEnvelope(a, body, items, segmented) else .{ .bytes = body };
    var arena = std.heap.ArenaAllocator.init(temporary);
    defer arena.deinit();
    const scratch = arena.allocator();
    const value = try std.json.parseFromSliceLeaky(std.json.Value, scratch, body, .{ .parse_numbers = false, .duplicate_field_behavior = .@"error" });
    var attachments: std.ArrayList(httpx.attachment_envelope.Attachment) = .empty;
    if (existing) |items| try attachments.appendSlice(scratch, items);
    var changed = false;
    if (value == .object) if (value.object.get("messages")) |messages| if (messages == .array) for (messages.array.items) |message| {
        if (message != .object) continue;
        const content = message.object.get("content") orelse continue;
        if (content != .array) continue;
        media_parts: for (content.array.items) |*part| {
            if (part.* != .object) continue;
            const kind = part.object.get("type") orelse continue;
            if (kind != .string or !std.mem.eql(u8, kind.string, "image_url")) continue;
            const image = part.object.get("image_url") orelse continue;
            if (image != .object) continue;
            for (part.object.keys()) |key| if (!std.mem.eql(u8, key, "type") and !std.mem.eql(u8, key, "image_url")) continue :media_parts;
            for (image.object.keys()) |key| if (!std.mem.eql(u8, key, "url") and !std.mem.eql(u8, key, "detail")) continue :media_parts;
            const url = image.object.get("url") orelse continue;
            if (url != .string) continue;
            if (image.object.get("detail")) |detail| if (detail != .string or !std.mem.eql(u8, detail.string, "auto")) continue;
            const found = for (media) |item| {
                if (item.matches(url.string)) break item;
            } else continue;
            var replacement: std.json.ObjectMap = .empty;
            try replacement.put(scratch, "type", .{ .string = "media" });
            try replacement.put(scratch, "mime_type", .{ .string = found.mime_type });
            // Public HTTP generation requires one attachment per occurrence.
            // Private worker transport can share bytes behind those logical
            // slots without changing that cardinality contract.
            try replacement.put(scratch, "data", .{ .string = try std.fmt.allocPrint(scratch, "attachment:{d}", .{attachments.items.len}) });
            try attachments.append(scratch, .{ .mime_type = found.mime_type, .data = found.bytes });
            part.* = .{ .object = replacement };
            changed = true;
        }
    };
    if (!changed) return if (existing) |items| try encodeEnvelope(a, body, items, segmented) else .{ .bytes = body };
    const metadata = try std.json.Stringify.valueAlloc(if (segmented) a else scratch, value, .{});
    errdefer if (segmented) a.free(metadata);
    var result = try encodeEnvelope(a, metadata, attachments.items, segmented);
    if (segmented) result.metadata = metadata;
    return result;
}

test "httpx inference gateway verified media encodes native attachments without rewriting tools or detail" {
    const a = std.testing.allocator;
    const url = "data:image/png;base64,aW1hZ2U=";
    const body = "{\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,aW1hZ2U=\"}},{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,aW1hZ2U=\",\"detail\":\"high\"}},{\"type\":\"media\",\"mime_type\":\"image/png\",\"data\":\"attachment:0\"}]}],\"tools\":[{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,aW1hZ2U=\"}}]}";
    var encoded = try encodeVerifiedGenerate(a, a, body, &.{.{ .mime_type = "image/png", .data = "original" }}, &.{.{ .url = url, .mime_type = "image/png", .bytes = "image" }});
    defer encoded.deinit(a);
    try std.testing.expectEqualStrings(httpx.attachment_envelope.content_type, encoded.content_type);
    var framed = try httpx.attachment_envelope.parseAlloc(a, encoded.bytes, .{});
    defer framed.deinit();
    try std.testing.expectEqual(@as(usize, 2), framed.attachments.len);
    try std.testing.expectEqualStrings("original", framed.attachments[0].data);
    try std.testing.expectEqualStrings("image", framed.attachments[1].data);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, framed.metadata, .{});
    defer parsed.deinit();
    const parts = parsed.value.object.get("messages").?.array.items[0].object.get("content").?.array.items;
    try std.testing.expectEqualStrings("attachment:1", parts[0].object.get("data").?.string);
    try std.testing.expectEqualStrings(url, parts[1].object.get("image_url").?.object.get("url").?.string);
    try std.testing.expectEqualStrings(url, parsed.value.object.get("tools").?.array.items[0].object.get("image_url").?.object.get("url").?.string);
    var untouched = try encodeVerifiedGenerate(a, a, body, null, &.{.{ .url = "different", .mime_type = "image/png", .bytes = "other" }});
    defer untouched.deinit(a);
    try std.testing.expect(!untouched.owned);
    try std.testing.expect(untouched.bytes.ptr == @as([]const u8, body).ptr);
}

test "httpx inference gateway verified attachment encoding preserves opaque number spellings" {
    const a = std.testing.allocator;
    const body = "{\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"image_url\",\"image_url\":{\"url\":\"verified\"}}]}],\"tools\":[{\"schema\":{\"large\":18446744073709551617,\"tiny\":1e-400,\"negative_zero\":-0}}]}";
    var encoded = try encodeVerifiedGenerate(a, a, body, null, &.{.{ .url = "verified", .mime_type = "image/png", .bytes = "image" }});
    defer encoded.deinit(a);
    var framed = try httpx.attachment_envelope.parseAlloc(a, encoded.bytes, .{});
    defer framed.deinit();
    inline for (.{ "18446744073709551617", "1e-400", ":-0" }) |number| try std.testing.expect(std.mem.indexOf(u8, framed.metadata, number) != null);
}

pub fn hasVerifiedHandles(media: []const VerifiedMedia) bool {
    return for (media) |view| {
        if (view.logical_url_bytes != 0) break true;
    } else false;
}

/// Late route changes and legacy/hosted adapters materialize only verified
/// handles in actual message image parts. Strings/tool arguments remain opaque.
pub fn materializeVerifiedJson(a: std.mem.Allocator, body: []const u8, media: []const VerifiedMedia) !MediaEncoding {
    if (!hasVerifiedHandles(media)) return .{ .bytes = body };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const value = try std.json.parseFromSliceLeaky(std.json.Value, scratch, body, .{ .parse_numbers = false, .duplicate_field_behavior = .@"error" });
    const Render = struct {
        a: std.mem.Allocator,
        media: []const VerifiedMedia,
        urls: std.StringHashMapUnmanaged([]const u8) = .empty,
        changed: bool = false,
        fn renderImage(self: *@This(), part: *std.json.Value) !void {
            if (part.* != .object) return;
            const kind = part.object.get("type") orelse return;
            if (kind != .string) return;
            const image = part.object.getPtr("image_url") orelse return;
            const url = if (std.mem.eql(u8, kind.string, "input_image") and image.* == .string) image else if (std.mem.eql(u8, kind.string, "image_url") and image.* == .object) image.object.getPtr("url") orelse return else return;
            if (url.* != .string) return;
            const view = for (self.media) |view| {
                if (view.logical_url_bytes != 0 and view.matches(url.string)) break view;
            } else return;
            const rendered = self.urls.get(view.url) orelse lazy: {
                const prefix = try std.fmt.allocPrint(self.a, "data:{s};base64,", .{view.mime_type});
                const data = try self.a.alloc(u8, prefix.len + std.base64.standard.Encoder.calcSize(view.bytes.len));
                @memcpy(data[0..prefix.len], prefix);
                _ = std.base64.standard.Encoder.encode(data[prefix.len..], view.bytes);
                try self.urls.put(self.a, view.url, data);
                break :lazy data;
            };
            url.* = .{ .string = rendered };
            self.changed = true;
        }
        fn messages(self: *@This(), value_: std.json.Value) !void {
            if (value_ != .array) return;
            for (value_.array.items) |message| {
                if (message != .object) continue;
                const content = message.object.get("content") orelse continue;
                if (content == .array) for (content.array.items) |*part| try self.renderImage(part);
            }
        }
        fn request(self: *@This(), value_: std.json.Value) !void {
            if (value_ != .object) return;
            if (value_.object.get("messages")) |messages_| try self.messages(messages_);
            if (value_.object.get("input")) |messages_| try self.messages(messages_);
            if (value_.object.get("inputs")) |inputs| if (inputs == .array) for (inputs.array.items) |input| {
                if (input == .object) if (input.object.get("input")) |messages_| try self.messages(messages_);
            };
        }
    };
    var render: Render = .{ .a = scratch, .media = media };
    try render.request(value);
    return if (render.changed) .{ .bytes = try std.json.Stringify.valueAlloc(a, value, .{}), .owned = true } else .{ .bytes = body };
}

test "execution context renders handles only in actual image parts on fallback" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const handle = "antfly-verified:image/png:fixture";
    const body = "{\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"image_url\",\"image_url\":{\"url\":\"" ++ handle ++ "\",\"detail\":\"auto\"}},{\"type\":\"text\",\"text\":\"" ++ handle ++ "\"}],\"tool_calls\":[{\"function\":{\"arguments\":\"" ++ handle ++ "\"}}]}],\"max_tokens\":37}";
    const media = [_]VerifiedMedia{.{ .url = handle, .bytes = "codec", .mime_type = "image/png", .logical_url_bytes = 30, .pixels = 1 }};
    var rendered = try materializeVerifiedJson(a, body, &media);
    defer rendered.deinit(a);
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, rendered.bytes, .{});
    const message = root.object.get("messages").?.array.items[0];
    const parts = message.object.get("content").?.array.items;
    try std.testing.expectEqualStrings("data:image/png;base64,Y29kZWM=", parts[0].object.get("image_url").?.object.get("url").?.string);
    try std.testing.expectEqualStrings(handle, parts[1].object.get("text").?.string);
    try std.testing.expectEqualStrings(handle, message.object.get("tool_calls").?.array.items[0].object.get("function").?.object.get("arguments").?.string);
    try std.testing.expectEqual(@as(i64, 37), root.object.get("max_tokens").?.integer);
    var encoded = try encodeVerifiedGenerate(a, a, body, null, &media);
    defer encoded.deinit(a);
    var frame = try httpx.attachment_envelope.parseAlloc(a, encoded.bytes, .{});
    defer frame.deinit();
    try std.testing.expectEqual(@as(usize, 1), frame.attachments.len);
    try std.testing.expectEqualStrings("codec", frame.attachments[0].data);
}

test "httpx inference gateway framed aliases retain one logical attachment per occurrence" {
    const a = std.testing.allocator;
    const body = "{\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"image_url\",\"image_url\":{\"url\":\"verified\"}},{\"type\":\"image_url\",\"image_url\":{\"url\":\"verified\"}}]}]}";
    var encoded = try encodeVerifiedGenerate(a, a, body, null, &.{.{ .url = "verified", .bytes = "codec", .mime_type = "image/png" }});
    defer encoded.deinit(a);
    var frame = try httpx.attachment_envelope.parseAlloc(a, encoded.bytes, .{});
    defer frame.deinit();
    try std.testing.expectEqual(@as(usize, 2), frame.attachments.len);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, frame.metadata, .{});
    defer parsed.deinit();
    const parts = parsed.value.object.get("messages").?.array.items[0].object.get("content").?.array.items;
    try std.testing.expectEqualStrings("attachment:0", parts[0].object.get("data").?.string);
    try std.testing.expectEqualStrings("attachment:1", parts[1].object.get("data").?.string);
}

test "httpx inference gateway segmented verified generation borrows codecs beyond metadata allocator capacity" {
    const a = std.testing.allocator;
    const codec = try a.alloc(u8, 1024 * 1024);
    defer a.free(codec);
    @memset(codec, 7);
    var storage: [65536]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    const body = "{\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"image_url\",\"image_url\":{\"url\":\"verified\"}}]}],\"opaque\":18446744073709551617}";
    const media = [_]VerifiedMedia{.{ .url = "verified", .bytes = codec, .mime_type = "image/png" }};
    var encoded = try encodeVerifiedGenerateSegments(fixed.allocator(), fixed.allocator(), body, null, &media);
    defer encoded.deinit(fixed.allocator());
    try std.testing.expectEqual(@as(usize, 0), encoded.bytes.len);
    const segments = encoded.segments.?.segments;
    try std.testing.expectEqual(@as([*]const u8, codec.ptr), segments[segments.len - 1].ptr);
    try std.testing.expect(std.mem.indexOf(u8, encoded.metadata.?, "18446744073709551617") != null);
    const flattened = try std.mem.concat(a, u8, segments);
    defer a.free(flattened);
    var frame = try httpx.attachment_envelope.parseAlloc(a, flattened, .{});
    defer frame.deinit();
    try std.testing.expectEqualSlices(u8, codec, frame.attachments[0].data);
    var original = try encodeVerifiedGenerate(a, a, body, null, &media);
    defer original.deinit(a);
    try std.testing.expectEqualSlices(u8, original.bytes, flattened);
}
