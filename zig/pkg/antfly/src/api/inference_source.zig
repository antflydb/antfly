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

//! Request-local table media references. Lookup must execute the ordinary
//! authorized query path with the ingress identity. Digests never grant reads.
//! Chat `source` parts also accept a pinned source-record resource, fetched here
//! under remote-content policy so its private URL never reaches a model vendor.
const std = @import("std");
const images = @import("antfly_image");
const scraping = @import("antfly_scraping");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Reference = @import("antfly_decisions").evidence.TableSourceReference;
pub const Port = struct {
    ptr: *anyopaque,
    /// Returned bytes belong to a. The host must recheck live table/row read
    /// permission, membership and the expected revision in the SAME query.
    lookup: *const fn (*anyopaque, A, Reference) anyerror![]const u8,
    lookup_many: ?*const fn (*anyopaque, A, []const Reference) anyerror![]const []const u8 = null,
    /// Returns exactly resource.length bytes of the pinned version.
    fetch: ?*const fn (*anyopaque, A, Resource) anyerror![]const u8 = null,
};
/// The `content.resource` object of a source record. Unknown fields are rejected.
pub const Resource = struct {
    type: []const u8,
    uri: []const u8,
    mime_type: []const u8,
    version: []const u8,
    version_kind: scraping.PinnedRange.VersionKind = .strong_etag,
    length: u64,

    pub fn validate(self: Resource) !void {
        if (!std.mem.eql(u8, self.type, "source") or self.version_kind != .strong_etag) return error.InvalidInferenceSource;
        const parsed = scraping.PinnedRange.validateReference(self.uri, self.version, self.version_kind, self.length) catch return error.InvalidInferenceSource;
        if (!std.ascii.eqlIgnoreCase(parsed.scheme, "http") and !std.ascii.eqlIgnoreCase(parsed.scheme, "https")) return error.InferenceSourceUnsupported;
        if (!supportedResourceMime(self.mime_type)) return error.InvalidInferenceSource;
    }
};
const max_expanded_request_bytes = 16 * 1024 * 1024;
const image_mime_types = [_][]const u8{ "image/png", "image/jpeg", "image/webp", "image/gif" };
fn isImageMime(mime: []const u8) bool {
    for (image_mime_types) |candidate| if (std.mem.eql(u8, mime, candidate)) return true;
    return false;
}
fn supportedResourceMime(mime: []const u8) bool {
    if (isImageMime(mime)) return true;
    const subtype = if (std.mem.startsWith(u8, mime, "audio/")) mime[6..] else if (std.mem.startsWith(u8, mime, "video/")) mime[6..] else return false;
    if (subtype.len == 0 or subtype.len > 127) return false;
    for (subtype) |c| if (!std.ascii.isLower(c) and !std.ascii.isDigit(c) and std.mem.indexOfScalar(u8, ".+-", c) == null) return false;
    return true;
}
const Selected = struct { part: *V, reference: Reference };
// Guard data before typed cloning or fixed-depth encoders. Recursion is bounded
// before entry; strings/base64 are scalar nodes and are not walked bytewise.
fn validateDepth(value: V, depth: usize) anyerror!void {
    if (depth >= 240) return error.InferenceRequestTooLarge;
    switch (value) {
        .array => |array| for (array.items) |child| try validateDepth(child, depth + 1),
        .object => |object| for (object.values()) |child| try validateDepth(child, depth + 1),
        else => {},
    }
}

fn collect(a: A, value: V, selected: *std.ArrayList(Selected)) !void {
    if (value != .array) return;
    for (value.array.items) |message| {
        if (message != .object) continue;
        const content = message.object.get("content") orelse continue;
        if (content != .array) continue;
        for (content.array.items) |*part| {
            if (part.* != .object) continue;
            const kind = part.object.get("type") orelse continue;
            if (kind != .string or !std.mem.eql(u8, kind.string, "input_image")) continue;
            const source = part.object.get("source") orelse continue;
            if (part.object.contains("image_url")) return error.InvalidInferenceSource;
            var fields = part.object.iterator();
            while (fields.next()) |entry| {
                const key = entry.key_ptr.*;
                if (!std.mem.eql(u8, key, "type") and !std.mem.eql(u8, key, "source") and !std.mem.eql(u8, key, "detail")) return error.InvalidInferenceSource;
            }
            try validateDepth(source, 0);
            const reference = std.json.parseFromValueLeaky(Reference, a, source, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidInferenceSource;
            try reference.validate();
            if (selected.items.len == 128) return error.InferenceRequestTooLarge;
            try selected.append(a, .{ .part = part, .reference = reference });
        }
    }
}
/// Chat `source` parts in `messages`, split by locator. Table parts become
/// `input_image` for the existing authorized path, then return to chat shape.
const ChatSources = struct {
    tables: std.ArrayList(*V) = .empty,
    resources: std.ArrayList(struct { part: *V, resource: Resource }) = .empty,
};
fn collectChatSources(a: A, request: V) !ChatSources {
    var found: ChatSources = .{};
    const messages = request.object.get("messages") orelse return found;
    if (messages != .array) return found;
    for (messages.array.items) |message| {
        if (message != .object) continue;
        const content = message.object.get("content") orelse continue;
        if (content != .array) continue;
        for (content.array.items) |*part| {
            if (part.* != .object) continue;
            const kind = part.object.get("type") orelse continue;
            if (kind != .string or !std.mem.eql(u8, kind.string, "source")) continue;
            const source = part.object.get("source") orelse return error.InvalidInferenceSource;
            if (source != .object or part.object.count() != 2) return error.InvalidInferenceSource;
            if (found.tables.items.len + found.resources.items.len == 128) return error.InferenceRequestTooLarge;
            if (source.object.contains("table")) {
                try part.object.put(a, "type", .{ .string = "input_image" });
                try found.tables.append(a, part);
                continue;
            }
            try validateDepth(source, 0);
            const resource = std.json.parseFromValueLeaky(Resource, a, source, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidInferenceSource;
            try resource.validate();
            try found.resources.append(a, .{ .part = part, .resource = resource });
        }
    }
    return found;
}
fn setChatImage(a: A, part: *V, url: []const u8) !void {
    var image: V = .{ .object = .empty };
    try image.object.put(a, "url", .{ .string = url });
    var replacement: V = .{ .object = .empty };
    try replacement.object.put(a, "type", .{ .string = "image_url" });
    try replacement.object.put(a, "image_url", image);
    part.* = replacement;
}
fn resolveChatResources(a: A, request: V, found: ChatSources, port: ?Port) !void {
    if (found.resources.items.len == 0) return;
    const fetch = (port orelse return error.InferenceSourceUnsupported).fetch orelse return error.InferenceSourceUnsupported;
    // The serializer below has a fixed nesting depth, so guard the whole request.
    try validateDepth(request, 0);
    // Reject before any I/O. Table parts are already expanded in this count.
    var counter: std.Io.Writer.Discarding = .init(&.{});
    try std.json.Stringify.value(request, .{}, &counter.writer);
    var expanded = std.math.cast(usize, counter.fullCount()) orelse return error.InferenceRequestTooLarge;
    for (found.resources.items) |item| {
        const length = std.math.cast(usize, item.resource.length) orelse return error.InferenceRequestTooLarge;
        if (length > max_expanded_request_bytes) return error.InferenceRequestTooLarge;
        expanded = std.math.add(usize, expanded, std.base64.standard.Encoder.calcSize(length) + 256) catch return error.InferenceRequestTooLarge;
        if (expanded > max_expanded_request_bytes) return error.InferenceRequestTooLarge;
    }
    for (found.resources.items) |item| {
        const resource = item.resource;
        const bytes = try fetch(port.?.ptr, a, resource);
        if (bytes.len != resource.length) return error.InferenceSourceMismatch;
        const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
        _ = std.base64.standard.Encoder.encode(encoded, bytes);
        if (isImageMime(resource.mime_type)) {
            const mime = images.inferenceMimeEssenceForFormat(images.detectFormat(bytes)) orelse return error.InferenceSourceMismatch;
            if (!std.mem.eql(u8, mime, resource.mime_type)) return error.InferenceSourceMismatch;
            const info = try images.inspectEncoded(bytes);
            if (try info.pixels() > 64 * 1024 * 1024) return error.InferenceRequestTooLarge;
            try setChatImage(a, item.part, try std.fmt.allocPrint(a, "data:{s};base64,{s}", .{ resource.mime_type, encoded }));
            continue;
        }
        var replacement: V = .{ .object = .empty };
        try replacement.object.put(a, "type", .{ .string = "media" });
        try replacement.object.put(a, "mime_type", .{ .string = resource.mime_type });
        try replacement.object.put(a, "data", .{ .string = encoded });
        item.part.* = replacement;
    }
}
/// Fetches one pinned resource. Unlike template fetches, an omitted allowlist
/// denies: a caller-supplied URL must never reach an unrestricted fetcher.
pub fn fetchResource(a: A, io: std.Io, remote_content: ?*const scraping.RemoteContentConfig, resource: Resource, cancellation: ?scraping.CancellationToken, timeout_ms: ?u64) ![]const u8 {
    const config = remote_content orelse return error.Forbidden;
    var snapshot = try config.acquire();
    defer snapshot.deinit();
    const configured = if (snapshot.config.security) |*value| value else return error.Forbidden;
    var security = try resourceSecurity(a, configured.*, resource.uri);
    const context: scraping.DownloadContext = .{ .io = io, .timeout_ms = timeout_ms, .cancellation = cancellation };
    const out = try a.alloc(u8, @intCast(resource.length));
    const range = scraping.PinnedRange.create(a, context, resource.uri, resource.version, resource.version_kind, resource.length, &security, null, &.{}) catch |err| return fetchFailure(err);
    defer range.destroy();
    const read = range.readAt(0, out) catch |err| return fetchFailure(err);
    if (read != out.len) return error.InferenceSourceMismatch;
    return out;
}
/// The policy for one caller-supplied URL. A configured companion base URL is the
/// only way to reach a private address; `block_private_ips: false` is not, because
/// host-level policy would also expose every other port on that host.
fn resourceSecurity(a: A, configured: scraping.ContentSecurityConfig, uri: []const u8) !scraping.ContentSecurityConfig {
    var security = configured;
    const parsed = std.Uri.parse(uri) catch return error.InvalidInferenceSource;
    for (configured.source_media_base_urls orelse &.{}) |base| {
        if (!scraping.httpUrlMatchesBase(parsed, base)) continue;
        const host = try a.alloc(u8, std.Io.net.HostName.max_len);
        const raw = (parsed.host orelse return error.InvalidInferenceSource).toRaw(host) catch return error.InvalidInferenceSource;
        const hosts = try a.alloc([]u8, 1);
        hosts[0] = @constCast(raw);
        security.allowed_hosts = hosts;
        security.block_private_ips = false;
        return security;
    }
    const hosts = configured.allowed_hosts orelse return error.Forbidden;
    if (hosts.len == 0) return error.Forbidden;
    security.block_private_ips = true;
    return security;
}
fn fetchFailure(err: anyerror) anyerror {
    return switch (err) {
        error.HostNotAllowed, error.PrivateIpBlocked => error.Forbidden,
        error.SourceVersionChanged, error.InvalidRemoteRange => error.InferenceSourceMismatch,
        error.StreamTooLong => error.InferenceRequestTooLarge,
        error.InvalidHost, error.InvalidSourceReference, error.ImmutableVersionRequired, error.UnsupportedUrlScheme => error.InvalidInferenceSource,
        else => err,
    };
}
pub fn resolve(a: A, request: V, port: ?Port) !void {
    if (request != .object) return;
    const chat = try collectChatSources(a, request);
    try resolveTables(a, request, port);
    for (chat.tables.items) |part| {
        const url = part.object.get("image_url") orelse return error.InferenceSourceMismatch;
        try setChatImage(a, part, url.string);
    }
    try resolveChatResources(a, request, chat, port);
}
fn resolveTables(a: A, request: V, port: ?Port) !void {
    var selected: std.ArrayList(Selected) = .empty;
    defer selected.deinit(a);
    if (request.object.get("input")) |input| try collect(a, input, &selected);
    if (request.object.get("inputs")) |inputs| {
        if (inputs == .array) for (inputs.array.items) |input| {
            if (input == .object) if (input.object.get("input")) |messages| try collect(a, messages, &selected);
        };
    }
    if (request.object.get("messages")) |messages| try collect(a, messages, &selected);
    if (selected.items.len == 0) return;
    try validateDepth(request, 0);
    const authorized = port orelse return error.InferenceSourceUnsupported;
    var bytes: usize = 0;
    for (selected.items) |item| {
        bytes = std.math.add(usize, bytes, std.base64.standard.Encoder.calcSize(item.reference.byte_length) + 256) catch return error.InferenceRequestTooLarge;
        if (bytes > 16 * 1024 * 1024) return error.InferenceRequestTooLarge;
    }
    // Count the compact request once, before replacing locators with images.
    var original: std.Io.Writer.Discarding = .init(&.{});
    try std.json.Stringify.value(request, .{}, &original.writer);
    var expanded: usize = std.math.cast(usize, original.fullCount()) orelse return error.InferenceRequestTooLarge;
    if (expanded > 16 * 1024 * 1024) return error.InferenceRequestTooLarge;
    // Validate all reference shapes and aggregate limits before reading any row.
    // Each unique source is fetched once; only this request shares its authority.
    var unique: std.StringHashMapUnmanaged(usize) = .empty;
    defer {
        var keys = unique.keyIterator();
        while (keys.next()) |key| a.free(key.*);
        unique.deinit(a);
    }
    var references: std.ArrayList(Reference) = .empty;
    defer references.deinit(a);
    const slots = try a.alloc(usize, selected.items.len);
    defer a.free(slots);
    for (selected.items, slots) |item, *slot| {
        const key = try std.json.Stringify.valueAlloc(a, item.reference, .{});
        if (unique.get(key)) |existing| {
            a.free(key);
            slot.* = existing;
        } else {
            slot.* = references.items.len;
            unique.put(a, key, slot.*) catch |err| {
                a.free(key);
                return err;
            };
            try references.append(a, item.reference);
        }
    }
    const urls = if (authorized.lookup_many) |batch| try batch(authorized.ptr, a, references.items) else sequential: {
        const output = try a.alloc([]const u8, references.items.len);
        for (references.items, output) |reference, *url| url.* = try authorized.lookup(authorized.ptr, a, reference);
        break :sequential output;
    };
    if (urls.len != references.items.len) return error.InferenceSourceMismatch;
    // Decode/inspect a distinct payload once per request. Matching references
    // still independently check digest, MIME, byte count and their pixel limit.
    var verified: std.StringHashMapUnmanaged(Verified) = .empty;
    defer verified.deinit(a);
    for (references.items, urls) |reference, url| {
        const info = verified.get(url) orelse checked: {
            const info = try verifyInfo(a, reference, url);
            try verified.put(a, url, info);
            break :checked info;
        };
        if (info.bytes != reference.byte_length or !std.mem.eql(u8, &info.digest, reference.sha256) or !std.mem.eql(u8, info.mime, reference.mime_type)) return error.InferenceSourceMismatch;
        if (info.pixels > reference.max_pixels) return error.InferenceRequestTooLarge;
    }
    // Base64 data URLs have JSON-safe ASCII spellings, proven by verification.
    // Replace exactly the serialized source value and the three extra key bytes;
    // do not walk expanded image bytes through a second counting encoder.
    for (selected.items, slots) |item, slot| {
        var counter: std.Io.Writer.Discarding = .init(&.{});
        try std.json.Stringify.value(item.part.object.get("source").?, .{}, &counter.writer);
        const previous = std.math.cast(usize, counter.fullCount()) orelse return error.InferenceRequestTooLarge;
        expanded = std.math.add(usize, expanded - previous, urls[slot].len + 5) catch return error.InferenceRequestTooLarge;
        if (expanded > 16 * 1024 * 1024) return error.InferenceRequestTooLarge;
    }
    for (selected.items, slots) |item, slot| {
        _ = item.part.object.swapRemove("source");
        try item.part.object.put(a, "image_url", .{ .string = urls[slot] });
    }
}
const Verified = struct { bytes: usize, digest: [64]u8, mime: []const u8, pixels: u64 };
fn verify(a: A, reference: Reference, url: []const u8) !void {
    _ = try verifyInfo(a, reference, url);
}
fn verifyInfo(a: A, reference: Reference, url: []const u8) !Verified {
    const prefix = try std.fmt.allocPrint(a, "data:{s};base64,", .{reference.mime_type});
    defer a.free(prefix);
    if (!std.mem.startsWith(u8, url, prefix)) return error.InferenceSourceMismatch;
    const encoded = url[prefix.len..];
    const length = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InferenceSourceMismatch;
    if (length != reference.byte_length) return error.InferenceSourceMismatch;
    const scratch = @import("antfly_local_sources").sql_memory_budget.reclaimingAllocator(a);
    const bytes = try scratch.alloc(u8, length);
    defer scratch.free(bytes);
    std.base64.standard.Decoder.decode(bytes, encoded) catch return error.InferenceSourceMismatch;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    if (!std.mem.eql(u8, &std.fmt.bytesToHex(digest, .lower), reference.sha256)) return error.InferenceSourceMismatch;
    const mime = images.inferenceMimeEssenceForFormat(images.detectFormat(bytes)) orelse return error.InferenceSourceMismatch;
    if (!std.mem.eql(u8, mime, reference.mime_type)) return error.InferenceSourceMismatch;
    const info = try images.inspectEncoded(bytes);
    const pixels = try info.pixels();
    if (pixels > reference.max_pixels) return error.InferenceRequestTooLarge;
    return .{ .bytes = length, .digest = std.fmt.bytesToHex(digest, .lower), .mime = mime, .pixels = pixels };
}

test "httpx inference gateway source references validate all locators before authorized materialization" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const encoded = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7S8AAAAASUVORK5CYII=";
    var bytes: [68]u8 = undefined;
    try std.base64.standard.Decoder.decode(&bytes, encoded);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&bytes, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const reference: Reference = .{ .table = "docs", .document_id = "a", .field = "assets.preview0", .revision_field = "grep.revision", .revision = "r1", .sha256 = &hex, .mime_type = "image/png", .byte_length = bytes.len, .max_pixels = 16 };
    const source = try std.json.Stringify.valueAlloc(temp, reference, .{});
    const body = try std.fmt.allocPrint(temp, "{{\"inputs\":[{{\"input\":[{{\"role\":\"user\",\"content\":[{{\"type\":\"input_image\",\"source\":{s}}},{{\"type\":\"input_image\",\"source\":{s}}}]}}]}}]}}", .{ source, source });
    const Fake = struct {
        url: []const u8,
        calls: usize = 0,
        allowed: bool = true,
        fn lookup(ptr: *anyopaque, alloc: A, expected: Reference) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (!self.allowed) return error.Forbidden;
            if (!std.mem.eql(u8, expected.revision, "r1")) return error.InferenceSourceMismatch;
            return try alloc.dupe(u8, self.url);
        }
    };
    var fake: Fake = .{ .url = "data:image/png;base64," ++ encoded };
    const port: Port = .{ .ptr = &fake, .lookup = Fake.lookup };
    const value = try std.json.parseFromSliceLeaky(V, temp, body, .{});
    try std.testing.expectError(error.InferenceSourceUnsupported, resolve(temp, value, null));
    try resolve(temp, value, port);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    const rendered = try std.json.Stringify.valueAlloc(temp, value, .{});
    try std.testing.expect(std.mem.indexOf(u8, rendered, "\"source\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "data:image/png;base64,") != null);
    fake.allowed = false;
    const denied = try std.json.parseFromSliceLeaky(V, temp, body, .{});
    try std.testing.expectError(error.Forbidden, resolve(temp, denied, port));
    fake.allowed = true;
    fake.calls = 0;
    const invalid = try std.json.parseFromSliceLeaky(V, temp, body, .{});
    const parts = invalid.object.get("inputs").?.array.items[0].object.get("input").?.array.items[0].object.get("content").?.array.items;
    try parts[1].object.getPtr("source").?.object.put(temp, "unrecognized", .{ .bool = true });
    try std.testing.expectError(error.InvalidInferenceSource, resolve(temp, invalid, port));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    var wrong = reference;
    wrong.sha256 = "0000000000000000000000000000000000000000000000000000000000000000";
    try std.testing.expectError(error.InferenceSourceMismatch, verify(temp, wrong, fake.url));
    wrong = reference;
    wrong.byte_length += 1;
    try std.testing.expectError(error.InferenceSourceMismatch, verify(temp, wrong, fake.url));
    wrong = reference;
    wrong.mime_type = "image/jpeg";
    try std.testing.expectError(error.InferenceSourceMismatch, verify(temp, wrong, fake.url));
    wrong = reference;
    wrong.field = "assets.*";
    try std.testing.expectError(error.InvalidInferenceSource, wrong.validate());
    // One URL with different claims must not inherit another reference's proof.
    const inconsistent = try std.json.parseFromSliceLeaky(V, temp, body, .{});
    const inconsistent_parts = inconsistent.object.get("inputs").?.array.items[0].object.get("input").?.array.items[0].object.get("content").?.array.items;
    try inconsistent_parts[1].object.getPtr("source").?.object.put(temp, "sha256", .{ .string = "0000000000000000000000000000000000000000000000000000000000000000" });
    try std.testing.expectError(error.InferenceSourceMismatch, resolve(temp, inconsistent, port));
    for (inconsistent_parts) |part| try std.testing.expect(part.object.contains("source") and !part.object.contains("image_url"));
    var oversized = try std.json.parseFromSliceLeaky(V, temp, body, .{});
    const padding = try temp.alloc(u8, 16 * 1024 * 1024);
    @memset(padding, 'p');
    try oversized.object.put(temp, "padding", .{ .string = padding });
    fake.calls = 0;
    try std.testing.expectError(error.InferenceRequestTooLarge, resolve(temp, oversized, port));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    const deep = try std.json.parseFromSliceLeaky(V, temp, body, .{});
    var tree: V = .null;
    for (0..300) |_| {
        var object: std.json.ObjectMap = .empty;
        try object.put(temp, "nested", tree);
        tree = .{ .object = object };
    }
    const deep_parts = deep.object.get("inputs").?.array.items[0].object.get("input").?.array.items[0].object.get("content").?.array.items;
    try deep_parts[0].object.getPtr("source").?.object.put(temp, "filter_query", tree);
    try std.testing.expectError(error.InferenceRequestTooLarge, resolve(temp, deep, port));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
}

/// Only actual absence becomes 404. Preserve retryable table failures through
/// the gateway instead of negatively caching a transient source lookup.
pub fn checkStatus(status: i64) !void {
    if (status >= 200 and status < 300) return;
    return switch (status) {
        401, 403 => error.Forbidden,
        404 => error.InferenceSourceUnavailable,
        429 => error.DecisionRateLimited,
        408, 504 => error.Timeout,
        500...503, 505...599 => error.InferenceUpstreamUnavailable,
        else => error.InvalidInferenceSource,
    };
}
pub const Group = struct {
    /// Numeric source masks compact arrays; resolve those paths before masking.
    /// Object-only paths can retain the cheaper public source projection.
    pub fn requiresFlatValues(self: @This()) bool {
        for (self.fields.items) |path| {
            var parts = std.mem.splitScalar(u8, path, '.');
            while (parts.next()) |part| {
                if (part.len != 0 and for (part) |byte| {
                    if (!std.ascii.isDigit(byte)) break false;
                } else true) return true;
            }
        }
        return false;
    }
    references: std.ArrayList(usize) = .empty,
    ids: std.ArrayList([]const u8) = .empty,
    fields: std.ArrayList([]const u8) = .empty,
    images: std.ArrayList([]const u8) = .empty,
    identity_images: std.ArrayList([]const u8) = .empty,
};
fn contains(values: []const []const u8, value: []const u8) bool {
    for (values) |item| if (std.mem.eql(u8, item, value)) return true;
    return false;
}
fn fits(group: Group, reference: Reference) bool {
    var added: [3][]const u8 = undefined;
    var count: usize = 0;
    const paths = [_]?[]const u8{ reference.field, reference.revision_field, reference.permissions_field };
    for (paths) |maybe| if (maybe) |path| {
        if (!contains(group.fields.items, path) and !contains(added[0..count], path)) {
            added[count] = path;
            count += 1;
        }
    };
    return group.fields.items.len + count + group.images.items.len + @intFromBool(!contains(group.images.items, reference.field)) + group.identity_images.items.len + @intFromBool(reference.identity != null and !contains(group.identity_images.items, reference.field)) <= 64;
}
fn add(a: A, list: *std.ArrayList([]const u8), field: []const u8) !void {
    for (list.items) |existing| if (std.mem.eql(u8, existing, field)) return;
    try list.append(a, field);
}
fn identityUnionCount(left: []const []const u8, right: []const []const u8) usize {
    var count = left.len;
    for (right) |field| if (!contains(left, field)) {
        count += 1;
    };
    return count;
}
fn equalFields(left: []const []const u8, right: []const []const u8) bool {
    if (left.len != right.len) return false;
    for (left) |field| {
        const found = for (right) |other| {
            if (std.mem.eql(u8, field, other)) break true;
        } else false;
        if (!found) return false;
    }
    return true;
}
/// First union fields within a parent, then batch parents with the SAME mask.
/// Never form a Cartesian product of unrelated image selectors across rows.
/// All storage belongs to the request arena; at most 32 references per query.
pub fn groups(a: A, references: []const Reference) ![]Group {
    // Intern each canonical filter once. Comparisons remain exact, including
    // collision checks, without encoding large filters inside quadratic loops.
    const scratch = @import("antfly_local_sources").sql_memory_budget.reclaimingAllocator(a);
    var filter_arena = std.heap.ArenaAllocator.init(scratch);
    defer filter_arena.deinit();
    const work = filter_arena.allocator();
    const filters = try work.alloc([]const u8, references.len);
    var interned: std.StringHashMapUnmanaged([]const u8) = .empty;
    defer interned.deinit(work);
    for (references, filters) |reference, *filter| {
        if (reference.filter_query == null) {
            filter.* = "null";
            continue;
        }
        const encoded = try std.json.Stringify.valueAlloc(scratch, reference.filter_query, .{});
        defer scratch.free(encoded);
        filter.* = interned.get(encoded) orelse inserted: {
            const owned = try work.dupe(u8, encoded);
            try interned.put(work, owned, owned);
            break :inserted owned;
        };
    }
    var parents: std.ArrayList(Group) = .empty;
    for (references, 0..) |reference, i| {
        const slot = found: {
            for (parents.items, 0..) |parent, j| {
                const first = references[parent.references.items[0]];
                if (parent.references.items.len < 32 and fits(parent, reference) and std.mem.eql(u8, first.document_id, reference.document_id) and std.mem.eql(u8, first.table, reference.table) and filters[parent.references.items[0]].ptr == filters[i].ptr) break :found j;
            }
            try parents.append(a, .{});
            break :found parents.items.len - 1;
        };
        const parent = &parents.items[slot];
        try parent.references.append(a, i);
        try add(a, &parent.ids, reference.document_id);
        try add(a, &parent.fields, reference.field);
        try add(a, &parent.fields, reference.revision_field);
        if (reference.permissions_field) |field| try add(a, &parent.fields, field);
        try add(a, &parent.images, reference.field);
        if (reference.identity != null) try add(a, &parent.identity_images, reference.field);
    }
    var output: std.ArrayList(Group) = .empty;
    for (parents.items) |parent| {
        const slot = found: {
            for (output.items, 0..) |other, i| {
                if (other.references.items.len + parent.references.items.len <= 32 and
                    equalFields(other.fields.items, parent.fields.items) and equalFields(other.images.items, parent.images.items) and
                    identityUnionCount(other.identity_images.items, parent.identity_images.items) + other.fields.items.len + other.images.items.len <= 64 and
                    std.mem.eql(u8, references[other.references.items[0]].table, references[parent.references.items[0]].table) and filters[other.references.items[0]].ptr == filters[parent.references.items[0]].ptr) break :found i;
            }
            try output.append(a, .{});
            break :found output.items.len - 1;
        };
        const group = &output.items[slot];
        if (group.fields.items.len == 0) {
            group.fields = parent.fields;
            group.images = parent.images;
        }
        for (parent.identity_images.items) |field| try add(a, &group.identity_images, field);
        try group.references.appendSlice(a, parent.references.items);
        for (parent.ids.items) |id| try add(a, &group.ids, id);
    }
    return output.items;
}

const ChatFake = struct {
    bytes: []const u8 = "audio bytes",
    calls: usize = 0,
    failure: ?anyerror = null,
    fn lookup(_: *anyopaque, _: A, _: Reference) ![]const u8 {
        return error.Unexpected;
    }
    fn fetch(ptr: *anyopaque, _: A, _: Resource) ![]const u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        if (self.failure) |err| return err;
        return self.bytes;
    }
    fn port(self: *@This()) Port {
        return .{ .ptr = self, .lookup = lookup, .fetch = fetch };
    }
};
fn chatRequest(a: A, mime: []const u8, length: u64) !V {
    const body = try std.fmt.allocPrint(a,
        \\{{"model":"m","messages":[{{"role":"user","content":[{{"type":"text","text":"hi"}},{{"type":"source","source":{{"type":"source","uri":"https://sources.example/v1/media/a/b","mime_type":"{s}","version":"\"abc\"","version_kind":"strong_etag","length":{d}}}}}]}}]}}
    , .{ mime, length });
    return std.json.parseFromSliceLeaky(V, a, body, .{});
}

test "httpx inference gateway chat source parts inline audio and video without their URL" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "audio/wav", "video/mp4" }) |mime| {
        var fake: ChatFake = .{};
        const request = try chatRequest(a, mime, fake.bytes.len);
        try resolve(a, request, fake.port());
        try std.testing.expectEqual(@as(usize, 1), fake.calls);
        const part = request.object.get("messages").?.array.items[0].object.get("content").?.array.items[1];
        try std.testing.expectEqualStrings("media", part.object.get("type").?.string);
        try std.testing.expectEqualStrings(mime, part.object.get("mime_type").?.string);
        try std.testing.expectEqualStrings("YXVkaW8gYnl0ZXM=", part.object.get("data").?.string);
        const rendered = try std.json.Stringify.valueAlloc(a, request, .{});
        try std.testing.expect(std.mem.indexOf(u8, rendered, "sources.example") == null);
        const requirements = try @import("antfly_local_sources").common_inference_tasks.chatRequirements(request);
        try std.testing.expectEqual(std.mem.startsWith(u8, mime, "audio/"), requirements.audio);
        try std.testing.expectEqual(std.mem.startsWith(u8, mime, "video/"), requirements.video);
    }
}

test "httpx inference gateway chat source parts verify images and return chat shape" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const encoded = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7S8AAAAASUVORK5CYII=";
    var png: [68]u8 = undefined;
    try std.base64.standard.Decoder.decode(&png, encoded);
    var fake: ChatFake = .{ .bytes = &png };
    const request = try chatRequest(a, "image/png", png.len);
    try resolve(a, request, fake.port());
    const part = request.object.get("messages").?.array.items[0].object.get("content").?.array.items[1];
    try std.testing.expectEqualStrings("image_url", part.object.get("type").?.string);
    try std.testing.expectEqualStrings("data:image/png;base64," ++ encoded, part.object.get("image_url").?.object.get("url").?.string);
    // Declared and actual formats must agree.
    const wrong = try chatRequest(a, "image/jpeg", png.len);
    try std.testing.expectError(error.InferenceSourceMismatch, resolve(a, wrong, fake.port()));
}

test "httpx inference gateway chat source parts bound and validate before fetching" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fake: ChatFake = .{};
    try std.testing.expectError(error.InferenceRequestTooLarge, resolve(a, try chatRequest(a, "video/mp4", max_expanded_request_bytes + 1), fake.port()));
    // Base64 expansion counts: 13 MiB of bytes exceeds the 16 MiB request bound.
    try std.testing.expectError(error.InferenceRequestTooLarge, resolve(a, try chatRequest(a, "video/mp4", 13 * 1024 * 1024), fake.port()));
    for ([_][]const u8{ "application/pdf", "text/plain", "image/svg+xml", "video/", "audio/WAV", "audio/x;y" }) |mime| {
        try std.testing.expectError(error.InvalidInferenceSource, resolve(a, try chatRequest(a, mime, 11), fake.port()));
    }
    for ([_][]const u8{
        \\{"messages":[{"role":"user","content":[{"type":"source"}]}]}
        ,
        \\{"messages":[{"role":"user","content":[{"type":"source","source":"https://example.org/a"}]}]}
        ,
        \\{"messages":[{"role":"user","content":[{"type":"source","detail":"auto","source":{"type":"source","uri":"https://e.org/a","mime_type":"audio/wav","version":"\"v\"","length":1}}]}]}
        ,
        \\{"messages":[{"role":"user","content":[{"type":"source","source":{"type":"source","uri":"https://e.org/a","mime_type":"audio/wav","version":"\"v\"","length":1,"extra":true}}]}]}
        ,
        \\{"messages":[{"role":"user","content":[{"type":"source","source":{"type":"source","uri":"https://e.org/a","mime_type":"audio/wav","version":"W/\"v\"","length":1}}]}]}
        ,
        \\{"messages":[{"role":"user","content":[{"type":"source","source":{"type":"source","uri":"https://u:p@e.org/a","mime_type":"audio/wav","version":"\"v\"","length":1}}]}]}
        ,
        \\{"messages":[{"role":"user","content":[{"type":"source","source":{"type":"source","uri":"file:///etc/passwd","mime_type":"audio/wav","version":"\"v\"","length":1}}]}]}
    }) |body| {
        const request = try std.json.parseFromSliceLeaky(V, a, body, .{});
        try std.testing.expectError(error.InvalidInferenceSource, resolve(a, request, fake.port()));
    }
    const s3 = try std.json.parseFromSliceLeaky(V, a,
        \\{"messages":[{"role":"user","content":[{"type":"source","source":{"type":"source","uri":"s3://bucket/a","mime_type":"audio/wav","version":"\"v\"","length":1}}]}]}
    , .{});
    try std.testing.expectError(error.InferenceSourceUnsupported, resolve(a, s3, fake.port()));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectError(error.InferenceSourceUnsupported, resolve(a, try chatRequest(a, "audio/wav", 11), null));
    var no_fetch: ChatFake = .{};
    var port = no_fetch.port();
    port.fetch = null;
    try std.testing.expectError(error.InferenceSourceUnsupported, resolve(a, try chatRequest(a, "audio/wav", 11), port));
    var short: ChatFake = .{};
    try std.testing.expectError(error.InferenceSourceMismatch, resolve(a, try chatRequest(a, "audio/wav", 12), short.port()));
    var failing: ChatFake = .{ .failure = error.Forbidden };
    try std.testing.expectError(error.Forbidden, resolve(a, try chatRequest(a, "audio/wav", 11), failing.port()));
}

const PinnedFixture = struct {
    io: std.Io,
    server: *std.Io.net.Server,
    served: bool = false,
    failure: ?anyerror = null,
    fn serve(self: *@This()) void {
        var stream = self.server.accept(self.io) catch |err| {
            if (err != error.Canceled) self.failure = err;
            return;
        };
        defer stream.close(self.io);
        var read_buffer: [4096]u8 = undefined;
        var reader = stream.reader(self.io, &read_buffer);
        while (true) {
            const line = reader.interface.takeDelimiterInclusive('\n') catch |err| {
                self.failure = err;
                return;
            };
            if (std.mem.eql(u8, line, "\r\n")) break;
        }
        var write_buffer: [512]u8 = undefined;
        var writer = stream.writer(self.io, &write_buffer);
        writer.interface.writeAll("HTTP/1.1 206 Partial Content\r\nETag: \"v\"\r\nContent-Range: bytes 0-4/5\r\nContent-Length: 5\r\nConnection: close\r\n\r\nvideo") catch |err| {
            self.failure = err;
            return;
        };
        writer.interface.flush() catch |err| {
            self.failure = err;
            return;
        };
        self.served = true;
    }
};

test "httpx inference gateway chat source parts reject deep requests before serializing" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var body: std.ArrayList(u8) = .empty;
    try body.appendSlice(a,
        \\{"messages":[{"role":"user","content":[{"type":"source","source":{"type":"source","uri":"https://e.org/a","mime_type":"audio/wav","version":"\"v\"","length":1}}]}],"extra":
    );
    try body.appendNTimes(a, '[', 300);
    try body.appendNTimes(a, ']', 300);
    try body.append(a, '}');
    const request = try std.json.parseFromSliceLeaky(V, a, body.items, .{ .max_value_len = body.items.len });
    var fake: ChatFake = .{};
    try std.testing.expectError(error.InferenceRequestTooLarge, resolve(a, request, fake.port()));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
}

test "httpx inference gateway chat source resources reach private addresses only through a base URL" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const io = std.testing.io;
    var server = try (try std.Io.net.IpAddress.parse("127.0.0.1", 0)).listen(io, .{});
    defer server.deinit(io);
    const port = server.socket.address.getPort();
    const base = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/v1/media/", .{port});
    const uri = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/v1/media/id/sig", .{port});
    const resource: Resource = .{ .type = "source", .uri = uri, .mime_type = "video/mp4", .version = "\"v\"", .length = 5 };

    var loopback = [_][]u8{try a.dupe(u8, "127.0.0.1")};
    var empty: [0][]u8 = .{};
    var other_hosts = [_][]u8{try a.dupe(u8, "sources.example")};
    var remote: scraping.RemoteContentConfig = .{};
    try std.testing.expectError(error.Forbidden, fetchResource(a, io, null, resource, null, 100));
    try std.testing.expectError(error.Forbidden, fetchResource(a, io, &remote, resource, null, 100));
    for ([_]scraping.ContentSecurityConfig{
        .{ .block_private_ips = false },
        .{ .allowed_hosts = &empty, .block_private_ips = false },
        .{ .allowed_hosts = &other_hosts, .block_private_ips = false },
        // Host-level policy must not reach loopback, even when globally unblocked.
        .{ .allowed_hosts = &loopback, .block_private_ips = false },
        .{ .allowed_hosts = &loopback },
    }) |security| {
        remote.security = security;
        try std.testing.expectError(error.Forbidden, fetchResource(a, io, &remote, resource, null, 100));
    }

    // The base URL pins scheme, host, port and path.
    var bases = [_][]u8{base};
    remote.security = .{ .source_media_base_urls = &bases };
    for ([_][]const u8{
        try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/v1/media/x", .{port +% 1}),
        try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/admin", .{port}),
        try std.fmt.allocPrint(a, "http://localhost:{d}/v1/media/x", .{port}),
        try std.fmt.allocPrint(a, "https://127.0.0.1:{d}/v1/media/x", .{port}),
    }) |denied| {
        var other = resource;
        other.uri = denied;
        try std.testing.expectError(error.Forbidden, fetchResource(a, io, &remote, other, null, 100));
    }

    var fixture: PinnedFixture = .{ .io = io, .server = &server };
    var thread = try io.concurrent(PinnedFixture.serve, .{&fixture});
    const bytes = try fetchResource(a, io, &remote, resource, null, 5000);
    thread.await(io);
    if (fixture.failure) |err| return err;
    try std.testing.expect(fixture.served);
    try std.testing.expectEqualStrings("video", bytes);
}

test "httpx inference gateway preserves temporary source lookup failures" {
    try checkStatus(200);
    try std.testing.expectError(error.DecisionRateLimited, checkStatus(429));
    try std.testing.expectError(error.InferenceUpstreamUnavailable, checkStatus(503));
    try std.testing.expectError(error.Timeout, checkStatus(504));
    try std.testing.expectError(error.InferenceSourceUnavailable, checkStatus(404));
    try std.testing.expectError(error.Forbidden, checkStatus(403));
}

test "httpx inference gateway batches same parents and equal masks without Cartesian overfetch" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const base: Reference = .{ .table = "docs", .document_id = "a", .field = "images.one", .revision_field = "revision", .revision = "r1", .sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .mime_type = "image/png", .byte_length = 68, .max_pixels = 16 };
    var second = base;
    second.field = "images.two";
    var third = base;
    third.document_id = "b";
    var fourth = second;
    fourth.document_id = "b";
    const combined = try groups(a, &.{ base, second, third, fourth });
    try std.testing.expectEqual(@as(usize, 1), combined.len);
    try std.testing.expectEqual(@as(usize, 2), combined[0].ids.items.len);
    try std.testing.expectEqual(@as(usize, 3), combined[0].fields.items.len);
    const separate = try groups(a, &.{ base, fourth });
    try std.testing.expectEqual(@as(usize, 2), separate.len);
    var limited: [65]Reference = @splat(base);
    for (&limited, 0..) |*reference, i| reference.document_id = try std.fmt.allocPrint(a, "id-{d}", .{i});
    const bounded = try groups(a, &limited);
    try std.testing.expectEqual(@as(usize, 3), bounded.len);
    for (bounded) |group| try std.testing.expect(group.references.items.len <= 32);
}

test "httpx inference gateway grouping large distinct filters retains linear bounded storage" {
    var input = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer input.deinit();
    const a = input.allocator();
    const padding = try a.alloc(u8, 3072);
    @memset(padding, 'x');
    var references: [128]Reference = undefined;
    for (&references, 0..) |*reference, i| {
        var filter: std.json.ObjectMap = .empty;
        var ids: std.json.Array = .init(a);
        try ids.append(.{ .string = try std.fmt.allocPrint(a, "filter-{d}-{s}", .{ i, padding }) });
        try filter.put(a, "ids", .{ .array = ids });
        reference.* = .{ .table = "docs", .document_id = try std.fmt.allocPrint(a, "doc-{d}", .{i}), .field = "images.preview", .revision_field = "revision", .revision = "r1", .sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .mime_type = "image/png", .byte_length = 68, .filter_query = .{ .object = filter } };
    }
    var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = std.testing.allocator, .limit = 2 * 1024 * 1024 };
    var bounded = std.heap.ArenaAllocator.init(budget.allocator());
    defer bounded.deinit();
    const grouped = try groups(bounded.allocator(), &references);
    try std.testing.expectEqual(references.len, grouped.len);
    try std.testing.expect(!budget.exhausted);
}

test "httpx inference gateway groups respect scalar binding limits and retain every reference" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var references: [32]Reference = undefined;
    for (&references, 0..) |*reference, i| reference.* = .{
        .table = "docs",
        .document_id = "a",
        .field = try std.fmt.allocPrint(a, "images.{d}", .{i}),
        .revision_field = try std.fmt.allocPrint(a, "revisions.{d}", .{i}),
        .revision = "r1",
        .permissions_field = try std.fmt.allocPrint(a, "permissions.{d}", .{i}),
        .identity = "sv1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        .sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        .mime_type = "image/png",
        .byte_length = 68,
    };
    const grouped = try groups(a, &references);
    var seen: u32 = 0;
    try std.testing.expect(grouped.len > 1);
    for (grouped) |group| {
        try std.testing.expect(group.fields.items.len + group.identity_images.items.len + group.images.items.len <= 64);
        for (group.references.items) |slot| {
            const bit = @as(u32, 1) << @intCast(slot);
            try std.testing.expect(seen & bit == 0);
            seen |= bit;
        }
    }
    try std.testing.expectEqual(std.math.maxInt(u32), seen);
}
