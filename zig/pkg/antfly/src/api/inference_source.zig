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
const std = @import("std");
const images = @import("antfly_image");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Reference = @import("antfly_decisions").evidence.TableSourceReference;
// Compact native payloads replace the retained URL rather than supplement it.
// The existing 16 MiB expanded request ceiling bounds useful codec retention.
pub const max_typed_codec_bytes: usize = 12 * 1024 * 1024;
pub const Port = struct {
    ptr: *anyopaque,
    /// Returned bytes belong to a. The host must recheck live table/row read
    /// permission, membership and the expected revision in the SAME query.
    lookup: *const fn (*anyopaque, A, Reference) anyerror![]const u8,
    lookup_many: ?*const fn (*anyopaque, A, []const Reference) anyerror![]const []const u8 = null,
    /// Optional move-owned payloads. The consumer releases the lease before its
    /// allocator/budget dies, after the last provider read of the resolved input.
    lookup_owned: ?*const fn (*anyopaque, A, []const Reference) anyerror!PayloadLease = null,
    /// Decode while producer views are live; return codecs and internal handles
    /// only to compatible native executors. The same ACL query still owns reads.
    lookup_verified_owned: ?*const fn (*anyopaque, A, []const Reference, usize) anyerror!PayloadLease = null,
};
pub const PayloadLease = struct {
    urls: []const []const u8 = &.{},
    verified: []const @import("antfly_inference_execution_context").VerifiedMedia = &.{},
    owner: ?*anyopaque = null,
    release: ?*const fn (*anyopaque) void = null,
    pub fn deinit(self: *PayloadLease) void {
        if (self.release) |release| release(self.owner.?);
        self.* = .{};
    }
};
const Selected = struct { part: *V, reference: Reference, chat: bool };
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

fn collect(a: A, value: V, selected: *std.ArrayList(Selected), chat: bool) !void {
    if (value != .array) return;
    for (value.array.items) |message| {
        if (message != .object) continue;
        const content = message.object.get("content") orelse continue;
        if (content != .array) continue;
        for (content.array.items) |*part| {
            if (part.* != .object) continue;
            const kind = part.object.get("type") orelse continue;
            if (kind != .string) continue;
            if (part.object.get("image_url")) |image| {
                const url: ?[]const u8 = if (image == .string) image.string else if (image == .object) nested: {
                    const url = image.object.get("url") orelse break :nested null;
                    break :nested if (url == .string) url.string else null;
                } else null;
                if (url) |text| if (std.mem.startsWith(u8, text, "antfly-verified:")) return error.InvalidInferenceSource;
            }
            if (!std.mem.eql(u8, kind.string, "input_image")) continue;
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
            try selected.append(a, .{ .part = part, .reference = reference, .chat = chat });
        }
    }
}
pub fn resolve(a: A, request: V, port: ?Port) !void {
    // Compatibility callers require allocator-owned values after this function.
    var copying = port;
    if (copying) |*value| value.lookup_owned = null;
    var lease = try resolveOwned(a, request, copying);
    lease.deinit();
}
pub fn resolveOwned(a: A, request: V, port: ?Port) !PayloadLease {
    return resolveMode(a, request, port, null);
}
/// Retain already decoded bytes only for a real executor invocation. The lease
/// owns them through synchronous/streaming consumption and every error path.
pub const RetentionPolicy = struct {
    ptr: *anyopaque,
    retain: *const fn (*anyopaque, V) bool,
    /// Optional reuse must not retain an entire maximum-sized media request.
    /// Exceeding this soft codec budget preserves ordinary verification and
    /// execution; it does not reject or truncate any source.
    max_bytes: usize = 1024 * 1024,
    typed_payloads: bool = false,
    max_typed_bytes: usize = max_typed_codec_bytes,
};
pub fn resolveForExecution(a: A, request: V, port: ?Port, policy: RetentionPolicy) !PayloadLease {
    return resolveMode(a, request, port, policy);
}
const VerifiedPayloads = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    upstream: PayloadLease,
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.arena.deinit();
        self.upstream.deinit();
        const a = self.a;
        a.destroy(self);
    }
};
fn resolveMode(a: A, request: V, port: ?Port, policy: ?RetentionPolicy) !PayloadLease {
    if (request != .object) return .{};
    var selected: std.ArrayList(Selected) = .empty;
    defer selected.deinit(a);
    if (request.object.get("input")) |input| try collect(a, input, &selected, false);
    if (request.object.get("inputs")) |inputs| {
        if (inputs == .array) for (inputs.array.items) |input| {
            if (input == .object) if (input.object.get("input")) |messages| try collect(a, messages, &selected, false);
        };
    }
    if (request.object.get("messages")) |messages| try collect(a, messages, &selected, true);
    if (selected.items.len == 0) return .{};
    var retain_decoded = if (policy) |p| p.retain(p.ptr, request) else false;
    for (selected.items) |item| if (item.part.object.get("detail")) |detail| {
        if (detail != .string or !std.mem.eql(u8, detail.string, "auto")) retain_decoded = false;
    };
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
    const typed_route = retain_decoded and policy.?.typed_payloads and authorized.lookup_verified_owned != null;
    const retention_limit = if (typed_route) policy.?.max_typed_bytes else if (policy) |p| p.max_bytes else 0;
    if (retain_decoded) {
        var retained_bytes: usize = 0;
        for (references.items, 0..) |reference, i| {
            // Authorization and per-reference pixel limits are checked below.
            // Retention is charged by the verified content identity rather than
            // locator/options identity, so distinct claims can reuse one codec.
            const seen = for (references.items[0..i]) |prior| {
                if (reference.byte_length == prior.byte_length and std.mem.eql(u8, reference.sha256, prior.sha256) and std.mem.eql(u8, reference.mime_type, prior.mime_type)) break true;
            } else false;
            if (!seen) retained_bytes +|= reference.byte_length;
        }
        if (retained_bytes > retention_limit) retain_decoded = false;
    }
    const typed = retain_decoded and typed_route;
    var lease: PayloadLease = if (typed) try authorized.lookup_verified_owned.?(authorized.ptr, a, references.items, retention_limit) else if (authorized.lookup_owned) |owned| try owned(authorized.ptr, a, references.items) else .{ .urls = if (authorized.lookup_many) |batch| try batch(authorized.ptr, a, references.items) else sequential: {
        const output = try a.alloc([]const u8, references.items.len);
        for (references.items, output) |reference, *url| url.* = try authorized.lookup(authorized.ptr, a, reference);
        break :sequential output;
    } };
    errdefer lease.deinit();
    var decoded = std.heap.ArenaAllocator.init(@import("antfly_local_sources").sql_memory_budget.reclaimingAllocator(a));
    var moved = false;
    defer if (!moved) decoded.deinit();
    const Media = @import("antfly_inference_execution_context").VerifiedMedia;
    const media: []Media = if (retain_decoded and !typed) try a.alloc(Media, references.items.len) else &.{};
    const urls = lease.urls;
    if (urls.len != references.items.len) return error.InferenceSourceMismatch;
    // Decode/inspect a distinct payload once per request. Matching references
    // still independently check digest, MIME, byte count and their pixel limit.
    var verified: std.StringHashMapUnmanaged(Verified) = .empty;
    defer verified.deinit(a);
    for (references.items, urls, 0..) |reference, url, i| {
        if (typed) {
            if (lease.verified.len != references.items.len) return error.InferenceSourceMismatch;
            const source = lease.verified[i];
            var token: [128]u8 = undefined;
            const expected = try std.fmt.bufPrint(&token, "antfly-verified:{s}:{s}", .{ reference.mime_type, reference.sha256 });
            if (!source.matches(url) or !std.mem.eql(u8, url, expected) or source.logical_url_bytes != 13 + reference.mime_type.len + std.base64.standard.Encoder.calcSize(reference.byte_length) or source.bytes.len != reference.byte_length or source.pixels == 0 or !std.mem.eql(u8, source.mime_type, reference.mime_type)) return error.InferenceSourceMismatch;
            if (source.pixels > reference.max_pixels) return error.InferenceRequestTooLarge;
            continue;
        }
        const info = verified.get(url) orelse checked: {
            const info = try verifyInfoRetained(a, reference, url, if (retain_decoded) decoded.allocator() else null);
            try verified.put(a, url, info);
            break :checked info;
        };
        if (info.bytes != reference.byte_length or !std.mem.eql(u8, &info.digest, reference.sha256) or !std.mem.eql(u8, info.mime, reference.mime_type)) return error.InferenceSourceMismatch;
        if (info.pixels > reference.max_pixels) return error.InferenceRequestTooLarge;
        if (retain_decoded) media[i] = .{ .url = url, .bytes = info.data, .mime_type = info.mime };
    }
    // Base64 data URLs have JSON-safe ASCII spellings, proven by verification.
    // Decision URLs add two string quotes and three extra key bytes. Chat
    // additionally wraps the URL in an object (+8) and shortens the type (-2).
    // Do not walk expanded image bytes through another counting encoder.
    for (selected.items, slots) |item, slot| {
        var counter: std.Io.Writer.Discarding = .init(&.{});
        try std.json.Stringify.value(item.part.object.get("source").?, .{}, &counter.writer);
        const previous = std.math.cast(usize, counter.fullCount()) orelse return error.InferenceRequestTooLarge;
        expanded = std.math.add(usize, expanded - previous, (if (typed) lease.verified[slot].logical_url_bytes else urls[slot].len) + @as(usize, if (item.chat) 11 else 5)) catch return error.InferenceRequestTooLarge;
        if (expanded > 16 * 1024 * 1024) return error.InferenceRequestTooLarge;
    }
    for (selected.items, slots) |item, slot| {
        _ = item.part.object.swapRemove("source");
        if (item.chat) {
            // Decision evidence and chat have different existing image shapes.
            // Normalize locators into the downstream chat contract here, before
            // native/vendor parsing and image admission.
            var image: std.json.ObjectMap = .empty;
            try image.put(a, "url", .{ .string = urls[slot] });
            if (item.part.object.get("detail")) |detail| {
                try image.put(a, "detail", detail);
                _ = item.part.object.swapRemove("detail");
            }
            try item.part.object.put(a, "type", .{ .string = "image_url" });
            try item.part.object.put(a, "image_url", .{ .object = image });
        } else try item.part.object.put(a, "image_url", .{ .string = urls[slot] });
    }
    // The typed producer already owns the complete invocation lease; it needs
    // neither another descriptor table nor a wrapper around an empty decoder.
    if (typed) return lease;
    if (retain_decoded) {
        const owner = try a.create(VerifiedPayloads);
        owner.* = .{ .a = a, .arena = decoded, .upstream = lease };
        moved = true;
        return .{ .urls = urls, .verified = media, .owner = owner, .release = VerifiedPayloads.release };
    }
    return lease;
}
pub const Verified = struct { bytes: usize, digest: [64]u8, mime: []const u8, pixels: u64, data: []const u8 = &.{} };

/// Prove equality against an invocation-owned verified codec, not a caller's
/// digest claim. Canonical base64 can be compared in bounded scratch without
/// another full decoded allocation, SHA pass or image inspection. Other valid
/// spellings keep ordinary verification as a compatibility fallback.
pub fn matchesVerifiedUrl(reference: Reference, url: []const u8, prior: Verified) bool {
    if (reference.byte_length != prior.bytes or !std.mem.eql(u8, reference.sha256, &prior.digest) or !std.mem.eql(u8, reference.mime_type, prior.mime)) return false;
    const prefix_len = 13 + prior.mime.len;
    if (url.len != prefix_len + std.base64.standard.Encoder.calcSize(prior.data.len)) return false;
    if (!std.mem.startsWith(u8, url, "data:") or !std.mem.eql(u8, url[5..][0..prior.mime.len], prior.mime) or !std.mem.eql(u8, url[5 + prior.mime.len .. prefix_len], ";base64,")) return false;
    var buffer: [4096]u8 = undefined;
    var raw: usize = 0;
    var encoded: usize = prefix_len;
    while (raw < prior.data.len) {
        const end = @min(raw + 3072, prior.data.len);
        const bytes = std.base64.standard.Encoder.encode(&buffer, prior.data[raw..end]);
        if (!std.mem.eql(u8, bytes, url[encoded..][0..bytes.len])) return false;
        raw = end;
        encoded += bytes.len;
    }
    return true;
}

test "httpx inference gateway verified URL comparison crosses chunk boundaries and rejects forged claims" {
    const a = std.testing.allocator;
    for ([_]usize{ 1, 2, 3, 3071, 3072, 3073, 8192 }) |length| {
        const data = try a.alloc(u8, length);
        defer a.free(data);
        for (data, 0..) |*byte, i| byte.* = @truncate(i);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(data, &digest, .{});
        const prior: Verified = .{ .bytes = length, .digest = std.fmt.bytesToHex(digest, .lower), .mime = "image/png", .pixels = 1, .data = data };
        const reference: Reference = .{ .table = "docs", .document_id = "a", .field = "image", .revision_field = "revision", .revision = "r1", .sha256 = &prior.digest, .mime_type = prior.mime, .byte_length = length, .max_pixels = 1 };
        const prefix = "data:image/png;base64,";
        const url = try a.alloc(u8, prefix.len + std.base64.standard.Encoder.calcSize(length));
        defer a.free(url);
        @memcpy(url[0..prefix.len], prefix);
        _ = std.base64.standard.Encoder.encode(url[prefix.len..], data);
        try std.testing.expect(matchesVerifiedUrl(reference, url, prior));
        url[prefix.len + (url.len - prefix.len) / 2] ^= 1;
        try std.testing.expect(!matchesVerifiedUrl(reference, url, prior));
        try std.testing.expect(!matchesVerifiedUrl(reference, url[0 .. url.len - 1], prior));
        var wrong = reference;
        wrong.byte_length += 1;
        try std.testing.expect(!matchesVerifiedUrl(wrong, url, prior));
    }
}
fn verify(a: A, reference: Reference, url: []const u8) !void {
    _ = try verifyInfo(a, reference, url);
}
fn verifyInfo(a: A, reference: Reference, url: []const u8) !Verified {
    return verifyInfoRetained(a, reference, url, null);
}
pub fn verifyInfoRetained(a: A, reference: Reference, url: []const u8, retained: ?A) !Verified {
    const prefix = try std.fmt.allocPrint(a, "data:{s};base64,", .{reference.mime_type});
    defer a.free(prefix);
    if (!std.mem.startsWith(u8, url, prefix)) return error.InferenceSourceMismatch;
    const encoded = url[prefix.len..];
    const length = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InferenceSourceMismatch;
    if (length != reference.byte_length) return error.InferenceSourceMismatch;
    const scratch = @import("antfly_local_sources").sql_memory_budget.reclaimingAllocator(a);
    const bytes = try (retained orelse scratch).alloc(u8, length);
    defer if (retained == null) scratch.free(bytes);
    errdefer if (retained) |owner| owner.free(bytes);
    std.base64.standard.Decoder.decode(bytes, encoded) catch return error.InferenceSourceMismatch;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    if (!std.mem.eql(u8, &std.fmt.bytesToHex(digest, .lower), reference.sha256)) return error.InferenceSourceMismatch;
    const mime = images.inferenceMimeEssenceForFormat(images.detectFormat(bytes)) orelse return error.InferenceSourceMismatch;
    if (!std.mem.eql(u8, mime, reference.mime_type)) return error.InferenceSourceMismatch;
    const info = try images.inspectEncoded(bytes);
    const pixels = try info.pixels();
    if (pixels > reference.max_pixels) return error.InferenceRequestTooLarge;
    return .{ .bytes = length, .digest = std.fmt.bytesToHex(digest, .lower), .mime = mime, .pixels = pixels, .data = if (retained != null) bytes else &.{} };
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
    // Leased values stay alive through consumption, then release exactly once.
    const Owned = struct {
        url: []const u8,
        releases: usize = 0,
        urls: [1][]const u8 = undefined,
        fn lookup(raw: *anyopaque, _: A, refs: []const Reference) !PayloadLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), refs.len);
            self.urls[0] = self.url;
            return .{ .urls = &self.urls, .owner = self, .release = release };
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
    };
    var owned: Owned = .{ .url = fake.url };
    const owned_port: Port = .{ .ptr = &owned, .lookup = Fake.lookup, .lookup_owned = Owned.lookup };
    const leased_value = try std.json.parseFromSliceLeaky(V, temp, body, .{});
    var lease = try resolveOwned(temp, leased_value, owned_port);
    try std.testing.expectEqual(@as(usize, 0), owned.releases);
    const leased_parts = leased_value.object.get("inputs").?.array.items[0].object.get("input").?.array.items[0].object.get("content").?.array.items;
    for (leased_parts) |part| try std.testing.expect(part.object.get("image_url").?.string.ptr == fake.url.ptr);
    lease.deinit();
    lease.deinit();
    try std.testing.expectEqual(@as(usize, 1), owned.releases);
    owned.url = "invalid";
    const failed_lease = try std.json.parseFromSliceLeaky(V, temp, body, .{});
    try std.testing.expectError(error.InferenceSourceMismatch, resolveOwned(temp, failed_lease, owned_port));
    try std.testing.expectEqual(@as(usize, 2), owned.releases);
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

test "httpx inference gateway verified bytes are invocation owned and policy is lazy" {
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
    const reference: Reference = .{ .table = "docs", .document_id = "a", .field = "image", .revision_field = "revision", .revision = "r1", .sha256 = &hex, .mime_type = "image/png", .byte_length = bytes.len, .max_pixels = 16 };
    const body = try std.json.Stringify.valueAlloc(temp, .{ .input = .{.{ .role = "user", .content = .{ .{ .type = "input_image", .source = reference }, .{ .type = "input_image", .source = reference } } }} }, .{});
    const State = struct {
        url: []const u8,
        calls: usize = 0,
        retain: bool = true,
        fn lookup(raw: *anyopaque, _: A, _: Reference) ![]const u8 {
            return @as(*@This(), @ptrCast(@alignCast(raw))).url;
        }
        fn policy(raw: *anyopaque, _: V) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return self.retain;
        }
    };
    var state = State{ .url = try std.fmt.allocPrint(temp, "data:image/png;base64,{s}", .{encoded}) };
    const port = Port{ .ptr = &state, .lookup = State.lookup };
    var policy = RetentionPolicy{ .ptr = &state, .retain = State.policy };
    const no_sources = try std.json.parseFromSliceLeaky(V, temp, "{\"input\":\"text\"}", .{});
    var empty = try resolveForExecution(temp, no_sources, port, policy);
    empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), state.calls);
    for ([_]struct { retain: bool, limit: usize, expected: bool }{
        .{ .retain = true, .limit = 1024 * 1024, .expected = true },
        .{ .retain = false, .limit = 1024 * 1024, .expected = false },
        .{ .retain = true, .limit = 67, .expected = false },
        .{ .retain = true, .limit = 68, .expected = true },
    }) |case| {
        state.retain = case.retain;
        policy.max_bytes = case.limit;
        const retain = case.expected;
        const value = try std.json.parseFromSliceLeaky(V, temp, body, .{});
        var lease = try resolveForExecution(temp, value, port, policy);
        defer lease.deinit();
        try std.testing.expectEqual(@as(usize, if (retain) 1 else 0), lease.verified.len);
        if (retain) {
            try std.testing.expectEqualSlices(u8, &bytes, lease.verified[0].bytes);
            try std.testing.expectEqualStrings(state.url, lease.verified[0].url);
        }
    }
    state.retain = true;
    state.url = "invalid";
    const failed = try std.json.parseFromSliceLeaky(V, temp, body, .{});
    try std.testing.expectError(error.InferenceSourceMismatch, resolveForExecution(temp, failed, port, policy));
}

test "httpx inference gateway retained codecs charge unique content across distinct claims" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const encoded = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7S8AAAAASUVORK5CYII=";
    var bytes: [68]u8 = undefined;
    try std.base64.standard.Decoder.decode(&bytes, encoded);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&bytes, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const ref: Reference = .{ .table = "docs", .document_id = "a", .field = "image", .revision_field = "revision", .revision = "r1", .sha256 = &hex, .mime_type = "image/png", .byte_length = bytes.len, .max_pixels = 16 };
    var other = ref;
    other.document_id = "b";
    other.max_pixels = 1;
    const value = try std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, .{ .input = .{.{ .role = "user", .content = .{ .{ .type = "input_image", .source = ref }, .{ .type = "input_image", .source = other } } }} }, .{}), .{});
    const State = struct {
        calls: usize = 0,
        fn lookup(raw: *anyopaque, _: A, _: Reference) ![]const u8 {
            @as(*@This(), @ptrCast(@alignCast(raw))).calls += 1;
            return "data:image/png;base64," ++ encoded;
        }
        fn policy(_: *anyopaque, _: V) bool {
            return true;
        }
    };
    var state: State = .{};
    var lease = try resolveForExecution(a, value, .{ .ptr = &state, .lookup = State.lookup }, .{ .ptr = &state, .retain = State.policy, .max_bytes = 68 });
    defer lease.deinit();
    try std.testing.expectEqual(@as(usize, 2), state.calls);
    try std.testing.expectEqual(@as(usize, 2), lease.verified.len);
    try std.testing.expectEqual(lease.verified[0].bytes.ptr, lease.verified[1].bytes.ptr);
    const forged = try std.json.parseFromSliceLeaky(V, a, "{\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"image_url\",\"image_url\":{\"url\":\"antfly-verified:image/png:forged\"}}]}]}", .{});
    try std.testing.expectError(error.InvalidInferenceSource, resolveOwned(a, forged, null));
    const chat = try std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, .{ .messages = .{.{ .role = "user", .content = .{.{ .type = "input_image", .source = ref, .detail = "low" }} }} }, .{}), .{});
    var chat_lease = try resolveOwned(a, chat, .{ .ptr = &state, .lookup = State.lookup });
    defer chat_lease.deinit();
    const part = chat.object.get("messages").?.array.items[0].object.get("content").?.array.items[0];
    try std.testing.expectEqualStrings("image_url", part.object.get("type").?.string);
    try std.testing.expectEqualStrings("data:image/png;base64," ++ encoded, part.object.get("image_url").?.object.get("url").?.string);
    try std.testing.expectEqualStrings("low", part.object.get("image_url").?.object.get("detail").?.string);
    try std.testing.expect(!part.object.contains("source") and !part.object.contains("detail"));
}
