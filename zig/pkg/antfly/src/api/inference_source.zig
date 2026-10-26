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
pub const Port = struct {
    ptr: *anyopaque,
    /// Returned bytes belong to a. The host must recheck live table/row read
    /// permission, membership and the expected revision in the SAME query.
    lookup: *const fn (*anyopaque, A, Reference) anyerror![]const u8,
};
const Selected = struct { part: *V, reference: Reference };
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
            const reference = std.json.parseFromValueLeaky(Reference, a, source, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidInferenceSource;
            try reference.validate();
            if (selected.items.len == 128) return error.InferenceRequestTooLarge;
            try selected.append(a, .{ .part = part, .reference = reference });
        }
    }
}
pub fn resolve(a: A, request: V, port: ?Port) !void {
    if (request != .object) return;
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
    const authorized = port orelse return error.InferenceSourceUnsupported;
    var bytes: usize = 0;
    for (selected.items) |item| {
        bytes = std.math.add(usize, bytes, std.base64.standard.Encoder.calcSize(item.reference.byte_length) + 256) catch return error.InferenceRequestTooLarge;
        if (bytes > 16 * 1024 * 1024) return error.InferenceRequestTooLarge;
    }
    // Validate all reference shapes and aggregate limits before reading any row.
    // Each unique source is fetched once; only this request shares its authority.
    var resolved: std.StringHashMapUnmanaged([]const u8) = .empty;
    defer {
        var keys = resolved.keyIterator();
        while (keys.next()) |key| a.free(key.*);
        resolved.deinit(a);
    }
    for (selected.items) |item| {
        const key = try std.json.Stringify.valueAlloc(a, item.reference, .{});
        defer a.free(key);
        const url = resolved.get(key) orelse lookup: {
            const value = try authorized.lookup(authorized.ptr, a, item.reference);
            try verify(a, item.reference, value);
            const owned_key = try a.dupe(u8, key);
            errdefer a.free(owned_key);
            try resolved.put(a, owned_key, value);
            break :lookup value;
        };
        _ = item.part.object.swapRemove("source");
        try item.part.object.put(a, "image_url", .{ .string = url });
    }
    // Existing inline requests retain the same expanded transport limit. This
    // counting sink retains no image-sized body; only cold materialization pays
    // the encoder walk, while identity-only cache hits never enter this path.
    var buffer: [256]u8 = undefined;
    var counter: std.Io.Writer.Discarding = .init(&buffer);
    try std.json.Stringify.value(request, .{}, &counter.writer);
    if (counter.fullCount() > 16 * 1024 * 1024) return error.InferenceRequestTooLarge;
}
fn verify(a: A, reference: Reference, url: []const u8) !void {
    const prefix = try std.fmt.allocPrint(a, "data:{s};base64,", .{reference.mime_type});
    defer a.free(prefix);
    if (!std.mem.startsWith(u8, url, prefix)) return error.InferenceSourceMismatch;
    const encoded = url[prefix.len..];
    const length = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InferenceSourceMismatch;
    if (length != reference.byte_length) return error.InferenceSourceMismatch;
    const bytes = try a.alloc(u8, length);
    defer a.free(bytes);
    std.base64.standard.Decoder.decode(bytes, encoded) catch return error.InferenceSourceMismatch;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    if (!std.mem.eql(u8, &std.fmt.bytesToHex(digest, .lower), reference.sha256)) return error.InferenceSourceMismatch;
    const mime = images.inferenceMimeEssenceForFormat(images.detectFormat(bytes)) orelse return error.InferenceSourceMismatch;
    if (!std.mem.eql(u8, mime, reference.mime_type)) return error.InferenceSourceMismatch;
    const info = try images.inspectEncoded(bytes);
    if (try info.pixels() > reference.max_pixels) return error.InferenceRequestTooLarge;
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
}
