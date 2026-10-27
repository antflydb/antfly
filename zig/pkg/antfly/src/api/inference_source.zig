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
    lookup_many: ?*const fn (*anyopaque, A, []const Reference) anyerror![]const []const u8 = null,
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
    // Verify the complete batch before rewriting any evidence in the request.
    for (references.items, urls) |reference, url| try verify(a, reference, url);
    for (selected.items, slots) |item, slot| {
        _ = item.part.object.swapRemove("source");
        try item.part.object.put(a, "image_url", .{ .string = urls[slot] });
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
    references: std.ArrayList(usize) = .empty,
    ids: std.ArrayList([]const u8) = .empty,
    fields: std.ArrayList([]const u8) = .empty,
    images: std.ArrayList([]const u8) = .empty,
};
fn add(a: A, list: *std.ArrayList([]const u8), field: []const u8) !void {
    for (list.items) |existing| if (std.mem.eql(u8, existing, field)) return;
    try list.append(a, field);
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
fn sameFilter(a: A, left: Reference, right: Reference) !bool {
    if (!std.mem.eql(u8, left.table, right.table)) return false;
    const l = try std.json.Stringify.valueAlloc(a, left.filter_query, .{});
    defer a.free(l);
    const r = try std.json.Stringify.valueAlloc(a, right.filter_query, .{});
    defer a.free(r);
    return std.mem.eql(u8, l, r);
}
/// First union fields within a parent, then batch parents with the SAME mask.
/// Never form a Cartesian product of unrelated image selectors across rows.
/// All storage belongs to the request arena; at most 32 references per query.
pub fn groups(a: A, references: []const Reference) ![]Group {
    var parents: std.ArrayList(Group) = .empty;
    for (references, 0..) |reference, i| {
        const slot = found: {
            for (parents.items, 0..) |parent, j| {
                const first = references[parent.references.items[0]];
                if (parent.references.items.len < 32 and std.mem.eql(u8, first.document_id, reference.document_id) and try sameFilter(a, first, reference)) break :found j;
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
    }
    var output: std.ArrayList(Group) = .empty;
    for (parents.items) |parent| {
        const slot = found: {
            for (output.items, 0..) |other, i| {
                if (other.references.items.len + parent.references.items.len <= 32 and
                    equalFields(other.fields.items, parent.fields.items) and equalFields(other.images.items, parent.images.items) and
                    try sameFilter(a, references[other.references.items[0]], references[parent.references.items[0]])) break :found i;
            }
            try output.append(a, .{});
            break :found output.items.len - 1;
        };
        const group = &output.items[slot];
        if (group.fields.items.len == 0) {
            group.fields = parent.fields;
            group.images = parent.images;
        }
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
