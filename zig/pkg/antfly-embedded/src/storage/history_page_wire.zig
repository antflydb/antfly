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

//! Private compiled-owner page transport. Binary tuple images remain binary;
//! metadata and payload are authenticated together, and decoding owns all data.
const std = @import("std");
const pages = @import("history_page.zig");
const frame = @import("committed_frame.zig");
pub const prefix_bytes = 96;
const completion_bytes = 40;
const effect_bytes = 40;
pub const max_bytes = prefix_bytes + completion_bytes + pages.max_projection_roots * effect_bytes + frame.max_bytes;
// JSON namespaces use up to four characters per decimal byte, plus brackets.
pub const max_request_bytes = 4096 + pages.max_projection_roots * (24 * 4 + 2);

pub fn prefixSize(request: pages.Request) usize {
    return prefix_bytes + if (request.include_completion) completion_bytes + request.completion_roots.len * effect_bytes else @as(usize, 0);
}

fn magic(request: pages.Request) *const [4]u8 {
    if (request.completion_roots.len != 0) return "HPG3";
    return if (request.include_completion) "HPG2" else "HPG1";
}
pub const OrderedRead = struct { request: pages.Request, minimum_applied_index: u64 };

fn checksum(prefix: []const u8, payload: []const u8, request: pages.Request) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/history-page-wire/v1");
    hash.update(&request.reference.namespace);
    hash.update(&request.reference.authority);
    hash.update(&request.reference.consumer);
    hash.update(&request.reference.definition);
    for ([_]u64{ request.reference.epoch, request.reference.grant, request.after.sequence, request.after.event, request.now_seconds, request.max_events, request.max_bytes }) |value| {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, value, .little);
        hash.update(&bytes);
    }
    for (request.completion_roots) |root| hash.update(&root);
    hash.update(prefix);
    hash.update(payload);
    return hash.finalResult();
}
pub fn validate(page: pages.Page, request: pages.Request) !void {
    try request.validateCompletionRoots();
    if (request.max_events == 0 or request.max_events > 1024 or request.max_bytes == 0 or request.max_bytes > frame.max_bytes) return error.InvalidHistoryAdmission;
    if (page.events.len > request.max_events or page.first_ordinal != request.after.event) return error.HistoryCorrupt;
    if (page.completion) |proof| {
        const header = page.header orelse return error.HistoryCorrupt;
        if (!request.include_completion or header.transaction_scope != .coordinated or
            std.mem.allEqual(u8, &proof.certificate_digest, 0) or proof.commit_version == 0 or page.completion_effects.len != request.completion_roots.len) return error.HistoryCorrupt;
        for (page.completion_effects, request.completion_roots) |effect, root| {
            const local = std.mem.eql(u8, &root, &header.namespace);
            if (effect) |value| {
                if (value.sequence == 0 or std.mem.allEqual(u8, &value.frame_digest, 0) or (local and value.sequence != header.sequence)) return error.HistoryCorrupt;
            } else if (local) return error.HistoryCorrupt;
        }
    } else if (page.completion_effects.len != 0) return error.HistoryCorrupt;
    if (page.header) |header| {
        if (!std.mem.eql(u8, &header.namespace, &request.reference.namespace) or request.after.sequence == std.math.maxInt(u64) or
            header.sequence != request.after.sequence + 1 or header.sequence > page.observed_latest or
            page.events.len == 0 or page.frame_events > frame.max_events or page.first_ordinal >= page.frame_events or
            page.events.len > page.frame_events - page.first_ordinal) return error.HistoryCorrupt;
        const complete = page.events.len == page.frame_events - page.first_ordinal;
        const next: pages.Position = if (complete) .{ .sequence = header.sequence } else .{ .sequence = request.after.sequence, .event = page.first_ordinal + @as(u32, @intCast(page.events.len)) };
        if (!std.meta.eql(next, page.next) or page.caught_up != (complete and header.sequence == page.observed_latest)) return error.HistoryCorrupt;
        if (try frame.encodedLength(header, page.events) > request.max_bytes) return error.HistoryTooLarge;
    } else if (page.events.len != 0 or page.frame_events != 0 or page.first_ordinal != 0 or !page.caught_up or
        !std.meta.eql(page.next, request.after) or page.next.event != 0 or page.next.sequence != page.observed_latest) return error.HistoryCorrupt;
}

pub fn encode(a: std.mem.Allocator, page: pages.Page, request: pages.Request) ![]u8 {
    try validate(page, request);
    const payload = if (page.header) |header| try frame.encode(a, header, page.events) else null;
    defer if (payload) |bytes| a.free(bytes);
    const size = if (payload) |bytes| bytes.len else 0;
    const prefix = prefixSize(request);
    const out = try a.alloc(u8, prefix + size);
    @memset(out[0..prefix], 0);
    @memcpy(out[0..4], magic(request));
    if (page.completion) |proof| {
        @memcpy(out[96..128], &proof.certificate_digest);
        std.mem.writeInt(u64, out[128..136], proof.commit_version, .little);
        for (page.completion_effects, 0..) |entry, i| if (entry) |effect| {
            const field = out[136 + i * effect_bytes ..][0..effect_bytes];
            std.mem.writeInt(u64, field[0..8], effect.sequence, .little);
            @memcpy(field[8..40], &effect.frame_digest);
        };
    }
    out[4] = @intFromBool(page.header != null);
    out[5] = @intFromBool(page.caught_up);
    std.mem.writeInt(u32, out[8..12], page.first_ordinal, .little);
    std.mem.writeInt(u32, out[12..16], page.frame_events, .little);
    std.mem.writeInt(u64, out[16..24], page.next.sequence, .little);
    std.mem.writeInt(u32, out[24..28], page.next.event, .little);
    std.mem.writeInt(u32, out[28..32], @intCast(size), .little);
    std.mem.writeInt(u64, out[32..40], page.observed_latest, .little);
    if (payload) |bytes| @memcpy(out[prefix..], bytes);
    @memcpy(out[64..96], &checksum(out[0..64], out[prefix_bytes..], request));
    return out;
}

pub fn decode(a: std.mem.Allocator, raw: []const u8, request: pages.Request) !pages.Page {
    try request.validateCompletionRoots();
    if (request.max_events == 0 or request.max_events > 1024 or request.max_bytes == 0 or request.max_bytes > frame.max_bytes) return error.InvalidHistoryAdmission;
    const prefix = prefixSize(request);
    if (raw.len < prefix or raw.len > prefix + @as(usize, request.max_bytes) or
        !std.mem.eql(u8, raw[0..4], magic(request)) or raw[4] > 1 or raw[5] > 1 or
        !std.mem.allEqual(u8, raw[6..8], 0) or !std.mem.allEqual(u8, raw[40..64], 0) or
        std.mem.readInt(u32, raw[28..32], .little) != raw.len - prefix or
        !std.mem.eql(u8, raw[64..96], &checksum(raw[0..64], raw[prefix_bytes..], request))) return error.HistoryCorrupt;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    var page: pages.Page = .{ .arena = arena, .first_ordinal = std.mem.readInt(u32, raw[8..12], .little), .frame_events = std.mem.readInt(u32, raw[12..16], .little), .next = .{ .sequence = std.mem.readInt(u64, raw[16..24], .little), .event = std.mem.readInt(u32, raw[24..28], .little) }, .observed_latest = std.mem.readInt(u64, raw[32..40], .little), .caught_up = raw[5] == 1 };
    if (request.include_completion and !std.mem.allEqual(u8, raw[96..136], 0)) {
        page.completion = .{ .certificate_digest = raw[96..128].*, .commit_version = std.mem.readInt(u64, raw[128..136], .little) };
        const effects = try scratch.alloc(?pages.CompletionEffect, request.completion_roots.len);
        for (effects, 0..) |*out, i| {
            const field = raw[136 + i * effect_bytes ..][0..effect_bytes];
            out.* = if (std.mem.allEqual(u8, field, 0)) null else .{
                .sequence = std.mem.readInt(u64, field[0..8], .little),
                .frame_digest = field[8..40].*,
            };
        }
        page.completion_effects = effects;
    } else if (!std.mem.allEqual(u8, raw[96..prefix], 0)) return error.HistoryCorrupt;
    if (raw[4] == 1) {
        const payload = raw[prefix..];
        if (payload.len < 112) return error.HistoryCorrupt;
        const count = std.mem.readInt(u32, payload[96..100], .little);
        if (count == 0 or count > request.max_events or request.after.sequence == std.math.maxInt(u64)) return error.HistoryCorrupt;
        const descriptor_raw = frame.Descriptor.encode(scratch, request.reference.namespace, request.after.sequence + 1, payload) catch |err| return if (err == error.OutOfMemory) err else error.HistoryCorrupt;
        const descriptor = frame.Descriptor.decode(descriptor_raw, request.reference.namespace, request.after.sequence + 1) catch return error.HistoryCorrupt;
        const Bytes = struct {
            bytes: []const u8,
            fn read(ptr: *anyopaque, ordinal: u32) ![]const u8 {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const start = @as(usize, ordinal) * frame.chunk_bytes;
                if (start >= self.bytes.len) return error.HistoryCorrupt;
                return self.bytes[start..@min(self.bytes.len, start + frame.chunk_bytes)];
            }
        };
        var bytes: Bytes = .{ .bytes = payload };
        var reader: frame.Reader = .{ .descriptor = descriptor, .source = .{ .context = &bytes, .read = Bytes.read } };
        page.header = (reader.header(scratch) catch |err| return if (err == error.OutOfMemory) err else error.HistoryCorrupt).value;
        const events = try scratch.alloc(frame.Event, count);
        for (events) |*event| event.* = (reader.next(scratch) catch |err| return if (err == error.OutOfMemory) err else error.HistoryCorrupt) orelse return error.HistoryCorrupt;
        if (try reader.next(scratch) != null) return error.HistoryCorrupt;
        page.events = events;
    } else if (raw.len != prefix) return error.HistoryCorrupt;
    validate(page, request) catch return error.HistoryCorrupt;
    // ArenaAllocator is a value: publish its current state after allocations.
    page.arena = arena;
    return page;
}

fn roundTrip(a: std.mem.Allocator) !void {
    const request: pages.Request = .{ .reference = .{ .namespace = @splat(1), .authority = @splat(2), .consumer = @splat(3), .epoch = 4, .grant = 5, .definition = @splat(6) }, .after = .{ .sequence = 4, .event = 2 }, .now_seconds = 10 };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const page: pages.Page = .{ .arena = arena, .header = .{ .namespace = request.reference.namespace, .sequence = 5, .transaction_id = @splat(7), .transaction_scope = .coordinated, .schema_before = "\x00\xffschema", .schema_after = "next" }, .events = &.{ .{ .key = "a\x00", .before = null, .after = "\xff\x00" }, .{ .key = "b", .before = "old", .after = null } }, .first_ordinal = 2, .frame_events = 5, .next = .{ .sequence = 4, .event = 4 }, .observed_latest = 9, .caught_up = false };
    const encoded = try encode(a, page, request);
    defer a.free(encoded);
    var decoded = try decode(a, encoded, request);
    defer decoded.deinit();
    @memset(encoded, 0);
    try std.testing.expectEqualDeep(page.header.?, decoded.header.?);
    try std.testing.expectEqualDeep(page.events, decoded.events);
    try std.testing.expectEqualDeep(page.next, decoded.next);
    try std.testing.expectEqual(page.frame_events, decoded.frame_events);
}

fn completionRoundTrip() !void {
    const a = std.testing.allocator;
    const request: pages.Request = .{
        .reference = .{ .namespace = @splat(1), .authority = @splat(2), .consumer = @splat(3), .epoch = 4, .grant = 5, .definition = @splat(6) },
        .after = .{ .sequence = 4 },
        .now_seconds = 10,
        .include_completion = true,
    };
    const page: pages.Page = .{
        .arena = undefined,
        .header = .{ .namespace = @splat(1), .sequence = 5, .transaction_id = @splat(7), .transaction_scope = .coordinated },
        .completion = .{ .certificate_digest = @splat(8), .commit_version = 12 },
        .events = &.{.{ .key = "key", .before = null, .after = "value" }},
        .frame_events = 1,
        .next = .{ .sequence = 5 },
        .observed_latest = 5,
        .caught_up = true,
    };
    const raw = try encode(a, page, request);
    defer a.free(raw);
    var decoded = try decode(a, raw, request);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(page.completion, decoded.completion);
    var legacy = request;
    legacy.include_completion = false;
    try std.testing.expectError(error.HistoryCorrupt, decode(a, raw, legacy));
    try std.testing.expectError(error.HistoryCorrupt, encode(a, page, legacy));
    raw[prefix_bytes] ^= 1;
    try std.testing.expectError(error.HistoryCorrupt, decode(a, raw, request));

    var projection = request;
    projection.completion_roots = &.{ @splat(1), @splat(2), @splat(3) };
    var projected = page;
    projected.completion_effects = &.{ .{ .sequence = 5, .frame_digest = @splat(9) }, null, .{ .sequence = 11, .frame_digest = @splat(10) } };
    const projected_raw = try encode(a, projected, projection);
    defer a.free(projected_raw);
    var decoded_projection = try decode(a, projected_raw, projection);
    defer decoded_projection.deinit();
    try std.testing.expectEqualDeep(projected.completion_effects, decoded_projection.completion_effects);
    try std.testing.expectError(error.HistoryCorrupt, decode(a, projected_raw, request));
    projection.completion_roots = &.{ @splat(1), @splat(2), @splat(4) };
    try std.testing.expectError(error.HistoryCorrupt, decode(a, projected_raw, projection));
    projected.completion_effects = &.{ null, null, null };
    try std.testing.expectError(error.HistoryCorrupt, encode(a, projected, projection));
}

pub const test_support = if (@import("builtin").is_test) struct {
    pub fn allocationFailures() !void {
        try roundTrip(std.testing.allocator);
        try std.testing.checkAllAllocationFailures(std.testing.allocator, roundTrip, .{});
    }

    pub fn malformedPages() !void {
        try completionRoundTrip();
        const a = std.testing.allocator;
        const request: pages.Request = .{ .reference = .{ .namespace = @splat(1), .authority = @splat(2), .consumer = @splat(3), .epoch = 4, .grant = 5, .definition = @splat(6) }, .after = .{ .sequence = 4 }, .now_seconds = 10 };
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var page: pages.Page = .{ .arena = arena, .header = .{ .namespace = request.reference.namespace, .sequence = 5, .transaction_id = @splat(7) }, .events = &.{.{ .key = "key", .before = "old", .after = "new" }}, .frame_events = 1, .next = .{ .sequence = 5 }, .observed_latest = 5, .caught_up = true };
        const encoded = try encode(a, page, request);
        defer a.free(encoded);
        var decoded = try decode(a, encoded, request);
        defer decoded.deinit();
        try std.testing.expect(decoded.caught_up);
        for (0..encoded.len) |offset| {
            encoded[offset] ^= 1;
            try std.testing.expectError(error.HistoryCorrupt, decode(a, encoded, request));
            encoded[offset] ^= 1;
        }
        // A valid response cannot be transplanted to another lease or request.
        var other = request;
        other.reference.grant += 1;
        try std.testing.expectError(error.HistoryCorrupt, decode(a, encoded, other));
        other = request;
        other.reference.definition[0] ^= 1;
        try std.testing.expectError(error.HistoryCorrupt, decode(a, encoded, other));
        other = request;
        other.reference.authority[0] ^= 1;
        try std.testing.expectError(error.HistoryCorrupt, decode(a, encoded, other));
        other = request;
        other.reference.consumer[0] ^= 1;
        try std.testing.expectError(error.HistoryCorrupt, decode(a, encoded, other));
        other = request;
        other.reference.epoch += 1;
        try std.testing.expectError(error.HistoryCorrupt, decode(a, encoded, other));
        other = request;
        other.now_seconds += 1;
        try std.testing.expectError(error.HistoryCorrupt, decode(a, encoded, other));
        other = request;
        other.max_events += 1;
        try std.testing.expectError(error.HistoryCorrupt, decode(a, encoded, other));
        other = request;
        other.max_bytes += 1;
        try std.testing.expectError(error.HistoryCorrupt, decode(a, encoded, other));
        // Even correctly checksummed metadata must agree with the requested cut.
        encoded[8] = 1;
        @memcpy(encoded[64..96], &checksum(encoded[0..64], encoded[prefix_bytes..], request));
        try std.testing.expectError(error.HistoryCorrupt, decode(a, encoded, request));
        var small = request;
        small.max_bytes = 1;
        try std.testing.expectError(error.HistoryTooLarge, encode(a, page, small));
        var invalid = request;
        invalid.max_events = 0;
        try std.testing.expectError(error.InvalidHistoryAdmission, decode(a, encoded, invalid));
        page.header = null;
        page.events = &.{};
        page.frame_events = 0;
        page.next = request.after;
        page.observed_latest = request.after.sequence;
        const empty = try encode(a, page, request);
        defer a.free(empty);
        var empty_page = try decode(a, empty, request);
        defer empty_page.deinit();
        try std.testing.expect(empty_page.caught_up);
        try std.testing.expectEqual(@as(usize, 0), empty_page.events.len);
    }
} else struct {};

test "history page wire owns binary images and cleans every allocation failure" {
    try test_support.allocationFailures();
}
test "history page wire rejects forged metadata, malformed payload and invalid budgets" {
    try test_support.malformedPages();
}
