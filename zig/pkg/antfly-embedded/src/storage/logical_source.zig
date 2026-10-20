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

//! Original-value logical reads over revision-bound document range authorities.
//! The authenticated scope pins bodies, directories and revision proofs together.
//! Missing/old authorities fall back to full bytes; invalid selected bytes fail.
const std = @import("std");
const A = std.mem.Allocator;
const projection = @import("document_projection.zig");
const revision = @import("document_mutation_revision.zig");
const ranges_mod = @import("value_ranges.zig");
pub const Row = struct {
    bytes: ?[]u8,
    // Logical work is charged on original size, never on projected payload.
    original_bytes: u64,
    projected: bool = false,
};

/// Stable-order deduplicating metadata/full-body fetch. Scope owns all bytes.
fn many(scope: anytype, keys: []const []const u8, output: []?[]const u8) !void {
    var order: [64]usize = undefined;
    var sorted: [64][]const u8 = undefined;
    var fetched: [64]?[]const u8 = undefined;
    var slots: [64]usize = undefined;
    for (order[0..keys.len], 0..) |*slot, i| slot.* = i;
    std.mem.sort(usize, order[0..keys.len], keys, struct {
        fn less(k: []const []const u8, left: usize, right: usize) bool {
            return std.mem.order(u8, k[left], k[right]) == .lt;
        }
    }.less);
    var count: usize = 0;
    for (order[0..keys.len]) |index| {
        if (count == 0 or !std.mem.eql(u8, sorted[count - 1], keys[index])) {
            sorted[count] = keys[index];
            count += 1;
        }
        slots[index] = count - 1;
    }
    try scope.getManySorted(sorted[0..count], fetched[0..count]);
    for (output, 0..) |*value, i| value.* = fetched[slots[i]];
}

/// Root names are unique and decoded; nested predicate paths select their root
/// subtree unchanged, preserving array elements and all source string semantics.
/// Output is caller-owned. Temporary directories/range reads use the scope owner.
pub fn load(a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authenticated: bool) ![]Row {
    if (physical.len > 64 or physical.len != ids.len or roots.len > 64) return error.InvalidBatch;
    const result = try a.alloc(Row, ids.len);
    @memset(result, .{ .bytes = null, .original_bytes = 0 });
    errdefer {
        for (result) |row| if (row.bytes) |bytes| a.free(bytes);
        a.free(result);
    }
    if (ids.len == 0) return result;
    var proofs: [64]?revision.Content = @splat(null);
    var timestamps: [64]u64 = undefined;
    // Without storage authentication a directory is not authority to omit bytes.
    if (authenticated and roots.len != 0) try revision.loadWindow(scope, scratch, physical, ids, proofs[0..ids.len], timestamps[0..ids.len]);
    var directory_keys: [64]projection.Key = undefined;
    var keys: [64][]const u8 = undefined;
    var raw: [64]?[]const u8 = @splat(null);
    var indices: [64]usize = undefined;
    var count: usize = 0;
    for (proofs[0..ids.len], 0..) |proof, i| if (proof) |p| {
        if (p.bytes < projection.minimum_bytes or p.kind != @import("internal_keys.zig").primary_kind) continue;
        directory_keys[count] = projection.keyForRevision(revision.keyForPhysical(physical[i]).?);
        keys[count] = &directory_keys[count];
        indices[count] = i;
        count += 1;
    };
    if (count != 0) try many(scope, keys[0..count], raw[0..count]);
    const Prepared = struct {
        index: usize,
        fields: []projection.Field,
        ranges: []ranges_mod.Range,
        values: [][]const u8,
    };
    var prepared: [64]Prepared = undefined;
    var requests: [64]ranges_mod.Request = undefined;
    var request_count: usize = 0;
    var prepared_count: usize = 0;
    for (indices[0..count], raw[0..count]) |i, encoded| {
        const duplicate = for (prepared[0..prepared_count]) |prior| {
            if (std.mem.eql(u8, physical[prior.index], physical[i])) break prior;
        } else null;
        if (duplicate) |prior| {
            prepared[prepared_count] = .{ .index = i, .fields = prior.fields, .ranges = prior.ranges, .values = prior.values };
            prepared_count += 1;
            continue;
        }
        const proof = proofs[i].?;
        const directory = (try projection.Directory.open(scratch, encoded orelse continue, proof)) orelse continue;
        var selected: std.ArrayList(projection.Field) = .empty;
        var bytes: u64 = 0;
        for (roots) |name| if (directory.find(name)) |field| {
            try selected.append(scratch, field);
            bytes +|= field.length;
        };
        // Range setup should not replace a cheaper complete read.
        if (bytes >= proof.bytes / 2) continue;
        const ranges = try scratch.alloc(ranges_mod.Range, selected.items.len);
        const values = try scratch.alloc([]const u8, selected.items.len);
        for (ranges, selected.items) |*range, field| range.* = .{ .offset = field.offset, .length = std.math.cast(usize, field.length) orelse return error.InvalidDocumentContentAuthority };
        prepared[prepared_count] = .{ .index = i, .fields = selected.items, .ranges = ranges, .values = values };
        prepared_count += 1;
        if (ranges.len != 0) {
            requests[request_count] = .{ .key = physical[i], .ranges = ranges, .output = values };
            request_count += 1;
        }
    }
    std.mem.sort(ranges_mod.Request, requests[0..request_count], {}, struct {
        fn less(_: void, left: ranges_mod.Request, right: ranges_mod.Request) bool {
            return std.mem.order(u8, left.key, right.key) == .lt;
        }
    }.less);
    if (request_count != 0) try scope.getRangesMany(requests[0..request_count]);
    for (prepared[0..prepared_count], 0..) |item, n| {
        const values = for (prepared[0..n]) |prior| {
            if (std.mem.eql(u8, physical[prior.index], physical[item.index])) break prior.values;
        } else item.values;
        var out: std.Io.Writer.Allocating = .init(a);
        defer out.deinit();
        try out.writer.writeByte('{');
        for (item.fields, values, 0..) |field, value, j| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
            if (!std.mem.eql(u8, &digest, &field.digest)) return error.InvalidDocumentContentAuthority;
            if (j != 0) try out.writer.writeByte(',');
            try std.json.Stringify.value(field.name, .{}, &out.writer);
            try out.writer.writeByte(':');
            try out.writer.writeAll(value);
        }
        try out.writer.writeByte('}');
        result[item.index] = .{ .bytes = try out.toOwnedSlice(), .original_bytes = proofs[item.index].?.bytes, .projected = true };
    }
    count = 0;
    for (result, 0..) |row, i| if (!row.projected) {
        keys[count] = physical[i];
        indices[count] = i;
        count += 1;
    };
    if (count != 0) try many(scope, keys[0..count], raw[0..count]);
    for (indices[0..count], raw[0..count]) |i, value| {
        result[i] = .{ .bytes = if (value) |v| try a.dupe(u8, v) else null, .original_bytes = if (value) |v| v.len else 0 };
    }
    return result;
}

test "logical value stored ranges bind revisions preserve arrays and reject corruption" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const internal = @import("internal_keys.zig");
    const physical = try internal.documentKeyAlloc(temp, "doc");
    const rev_key = revision.keyForPhysical(physical).?;
    const content_key = revision.contentKey(rev_key);
    const directory_key = projection.keyForRevision(rev_key);
    const ignored = try temp.alloc(u8, 32 * 1024);
    @memset(ignored, 'x');
    const body = try std.fmt.allocPrint(temp, "{{\"ignored\":\"{s}\",\"nested\":[{{\"name\":\"hello\"}},{{\"name\":[null,\"world\"]}}]}}", .{ignored});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    const directory = (try projection.build(temp, body, internal.primary_kind, digest)).?;
    var content: [49]u8 = undefined;
    std.mem.writeInt(u64, content[0..8], 7, .little);
    std.mem.writeInt(u64, content[8..16], body.len, .little);
    content[16] = internal.primary_kind;
    @memcpy(content[17..49], &digest);
    var mutation: [8]u8 = undefined;
    std.mem.writeInt(u64, &mutation, 7, .little);
    const Mock = struct {
        physical: []const u8,
        body: []const u8,
        rev_key: []const u8,
        content_key: []const u8,
        directory_key: []const u8,
        content: []const u8,
        mutation: []const u8,
        directory: ?[]const u8,
        full_reads: usize = 0,
        range_bytes: usize = 0,
        corrupt: bool = false,
        pub fn getManySorted(self: *@This(), keys: []const []const u8, output: []?[]const u8) !void {
            for (keys, output, 0..) |key, *out, i| {
                if (i != 0) try std.testing.expect(std.mem.order(u8, keys[i - 1], key) == .lt);
                out.* = if (std.mem.eql(u8, key, self.physical)) full: {
                    self.full_reads += 1;
                    break :full self.body;
                } else if (std.mem.eql(u8, key, self.rev_key)) self.mutation else if (std.mem.eql(u8, key, self.content_key)) self.content else if (std.mem.eql(u8, key, self.directory_key)) self.directory else null;
            }
        }
        pub fn getRangesMany(self: *@This(), requests: []const ranges_mod.Request) !void {
            for (requests) |request| {
                try std.testing.expectEqualStrings(self.physical, request.key);
                for (request.ranges, request.output) |range, *out| {
                    self.range_bytes += range.length;
                    out.* = if (self.corrupt) self.body[0..range.length] else self.body[@intCast(range.offset)..][0..range.length];
                }
            }
        }
    };
    var mock: Mock = .{ .physical = physical, .body = body, .rev_key = &rev_key, .content_key = &content_key, .directory_key = &directory_key, .content = &content, .mutation = &mutation, .directory = directory };
    const wanted = &[_][]const u8{"nested"};
    const rows = try load(temp, temp, &mock, &.{ physical, physical }, &.{ "doc", "doc" }, wanted, true);
    try std.testing.expect(rows[0].projected and rows[1].projected);
    try std.testing.expectEqualStrings(rows[0].bytes.?, rows[1].bytes.?);
    try std.testing.expectEqual(@as(u64, body.len), rows[0].original_bytes);
    try std.testing.expectEqual(@as(usize, 0), mock.full_reads);
    try std.testing.expect(mock.range_bytes < 128);
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, temp, rows[0].bytes.?, .{});
    try std.testing.expectEqualStrings("world", parsed.object.get("nested").?.array.items[1].object.get("name").?.array.items[1].string);
    // Missing selected fields are proven absent without a primary read.
    const missing = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, &.{"absent"}, true);
    try std.testing.expectEqualStrings("{}", missing[0].bytes.?);
    mock.corrupt = true;
    try std.testing.expectError(error.InvalidDocumentContentAuthority, load(temp, temp, &mock, &.{physical}, &.{"doc"}, wanted, true));
    mock.corrupt = false;
    // Older writers update mutation revision without replacing the directory.
    std.mem.writeInt(u64, &mutation, 8, .little);
    const stale = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, wanted, true);
    try std.testing.expect(!stale[0].projected);
    try std.testing.expectEqualStrings(body, stale[0].bytes.?);
    std.mem.writeInt(u64, &mutation, 7, .little);
    mock.directory = null;
    const legacy = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, wanted, true);
    try std.testing.expect(!legacy[0].projected);
    mock.directory = directory;
    const untrusted = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, wanted, false);
    try std.testing.expect(!untrusted[0].projected);
}
