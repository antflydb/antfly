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

//! Enrichment output counts by origin across a distributed table's ranges
//! (`POST /tables/{tableName}/enrichments/counts`, #1791). Each range owner
//! counts one bounded page of its own documents
//! (`DB.artifactOriginCountsRangeJson`, reached through the private
//! seed-plan lookup); ranges are visited in key order from `from` until
//! `limit` documents are counted, and `next` resumes the walk. Counts are
//! exact per page, not a snapshot across ranges.
const std = @import("std");
const tables = @import("tables.zig");
const metadata = @import("../metadata/table_manager.zig");

pub const Request = struct { from: []const u8 = "", limit: u32 = 1000 };

const Count = struct {
    name: []const u8,
    kind: []const u8,
    seeded: u64 = 0,
    generated: u64 = 0,
    external: u64 = 0,
    pending: u64 = 0,
};

const OwnerPage = struct {
    documents: u64,
    next: ?[]const u8 = null,
    enrichments: []const Count = &.{},
};

pub fn parseRequest(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(Request) {
    const trimmed = std.mem.trim(u8, body, " \t\r\n");
    const parsed = std.json.parseFromSlice(Request, alloc, if (trimmed.len == 0) "{}" else trimmed, .{}) catch return error.InvalidArgument;
    errdefer parsed.deinit();
    if (parsed.value.limit == 0 or parsed.value.limit > 100_000) return error.InvalidArgument;
    return parsed;
}

/// Walks the table's ranges in key order; `lookup.call(alloc, start_key,
/// plan)` returns one owner's JSON page. Returns the owned response body.
pub fn collectRanges(alloc: std.mem.Allocator, ranges: []const metadata.RangeRecord, table_id: u64, request: Request, lookup: anytype) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var owners: std.ArrayList(*const metadata.RangeRecord) = .empty;
    for (ranges) |*range| if (range.table_id == table_id) try owners.append(a, range);
    std.mem.sort(*const metadata.RangeRecord, owners.items, {}, struct {
        fn less(_: void, left: *const metadata.RangeRecord, right: *const metadata.RangeRecord) bool {
            return std.mem.order(u8, left.start_key, right.start_key) == .lt;
        }
    }.less);
    var totals = std.StringArrayHashMapUnmanaged(Count).empty;
    var documents: u64 = 0;
    var next: ?[]const u8 = null;
    var remaining: u64 = request.limit;
    for (owners.items, 0..) |owner, i| {
        const end = owner.end_key orelse "";
        // Ranges entirely before the cursor.
        if (owner.end_key != null and std.mem.order(u8, end, request.from) != .gt) continue;
        if (remaining == 0) {
            next = owner.start_key;
            break;
        }
        const start = if (std.mem.order(u8, request.from, owner.start_key) == .gt) request.from else owner.start_key;
        const plan = try std.json.Stringify.valueAlloc(a, .{ .origin_counts = .{ .from = start, .end = end, .limit = remaining } }, .{});
        const raw = try lookup.call(a, owner.start_key, plan);
        const page = std.json.parseFromSliceLeaky(OwnerPage, a, raw, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }) catch return error.InvalidResponse;
        documents += page.documents;
        remaining -|= page.documents;
        for (page.enrichments) |count| {
            const entry = try totals.getOrPut(a, count.name);
            if (!entry.found_existing) entry.value_ptr.* = .{ .name = count.name, .kind = count.kind };
            entry.value_ptr.seeded += count.seeded;
            entry.value_ptr.generated += count.generated;
            entry.value_ptr.external += count.external;
            entry.value_ptr.pending += count.pending;
        }
        if (page.next) |resume_key| {
            next = resume_key;
            break;
        }
        if (remaining == 0 and i + 1 < owners.items.len) {
            next = owners.items[i + 1].start_key;
            break;
        }
    }
    return std.json.Stringify.valueAlloc(alloc, .{ .documents = documents, .next = next, .enrichments = totals.values() }, .{});
}

/// The handler entry: admin snapshot from `source`, owner lookups through
/// `reader` (a table read source).
pub fn collect(alloc: std.mem.Allocator, source: anytype, reader: anytype, name: []const u8, body: []const u8) ![]u8 {
    var parsed = try parseRequest(alloc, body);
    defer parsed.deinit();
    var snapshot = (try source.adminSnapshot()) orelse return error.TopologyChanged;
    defer source.freeAdminSnapshot(&snapshot);
    const table = tables.findTableByName(&snapshot, name) orelse return error.TableNotFound;
    if (table.sql_persistence != null) return error.RelationalTableUnsupported;
    const Lookup = struct {
        reader: @TypeOf(reader),
        name: []const u8,
        pub fn call(self: @This(), a: std.mem.Allocator, key: []const u8, plan: []const u8) ![]const u8 {
            var response = (try self.reader.lookup(a, self.name, key, .{ .seed_plan_json = plan, .include_all_fields = false }, .read_index)) orelse return error.TopologyChanged;
            defer response.deinit(a);
            return try a.dupe(u8, response.json);
        }
    };
    return collectRanges(alloc, snapshot.ranges, table.table_id, parsed.value, Lookup{ .reader = reader, .name = name });
}

test "enrichment counts page across ranges in key order and resume with next" {
    const alloc = std.testing.allocator;
    const ranges = [_]metadata.RangeRecord{
        .{ .table_id = 7, .group_id = 2, .start_key = "m", .end_key = null },
        .{ .table_id = 7, .group_id = 1, .start_key = "", .end_key = "m" },
        .{ .table_id = 9, .group_id = 3, .start_key = "", .end_key = null },
    };
    const Fake = struct {
        calls: *usize,
        pub fn call(self: @This(), a: std.mem.Allocator, key: []const u8, plan: []const u8) ![]const u8 {
            self.calls.* += 1;
            // The first range holds 3 documents, the second 5.
            if (key.len == 0) {
                try std.testing.expect(std.mem.indexOf(u8, plan, "\"end\":\"m\"") != null);
                return a.dupe(u8, "{\"documents\":3,\"next\":null,\"enrichments\":[{\"name\":\"s\",\"kind\":\"asset\",\"seeded\":2,\"generated\":1,\"external\":0,\"pending\":0}]}");
            }
            try std.testing.expect(std.mem.indexOf(u8, plan, "\"limit\":2") != null);
            return a.dupe(u8, "{\"documents\":2,\"next\":\"q\",\"enrichments\":[{\"name\":\"s\",\"kind\":\"asset\",\"seeded\":0,\"generated\":1,\"external\":0,\"pending\":1}]}");
        }
    };
    var calls: usize = 0;
    const body = try collectRanges(alloc, &ranges, 7, .{ .limit = 5 }, Fake{ .calls = &calls });
    defer alloc.free(body);
    try std.testing.expectEqual(@as(usize, 2), calls);
    try std.testing.expectEqualStrings("{\"documents\":5,\"next\":\"q\",\"enrichments\":[{\"name\":\"s\",\"kind\":\"asset\",\"seeded\":2,\"generated\":2,\"external\":0,\"pending\":1}]}", body);

    // A full page that ends on a range boundary resumes at the next range.
    calls = 0;
    const first = try collectRanges(alloc, &ranges, 7, .{ .limit = 3 }, Fake{ .calls = &calls });
    defer alloc.free(first);
    try std.testing.expect(std.mem.indexOf(u8, first, "\"next\":\"m\"") != null);
    try std.testing.expectEqual(@as(usize, 1), calls);
    try std.testing.expectError(error.InvalidArgument, parseRequest(alloc, "{\"limit\":0}"));
}
