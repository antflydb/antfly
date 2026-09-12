// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const format = @import("format.zig");

pub const FreeRecord = struct {
    txnid: format.Txnid,
    pages: []format.Pgno,
};

pub fn BuildResult(comptime ImageBuilderType: type) type {
    return struct {
        builder: ImageBuilderType,
        db: format.Db,
    };
}

pub fn buildDb(
    comptime ImageBuilderType: type,
    comptime PageImageType: type,
    comptime LeafWriteEntryType: type,
    allocator: std.mem.Allocator,
    page_size: usize,
    base_pages: []const PageImageType,
    base_next_pgno: format.Pgno,
    base_reusable_pages: []const format.Pgno,
    retained_free_records: []const FreeRecord,
    retired_snapshot_pages: []const format.Pgno,
    next_txnid: format.Txnid,
) !BuildResult(ImageBuilderType) {
    var candidate_free_pages = try combinePgnoLists(allocator, retired_snapshot_pages, base_reusable_pages);
    var iteration: usize = 0;
    while (iteration < 8) : (iteration += 1) {
        var builder = ImageBuilderType{
            .allocator = allocator,
            .page_size = page_size,
            .next_pgno = base_next_pgno,
            .reusable_pages = try allocator.dupe(format.Pgno, base_reusable_pages),
        };
        try builder.pages.appendSlice(allocator, base_pages);

        const free_page_entries = try buildFreePageEntries(LeafWriteEntryType, allocator, retained_free_records, next_txnid, candidate_free_pages);
        const free_db = (try builder.buildDb(free_page_entries, format.DbFlags.integer_key)).db;
        const actual_free_pages = try combinePgnoLists(allocator, retired_snapshot_pages, builder.reusable_pages);
        if (std.mem.eql(format.Pgno, candidate_free_pages, actual_free_pages)) {
            return .{
                .builder = builder,
                .db = free_db,
            };
        }
        candidate_free_pages = actual_free_pages;
    }

    // The free DB consumes the very pages it records. At an inline/overflow
    // boundary, consuming one extra page shrinks its value enough to need one
    // fewer page, so iteration can oscillate instead of reaching a fixed point.
    // Break that dependency by allocating this free DB at the append frontier.
    // Every original reusable page stays recorded as free; no page is lost or
    // advertised while in use. Ordinary commits still reuse pages above, and
    // these appended metadata pages retire normally on the next replacement.
    var builder = ImageBuilderType{
        .allocator = allocator,
        .page_size = page_size,
        .next_pgno = base_next_pgno,
    };
    try builder.pages.appendSlice(allocator, base_pages);
    const all_free_pages = try combinePgnoLists(allocator, retired_snapshot_pages, base_reusable_pages);
    const entries = try buildFreePageEntries(LeafWriteEntryType, allocator, retained_free_records, next_txnid, all_free_pages);
    const db = (try builder.buildDb(entries, format.DbFlags.integer_key)).db;
    // Preserve the builder contract even though allocation was append-only.
    builder.reusable_pages = try allocator.dupe(format.Pgno, base_reusable_pages);
    return .{ .builder = builder, .db = db };
}

pub fn buildFreePageEntries(
    comptime LeafWriteEntryType: type,
    allocator: std.mem.Allocator,
    retained_free_records: []const FreeRecord,
    next_txnid: format.Txnid,
    current_free_pages: []const format.Pgno,
) ![]const LeafWriteEntryType {
    var free_page_entries: std.ArrayListUnmanaged(LeafWriteEntryType) = .empty;
    for (retained_free_records) |record| {
        try free_page_entries.append(allocator, .{
            .key = try encodeTxnid(allocator, record.txnid),
            .value = try encodePgnoList(allocator, record.pages),
        });
    }
    if (current_free_pages.len > 0) {
        try free_page_entries.append(allocator, .{
            .key = try encodeTxnid(allocator, next_txnid),
            .value = try encodePgnoList(allocator, current_free_pages),
        });
    }
    std.sort.insertion(LeafWriteEntryType, free_page_entries.items, {}, freeRecordLessThan(LeafWriteEntryType));
    return free_page_entries.items;
}

pub fn combinePgnoLists(
    allocator: std.mem.Allocator,
    left: []const format.Pgno,
    right: []const format.Pgno,
) ![]format.Pgno {
    const pages = try allocator.alloc(format.Pgno, left.len + right.len);
    @memcpy(pages[0..left.len], left);
    @memcpy(pages[left.len..][0..right.len], right);
    const unique_len = sortAndUniquePgnoList(pages);
    return pages[0..unique_len];
}

pub fn encodeTxnid(allocator: std.mem.Allocator, txnid: format.Txnid) ![]u8 {
    const bytes = try allocator.alloc(u8, @sizeOf(format.Txnid));
    format.writeNativeInt(format.Txnid, bytes, txnid);
    return bytes;
}

pub fn encodePgnoList(allocator: std.mem.Allocator, pages: []const format.Pgno) ![]u8 {
    const count = pages.len + 1;
    const bytes = try allocator.alloc(u8, count * @sizeOf(format.Pgno));
    format.writeNativeInt(format.Pgno, bytes[0..@sizeOf(format.Pgno)], pages.len);
    for (pages, 0..) |pgno, i| {
        const offset = (i + 1) * @sizeOf(format.Pgno);
        format.writeNativeInt(format.Pgno, bytes[offset..][0..@sizeOf(format.Pgno)], pgno);
    }
    return bytes;
}

pub fn decodePgnoList(allocator: std.mem.Allocator, value: []const u8) ![]format.Pgno {
    if (value.len < @sizeOf(format.Pgno) or value.len % @sizeOf(format.Pgno) != 0) return error.Corrupted;
    const count = format.readNativeInt(format.Pgno, value[0..@sizeOf(format.Pgno)]);
    if (value.len != (count + 1) * @sizeOf(format.Pgno)) return error.Corrupted;

    const pages = try allocator.alloc(format.Pgno, count);
    for (0..count) |i| {
        const offset = (i + 1) * @sizeOf(format.Pgno);
        pages[i] = format.readNativeInt(format.Pgno, value[offset..][0..@sizeOf(format.Pgno)]);
    }
    return pages;
}

pub fn sortAndUniquePgnoList(items: []format.Pgno) usize {
    if (items.len == 0) return 0;
    std.sort.insertion(format.Pgno, items, {}, lessThanPgno);
    var out: usize = 1;
    for (1..items.len) |i| {
        if (items[i] == items[out - 1]) continue;
        items[out] = items[i];
        out += 1;
    }
    return out;
}

pub fn lessThanPgno(_: void, left: format.Pgno, right: format.Pgno) bool {
    return left < right;
}

pub fn freeRecordLessThan(comptime LeafWriteEntryType: type) fn (void, LeafWriteEntryType, LeafWriteEntryType) bool {
    return struct {
        fn lessThan(_: void, left: LeafWriteEntryType, right: LeafWriteEntryType) bool {
            return format.readNativeInt(format.Txnid, left.key) < format.readNativeInt(format.Txnid, right.key);
        }
    }.lessThan;
}

test "free DB converges across inline and overflow boundaries without losing pages" {
    const materialize = @import("materialize_support.zig");
    const PageImage = @import("commit_support.zig").PageImage;
    var appended_cases: usize = 0;
    // Exercise Linux and Darwin page sizes on either host. Fragmented and
    // contiguous free extents exercise different overflow allocation choices.
    for ([_]usize{ 512, 4096, 16384 }) |page_size| for ([_]usize{ 1, 2 }) |stride| {
        for (page_size / 8 - 12..page_size / 8 + 4) |count| {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const alloc = arena.allocator();
            const reusable = try alloc.alloc(format.Pgno, count);
            for (reusable, 0..) |*pgno, i| pgno.* = @intCast(10 + i * stride);
            const retired = [_]format.Pgno{ 3, 4 };
            const frontier = reusable[reusable.len - 1] + 1;
            const built = try buildDb(materialize.ImageBuilder, PageImage, materialize.LeafWriteEntry, alloc, page_size, &.{}, frontier, reusable, &.{}, &retired, 7);
            if (built.builder.next_pgno > frontier and built.builder.reusable_pages.len == reusable.len) appended_cases += 1;
            var encoded: ?[]const u8 = null;
            for (built.builder.pages.items) |image| switch (image) {
                .leaf => |leaf| for (leaf.entries) |entry| {
                    if (format.readNativeInt(format.Txnid, entry.key) != 7) continue;
                    if (entry.flags & format.NodeFlags.bigdata == 0) {
                        encoded = entry.value;
                    } else {
                        const pgno = format.readNativeInt(format.Pgno, entry.value);
                        for (built.builder.pages.items) |payload| if (payload == .overflow and payload.overflow.pgno == pgno) {
                            encoded = payload.overflow.data;
                        };
                    }
                },
                else => {},
            };
            const free = try decodePgnoList(alloc, encoded orelse return error.TestExpectedFreeRecord);
            const expected = try combinePgnoLists(alloc, &retired, built.builder.reusable_pages);
            try std.testing.expectEqualSlices(format.Pgno, expected, free);
            var reused: usize = 0;
            for (built.builder.pages.items) |image| {
                const first = switch (image) {
                    inline else => |value| value.pgno,
                };
                const pages: usize = if (image == .overflow) image.overflow.page_count else 1;
                for (first..first + pages) |pgno| {
                    try std.testing.expect(std.mem.indexOfScalar(format.Pgno, free, pgno) == null);
                    if (pgno < frontier) reused += 1;
                }
            }
            try std.testing.expectEqual(reusable.len, reused + built.builder.reusable_pages.len);
        }
    };
    try std.testing.expect(appended_cases > 0);
}
