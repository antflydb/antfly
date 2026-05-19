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

    return error.Unexpected;
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
