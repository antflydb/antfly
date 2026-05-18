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
const node = @import("node.zig");
const page = @import("page.zig");

pub const SerializedLeafEntry = struct {
    key: []const u8,
    value: []const u8,
    flags: u16 = 0,
    data_size: usize,
};

pub const min_keys: usize = 1;
pub const fill_threshold_permille: u32 = 250;

pub fn cloneEntries(allocator: std.mem.Allocator, txn: anytype, leaf_page: page.View) ![]SerializedLeafEntry {
    var entries = std.ArrayListUnmanaged(SerializedLeafEntry).empty;
    for (0..leaf_page.nodeCount()) |i| {
        const leaf = try node.View.fromPage(leaf_page, i);
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, leaf.key()),
            .value = if (leaf.isBigData())
                try allocator.dupe(u8, leaf.inlineValue())
            else
                try copyLeafValue(txn, leaf, allocator),
            .flags = leaf.flags(),
            .data_size = leaf.dataSize(),
        });
    }
    return entries.items;
}

pub fn findSplitIndex(
    allocator: std.mem.Allocator,
    page_size: usize,
    entries: []const SerializedLeafEntry,
) ?usize {
    const scratch = allocator.alloc(u8, page_size) catch return null;
    defer allocator.free(scratch);

    const midpoint = entries.len / 2;
    var best: ?usize = null;
    var best_distance: usize = std.math.maxInt(usize);
    for (1..entries.len) |split_at| {
        if (writePage(scratch, 1, entries[0..split_at])) |_| {} else |_| continue;
        if (writePage(scratch, 2, entries[split_at..])) |_| {} else |_| continue;

        const distance = if (split_at > midpoint) split_at - midpoint else midpoint - split_at;
        if (distance < best_distance) {
            best = split_at;
            best_distance = distance;
        }
    }
    return best;
}

pub fn writePage(page_bytes: []u8, pgno: format.Pgno, entries: []const SerializedLeafEntry) !void {
    return writePageOptions(page_bytes, pgno, entries, true);
}

pub fn writePageOptions(page_bytes: []u8, pgno: format.Pgno, entries: []const SerializedLeafEntry, zero_fill: bool) !void {
    if (zero_fill) @memset(page_bytes, 0);

    var upper = page_bytes.len;
    for (entries, 0..) |entry, i| {
        const encoded_data_size = entry.data_size;
        const stored_len = entry.value.len;
        const node_len = node.header_size + entry.key.len + stored_len;
        const ptr_offset = format.page_header_size + i * @sizeOf(format.Indx);
        const lower = ptr_offset + @sizeOf(format.Indx);

        if (node_len > upper or lower > upper - node_len) return error.MapFull;
        upper -= node_len;

        const hdr = node.Header{
            .mn_lo = @intCast(encoded_data_size & 0xffff),
            .mn_hi = @intCast((encoded_data_size >> 16) & 0xffff),
            .mn_flags = entry.flags,
            .mn_ksize = @intCast(entry.key.len),
        };
        format.writeStruct(node.Header, page_bytes[upper..][0..node.header_size], hdr);
        @memcpy(page_bytes[upper + node.header_size .. upper + node.header_size + entry.key.len], entry.key);
        @memcpy(
            page_bytes[upper + node.header_size + entry.key.len .. upper + node.header_size + entry.key.len + stored_len],
            entry.value,
        );
        format.writeNativeInt(format.Indx, page_bytes[ptr_offset..][0..@sizeOf(format.Indx)], @intCast(upper));
    }

    const lower = format.page_header_size + entries.len * @sizeOf(format.Indx);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.leaf,
        .mp_lower = @intCast(lower),
        .mp_upper = @intCast(upper),
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);
}

pub fn pageFillPermille(
    allocator: std.mem.Allocator,
    page_size: usize,
    pgno: format.Pgno,
    entries: []const SerializedLeafEntry,
) !u32 {
    const scratch = try allocator.alloc(u8, page_size);
    defer allocator.free(scratch);

    try writePage(scratch, pgno, entries);
    const view = try page.View.init(scratch);
    const usable = page_size - format.page_header_size;
    const free_space: usize = view.upper() - view.lower();
    const used = usable - free_space;
    return @intCast((1000 * used) / usable);
}

pub fn appendEntryInPlace(page_bytes: []u8, entry: SerializedLeafEntry) !void {
    const view = try page.View.init(page_bytes);
    if (view.kind() != .leaf) return error.MapFull;

    const node_len = node.header_size + entry.key.len + entry.value.len;
    const ptr_offset = view.lower();
    const lower = ptr_offset + @sizeOf(format.Indx);
    const upper = view.upper();

    if (node_len > upper or lower > upper - node_len) return error.MapFull;
    const new_upper = upper - node_len;

    const hdr = node.Header{
        .mn_lo = @intCast(entry.data_size & 0xffff),
        .mn_hi = @intCast((entry.data_size >> 16) & 0xffff),
        .mn_flags = entry.flags,
        .mn_ksize = @intCast(entry.key.len),
    };
    format.writeStruct(node.Header, page_bytes[new_upper..][0..node.header_size], hdr);
    @memcpy(page_bytes[new_upper + node.header_size .. new_upper + node.header_size + entry.key.len], entry.key);
    @memcpy(
        page_bytes[new_upper + node.header_size + entry.key.len .. new_upper + node.header_size + entry.key.len + entry.value.len],
        entry.value,
    );
    format.writeNativeInt(format.Indx, page_bytes[ptr_offset..][0..@sizeOf(format.Indx)], @intCast(new_upper));

    const page_hdr = format.PageHeader{
        .mp_pgno = view.pgno(),
        .mp_pad = view.pad(),
        .mp_flags = view.flags(),
        .mp_lower = @intCast(lower),
        .mp_upper = @intCast(new_upper),
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], page_hdr);
}

fn copyLeafValue(txn: anytype, leaf: node.View, allocator: std.mem.Allocator) ![]u8 {
    if (!leaf.isBigData()) return allocator.dupe(u8, leaf.inlineValue());

    const value = try readLeafValue(txn, leaf);
    return allocator.dupe(u8, value);
}

fn readLeafValue(txn: anytype, leaf: node.View) ![]const u8 {
    if (!leaf.isBigData()) return leaf.inlineValue();

    const overflow_ref = leaf.inlineValue();
    if (overflow_ref.len < @sizeOf(format.Pgno)) return error.Corrupted;

    const overflow_pgno = format.readNativeInt(format.Pgno, overflow_ref[0..@sizeOf(format.Pgno)]);
    const overflow_page = try txn.pageView(overflow_pgno);
    if (overflow_page.kind() != .overflow) return error.Corrupted;

    const page_size = try txn.pageSize();
    const total_bytes = std.math.mul(usize, overflow_page.overflowPageCount(), page_size) catch return error.Corrupted;
    if (leaf.dataSize() > total_bytes - format.page_header_size) return error.Corrupted;

    const map = try txn.data();
    const start = std.math.mul(usize, overflow_pgno, page_size) catch return error.Corrupted;
    const data_start = std.math.add(usize, start, format.page_header_size) catch return error.Corrupted;
    const data_end = std.math.add(usize, data_start, leaf.dataSize()) catch return error.Corrupted;
    if (data_end > map.len) return error.Corrupted;
    return map[data_start..data_end];
}
