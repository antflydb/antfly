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

pub const BranchPageEntry = struct {
    key: []const u8,
    child_pgno: format.Pgno,
};

pub fn cloneEntries(allocator: std.mem.Allocator, branch_page: page.View) ![]BranchPageEntry {
    var entries = std.ArrayListUnmanaged(BranchPageEntry).empty;
    for (0..branch_page.nodeCount()) |i| {
        const branch = try node.View.fromPage(branch_page, i);
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, branch.key()),
            .child_pgno = branch.branchPgno(),
        });
    }
    return entries.items;
}

pub fn setChildKey(
    allocator: std.mem.Allocator,
    entries: []BranchPageEntry,
    child_index: usize,
    new_key: []const u8,
) !void {
    entries[child_index].key = try allocator.dupe(u8, new_key);
}

pub fn replaceChild(
    allocator: std.mem.Allocator,
    entries: []BranchPageEntry,
    child_index: usize,
    new_key: []const u8,
    child_pgno: format.Pgno,
) !void {
    entries[child_index] = .{
        .key = if (child_index == 0) "" else try allocator.dupe(u8, new_key),
        .child_pgno = child_pgno,
    };
}

pub fn insertChildAfter(
    allocator: std.mem.Allocator,
    entries: []const BranchPageEntry,
    child_index: usize,
    right_first_key: []const u8,
    right_pgno: format.Pgno,
) ![]BranchPageEntry {
    var inserted = std.ArrayListUnmanaged(BranchPageEntry).empty;
    for (entries[0 .. child_index + 1]) |entry| {
        try inserted.append(allocator, entry);
    }
    try inserted.append(allocator, .{
        .key = try allocator.dupe(u8, right_first_key),
        .child_pgno = right_pgno,
    });
    for (entries[child_index + 1 ..]) |entry| {
        try inserted.append(allocator, entry);
    }
    return inserted.items;
}

pub fn appendChild(
    allocator: std.mem.Allocator,
    entries: []const BranchPageEntry,
    child_key: []const u8,
    child_pgno: format.Pgno,
) ![]BranchPageEntry {
    var merged = std.ArrayListUnmanaged(BranchPageEntry).empty;
    for (entries) |entry| {
        try merged.append(allocator, entry);
    }
    try merged.append(allocator, .{
        .key = try allocator.dupe(u8, child_key),
        .child_pgno = child_pgno,
    });
    return merged.items;
}

pub fn prependChild(
    allocator: std.mem.Allocator,
    entries: []const BranchPageEntry,
    old_first_key: []const u8,
    child_pgno: format.Pgno,
) ![]BranchPageEntry {
    var merged = std.ArrayListUnmanaged(BranchPageEntry).empty;
    try merged.append(allocator, .{
        .key = "",
        .child_pgno = child_pgno,
    });
    for (entries, 0..) |entry, i| {
        try merged.append(allocator, .{
            .key = if (i == 0) try allocator.dupe(u8, old_first_key) else entry.key,
            .child_pgno = entry.child_pgno,
        });
    }
    return merged.items;
}

pub fn removeChild(
    allocator: std.mem.Allocator,
    entries: []const BranchPageEntry,
    child_index: usize,
) ![]BranchPageEntry {
    var remaining = std.ArrayListUnmanaged(BranchPageEntry).empty;
    for (entries, 0..) |entry, i| {
        if (i == child_index) continue;
        try remaining.append(allocator, entry);
    }
    if (remaining.items.len > 0) {
        remaining.items[0].key = "";
    }
    return remaining.items;
}

pub fn findSplitIndex(
    allocator: std.mem.Allocator,
    page_size: usize,
    entries: []const BranchPageEntry,
) ?usize {
    const scratch = allocator.alloc(u8, page_size) catch return null;
    defer allocator.free(scratch);

    const midpoint = entries.len / 2;
    var best: ?usize = null;
    var best_distance: usize = std.math.maxInt(usize);
    for (1..entries.len) |split_at| {
        const left = entries[0..split_at];
        var right_buf = std.ArrayListUnmanaged(BranchPageEntry).empty;
        defer right_buf.deinit(allocator);
        for (entries[split_at..], 0..) |entry, i| {
            right_buf.append(allocator, .{
                .key = if (i == 0) "" else entry.key,
                .child_pgno = entry.child_pgno,
            }) catch return null;
        }
        if (writePage(scratch, 1, left)) |_| {} else |_| continue;
        if (writePage(scratch, 2, right_buf.items)) |_| {} else |_| continue;

        const distance = if (split_at > midpoint) split_at - midpoint else midpoint - split_at;
        if (distance < best_distance) {
            best = split_at;
            best_distance = distance;
        }
    }
    return best;
}

pub fn writePage(page_bytes: []u8, pgno: format.Pgno, entries: []const BranchPageEntry) !void {
    return writePageOptions(page_bytes, pgno, entries, true);
}

pub fn writePageOptions(page_bytes: []u8, pgno: format.Pgno, entries: []const BranchPageEntry, zero_fill: bool) !void {
    if (zero_fill) @memset(page_bytes, 0);

    var upper = page_bytes.len;
    for (entries, 0..) |entry, i| {
        const node_len = node.header_size + entry.key.len;
        const ptr_offset = format.page_header_size + i * @sizeOf(format.Indx);
        const lower = ptr_offset + @sizeOf(format.Indx);

        if (node_len > upper or lower > upper - node_len) return error.MapFull;
        upper -= node_len;

        const hdr = node.Header{
            .mn_lo = @intCast(entry.child_pgno & 0xffff),
            .mn_hi = @intCast((entry.child_pgno >> 16) & 0xffff),
            .mn_flags = @intCast((entry.child_pgno >> 32) & 0xffff),
            .mn_ksize = @intCast(entry.key.len),
        };
        format.writeStruct(node.Header, page_bytes[upper..][0..node.header_size], hdr);
        @memcpy(page_bytes[upper + node.header_size .. upper + node.header_size + entry.key.len], entry.key);
        format.writeNativeInt(format.Indx, page_bytes[ptr_offset..][0..@sizeOf(format.Indx)], @intCast(upper));
    }

    const lower = format.page_header_size + entries.len * @sizeOf(format.Indx);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.branch,
        .mp_lower = @intCast(lower),
        .mp_upper = @intCast(upper),
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);
}
