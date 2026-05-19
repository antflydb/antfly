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
const txn_mod = @import("txn.zig");
const read_support = @import("read_support.zig");

pub const Error = txn_mod.Error || node.Error || error{
    NotFound,
    Corrupted,
    UnsupportedPageType,
    UnsupportedNodeFlags,
};

pub fn get(txn: *const txn_mod.Transaction, dbi: txn_mod.Dbi, key: []const u8) Error![]const u8 {
    return read_support.get(txn, dbi, key);
}

const LeafEntry = struct {
    key: []const u8,
    value: []const u8,
    flags: u16 = 0,
    data_size: ?usize = null,
};

const BranchEntry = struct {
    key: []const u8,
    child_pgno: format.Pgno,
};

fn writeMetaPage(page_bytes: []u8, pgno: format.Pgno, page_size: u32, root_pgno: format.Pgno, txnid: format.Txnid) void {
    @memset(page_bytes, 0);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.meta,
        .mp_lower = 0,
        .mp_upper = 0,
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);

    const free_db = format.Db{
        .md_pad = page_size,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const main_db = format.Db{
        .md_pad = 0,
        .md_flags = 0,
        .md_depth = 1,
        .md_branch_pages = 0,
        .md_leaf_pages = 1,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = root_pgno,
    };
    const meta_value = format.Meta{
        .mm_magic = format.mdb_magic,
        .mm_version = format.mdb_data_version,
        .mm_address = null,
        .mm_mapsize = page_bytes.len * format.num_metas,
        .mm_dbs = .{ free_db, main_db },
        .mm_last_pg = root_pgno,
        .mm_txnid = txnid,
    };
    format.writeStruct(format.Meta, page_bytes[format.page_header_size..][0..format.meta_body_size], meta_value);
}

fn writeLeafPage(page_bytes: []u8, pgno: format.Pgno, entries: []const LeafEntry) !void {
    @memset(page_bytes, 0);

    var upper = page_bytes.len;
    for (entries, 0..) |entry, i| {
        const encoded_data_size = entry.data_size orelse entry.value.len;
        const stored_len = if ((entry.flags & format.NodeFlags.bigdata) != 0) @sizeOf(format.Pgno) else entry.value.len;
        const node_len = node.header_size + entry.key.len + stored_len;
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
            entry.value[0..stored_len],
        );

        const ptr_offset = format.page_header_size + i * @sizeOf(format.Indx);
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

fn writeBranchPage(page_bytes: []u8, pgno: format.Pgno, entries: []const BranchEntry) !void {
    @memset(page_bytes, 0);

    var upper = page_bytes.len;
    for (entries, 0..) |entry, i| {
        const node_len = node.header_size + entry.key.len;
        upper -= node_len;

        const hdr = node.Header{
            .mn_lo = @intCast(entry.child_pgno & 0xffff),
            .mn_hi = @intCast((entry.child_pgno >> 16) & 0xffff),
            .mn_flags = @intCast((entry.child_pgno >> 32) & 0xffff),
            .mn_ksize = @intCast(entry.key.len),
        };
        format.writeStruct(node.Header, page_bytes[upper..][0..node.header_size], hdr);
        @memcpy(page_bytes[upper + node.header_size .. upper + node.header_size + entry.key.len], entry.key);

        const ptr_offset = format.page_header_size + i * @sizeOf(format.Indx);
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

fn writeOverflowPage(page_bytes: []u8, pgno: format.Pgno, page_count: u32, data: []const u8) void {
    @memset(page_bytes, 0);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.overflow,
        .mp_lower = 0,
        .mp_upper = 0,
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);
    const union_offset = @offsetOf(format.PageHeader, "mp_lower");
    format.writeNativeInt(u32, page_bytes[union_offset..][0..@sizeOf(u32)], page_count);
    @memcpy(page_bytes[format.page_header_size .. format.page_header_size + data.len], data);
}

test "tree get returns inline values from a leaf root" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 3]u8 = undefined;
    writeMetaPage(bytes[0..page_size], 0, page_size, 2, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, page_size, 2, 2);
    try writeLeafPage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "apple", .value = "red" },
        .{ .key = "banana", .value = "yellow" },
    });

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "inline.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/inline.mdb", .{tmp.sub_path});

    var env = try @import("env.zig").Environment.open(file_path, .{ .no_subdir = true });
    defer env.close();

    var txn = try txn_mod.Transaction.begin(&env, .{});
    defer txn.abort();

    try std.testing.expectEqualStrings("yellow", try get(&txn, txn_mod.main_dbi, "banana"));
    try std.testing.expectError(error.NotFound, get(&txn, txn_mod.main_dbi, "pear"));
}

test "tree get follows branch pages and reads overflow values" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    const overflow_payload = "this value lives on an overflow page";
    var bytes: [page_size * 6]u8 = undefined;
    writeMetaPage(bytes[0..page_size], 0, page_size, 2, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, page_size, 2, 2);
    try writeBranchPage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try writeLeafPage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "apple", .value = "green" },
    });

    var overflow_pgno_buf: [@sizeOf(format.Pgno)]u8 = undefined;
    format.writeNativeInt(format.Pgno, &overflow_pgno_buf, 5);
    try writeLeafPage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{
            .key = "orange",
            .value = &overflow_pgno_buf,
            .flags = format.NodeFlags.bigdata,
            .data_size = overflow_payload.len,
        },
    });
    writeOverflowPage(bytes[page_size * 5 .. page_size * 6], 5, 1, overflow_payload);

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "branch.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/branch.mdb", .{tmp.sub_path});

    var env = try @import("env.zig").Environment.open(file_path, .{ .no_subdir = true });
    defer env.close();

    var txn = try txn_mod.Transaction.begin(&env, .{});
    defer txn.abort();

    try std.testing.expectEqualStrings("green", try get(&txn, txn_mod.main_dbi, "apple"));
    try std.testing.expectEqualStrings(overflow_payload, try get(&txn, txn_mod.main_dbi, "orange"));
}
