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

const env_mod = @import("env.zig");
const format = @import("format.zig");
const node = @import("node.zig");
const page = @import("page.zig");
const dupdata = @import("dupdata.zig");
const readers = @import("readers.zig");
const txn_support = @import("txn_support.zig");
const writer_lock = @import("writer_lock.zig");
const std = @import("std");

pub const Error = env_mod.Error || node.Error || readers.Error || writer_lock.Error || std.mem.Allocator.Error || error{
    InvalidDbi,
    TransactionClosed,
    ChildTransactionActive,
    WriteTransactionsUnsupported,
    CreateUnsupported,
    NotFound,
    Incompatible,
    Corrupted,
    MapFull,
    KeyExists,
    UnsupportedNodeFlags,
    Unexpected,
};

pub const SearchResult = txn_support.SearchResult;
pub const searchPage = txn_support.searchPage;
pub const findBranchChildIndex = txn_support.findBranchChildIndex;

pub fn get(txn: anytype, dbi: anytype, key: []const u8) Error![]const u8 {
    if (key.len == 0) return error.NotFound;

    const db = try txn.db(dbi);
    if (db.md_root == format.invalid_pgno) return error.NotFound;

    var current_pgno = db.md_root;
    while (true) {
        const current_page = try txn.pageView(current_pgno);
        switch (current_page.kind()) {
            .branch => {
                const child_index = try findBranchChildIndex(current_page, db.md_flags, key);
                const child = try node.View.fromPage(current_page, child_index);
                current_pgno = child.branchPgno();
            },
            .leaf => {
                const leaf = (try findLeafNode(current_page, db.md_flags, key)) orelse return error.NotFound;
                if (leaf.isDupdata()) {
                    if ((db.md_flags & format.DbFlags.dup_sort) == 0) return error.UnsupportedNodeFlags;
                    return try readDupsortValue(txn, leaf, 0);
                }
                return try readLeafValue(txn, leaf);
            },
            .leaf2 => return error.Incompatible,
            else => return error.Corrupted,
        }
    }
}

pub fn findLeafNode(leaf_page: page.View, db_flags: u16, key: []const u8) Error!?node.View {
    const result = try searchPage(leaf_page, db_flags, key, false);
    if (!result.exact) return null;
    return result.node.?;
}

pub fn readDupsortValue(txn: anytype, leaf: node.View, dup_index: usize) Error![]const u8 {
    if (!leaf.isDupdata()) return error.UnsupportedNodeFlags;
    if (!leaf.isSubdata()) return dupdata.valueAt(leaf, dup_index);

    const value = leaf.inlineValue();
    if (value.len < @sizeOf(format.Db)) return error.Corrupted;
    const dup_db = format.readStruct(format.Db, value[0..@sizeOf(format.Db)]);
    return readDupsortSubdbValueAt(txn, dup_db, dup_index);
}

pub fn readDupsortSubdbValueAt(txn: anytype, db: format.Db, target_index: usize) Error![]const u8 {
    if (target_index >= db.md_entries) return error.NotFound;
    if (db.md_root == format.invalid_pgno) return error.NotFound;

    var remaining = target_index;
    return readDupsortSubdbValueAtPage(txn, db.md_root, &remaining);
}

fn readDupsortSubdbValueAtPage(txn: anytype, pgno: format.Pgno, remaining: *usize) Error![]const u8 {
    const current_page = try txn.pageView(pgno);
    switch (current_page.kind()) {
        .branch => {
            for (0..current_page.nodeCount()) |i| {
                const child = try node.View.fromPage(current_page, i);
                const maybe = readDupsortSubdbValueAtPage(txn, child.branchPgno(), remaining) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                return maybe;
            }
            return error.NotFound;
        },
        .leaf => {
            for (0..current_page.nodeCount()) |i| {
                const dup_leaf = try node.View.fromPage(current_page, i);
                if (dup_leaf.flags() != 0 or dup_leaf.dataSize() != 0 or dup_leaf.storedValueLen() != 0) return error.Corrupted;
                if (remaining.* == 0) return dup_leaf.key();
                remaining.* -= 1;
            }
            return error.NotFound;
        },
        .leaf2 => {
            for (0..current_page.nodeCount()) |i| {
                if (remaining.* == 0) return dupdata.leaf2ValueAt(current_page, i);
                remaining.* -= 1;
            }
            return error.NotFound;
        },
        else => return error.Corrupted,
    }
}

pub fn readLeafValue(txn: anytype, leaf: node.View) Error![]const u8 {
    if (!leaf.isBigData()) return leaf.inlineValue();

    const overflow_ref = leaf.inlineValue();
    if (overflow_ref.len < @sizeOf(format.Pgno)) return error.Corrupted;
    if (txn.stagedOverflowData(overflow_ref)) |staged| {
        if (leaf.dataSize() > staged.len) return error.Corrupted;
        return staged[0..leaf.dataSize()];
    }

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
