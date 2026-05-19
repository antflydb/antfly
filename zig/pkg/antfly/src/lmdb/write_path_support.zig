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
const env_mod = @import("env.zig");
const format = @import("format.zig");
const node = @import("node.zig");
const page = @import("page.zig");
const readers = @import("readers.zig");
const writer_lock = @import("writer_lock.zig");
const mutate_leaf = @import("mutate_leaf.zig");
const rebalance_branch = @import("rebalance_branch.zig");
const txn_support = @import("txn_support.zig");
const materialize_support = @import("materialize_support.zig");

const PathStep = txn_support.PathStep;
const LeafPath = txn_support.LeafPath;
const appendUniquePgno = txn_support.appendUniquePgno;
const findBranchChildIndex = txn_support.findBranchChildIndex;
const searchPage = txn_support.searchPage;
const writeLeaf2PageOptions = txn_support.writeLeaf2PageOptions;
const SerializedLeafEntry = materialize_support.SerializedLeafEntry;
const BranchPageEntry = materialize_support.BranchPageEntry;

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

pub fn nextLeafLowerBound(self: anytype, txn: anytype, parents: []const PathStep) Error!?[]const u8 {
    var index = parents.len;
    while (index > 0) {
        index -= 1;
        const step = parents[index];
        const branch_page = try pageViewForMutation(self, txn, step.pgno);
        if (step.child_index + 1 >= branch_page.nodeCount()) continue;

        const next_child = try node.View.fromPage(branch_page, step.child_index + 1);
        return try self.arena.allocator().dupe(u8, try subtreeFirstKey(self, txn, next_child.branchPgno()));
    }
    return null;
}

fn recordDirtyPage(self: anytype, txn: anytype, current_page: page.View) Error!void {
    const gop = try self.dirty_pages.getOrPut(self.arena.allocator(), current_page.pgno());
    if (gop.found_existing) return;
    gop.value_ptr.* = .{
        .pgno = current_page.pgno(),
        .kind = current_page.kind(),
        .staged = false,
        .bytes = try self.arena.allocator().dupe(u8, try txn.env.pageBytes(current_page.pgno())),
    };
}

pub fn findLeafPath(self: anytype, txn: anytype, db_state: anytype, key: []const u8) Error!LeafPath {
    var parents: std.ArrayListUnmanaged(PathStep) = .empty;
    var current_pgno = db_state.meta.md_root;
    while (true) {
        const current_page = try pageViewForMutation(self, txn, current_pgno);
        try recordDirtyPage(self, txn, current_page);
        try appendUniquePgno(self.arena.allocator(), &db_state.touched_pages, current_pgno);
        switch (current_page.kind()) {
            .branch => {
                const child_index = try findBranchChildIndex(current_page, db_state.meta.md_flags, key);
                try parents.append(self.arena.allocator(), .{
                    .pgno = current_pgno,
                    .child_index = child_index,
                });
                const child = try node.View.fromPage(current_page, child_index);
                current_pgno = child.branchPgno();
            },
            .leaf => {
                const result = try searchPage(current_page, db_state.meta.md_flags, key, false);
                return .{
                    .parents = parents.items,
                    .leaf_pgno = current_pgno,
                    .leaf_index = result.index,
                    .exact = result.exact,
                };
            },
            else => return error.Corrupted,
        }
    }
}

pub fn canPropagateFirstKeyChange(self: anytype, txn: anytype, parents: []const PathStep, old_first_key: []const u8, new_first_key: []const u8) Error!bool {
    if (std.mem.eql(u8, old_first_key, new_first_key)) return true;

    var index = parents.len;
    while (index > 0) {
        index -= 1;
        const step = parents[index];
        if (step.child_index == 0) continue;

        const branch_page = try pageViewForMutation(self, txn, step.pgno);
        const branch_entries = try rebalance_branch.cloneEntries(self.arena.allocator(), branch_page);
        try rebalance_branch.setChildKey(self.arena.allocator(), branch_entries, step.child_index, new_first_key);

        const staged_branch = try self.arena.allocator().alloc(u8, branch_page.bytes.len);
        rebalance_branch.writePage(staged_branch, step.pgno, branch_entries) catch return false;
        return true;
    }
    return true;
}

pub fn applyFirstKeyChange(self: anytype, txn: anytype, parents: []const PathStep, old_first_key: []const u8, new_first_key: []const u8) Error!void {
    if (std.mem.eql(u8, old_first_key, new_first_key)) return;

    var index = parents.len;
    while (index > 0) {
        index -= 1;
        const step = parents[index];
        if (step.child_index == 0) continue;

        const branch_page = try pageViewForMutation(self, txn, step.pgno);
        const branch_entries = try rebalance_branch.cloneEntries(self.arena.allocator(), branch_page);
        try rebalance_branch.setChildKey(self.arena.allocator(), branch_entries, step.child_index, new_first_key);

        const branch_bytes = try mutablePageBytes(self, txn, step.pgno);
        try rebalance_branch.writePage(branch_bytes, step.pgno, branch_entries);
        return;
    }
}

pub fn subtreeFirstKey(self: anytype, txn: anytype, pgno: format.Pgno) Error![]const u8 {
    const current_page = try pageViewForMutation(self, txn, pgno);
    switch (current_page.kind()) {
        .leaf => {
            if (current_page.nodeCount() == 0) return error.Corrupted;
            const leaf = try node.View.fromPage(current_page, 0);
            return leaf.key();
        },
        .branch => {
            if (current_page.nodeCount() == 0) return error.Corrupted;
            const branch = try node.View.fromPage(current_page, 0);
            return subtreeFirstKey(self, txn, branch.branchPgno());
        },
        else => return error.Corrupted,
    }
}

pub fn pageViewForMutation(self: anytype, txn: anytype, pgno: format.Pgno) Error!page.View {
    if (self.dirty_pages.get(pgno)) |dirty_page| {
        return page.View.init(dirty_page.bytes);
    }
    return txn.pageView(pgno);
}

pub fn mutablePageBytes(self: anytype, txn: anytype, pgno: format.Pgno) Error![]u8 {
    if (self.dirty_pages.getPtr(pgno)) |dirty_page| return dirty_page.bytes;
    const current_page = try txn.pageView(pgno);
    try recordDirtyPage(self, txn, current_page);
    return self.dirty_pages.getPtr(pgno).?.bytes;
}

pub fn allocateTempPage(self: anytype, txn: anytype, page_size: usize, kind: page.Kind, no_mem_init: bool) Error!format.Pgno {
    _ = txn;
    const pgno = self.next_temp_pgno;
    self.next_temp_pgno += 1;
    try ensureTempPage(self, page_size, pgno, kind, no_mem_init);
    return pgno;
}

pub fn ensureTempPage(self: anytype, page_size: usize, pgno: format.Pgno, kind: page.Kind, no_mem_init: bool) Error!void {
    const gop = try self.dirty_pages.getOrPut(self.arena.allocator(), pgno);
    if (!gop.found_existing) {
        gop.value_ptr.* = .{
            .pgno = pgno,
            .kind = kind,
            .staged = true,
            .bytes = try self.arena.allocator().alloc(u8, page_size),
        };
        if (!no_mem_init) @memset(gop.value_ptr.bytes, 0);
    } else {
        std.debug.assert(gop.value_ptr.staged);
        std.debug.assert(gop.value_ptr.kind == kind);
    }
}

pub fn writeLeafEntriesToPgno(self: anytype, txn: anytype, pgno: format.Pgno, entries: []const SerializedLeafEntry) Error!void {
    const page_bytes = try mutableOrTempPageBytes(self, txn, pgno, .leaf);
    try mutate_leaf.writePageOptions(page_bytes, pgno, entries, !txn.env.opts.no_mem_init);
}

pub fn writeBranchEntriesToPgno(self: anytype, txn: anytype, pgno: format.Pgno, entries: []const BranchPageEntry) Error!void {
    const page_bytes = try mutableOrTempPageBytes(self, txn, pgno, .branch);
    try rebalance_branch.writePageOptions(page_bytes, pgno, entries, !txn.env.opts.no_mem_init);
}

pub fn writeLeaf2ValuesToPgno(self: anytype, txn: anytype, pgno: format.Pgno, key_size: u16, values: []const []const u8) Error!void {
    const page_bytes = try mutableOrTempPageBytes(self, txn, pgno, .leaf2);
    try writeLeaf2PageOptions(page_bytes, pgno, key_size, values, !txn.env.opts.no_mem_init);
}

pub fn mutableOrTempPageBytes(self: anytype, txn: anytype, pgno: format.Pgno, kind: page.Kind) Error![]u8 {
    if (self.dirty_pages.getPtr(pgno)) |dirty_page| {
        std.debug.assert(dirty_page.kind == kind);
        return dirty_page.bytes;
    }
    if (pgno > txn.snapshot.mm_last_pg) {
        try ensureTempPage(self, try txn.pageSize(), pgno, kind, txn.env.opts.no_mem_init);
        return self.dirty_pages.getPtr(pgno).?.bytes;
    }
    return mutablePageBytes(self, txn, pgno);
}
