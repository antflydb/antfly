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
const node = @import("node.zig");
const readers = @import("readers.zig");
const writer_lock = @import("writer_lock.zig");
const mutate_leaf = @import("mutate_leaf.zig");
const rebalance_branch = @import("rebalance_branch.zig");
const format = @import("format.zig");
const materialize_support = @import("materialize_support.zig");

const SerializedLeafEntry = materialize_support.SerializedLeafEntry;
const BranchPageEntry = materialize_support.BranchPageEntry;
const appendOverflowPagesForSerializedEntry = materialize_support.appendOverflowPagesForSerializedEntry;
const needsOverflow = materialize_support.needsOverflow;

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

pub const PutPageResult = enum {
    fallback,
    inserted,
    replaced,
};

pub const DeletePageResult = enum {
    fallback,
    deleted,
    not_found,
};

pub fn tryPutPageLevel(self: anytype, txn: anytype, dbi: anytype, key: []const u8, value: []const u8) Error!PutPageResult {
    const db_state = try self.dbStateForWrite(dbi);
    if (db_state.meta.md_root == format.invalid_pgno) {
        const initial_entry = if (needsOverflow(.{ .key = key, .value = value }, try txn.pageSize()))
            try self.stageOverflowEntry(txn, key, value)
        else
            SerializedLeafEntry{
                .key = try self.arena.allocator().dupe(u8, key),
                .value = try self.arena.allocator().dupe(u8, value),
                .flags = 0,
                .data_size = value.len,
            };
        try self.initializeLeafRoot(txn, db_state, &.{initial_entry});
        db_state.append_hint = .{
            .leaf_pgno = db_state.meta.md_root,
            .parents = &.{},
            .last_key = initial_entry.key,
            .next_key = null,
        };
        return .inserted;
    }

    const path = try self.findLeafPath(txn, db_state, key);
    const leaf_page = try self.pageViewForMutation(txn, path.leaf_pgno);
    var entries = try mutate_leaf.cloneEntries(self.arena.allocator(), txn, leaf_page);
    const old_first_key = if (entries.len > 0) entries[0].key else "";
    const appended_to_leaf_end = !path.exact and path.leaf_index == entries.len;

    if (path.exact) {
        if ((entries[path.leaf_index].flags & (format.NodeFlags.subdata | format.NodeFlags.dupdata)) != 0) {
            return .fallback;
        }
        if ((entries[path.leaf_index].flags & format.NodeFlags.bigdata) != 0) {
            try appendOverflowPagesForSerializedEntry(
                self.arena.allocator(),
                txn,
                entries[path.leaf_index],
                &db_state.retired_pages,
            );
        }
        const replacement = if (needsOverflow(.{ .key = key, .value = value }, leaf_page.bytes.len))
            try self.stageOverflowEntry(txn, key, value)
        else
            SerializedLeafEntry{
                .key = try self.arena.allocator().dupe(u8, key),
                .value = try self.arena.allocator().dupe(u8, value),
                .flags = 0,
                .data_size = value.len,
            };
        entries[path.leaf_index] = .{
            .key = replacement.key,
            .value = replacement.value,
            .flags = replacement.flags,
            .data_size = replacement.data_size,
        };
    } else {
        const inserted_entry = if (needsOverflow(.{ .key = key, .value = value }, leaf_page.bytes.len))
            try self.stageOverflowEntry(txn, key, value)
        else
            SerializedLeafEntry{
                .key = try self.arena.allocator().dupe(u8, key),
                .value = try self.arena.allocator().dupe(u8, value),
                .flags = 0,
                .data_size = value.len,
            };
        var inserted: std.ArrayListUnmanaged(SerializedLeafEntry) = .empty;
        for (entries[0..path.leaf_index]) |entry| {
            try inserted.append(self.arena.allocator(), entry);
        }
        try inserted.append(self.arena.allocator(), inserted_entry);
        for (entries[path.leaf_index..]) |entry| {
            try inserted.append(self.arena.allocator(), entry);
        }
        entries = inserted.items;
    }

    const new_first_key = if (entries.len > 0) entries[0].key else "";
    const staged_leaf = try self.arena.allocator().alloc(u8, leaf_page.bytes.len);
    mutate_leaf.writePage(staged_leaf, path.leaf_pgno, entries) catch |err| {
        if (err == error.MapFull) {
            db_state.append_hint = null;
            return if (try self.splitLeafAfterInsert(txn, db_state, path, old_first_key, entries))
                if (path.exact) .replaced else .inserted
            else
                .fallback;
        }
        return err;
    };
    if (!try self.canPropagateFirstKeyChange(path.parents, txn, old_first_key, new_first_key)) return .fallback;

    @memcpy(try self.mutablePageBytes(txn, path.leaf_pgno), staged_leaf);
    try self.applyFirstKeyChange(path.parents, txn, old_first_key, new_first_key);
    if (appended_to_leaf_end) {
        db_state.append_hint = .{
            .leaf_pgno = path.leaf_pgno,
            .parents = path.parents,
            .last_key = entries[entries.len - 1].key,
            .next_key = try self.nextLeafLowerBound(txn, path.parents),
        };
    } else {
        db_state.append_hint = null;
    }
    return if (path.exact) .replaced else .inserted;
}

pub fn tryDeletePageLevel(self: anytype, txn: anytype, dbi: anytype, key: []const u8) Error!DeletePageResult {
    const db_state = try self.dbStateForWrite(dbi);
    if (db_state.created or db_state.meta.md_root == format.invalid_pgno) return .not_found;

    const path = try self.findLeafPath(txn, db_state, key);
    if (!path.exact) return .not_found;

    const leaf_page = try self.pageViewForMutation(txn, path.leaf_pgno);
    var entries = try mutate_leaf.cloneEntries(self.arena.allocator(), txn, leaf_page);
    if ((entries[path.leaf_index].flags & (format.NodeFlags.subdata | format.NodeFlags.dupdata)) != 0) {
        return .fallback;
    }

    const old_first_key = entries[0].key;
    if ((entries[path.leaf_index].flags & format.NodeFlags.bigdata) != 0) {
        try appendOverflowPagesForSerializedEntry(
            self.arena.allocator(),
            txn,
            entries[path.leaf_index],
            &db_state.retired_pages,
        );
    }
    if (entries.len == 1) {
        return if (try self.removeEmptyLeaf(txn, db_state, path, old_first_key)) .deleted else .fallback;
    }

    var remaining: std.ArrayListUnmanaged(SerializedLeafEntry) = .empty;
    for (entries, 0..) |entry, i| {
        if (i == path.leaf_index) continue;
        try remaining.append(self.arena.allocator(), entry);
    }
    entries = remaining.items;
    const new_first_key = entries[0].key;
    const page_size = try txn.pageSize();

    if (path.parents.len > 0 and
        try mutate_leaf.pageFillPermille(self.arena.allocator(), page_size, path.leaf_pgno, entries) < mutate_leaf.fill_threshold_permille)
    {
        return if (try rebalanceLeafAfterDelete(self, txn, db_state, path, old_first_key, entries)) .deleted else .fallback;
    }

    const staged_leaf = try self.arena.allocator().alloc(u8, leaf_page.bytes.len);
    mutate_leaf.writePage(staged_leaf, path.leaf_pgno, entries) catch |err| {
        if (err == error.MapFull) return .fallback;
        return err;
    };
    if (!try self.canPropagateFirstKeyChange(path.parents, txn, old_first_key, new_first_key)) return .fallback;

    @memcpy(try self.mutablePageBytes(txn, path.leaf_pgno), staged_leaf);
    try self.applyFirstKeyChange(path.parents, txn, old_first_key, new_first_key);
    return .deleted;
}

pub fn rebalanceLeafAfterDelete(self: anytype, txn: anytype, db_state: anytype, path: anytype, old_first_key: []const u8, entries: []SerializedLeafEntry) Error!bool {
    if (path.parents.len == 0) return false;

    const step = path.parents[path.parents.len - 1];
    const parent_page = try self.pageViewForMutation(txn, step.pgno);
    const parent_entries = try rebalance_branch.cloneEntries(self.arena.allocator(), parent_page);
    const old_parent_subtree_first_key = try self.subtreeFirstKey(txn, step.pgno);

    if (step.child_index > 0) {
        return rebalanceLeafWithLeftSibling(self, txn, db_state, path, old_first_key, entries, parent_entries, old_parent_subtree_first_key);
    }
    if (step.child_index + 1 < parent_entries.len) {
        return rebalanceLeafWithRightSibling(self, txn, db_state, path, old_first_key, entries, parent_entries, old_parent_subtree_first_key);
    }
    return false;
}

fn rebalanceLeafWithLeftSibling(self: anytype, txn: anytype, db_state: anytype, path: anytype, old_first_key: []const u8, entries: []SerializedLeafEntry, parent_entries: []BranchPageEntry, old_parent_subtree_first_key: []const u8) Error!bool {
    const step = path.parents[path.parents.len - 1];
    const left_index = step.child_index - 1;
    const left_pgno = parent_entries[left_index].child_pgno;
    const left_page = try self.pageViewForMutation(txn, left_pgno);
    if (left_page.kind() != .leaf) return false;

    var left_entries = try mutate_leaf.cloneEntries(self.arena.allocator(), txn, left_page);
    const left_fill = try mutate_leaf.pageFillPermille(self.arena.allocator(), try txn.pageSize(), left_pgno, left_entries);
    if (left_fill >= mutate_leaf.fill_threshold_permille and left_entries.len > mutate_leaf.min_keys) {
        const donated = left_entries[left_entries.len - 1];
        left_entries = left_entries[0 .. left_entries.len - 1];

        var borrowed_entries: std.ArrayListUnmanaged(SerializedLeafEntry) = .empty;
        try borrowed_entries.append(self.arena.allocator(), donated);
        for (entries) |entry| try borrowed_entries.append(self.arena.allocator(), entry);

        try self.writeLeafEntriesToPgno(txn, left_pgno, left_entries);
        try self.writeLeafEntriesToPgno(txn, path.leaf_pgno, borrowed_entries.items);
        try self.applyFirstKeyChange(path.parents, txn, old_first_key, borrowed_entries.items[0].key);
        return true;
    }

    var merged: std.ArrayListUnmanaged(SerializedLeafEntry) = .empty;
    for (left_entries) |entry| try merged.append(self.arena.allocator(), entry);
    for (entries) |entry| try merged.append(self.arena.allocator(), entry);

    try self.writeLeafEntriesToPgno(txn, left_pgno, merged.items);
    const remaining_parent = try rebalance_branch.removeChild(self.arena.allocator(), parent_entries, step.child_index);
    try self.writeBranchEntriesToPgno(txn, step.pgno, remaining_parent);
    try @import("txn_support.zig").appendUniquePgno(self.arena.allocator(), &db_state.retired_pages, path.leaf_pgno);
    if (remaining_parent.len == 1) {
        return rebalanceOneChildBranch(self, txn, db_state, path.parents, old_parent_subtree_first_key);
    }
    return true;
}

fn rebalanceLeafWithRightSibling(self: anytype, txn: anytype, db_state: anytype, path: anytype, old_first_key: []const u8, entries: []SerializedLeafEntry, parent_entries: []BranchPageEntry, old_parent_subtree_first_key: []const u8) Error!bool {
    const step = path.parents[path.parents.len - 1];
    const right_index = step.child_index + 1;
    const right_pgno = parent_entries[right_index].child_pgno;
    const right_page = try self.pageViewForMutation(txn, right_pgno);
    if (right_page.kind() != .leaf) return false;

    var right_entries = try mutate_leaf.cloneEntries(self.arena.allocator(), txn, right_page);
    const right_fill = try mutate_leaf.pageFillPermille(self.arena.allocator(), try txn.pageSize(), right_pgno, right_entries);
    if (right_fill >= mutate_leaf.fill_threshold_permille and right_entries.len > mutate_leaf.min_keys) {
        const donated = right_entries[0];
        right_entries = right_entries[1..];

        var borrowed_entries: std.ArrayListUnmanaged(SerializedLeafEntry) = .empty;
        for (entries) |entry| try borrowed_entries.append(self.arena.allocator(), entry);
        try borrowed_entries.append(self.arena.allocator(), donated);

        if (path.parents.len > 1 and !try self.canPropagateFirstKeyChange(
            path.parents[0 .. path.parents.len - 1],
            txn,
            old_first_key,
            borrowed_entries.items[0].key,
        )) return false;

        try self.writeLeafEntriesToPgno(txn, path.leaf_pgno, borrowed_entries.items);
        try self.writeLeafEntriesToPgno(txn, right_pgno, right_entries);

        const updated_parent = try rebalance_branch.cloneEntries(self.arena.allocator(), try self.pageViewForMutation(txn, step.pgno));
        try rebalance_branch.setChildKey(self.arena.allocator(), updated_parent, right_index, right_entries[0].key);
        try self.writeBranchEntriesToPgno(txn, step.pgno, updated_parent);

        if (path.parents.len > 1) {
            try self.applyFirstKeyChange(path.parents[0 .. path.parents.len - 1], txn, old_first_key, borrowed_entries.items[0].key);
        }
        return true;
    }

    var merged: std.ArrayListUnmanaged(SerializedLeafEntry) = .empty;
    for (entries) |entry| try merged.append(self.arena.allocator(), entry);
    for (right_entries) |entry| try merged.append(self.arena.allocator(), entry);

    if (path.parents.len > 1 and !try self.canPropagateFirstKeyChange(
        path.parents[0 .. path.parents.len - 1],
        txn,
        old_first_key,
        merged.items[0].key,
    )) return false;

    try self.writeLeafEntriesToPgno(txn, path.leaf_pgno, merged.items);
    const remaining_parent = try rebalance_branch.removeChild(self.arena.allocator(), parent_entries, right_index);
    try self.writeBranchEntriesToPgno(txn, step.pgno, remaining_parent);
    try @import("txn_support.zig").appendUniquePgno(self.arena.allocator(), &db_state.retired_pages, right_pgno);

    if (remaining_parent.len == 1) {
        return rebalanceOneChildBranch(self, txn, db_state, path.parents, old_parent_subtree_first_key);
    }
    if (path.parents.len > 1) {
        try self.applyFirstKeyChange(path.parents[0 .. path.parents.len - 1], txn, old_first_key, merged.items[0].key);
    }
    return true;
}

pub fn splitLeafAfterInsert(self: anytype, txn: anytype, db_state: anytype, path: anytype, old_first_key: []const u8, entries: []SerializedLeafEntry) Error!bool {
    const page_size = try txn.pageSize();
    const split_at = mutate_leaf.findSplitIndex(self.arena.allocator(), page_size, entries) orelse return false;
    const left_entries = entries[0..split_at];
    const right_entries = entries[split_at..];
    const new_first_key = left_entries[0].key;

    try self.writeLeafEntriesToPgno(txn, path.leaf_pgno, left_entries);
    const right_pgno = try self.allocateTempPage(page_size, .leaf, txn.env.opts.no_mem_init);
    try self.writeLeafEntriesToPgno(txn, right_pgno, right_entries);
    try insertRightChildIntoParents(self, txn, db_state, path.parents, old_first_key, new_first_key, right_pgno, right_entries[0].key);
    return true;
}

fn insertRightChildIntoParents(self: anytype, txn: anytype, db_state: anytype, parents: anytype, old_left_first_key: []const u8, new_left_first_key: []const u8, right_pgno: format.Pgno, right_first_key: []const u8) Error!void {
    if (parents.len == 0) {
        const page_size = try txn.pageSize();
        const root_pgno = try self.allocateTempPage(page_size, .branch, txn.env.opts.no_mem_init);
        try self.writeBranchEntriesToPgno(txn, root_pgno, &.{
            .{ .key = "", .child_pgno = db_state.meta.md_root },
            .{ .key = try self.arena.allocator().dupe(u8, right_first_key), .child_pgno = right_pgno },
        });
        db_state.meta.md_root = root_pgno;
        db_state.meta.md_depth += 1;
        db_state.meta.md_leaf_pages += 1;
        db_state.meta.md_branch_pages += 1;
        return;
    }

    const step = parents[parents.len - 1];
    const branch_page = try self.pageViewForMutation(txn, step.pgno);
    const branch_entries = try rebalance_branch.cloneEntries(self.arena.allocator(), branch_page);
    if (step.child_index > 0 and !std.mem.eql(u8, old_left_first_key, new_left_first_key)) {
        try rebalance_branch.setChildKey(self.arena.allocator(), branch_entries, step.child_index, new_left_first_key);
    }

    const inserted = try rebalance_branch.insertChildAfter(self.arena.allocator(), branch_entries, step.child_index, right_first_key, right_pgno);
    const staged_branch = try self.arena.allocator().alloc(u8, branch_page.bytes.len);
    var fits = true;
    rebalance_branch.writePage(staged_branch, step.pgno, inserted) catch {
        fits = false;
    };
    if (fits) {
        @memcpy(try self.mutablePageBytes(txn, step.pgno), staged_branch);
        return;
    }

    const split_at = rebalance_branch.findSplitIndex(self.arena.allocator(), branch_page.bytes.len, inserted) orelse return error.MapFull;
    const left_entries = inserted[0..split_at];
    const right_entries_full = inserted[split_at..];
    const promoted_key = right_entries_full[0].key;
    const right_branch_pgno = try self.allocateTempPage(try txn.pageSize(), .branch, txn.env.opts.no_mem_init);

    try self.writeBranchEntriesToPgno(txn, step.pgno, left_entries);

    var right_entries: std.ArrayListUnmanaged(BranchPageEntry) = .empty;
    for (right_entries_full, 0..) |entry, i| {
        try right_entries.append(self.arena.allocator(), .{
            .key = if (i == 0) "" else entry.key,
            .child_pgno = entry.child_pgno,
        });
    }
    try self.writeBranchEntriesToPgno(txn, right_branch_pgno, right_entries.items);

    try insertRightChildIntoParents(self, txn, db_state, parents[0 .. parents.len - 1], "", "", right_branch_pgno, promoted_key);
}

pub fn removeEmptyLeaf(self: anytype, txn: anytype, db_state: anytype, path: anytype, old_first_key: []const u8) Error!bool {
    if (path.parents.len == 0) return false;
    const step = path.parents[path.parents.len - 1];

    const branch_page = try self.pageViewForMutation(txn, step.pgno);
    const branch_entries = try rebalance_branch.cloneEntries(self.arena.allocator(), branch_page);
    if (branch_entries.len <= 1) return false;

    const remaining = try rebalance_branch.removeChild(self.arena.allocator(), branch_entries, step.child_index);
    if (remaining.len == 0) return false;

    if (remaining.len == 1) {
        try @import("txn_support.zig").appendUniquePgno(self.arena.allocator(), &db_state.retired_pages, path.leaf_pgno);
        try self.writeBranchEntriesToPgno(txn, step.pgno, remaining);
        return rebalanceOneChildBranch(self, txn, db_state, path.parents, old_first_key);
    }

    var new_subtree_first_key: []const u8 = "";
    if (step.child_index == 0 and path.parents.len > 1) {
        new_subtree_first_key = try self.subtreeFirstKey(txn, remaining[0].child_pgno);
        if (!try self.canPropagateFirstKeyChange(path.parents[0 .. path.parents.len - 1], txn, old_first_key, new_subtree_first_key)) return false;
    }

    try self.writeBranchEntriesToPgno(txn, step.pgno, remaining);
    try @import("txn_support.zig").appendUniquePgno(self.arena.allocator(), &db_state.retired_pages, path.leaf_pgno);

    if (step.child_index == 0 and path.parents.len > 1) {
        try self.applyFirstKeyChange(path.parents[0 .. path.parents.len - 1], txn, old_first_key, new_subtree_first_key);
    }
    return true;
}

fn rebalanceOneChildBranch(self: anytype, txn: anytype, db_state: anytype, branch_path: anytype, old_subtree_first_key: []const u8) Error!bool {
    _ = old_subtree_first_key;
    const branch_step = branch_path[branch_path.len - 1];
    const branch_page = try self.pageViewForMutation(txn, branch_step.pgno);
    if (branch_page.kind() != .branch) return false;
    const branch_entries = try rebalance_branch.cloneEntries(self.arena.allocator(), branch_page);
    if (branch_entries.len != 1) return false;
    const surviving_child_pgno = branch_entries[0].child_pgno;

    if (branch_path.len == 1) {
        try @import("txn_support.zig").appendUniquePgno(self.arena.allocator(), &db_state.retired_pages, branch_step.pgno);
        db_state.meta.md_root = surviving_child_pgno;
        if (db_state.meta.md_depth > 0) db_state.meta.md_depth -= 1;
        return true;
    }

    const parent_path = branch_path[0 .. branch_path.len - 1];
    const parent_step = parent_path[parent_path.len - 1];
    const parent_index = parent_step.child_index;
    const parent_page = try self.pageViewForMutation(txn, parent_step.pgno);
    const parent_entries = try rebalance_branch.cloneEntries(self.arena.allocator(), parent_page);
    const old_parent_subtree_first_key = try self.subtreeFirstKey(txn, parent_step.pgno);

    if (parent_index > 0) {
        const left_sibling_pgno = parent_entries[parent_index - 1].child_pgno;
        const left_page = try self.pageViewForMutation(txn, left_sibling_pgno);
        if (left_page.kind() == .branch) {
            const left_entries = try rebalance_branch.cloneEntries(self.arena.allocator(), left_page);
            if (left_entries.len > 2) {
                const donated = left_entries[left_entries.len - 1];
                const remaining_left = left_entries[0 .. left_entries.len - 1];
                const current_branch_key = parent_entries[parent_index].key;
                const borrowed_entries: []const BranchPageEntry = &.{
                    .{ .key = "", .child_pgno = donated.child_pgno },
                    .{ .key = current_branch_key, .child_pgno = surviving_child_pgno },
                };

                const left_scratch = try self.arena.allocator().alloc(u8, left_page.bytes.len);
                const branch_scratch = try self.arena.allocator().alloc(u8, branch_page.bytes.len);
                if (rebalance_branch.writePage(left_scratch, left_sibling_pgno, remaining_left)) |_| {
                    if (rebalance_branch.writePage(branch_scratch, branch_step.pgno, borrowed_entries)) |_| {
                        try self.writeBranchEntriesToPgno(txn, left_sibling_pgno, remaining_left);
                        try self.writeBranchEntriesToPgno(txn, branch_step.pgno, borrowed_entries);
                        const updated_parent = try rebalance_branch.cloneEntries(self.arena.allocator(), try self.pageViewForMutation(txn, parent_step.pgno));
                        try rebalance_branch.setChildKey(self.arena.allocator(), updated_parent, parent_index, donated.key);
                        try self.writeBranchEntriesToPgno(txn, parent_step.pgno, updated_parent);
                        return true;
                    } else |_| {}
                } else |_| {}
            }
            const merged_left = try rebalance_branch.appendChild(self.arena.allocator(), left_entries, try self.subtreeFirstKey(txn, surviving_child_pgno), surviving_child_pgno);
            const scratch = try self.arena.allocator().alloc(u8, left_page.bytes.len);
            if (rebalance_branch.writePage(scratch, left_sibling_pgno, merged_left)) |_| {
                try self.writeBranchEntriesToPgno(txn, left_sibling_pgno, merged_left);
                const remaining_parent = try rebalance_branch.removeChild(self.arena.allocator(), parent_entries, parent_index);
                try self.writeBranchEntriesToPgno(txn, parent_step.pgno, remaining_parent);
                try @import("txn_support.zig").appendUniquePgno(self.arena.allocator(), &db_state.retired_pages, branch_step.pgno);
                return finishBranchRebalance(self, txn, db_state, parent_path, old_parent_subtree_first_key);
            } else |_| {}
        }
    }

    if (parent_index + 1 < parent_entries.len) {
        const right_index = parent_index + 1;
        const right_sibling_pgno = parent_entries[right_index].child_pgno;
        const right_page = try self.pageViewForMutation(txn, right_sibling_pgno);
        if (right_page.kind() == .branch) {
            const right_entries = try rebalance_branch.cloneEntries(self.arena.allocator(), right_page);
            if (right_entries.len > 2) {
                const donated = right_entries[0];
                const new_right_first_key = right_entries[1].key;
                var remaining_right_buf: std.ArrayListUnmanaged(BranchPageEntry) = .empty;
                for (right_entries[1..], 0..) |entry, i| {
                    try remaining_right_buf.append(self.arena.allocator(), .{
                        .key = if (i == 0) "" else entry.key,
                        .child_pgno = entry.child_pgno,
                    });
                }
                const borrowed_entries: []const BranchPageEntry = &.{
                    .{ .key = "", .child_pgno = surviving_child_pgno },
                    .{ .key = parent_entries[right_index].key, .child_pgno = donated.child_pgno },
                };

                const right_scratch = try self.arena.allocator().alloc(u8, right_page.bytes.len);
                const branch_scratch = try self.arena.allocator().alloc(u8, branch_page.bytes.len);
                if (rebalance_branch.writePage(right_scratch, right_sibling_pgno, remaining_right_buf.items)) |_| {
                    if (rebalance_branch.writePage(branch_scratch, branch_step.pgno, borrowed_entries)) |_| {
                        try self.writeBranchEntriesToPgno(txn, right_sibling_pgno, remaining_right_buf.items);
                        try self.writeBranchEntriesToPgno(txn, branch_step.pgno, borrowed_entries);
                        const updated_parent = try rebalance_branch.cloneEntries(self.arena.allocator(), try self.pageViewForMutation(txn, parent_step.pgno));
                        try rebalance_branch.setChildKey(self.arena.allocator(), updated_parent, right_index, new_right_first_key);
                        try self.writeBranchEntriesToPgno(txn, parent_step.pgno, updated_parent);
                        return true;
                    } else |_| {}
                } else |_| {}
            }
            const merged_right = try rebalance_branch.prependChild(self.arena.allocator(), right_entries, parent_entries[right_index].key, surviving_child_pgno);
            const scratch = try self.arena.allocator().alloc(u8, right_page.bytes.len);
            if (rebalance_branch.writePage(scratch, right_sibling_pgno, merged_right)) |_| {
                try self.writeBranchEntriesToPgno(txn, right_sibling_pgno, merged_right);
                const remaining_parent = try rebalance_branch.removeChild(self.arena.allocator(), parent_entries, parent_index);
                if (parent_index > 0) {
                    try rebalance_branch.setChildKey(self.arena.allocator(), remaining_parent, parent_index, try self.subtreeFirstKey(txn, right_sibling_pgno));
                }
                try self.writeBranchEntriesToPgno(txn, parent_step.pgno, remaining_parent);
                try @import("txn_support.zig").appendUniquePgno(self.arena.allocator(), &db_state.retired_pages, branch_step.pgno);
                return finishBranchRebalance(self, txn, db_state, parent_path, old_parent_subtree_first_key);
            } else |_| {}
        }
    }

    return false;
}

fn finishBranchRebalance(self: anytype, txn: anytype, db_state: anytype, branch_path: anytype, old_subtree_first_key: []const u8) Error!bool {
    const branch_step = branch_path[branch_path.len - 1];
    const branch_page = try self.pageViewForMutation(txn, branch_step.pgno);
    if (branch_page.kind() != .branch) return false;

    if (branch_page.nodeCount() == 1) {
        return rebalanceOneChildBranch(self, txn, db_state, branch_path, old_subtree_first_key);
    }
    if (branch_path.len == 1) return true;

    const new_subtree_first_key = try self.subtreeFirstKey(txn, branch_step.pgno);
    if (!try self.canPropagateFirstKeyChange(branch_path[0 .. branch_path.len - 1], txn, old_subtree_first_key, new_subtree_first_key)) return false;
    try self.applyFirstKeyChange(branch_path[0 .. branch_path.len - 1], txn, old_subtree_first_key, new_subtree_first_key);
    return true;
}
