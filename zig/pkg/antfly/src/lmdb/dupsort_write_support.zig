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
const dupdata = @import("dupdata.zig");
const read_support = @import("read_support.zig");
const write_state_mod = @import("write_state.zig");
const materialize_support = @import("materialize_support.zig");

const SerializedLeafEntry = materialize_support.SerializedLeafEntry;
const LeafWriteEntry = materialize_support.LeafWriteEntry;
const PageImage = @import("commit_support.zig").PageImage;
const needsOverflow = materialize_support.needsOverflow;
const leafEntryStorageSize = materialize_support.leafEntryStorageSize;
const appendOverflowPagesForSerializedEntry = materialize_support.appendOverflowPagesForSerializedEntry;
const collectDbPageNumbers = write_state_mod.collectDbPageNumbers;
const readDupsortSubdbValueAt = read_support.readDupsortSubdbValueAt;
const dupSortSubdbFlags = @import("txn_support.zig").dupSortSubdbFlags;

pub fn tryPutPageLevel(self: anytype, txn: anytype, dbi: anytype, key: []const u8, value: []const u8) !bool {
    const db_state = try self.dbStateForWrite(dbi);
    const new_value = try self.arena.allocator().dupe(u8, value);
    if (db_state.meta.md_root == format.invalid_pgno) {
        const initial_entry = try buildDupSortLeafEntry(self, txn, db_state.meta.md_flags, key, &.{new_value});
        if (initial_entry == null) return false;
        try self.initializeLeafRoot(txn, db_state, &.{initial_entry.?});
        return true;
    }

    const path = try self.findLeafPath(txn, db_state, key);
    const leaf_page = try self.pageViewForMutation(txn, path.leaf_pgno);
    var entries = try @import("mutate_leaf.zig").cloneEntries(self.arena.allocator(), txn, leaf_page);
    const old_first_key = if (entries.len > 0) entries[0].key else "";

    if (path.exact) {
        var dup_values: std.ArrayListUnmanaged([]u8) = .empty;
        appendDupSortEntryValues(self, txn, entries[path.leaf_index], &dup_values) catch |err| switch (err) {
            error.UnsupportedNodeFlags => return false,
            else => return err,
        };
        const insert_at = @import("txn_support.zig").findDupValueInsertIndex(dup_values.items, db_state.meta.md_flags, new_value);
        try dup_values.insert(self.arena.allocator(), insert_at, new_value);
        const replacement = try buildDupSortLeafEntry(self, txn, db_state.meta.md_flags, key, dup_values.items);
        if (replacement == null) return false;
        try retireDupSortEntryResources(self, txn, entries[path.leaf_index], &db_state.retired_pages);
        entries[path.leaf_index] = replacement.?;
    } else {
        const inserted_entry = try buildDupSortLeafEntry(self, txn, db_state.meta.md_flags, key, &.{new_value});
        if (inserted_entry == null) return false;
        var inserted: std.ArrayListUnmanaged(SerializedLeafEntry) = .empty;
        for (entries[0..path.leaf_index]) |entry| {
            try inserted.append(self.arena.allocator(), entry);
        }
        try inserted.append(self.arena.allocator(), inserted_entry.?);
        for (entries[path.leaf_index..]) |entry| {
            try inserted.append(self.arena.allocator(), entry);
        }
        entries = inserted.items;
    }

    const new_first_key = if (entries.len > 0) entries[0].key else "";
    const staged_leaf = try self.arena.allocator().alloc(u8, leaf_page.bytes.len);
    @import("mutate_leaf.zig").writePage(staged_leaf, path.leaf_pgno, entries) catch |err| {
        if (err == error.MapFull) {
            return self.splitLeafAfterInsert(txn, db_state, path, old_first_key, entries);
        }
        return err;
    };
    if (!try self.canPropagateFirstKeyChange(path.parents, txn, old_first_key, new_first_key)) return false;

    @memcpy(try self.mutablePageBytes(txn, path.leaf_pgno), staged_leaf);
    try self.applyFirstKeyChange(path.parents, txn, old_first_key, new_first_key);
    return true;
}

pub fn tryDeletePageLevel(self: anytype, txn: anytype, dbi: anytype, key: []const u8) !bool {
    const db_state = try self.dbStateForWrite(dbi);
    if (db_state.created or db_state.meta.md_root == format.invalid_pgno) return false;

    const path = try self.findLeafPath(txn, db_state, key);
    if (!path.exact) return false;

    const leaf_page = try self.pageViewForMutation(txn, path.leaf_pgno);
    var entries = try @import("mutate_leaf.zig").cloneEntries(self.arena.allocator(), txn, leaf_page);
    const target = entries[path.leaf_index];
    if ((target.flags & format.NodeFlags.subdata) != 0 and (target.flags & format.NodeFlags.dupdata) == 0) return false;

    const old_first_key = entries[0].key;
    try retireDupSortEntryResources(self, txn, target, &db_state.retired_pages);
    if (entries.len == 1) {
        return self.removeEmptyLeaf(txn, db_state, path, old_first_key);
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
        try @import("mutate_leaf.zig").pageFillPermille(self.arena.allocator(), page_size, path.leaf_pgno, entries) < @import("mutate_leaf.zig").fill_threshold_permille)
    {
        return self.rebalanceLeafAfterDelete(txn, db_state, path, old_first_key, entries);
    }

    const staged_leaf = try self.arena.allocator().alloc(u8, leaf_page.bytes.len);
    @import("mutate_leaf.zig").writePage(staged_leaf, path.leaf_pgno, entries) catch |err| {
        if (err == error.MapFull) return false;
        return err;
    };
    if (!try self.canPropagateFirstKeyChange(path.parents, txn, old_first_key, new_first_key)) return false;

    @memcpy(try self.mutablePageBytes(txn, path.leaf_pgno), staged_leaf);
    try self.applyFirstKeyChange(path.parents, txn, old_first_key, new_first_key);
    return true;
}

pub fn tryDeleteValuePageLevel(self: anytype, txn: anytype, dbi: anytype, key: []const u8, value: []const u8) !bool {
    const db_state = try self.dbStateForWrite(dbi);
    if (db_state.created or db_state.meta.md_root == format.invalid_pgno) return false;

    const path = try self.findLeafPath(txn, db_state, key);
    if (!path.exact) return false;

    const leaf_page = try self.pageViewForMutation(txn, path.leaf_pgno);
    var entries = try @import("mutate_leaf.zig").cloneEntries(self.arena.allocator(), txn, leaf_page);
    const target = entries[path.leaf_index];
    if ((target.flags & format.NodeFlags.subdata) != 0 and (target.flags & format.NodeFlags.dupdata) == 0) return false;

    const old_first_key = entries[0].key;

    if ((target.flags & format.NodeFlags.dupdata) == 0) {
        const existing_value = try readSerializedLeafValue(self, txn, target);
        if (!std.mem.eql(u8, existing_value, value)) return error.NotFound;
        try retireDupSortEntryResources(self, txn, target, &db_state.retired_pages);
        if (entries.len == 1) {
            return self.removeEmptyLeaf(txn, db_state, path, old_first_key);
        }

        var remaining_single: std.ArrayListUnmanaged(SerializedLeafEntry) = .empty;
        for (entries, 0..) |entry, i| {
            if (i == path.leaf_index) continue;
            try remaining_single.append(self.arena.allocator(), entry);
        }
        entries = remaining_single.items;
        const new_first_key = entries[0].key;
        const page_size = try txn.pageSize();

        if (path.parents.len > 0 and
            try @import("mutate_leaf.zig").pageFillPermille(self.arena.allocator(), page_size, path.leaf_pgno, entries) < @import("mutate_leaf.zig").fill_threshold_permille)
        {
            return self.rebalanceLeafAfterDelete(txn, db_state, path, old_first_key, entries);
        }

        const staged_single = try self.arena.allocator().alloc(u8, leaf_page.bytes.len);
        @import("mutate_leaf.zig").writePage(staged_single, path.leaf_pgno, entries) catch |err| {
            if (err == error.MapFull) return false;
            return err;
        };
        if (!try self.canPropagateFirstKeyChange(path.parents, txn, old_first_key, new_first_key)) return false;

        @memcpy(try self.mutablePageBytes(txn, path.leaf_pgno), staged_single);
        try self.applyFirstKeyChange(path.parents, txn, old_first_key, new_first_key);
        return true;
    }

    var dup_values: std.ArrayListUnmanaged([]u8) = .empty;
    appendDupSortEntryValues(self, txn, target, &dup_values) catch |err| switch (err) {
        error.UnsupportedNodeFlags => return false,
        else => return err,
    };
    const value_index = @import("txn_support.zig").findDupValueInsertIndex(dup_values.items, db_state.meta.md_flags, value);
    if (value_index >= dup_values.items.len or dupdata.compareValues(db_state.meta.md_flags, dup_values.items[value_index], value) != .eq) {
        return error.NotFound;
    }

    try retireDupSortEntryResources(self, txn, target, &db_state.retired_pages);
    _ = dup_values.orderedRemove(value_index);
    if (dup_values.items.len == 0) {
        if (entries.len == 1) {
            return self.removeEmptyLeaf(txn, db_state, path, old_first_key);
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
            try @import("mutate_leaf.zig").pageFillPermille(self.arena.allocator(), page_size, path.leaf_pgno, entries) < @import("mutate_leaf.zig").fill_threshold_permille)
        {
            return self.rebalanceLeafAfterDelete(txn, db_state, path, old_first_key, entries);
        }

        const staged_leaf = try self.arena.allocator().alloc(u8, leaf_page.bytes.len);
        @import("mutate_leaf.zig").writePage(staged_leaf, path.leaf_pgno, entries) catch |err| {
            if (err == error.MapFull) return false;
            return err;
        };
        if (!try self.canPropagateFirstKeyChange(path.parents, txn, old_first_key, new_first_key)) return false;

        @memcpy(try self.mutablePageBytes(txn, path.leaf_pgno), staged_leaf);
        try self.applyFirstKeyChange(path.parents, txn, old_first_key, new_first_key);
        return true;
    }

    const replacement = try buildDupSortLeafEntry(self, txn, db_state.meta.md_flags, key, dup_values.items);
    if (replacement == null) return false;
    entries[path.leaf_index] = replacement.?;

    const staged_leaf = try self.arena.allocator().alloc(u8, leaf_page.bytes.len);
    @import("mutate_leaf.zig").writePage(staged_leaf, path.leaf_pgno, entries) catch |err| {
        if (err == error.MapFull) return false;
        return err;
    };
    @memcpy(try self.mutablePageBytes(txn, path.leaf_pgno), staged_leaf);
    return true;
}

fn buildDupSortLeafEntry(self: anytype, txn: anytype, db_flags: u16, key: []const u8, values: []const []u8) !?SerializedLeafEntry {
    if (values.len == 0) return null;
    if (values.len == 1) {
        const only_value = values[0];
        if (needsOverflow(.{ .key = key, .value = only_value }, try txn.pageSize())) {
            return try self.stageOverflowEntry(txn, key, only_value);
        }
        return .{
            .key = try self.arena.allocator().dupe(u8, key),
            .value = try self.arena.allocator().dupe(u8, only_value),
            .flags = 0,
            .data_size = only_value.len,
        };
    }

    const value_views = try self.arena.allocator().alloc([]const u8, values.len);
    for (values, 0..) |dup_value, i| value_views[i] = dup_value;
    const subpage = dupdata.encodeSubpage(self.arena.allocator(), db_flags, value_views) catch |err| switch (err) {
        error.MapFull, error.Incompatible => {
            const dup_db = try stageDupSortSubdb(self, txn, db_flags, value_views);
            return .{
                .key = try self.arena.allocator().dupe(u8, key),
                .value = try self.arena.allocator().dupe(u8, std.mem.asBytes(&dup_db)),
                .flags = format.NodeFlags.dupdata | format.NodeFlags.subdata,
                .data_size = @sizeOf(format.Db),
            };
        },
        else => return err,
    };
    if (leafEntryStorageSize(.{
        .key = key,
        .value = subpage,
        .flags = format.NodeFlags.dupdata,
        .data_size = subpage.len,
    }, try txn.pageSize())) |_| {} else |_| {
        const dup_db = try stageDupSortSubdb(self, txn, db_flags, value_views);
        return .{
            .key = try self.arena.allocator().dupe(u8, key),
            .value = try self.arena.allocator().dupe(u8, std.mem.asBytes(&dup_db)),
            .flags = format.NodeFlags.dupdata | format.NodeFlags.subdata,
            .data_size = @sizeOf(format.Db),
        };
    }
    return .{
        .key = try self.arena.allocator().dupe(u8, key),
        .value = subpage,
        .flags = format.NodeFlags.dupdata,
        .data_size = subpage.len,
    };
}

fn stageDupSortSubdb(self: anytype, txn: anytype, db_flags: u16, values: []const []const u8) !format.Db {
    const subdb_flags = dupSortSubdbFlags(db_flags);
    var dup_db_entries: std.ArrayListUnmanaged(LeafWriteEntry) = .empty;
    for (values) |value| {
        try dup_db_entries.append(self.arena.allocator(), .{
            .key = value,
            .value = &.{},
        });
    }

    var builder = materialize_support.ImageBuilder{
        .allocator = self.arena.allocator(),
        .page_size = try txn.pageSize(),
        .next_pgno = self.next_temp_pgno,
        .reusable_pages = &.{},
    };
    const dup_db = (try builder.buildDb(dup_db_entries.items, subdb_flags)).db;
    self.next_temp_pgno = builder.next_pgno;
    try stageTempPageImages(self, txn, builder.pages.items);
    return dup_db;
}

fn stageTempPageImages(self: anytype, txn: anytype, page_images: []const PageImage) !void {
    for (page_images) |page_image| {
        switch (page_image) {
            .leaf => |leaf_page| try self.writeLeafEntriesToPgno(txn, leaf_page.pgno, leaf_page.entries),
            .leaf2 => |leaf2_page| try self.writeLeaf2ValuesToPgno(txn, leaf2_page.pgno, leaf2_page.key_size, leaf2_page.values),
            .branch => |branch_page| try self.writeBranchEntriesToPgno(txn, branch_page.pgno, branch_page.entries),
            .overflow => |overflow| {
                try self.staged_overflows.append(self.arena.allocator(), .{
                    .pgno = overflow.pgno,
                    .page_count = overflow.page_count,
                    .data = overflow.data,
                });
            },
        }
    }
}

fn appendDupSortEntryValues(self: anytype, txn: anytype, entry: SerializedLeafEntry, out: *std.ArrayListUnmanaged([]u8)) !void {
    if ((entry.flags & format.NodeFlags.dupdata) != 0) {
        if ((entry.flags & format.NodeFlags.subdata) != 0) {
            if (entry.value.len < @sizeOf(format.Db)) return error.Corrupted;
            const dup_db = format.readStruct(format.Db, entry.value[0..@sizeOf(format.Db)]);
            var i: usize = 0;
            while (i < dup_db.md_entries) : (i += 1) {
                try out.append(self.arena.allocator(), try self.arena.allocator().dupe(u8, try readDupsortSubdbValueAt(txn, dup_db, i)));
            }
            return;
        }
        return dupdata.appendClonedValuesFromBytes(self.arena.allocator(), entry.value, out);
    }

    try out.append(self.arena.allocator(), try readSerializedLeafValue(self, txn, entry));
}

fn readSerializedLeafValue(self: anytype, txn: anytype, entry: SerializedLeafEntry) ![]u8 {
    if ((entry.flags & format.NodeFlags.bigdata) == 0) return self.arena.allocator().dupe(u8, entry.value);
    if (self.findStagedOverflowRef(entry.value)) |staged| {
        return self.arena.allocator().dupe(u8, staged.data);
    }

    const overflow_ref = entry.value;
    if (overflow_ref.len < @sizeOf(format.Pgno)) return error.Corrupted;
    const overflow_pgno = format.readNativeInt(format.Pgno, overflow_ref[0..@sizeOf(format.Pgno)]);
    const overflow_page = try self.pageViewForMutation(txn, overflow_pgno);
    if (overflow_page.kind() != .overflow) return error.Corrupted;

    const page_size = try txn.pageSize();
    const total_bytes = std.math.mul(usize, overflow_page.overflowPageCount(), page_size) catch return error.Corrupted;
    if (entry.data_size > total_bytes - format.page_header_size) return error.Corrupted;

    const map = try txn.data();
    const start = std.math.mul(usize, overflow_pgno, page_size) catch return error.Corrupted;
    const data_start = std.math.add(usize, start, format.page_header_size) catch return error.Corrupted;
    const data_end = std.math.add(usize, data_start, entry.data_size) catch return error.Corrupted;
    if (data_end > map.len) return error.Corrupted;
    return self.arena.allocator().dupe(u8, map[data_start..data_end]);
}

fn retireDupSortEntryResources(self: anytype, txn: anytype, entry: SerializedLeafEntry, retired_pages: *std.ArrayListUnmanaged(format.Pgno)) !void {
    if ((entry.flags & format.NodeFlags.bigdata) != 0 and (entry.flags & format.NodeFlags.dupdata) == 0) {
        if (self.findStagedOverflowRef(entry.value) != null) return;
        return appendOverflowPagesForSerializedEntry(self.arena.allocator(), txn, entry, retired_pages);
    }
    if ((entry.flags & (format.NodeFlags.dupdata | format.NodeFlags.subdata)) == (format.NodeFlags.dupdata | format.NodeFlags.subdata)) {
        if (entry.value.len < @sizeOf(format.Db)) return error.Corrupted;
        const dup_db = format.readStruct(format.Db, entry.value[0..@sizeOf(format.Db)]);
        return collectDbPageNumbers(txn, dup_db, false, self.arena.allocator(), retired_pages);
    }
}
