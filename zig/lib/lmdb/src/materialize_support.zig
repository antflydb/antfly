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
const dupdata = @import("dupdata.zig");
const mutate_leaf = @import("mutate_leaf.zig");
const rebalance_branch = @import("rebalance_branch.zig");
const readers = @import("readers.zig");
const writer_lock = @import("writer_lock.zig");
const support = @import("txn_support.zig");
const commit_support = @import("commit_support.zig");

pub const Error = env_mod.Error || dupdata.Error || readers.Error || writer_lock.Error || std.mem.Allocator.Error || error{
    InvalidDbi,
    TransactionClosed,
    ChildTransactionActive,
    WriteTransactionsUnsupported,
    CreateUnsupported,
    NotFound,
    KeyExists,
    Corrupted,
    Unexpected,
    MapFull,
    Incompatible,
};

pub const LeafWriteEntry = struct {
    key: []const u8,
    value: []const u8,
    flags: u16 = 0,
    data_size: ?usize = null,
};

pub const SerializedLeafEntry = mutate_leaf.SerializedLeafEntry;
pub const BranchPageEntry = rebalance_branch.BranchPageEntry;

pub const ChildRef = struct {
    first_key: []const u8,
    pgno: format.Pgno,
};

pub const DbImage = struct {
    db: format.Db,
};

pub const KeySortContext = struct {
    db_flags: u16,
};

pub fn leafWriteEntryLessThan(ctx: KeySortContext, left: LeafWriteEntry, right: LeafWriteEntry) bool {
    return format.compareDbKeys(ctx.db_flags, left.key, right.key) == .lt;
}

pub const DupSortSortContext = struct {
    db_flags: u16,
};

pub fn leafWriteEntryDupSortLessThan(ctx: DupSortSortContext, left: LeafWriteEntry, right: LeafWriteEntry) bool {
    return support.compareEntryPair(ctx.db_flags, left.key, left.value, right.key, right.value) == .lt;
}

pub const ImageBuilder = struct {
    allocator: std.mem.Allocator,
    page_size: usize,
    next_pgno: format.Pgno = format.num_metas,
    reusable_pages: []format.Pgno = &.{},
    pages: std.ArrayListUnmanaged(commit_support.PageImage) = .empty,

    pub fn allocPgno(self: *ImageBuilder, count: usize) format.Pgno {
        if (count <= self.reusable_pages.len) {
            var i: usize = 0;
            while (i + count <= self.reusable_pages.len) : (i += 1) {
                const start = self.reusable_pages[i];
                var contiguous = true;
                for (1..count) |offset| {
                    if (self.reusable_pages[i + offset] != start + offset) {
                        contiguous = false;
                        break;
                    }
                }
                if (contiguous) {
                    std.mem.copyForwards(format.Pgno, self.reusable_pages[i..], self.reusable_pages[i + count ..]);
                    self.reusable_pages = self.reusable_pages[0 .. self.reusable_pages.len - count];
                    return start;
                }
            }
        }

        const start = self.next_pgno;
        self.next_pgno += count;
        return start;
    }

    pub fn buildDb(self: *ImageBuilder, entries: []const LeafWriteEntry, db_flags: u16) Error!DbImage {
        if (entries.len == 0) {
            return .{
                .db = .{
                    .md_pad = 0,
                    .md_flags = db_flags,
                    .md_depth = 0,
                    .md_branch_pages = 0,
                    .md_leaf_pages = 0,
                    .md_overflow_pages = 0,
                    .md_entries = 0,
                    .md_root = format.invalid_pgno,
                },
            };
        }

        const effective_entries = if (support.dbHasDupSortFlags(db_flags))
            try self.groupDupSortEntries(entries, db_flags)
        else
            entries;
        var leaf_page_count: usize = 0;
        var overflow_page_count: usize = 0;
        var children = if ((db_flags & format.DbFlags.dup_fixed) != 0 and !support.dbHasDupSortFlags(db_flags))
            try self.buildLeaf2Level(effective_entries, &leaf_page_count)
        else
            try self.buildLeafLevel(effective_entries, &leaf_page_count, &overflow_page_count);
        var depth: u16 = 1;
        var branch_page_count: usize = 0;

        while (children.len > 1) {
            children = try self.buildBranchLevel(children, &branch_page_count);
            depth += 1;
        }

        return .{
            .db = .{
                .md_pad = 0,
                .md_flags = db_flags,
                .md_depth = depth,
                .md_branch_pages = branch_page_count,
                .md_leaf_pages = leaf_page_count,
                .md_overflow_pages = overflow_page_count,
                .md_entries = entries.len,
                .md_root = children[0].pgno,
            },
        };
    }

    fn buildLeaf2Level(
        self: *ImageBuilder,
        entries: []const LeafWriteEntry,
        leaf_page_count: *usize,
    ) Error![]const ChildRef {
        if (entries.len == 0) return &.{};
        const key_size: u16 = @intCast(entries[0].key.len);
        for (entries) |entry| {
            if (entry.flags != 0 or entry.value.len != 0 or entry.key.len != key_size) return error.Incompatible;
        }

        const max_per_page = @max(@as(usize, 1), (self.page_size - format.page_header_size) / key_size);
        var children: std.ArrayListUnmanaged(ChildRef) = .empty;

        var start: usize = 0;
        while (start < entries.len) {
            const end = @min(entries.len, start + max_per_page);
            const pgno = self.allocPgno(1);
            const values = try self.allocator.alloc([]const u8, end - start);
            for (entries[start..end], 0..) |entry, i| values[i] = entry.key;
            try self.pages.append(self.allocator, .{
                .leaf2 = .{
                    .pgno = pgno,
                    .key_size = key_size,
                    .values = values,
                },
            });
            try children.append(self.allocator, .{
                .first_key = entries[start].key,
                .pgno = pgno,
            });
            leaf_page_count.* += 1;
            start = end;
        }

        return children.items;
    }

    fn buildLeafLevel(
        self: *ImageBuilder,
        entries: []const LeafWriteEntry,
        leaf_page_count: *usize,
        overflow_page_count: *usize,
    ) Error![]const ChildRef {
        var children: std.ArrayListUnmanaged(ChildRef) = .empty;

        var start: usize = 0;
        while (start < entries.len) {
            var end = start;
            var used: usize = format.page_header_size;
            while (end < entries.len) {
                const entry_size = try leafEntryStorageSize(entries[end], self.page_size);
                const next_used = used + @sizeOf(format.Indx) + entry_size;
                if (next_used > self.page_size and end > start) break;
                if (next_used > self.page_size) return error.MapFull;
                used = next_used;
                end += 1;
            }

            const pgno = self.allocPgno(1);
            var serialized_entries: std.ArrayListUnmanaged(SerializedLeafEntry) = .empty;
            for (entries[start..end]) |entry| {
                try serialized_entries.append(self.allocator, try self.serializeLeafEntry(entry, overflow_page_count));
            }
            try self.pages.append(self.allocator, .{
                .leaf = .{
                    .pgno = pgno,
                    .entries = serialized_entries.items,
                },
            });
            try children.append(self.allocator, .{
                .first_key = entries[start].key,
                .pgno = pgno,
            });
            leaf_page_count.* += 1;
            start = end;
        }

        return children.items;
    }

    fn buildBranchLevel(
        self: *ImageBuilder,
        children: []const ChildRef,
        branch_page_count: *usize,
    ) Error![]const ChildRef {
        var parents: std.ArrayListUnmanaged(ChildRef) = .empty;

        var start: usize = 0;
        while (start < children.len) {
            var end = start;
            var used: usize = format.page_header_size;
            while (end < children.len) {
                const key_len = if (end == start) 0 else children[end].first_key.len;
                const entry_size = node.header_size + key_len;
                const next_used = used + @sizeOf(format.Indx) + entry_size;
                if (next_used > self.page_size and end > start) break;
                if (next_used > self.page_size) return error.MapFull;
                used = next_used;
                end += 1;
            }

            const pgno = self.allocPgno(1);
            var branch_entries: std.ArrayListUnmanaged(BranchPageEntry) = .empty;
            for (children[start..end], start..) |child, i| {
                try branch_entries.append(self.allocator, .{
                    .key = if (i == start) "" else child.first_key,
                    .child_pgno = child.pgno,
                });
            }
            try self.pages.append(self.allocator, .{
                .branch = .{
                    .pgno = pgno,
                    .entries = branch_entries.items,
                },
            });
            try parents.append(self.allocator, .{
                .first_key = children[start].first_key,
                .pgno = pgno,
            });
            branch_page_count.* += 1;
            start = end;
        }

        return parents.items;
    }

    fn serializeLeafEntry(self: *ImageBuilder, entry: LeafWriteEntry, overflow_page_count: *usize) Error!SerializedLeafEntry {
        const data_size = entry.data_size orelse entry.value.len;
        if ((entry.flags & format.NodeFlags.dupdata) != 0) {
            return .{
                .key = entry.key,
                .value = entry.value,
                .flags = entry.flags,
                .data_size = data_size,
            };
        }
        if (needsOverflow(entry, self.page_size)) {
            const overflow_pgno = try self.appendOverflow(entry.value, overflow_page_count);
            const ref_bytes = try self.allocator.alloc(u8, @sizeOf(format.Pgno));
            format.writeNativeInt(format.Pgno, ref_bytes, overflow_pgno);
            return .{
                .key = entry.key,
                .value = ref_bytes,
                .flags = entry.flags | format.NodeFlags.bigdata,
                .data_size = data_size,
            };
        }

        return .{
            .key = entry.key,
            .value = entry.value,
            .flags = entry.flags,
            .data_size = data_size,
        };
    }

    fn appendOverflow(self: *ImageBuilder, data: []const u8, overflow_page_count: *usize) Error!format.Pgno {
        const bytes_per_overflow = self.page_size - format.page_header_size;
        const page_count = std.math.divCeil(usize, data.len, bytes_per_overflow) catch return error.MapFull;
        const pgno = self.allocPgno(page_count);
        try self.pages.append(self.allocator, .{
            .overflow = .{
                .pgno = pgno,
                .page_count = @intCast(page_count),
                .data = data,
            },
        });
        overflow_page_count.* += page_count;
        return pgno;
    }

    fn groupDupSortEntries(self: *ImageBuilder, entries: []const LeafWriteEntry, db_flags: u16) Error![]const LeafWriteEntry {
        var grouped = std.ArrayListUnmanaged(LeafWriteEntry).empty;

        var start: usize = 0;
        while (start < entries.len) {
            var end = start + 1;
            while (end < entries.len and std.mem.eql(u8, entries[end].key, entries[start].key)) : (end += 1) {}

            if (end == start + 1) {
                try grouped.append(self.allocator, .{
                    .key = entries[start].key,
                    .value = entries[start].value,
                });
                start = end;
                continue;
            }

            const subpage_bytes = try self.serializeDupSortSubpage(entries[start..end], db_flags);
            const inline_entry = LeafWriteEntry{
                .key = entries[start].key,
                .value = subpage_bytes,
                .flags = format.NodeFlags.dupdata,
                .data_size = subpage_bytes.len,
            };
            if (leafEntryStorageSize(inline_entry, self.page_size)) |_| {
                try grouped.append(self.allocator, inline_entry);
            } else |_| {
                const dup_db_flags = support.dupSortSubdbFlags(db_flags);
                var dup_db_entries = std.ArrayListUnmanaged(LeafWriteEntry).empty;
                for (entries[start..end]) |entry| {
                    try dup_db_entries.append(self.allocator, .{
                        .key = entry.value,
                        .value = &.{},
                    });
                }
                const dup_db = (try self.buildDb(dup_db_entries.items, dup_db_flags)).db;
                const dup_db_bytes = try self.allocator.dupe(u8, std.mem.asBytes(&dup_db));
                try grouped.append(self.allocator, .{
                    .key = entries[start].key,
                    .value = dup_db_bytes,
                    .flags = format.NodeFlags.dupdata | format.NodeFlags.subdata,
                    .data_size = @sizeOf(format.Db),
                });
            }
            start = end;
        }

        return grouped.items;
    }

    fn serializeDupSortSubpage(self: *ImageBuilder, entries: []const LeafWriteEntry, db_flags: u16) Error![]u8 {
        if ((db_flags & format.DbFlags.dup_fixed) != 0) {
            if (entries[0].value.len == 0) return error.Incompatible;
            const value_size = entries[0].value.len;
            for (entries) |entry| {
                if (entry.flags != 0 or entry.value.len != value_size) return error.Incompatible;
            }
            const total = format.page_header_size + entries.len * value_size;
            const subpage = try self.allocator.alloc(u8, total);
            const values = try self.allocator.alloc([]const u8, entries.len);
            for (entries, 0..) |entry, i| values[i] = entry.value;
            try dupdata.writeLeaf2Subpage(subpage, 0, @intCast(value_size), values);
            return subpage;
        }

        var total: usize = format.page_header_size;
        for (entries) |entry| {
            if (entry.flags != 0) return error.Incompatible;
            total += @sizeOf(format.Indx) + node.header_size + entry.value.len;
        }

        const subpage = try self.allocator.alloc(u8, total);
        const values = try self.allocator.alloc([]const u8, entries.len);
        for (entries, 0..) |entry, i| values[i] = entry.value;
        try dupdata.writeSubpage(subpage, 0, values);
        return subpage;
    }
};

pub fn leafEntryStorageSize(entry: LeafWriteEntry, page_size: usize) Error!usize {
    if ((entry.flags & format.NodeFlags.dupdata) != 0) {
        const inline_size = node.header_size + entry.key.len + entry.value.len;
        const inline_total = format.page_header_size + @sizeOf(format.Indx) + inline_size;
        if (inline_total > page_size) return error.MapFull;
        return inline_size;
    }
    const inline_size = node.header_size + entry.key.len + entry.value.len;
    const inline_total = format.page_header_size + @sizeOf(format.Indx) + inline_size;
    if (inline_total <= page_size) return inline_size;
    if ((entry.flags & format.NodeFlags.subdata) != 0) return error.MapFull;

    const overflow_size = node.header_size + entry.key.len + @sizeOf(format.Pgno);
    const overflow_total = format.page_header_size + @sizeOf(format.Indx) + overflow_size;
    if (overflow_total > page_size) return error.MapFull;
    return overflow_size;
}

pub fn needsOverflow(entry: LeafWriteEntry, page_size: usize) bool {
    if ((entry.flags & format.NodeFlags.dupdata) != 0) return false;
    return leafEntryStorageSize(entry, page_size) catch unreachable ==
        node.header_size + entry.key.len + @sizeOf(format.Pgno);
}

pub fn countOverflowPagesInLeaf(txn: anytype, leaf_page: page.View) Error!format.Pgno {
    var total: format.Pgno = 0;
    for (0..leaf_page.nodeCount()) |i| {
        const leaf = try node.View.fromPage(leaf_page, i);
        if (!leaf.isBigData()) continue;
        const overflow_ref = leaf.inlineValue();
        if (overflow_ref.len < @sizeOf(format.Pgno)) return error.Corrupted;
        const overflow_pgno = format.readNativeInt(format.Pgno, overflow_ref[0..@sizeOf(format.Pgno)]);
        const overflow_page = try txn.pageView(overflow_pgno);
        total += overflow_page.overflowPageCount();
    }
    return total;
}

pub fn appendOverflowPagesForSerializedEntry(
    allocator: std.mem.Allocator,
    txn: anytype,
    entry: SerializedLeafEntry,
    out: *std.ArrayListUnmanaged(format.Pgno),
) Error!void {
    if ((entry.flags & format.NodeFlags.bigdata) == 0) return;
    if (txn.stagedOverflowData(entry.value) != null) return;
    if (entry.value.len < @sizeOf(format.Pgno)) return error.Corrupted;

    const overflow_pgno = format.readNativeInt(format.Pgno, entry.value[0..@sizeOf(format.Pgno)]);
    const overflow_page = try txn.pageView(overflow_pgno);
    for (0..overflow_page.overflowPageCount()) |offset| {
        try support.appendUniquePgno(allocator, out, overflow_pgno + offset);
    }
}

pub fn overflowPageCountFromRef(txn: anytype, value: []const u8) Error!format.Pgno {
    if (txn.stagedOverflowData(value)) |staged| {
        const page_size = try txn.pageSize();
        const bytes_per_overflow = page_size - format.page_header_size;
        return std.math.divCeil(format.Pgno, @as(format.Pgno, @intCast(staged.len)), @as(format.Pgno, @intCast(bytes_per_overflow))) catch return error.MapFull;
    }
    if (value.len < @sizeOf(format.Pgno)) return error.Corrupted;
    const overflow_pgno = format.readNativeInt(format.Pgno, value[0..@sizeOf(format.Pgno)]);
    const overflow_page = try txn.pageView(overflow_pgno);
    return overflow_page.overflowPageCount();
}

pub fn cloneLeaf2Values(allocator: std.mem.Allocator, leaf2_page: page.View) Error![]const []const u8 {
    var values = std.ArrayListUnmanaged([]const u8).empty;
    for (0..leaf2_page.nodeCount()) |i| {
        try values.append(allocator, try dupdata.leaf2ValueAt(leaf2_page, i));
    }
    return values.items;
}
