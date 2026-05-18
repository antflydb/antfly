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
const free_db = @import("free_db.zig");
const read_support = @import("read_support.zig");
const support = @import("txn_support.zig");

pub const Error = read_support.Error || std.mem.Allocator.Error;

pub const RawEntry = support.RawEntry;
pub const Entry = support.Entry;
pub const DirtyPage = support.DirtyPage;
pub const StagedOverflow = support.StagedOverflow;
pub const Dbi = support.Dbi;
pub const DbOptions = support.DbOptions;
pub const FreeRecord = free_db.FreeRecord;

const emptyDb = support.emptyDb;
const findEntryIndex = support.findEntryIndex;
const findKeyRange = support.findKeyRange;
const dbHasDupSort = support.dbHasDupSort;
const dbFlagsFromOptions = support.dbFlagsFromOptions;

pub const DirtyDbState = struct {
    pub const AppendHint = struct {
        leaf_pgno: format.Pgno,
        parents: []const support.PathStep = &.{},
        last_key: []const u8,
        next_key: ?[]const u8 = null,
    };

    name: ?[]u8 = null,
    base_meta: format.Db,
    meta: format.Db,
    created: bool = false,
    dirty: bool = false,
    rebuild_required: bool = false,
    entries_loaded: bool = false,
    append_hint: ?AppendHint = null,
    touched_pages: std.ArrayListUnmanaged(format.Pgno) = .empty,
    retired_pages: std.ArrayListUnmanaged(format.Pgno) = .empty,
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    pub fn deinit(self: *DirtyDbState, allocator: std.mem.Allocator) void {
        self.touched_pages.deinit(allocator);
        self.retired_pages.deinit(allocator);
        self.entries.deinit(allocator);
    }
};

pub const WriteState = struct {
    arena: std.heap.ArenaAllocator,
    main_db: DirtyDbState = .{
        .base_meta = emptyDb(0),
        .meta = emptyDb(0),
    },
    named_dbs: std.ArrayListUnmanaged(DirtyDbState) = .empty,
    free_records: std.ArrayListUnmanaged(FreeRecord) = .empty,
    dirty_pages: std.AutoHashMapUnmanaged(format.Pgno, DirtyPage) = .empty,
    staged_overflows: std.ArrayListUnmanaged(StagedOverflow) = .empty,
    next_temp_pgno: format.Pgno = format.num_metas,

    pub fn init(comptime TxnType: type, txn: *const TxnType) Error!WriteState {
        var state = WriteState{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .next_temp_pgno = txn.snapshot.mm_last_pg + 1,
        };
        errdefer state.deinit();
        try state.loadSnapshot(TxnType, txn);
        return state;
    }

    pub fn clone(self: *const WriteState) Error!WriteState {
        var cloned = WriteState{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .next_temp_pgno = self.next_temp_pgno,
        };
        errdefer cloned.deinit();

        cloned.main_db = try cloned.cloneDirtyDbState(self.main_db);

        for (self.named_dbs.items) |named_db| {
            try cloned.named_dbs.append(cloned.arena.allocator(), try cloned.cloneDirtyDbState(named_db));
        }

        for (self.free_records.items) |record| {
            try cloned.free_records.append(cloned.arena.allocator(), .{
                .txnid = record.txnid,
                .pages = try cloned.arena.allocator().dupe(format.Pgno, record.pages),
            });
        }

        var dirty_iter = self.dirty_pages.iterator();
        while (dirty_iter.next()) |entry| {
            try cloned.dirty_pages.put(cloned.arena.allocator(), entry.key_ptr.*, .{
                .pgno = entry.value_ptr.pgno,
                .kind = entry.value_ptr.kind,
                .staged = entry.value_ptr.staged,
                .bytes = try cloned.arena.allocator().dupe(u8, entry.value_ptr.bytes),
            });
        }

        for (self.staged_overflows.items) |overflow| {
            try cloned.staged_overflows.append(cloned.arena.allocator(), .{
                .pgno = overflow.pgno,
                .page_count = overflow.page_count,
                .data = try cloned.arena.allocator().dupe(u8, overflow.data),
            });
        }

        return cloned;
    }

    pub fn deinit(self: *WriteState) void {
        const allocator = self.arena.allocator();
        var dirty_pages = self.dirty_pages.iterator();
        while (dirty_pages.next()) |entry| {
            allocator.free(entry.value_ptr.bytes);
        }
        self.dirty_pages.deinit(allocator);
        self.staged_overflows.deinit(allocator);
        for (self.named_dbs.items) |*named_db| {
            named_db.deinit(allocator);
        }
        self.free_records.deinit(allocator);
        self.named_dbs.deinit(allocator);
        self.main_db.deinit(allocator);
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn openNamedDb(self: *WriteState, name: []const u8, opts: DbOptions) Error!Dbi {
        if (findNamedDbIndex(self.named_dbs.items, name)) |index| {
            return .{ .write_named = index };
        }
        if (!opts.create) return error.NotFound;
        if ((opts.dup_fixed or opts.integer_dup or opts.reverse_dup) and !opts.dup_sort) return error.Incompatible;

        const allocator = self.arena.allocator();
        try self.named_dbs.append(allocator, .{
            .name = try allocator.dupe(u8, name),
            .base_meta = emptyDb(0),
            .meta = emptyDb(dbFlagsFromOptions(opts)),
            .created = true,
            .dirty = true,
            .entries_loaded = true,
        });
        return .{ .write_named = self.named_dbs.items.len - 1 };
    }

    pub fn get(self: *const WriteState, dbi: Dbi, key: []const u8) Error![]const u8 {
        const db_state = try self.dbStateForRead(dbi);
        if (dbHasDupSort(db_state.meta)) {
            const range = findKeyRange(db_state.entries.items, db_state.meta.md_flags, key) orelse return error.NotFound;
            return db_state.entries.items[range.start].value;
        }
        const index = findEntryIndex(db_state.entries.items, db_state.meta.md_flags, key) orelse return error.NotFound;
        return db_state.entries.items[index].value;
    }

    pub fn dbStateForRead(self: *const WriteState, dbi: Dbi) Error!*const DirtyDbState {
        return switch (dbi) {
            .core => |slot| switch (slot) {
                format.free_dbi => error.InvalidDbi,
                format.main_dbi => &self.main_db,
                else => error.InvalidDbi,
            },
            .write_named => |index| {
                if (index >= self.named_dbs.items.len) return error.InvalidDbi;
                return &self.named_dbs.items[index];
            },
            .named => error.InvalidDbi,
        };
    }

    pub fn dbStateForWrite(self: *WriteState, dbi: Dbi) Error!*DirtyDbState {
        return switch (dbi) {
            .core => |slot| switch (slot) {
                format.free_dbi => error.InvalidDbi,
                format.main_dbi => &self.main_db,
                else => error.InvalidDbi,
            },
            .write_named => |index| {
                if (index >= self.named_dbs.items.len) return error.InvalidDbi;
                return &self.named_dbs.items[index];
            },
            .named => error.InvalidDbi,
        };
    }

    fn cloneDirtyDbState(self: *WriteState, src: DirtyDbState) Error!DirtyDbState {
        var dst = DirtyDbState{
            .name = if (src.name) |name| try self.arena.allocator().dupe(u8, name) else null,
            .base_meta = src.base_meta,
            .meta = src.meta,
            .created = src.created,
            .dirty = src.dirty,
            .rebuild_required = src.rebuild_required,
            .entries_loaded = src.entries_loaded,
            .append_hint = if (src.append_hint) |hint| .{
                .leaf_pgno = hint.leaf_pgno,
                .parents = try self.arena.allocator().dupe(support.PathStep, hint.parents),
                .last_key = try self.arena.allocator().dupe(u8, hint.last_key),
                .next_key = if (hint.next_key) |next_key| try self.arena.allocator().dupe(u8, next_key) else null,
            } else null,
        };
        errdefer dst.deinit(self.arena.allocator());

        try dst.touched_pages.appendSlice(self.arena.allocator(), src.touched_pages.items);
        try dst.retired_pages.appendSlice(self.arena.allocator(), src.retired_pages.items);
        for (src.entries.items) |entry| {
            try dst.entries.append(self.arena.allocator(), .{
                .key = try self.arena.allocator().dupe(u8, entry.key),
                .value = try self.arena.allocator().dupe(u8, entry.value),
                .flags = entry.flags,
                .data_size = entry.data_size,
            });
        }
        return dst;
    }

    fn loadSnapshot(self: *WriteState, comptime TxnType: type, txn: *const TxnType) Error!void {
        self.main_db.base_meta = txn.snapshot.mm_dbs[format.main_dbi];
        self.main_db.meta = txn.snapshot.mm_dbs[format.main_dbi];
        try loadFreeRecords(txn, txn.snapshot.mm_dbs[format.free_dbi], self.arena.allocator(), &self.free_records);
    }
};

fn findNamedDbIndex(named_dbs: []const DirtyDbState, name: []const u8) ?usize {
    for (named_dbs, 0..) |named_db, i| {
        if (named_db.name != null and std.mem.eql(u8, named_db.name.?, name)) return i;
    }
    return null;
}

pub fn collectDbEntries(txn: anytype, db: format.Db, allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(RawEntry)) Error!void {
    if (db.md_root == format.invalid_pgno) return;
    try collectPageEntries(txn, db.md_root, db.md_flags, allocator, out);
}

fn collectPageEntries(
    txn: anytype,
    pgno: format.Pgno,
    db_flags: u16,
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(RawEntry),
) Error!void {
    const current_page = try txn.pageView(pgno);
    switch (current_page.kind()) {
        .branch => {
            for (0..current_page.nodeCount()) |i| {
                const child = try node.View.fromPage(current_page, i);
                try collectPageEntries(txn, child.branchPgno(), db_flags, allocator, out);
            }
        },
        .leaf => {
            for (0..current_page.nodeCount()) |i| {
                const leaf = try node.View.fromPage(current_page, i);
                if (leaf.isDupdata()) {
                    if (!support.dbHasDupSortFlags(db_flags)) return error.UnsupportedNodeFlags;
                    if (leaf.isSubdata()) {
                        const value = leaf.inlineValue();
                        if (value.len < @sizeOf(format.Db)) return error.Corrupted;
                        const dup_db = format.readStruct(format.Db, value[0..@sizeOf(format.Db)]);
                        try collectDupsortDbEntries(txn, dup_db, allocator, leaf.key(), out);
                        continue;
                    }
                    var dup_values: std.ArrayListUnmanaged([]u8) = .empty;
                    defer dup_values.deinit(allocator);
                    try @import("dupdata.zig").appendClonedValues(allocator, leaf, &dup_values);
                    for (dup_values.items) |value| {
                        try out.append(allocator, .{
                            .key = try allocator.dupe(u8, leaf.key()),
                            .value = value,
                            .flags = 0,
                            .data_size = value.len,
                        });
                    }
                    continue;
                }
                try out.append(allocator, .{
                    .key = try allocator.dupe(u8, leaf.key()),
                    .value = try copyLeafValue(txn, leaf, allocator),
                    .flags = leaf.flags(),
                    .data_size = leaf.dataSize(),
                });
            }
        },
        else => return error.Corrupted,
    }
}

fn collectDupsortDbEntries(
    txn: anytype,
    db: format.Db,
    allocator: std.mem.Allocator,
    parent_key: []const u8,
    out: *std.ArrayListUnmanaged(RawEntry),
) Error!void {
    if (db.md_root == format.invalid_pgno) return;
    try collectDupsortPageEntries(txn, db.md_root, allocator, parent_key, out);
}

fn collectDupsortPageEntries(
    txn: anytype,
    pgno: format.Pgno,
    allocator: std.mem.Allocator,
    parent_key: []const u8,
    out: *std.ArrayListUnmanaged(RawEntry),
) Error!void {
    const current_page = try txn.pageView(pgno);
    switch (current_page.kind()) {
        .branch => {
            for (0..current_page.nodeCount()) |i| {
                const child = try node.View.fromPage(current_page, i);
                try collectDupsortPageEntries(txn, child.branchPgno(), allocator, parent_key, out);
            }
        },
        .leaf => {
            for (0..current_page.nodeCount()) |i| {
                const dup_leaf = try node.View.fromPage(current_page, i);
                if (dup_leaf.flags() != 0 or dup_leaf.dataSize() != 0 or dup_leaf.storedValueLen() != 0) return error.Corrupted;
                try out.append(allocator, .{
                    .key = try allocator.dupe(u8, parent_key),
                    .value = try allocator.dupe(u8, dup_leaf.key()),
                    .flags = 0,
                    .data_size = dup_leaf.key().len,
                });
            }
        },
        .leaf2 => {
            for (0..current_page.nodeCount()) |i| {
                const value = try @import("dupdata.zig").leaf2ValueAt(current_page, i);
                try out.append(allocator, .{
                    .key = try allocator.dupe(u8, parent_key),
                    .value = try allocator.dupe(u8, value),
                    .flags = 0,
                    .data_size = value.len,
                });
            }
        },
        else => return error.Corrupted,
    }
}

pub fn loadUserEntries(txn: anytype, db: format.Db, allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(Entry)) Error!void {
    var raw_entries: std.ArrayListUnmanaged(RawEntry) = .empty;
    defer raw_entries.deinit(allocator);

    try collectDbEntries(txn, db, allocator, &raw_entries);
    for (raw_entries.items) |raw_entry| {
        try out.append(allocator, .{
            .key = raw_entry.key,
            .value = raw_entry.value,
            .flags = raw_entry.flags,
            .data_size = raw_entry.data_size,
        });
    }
}

fn copyLeafValue(txn: anytype, leaf: node.View, allocator: std.mem.Allocator) Error![]u8 {
    if (!leaf.isBigData()) return allocator.dupe(u8, leaf.inlineValue());

    const value = try read_support.readLeafValue(txn, leaf);
    return allocator.dupe(u8, value);
}

pub fn collectDbPageNumbers(
    txn: anytype,
    db: format.Db,
    include_named_subdbs: bool,
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(format.Pgno),
) Error!void {
    return collectDbPageNumbersImpl(txn, db, include_named_subdbs, allocator, out, true);
}

pub fn collectDbPageNumbersSnapshot(
    txn: anytype,
    db: format.Db,
    include_named_subdbs: bool,
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(format.Pgno),
) Error!void {
    return collectDbPageNumbersImpl(txn, db, include_named_subdbs, allocator, out, false);
}

fn collectDbPageNumbersImpl(
    txn: anytype,
    db: format.Db,
    include_named_subdbs: bool,
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(format.Pgno),
    use_dirty_pages: bool,
) Error!void {
    if (db.md_root == format.invalid_pgno) return;
    try collectPageNumbersRecursive(txn, db.md_root, include_named_subdbs, allocator, out, use_dirty_pages);
}

fn collectPageNumbersRecursive(
    txn: anytype,
    pgno: format.Pgno,
    include_named_subdbs: bool,
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(format.Pgno),
    use_dirty_pages: bool,
) Error!void {
    const current_page = if (use_dirty_pages)
        try txn.pageView(pgno)
    else
        try txn.env.pageView(pgno);
    try out.append(allocator, pgno);

    switch (current_page.kind()) {
        .branch => {
            for (0..current_page.nodeCount()) |i| {
                const child = try node.View.fromPage(current_page, i);
                try collectPageNumbersRecursive(txn, child.branchPgno(), include_named_subdbs, allocator, out, use_dirty_pages);
            }
        },
        .leaf => {
            for (0..current_page.nodeCount()) |i| {
                const leaf = try node.View.fromPage(current_page, i);
                if (leaf.isBigData()) {
                    const overflow_pgno = format.readNativeInt(format.Pgno, leaf.inlineValue()[0..@sizeOf(format.Pgno)]);
                    const overflow_page = if (use_dirty_pages)
                        try txn.pageView(overflow_pgno)
                    else
                        try txn.env.pageView(overflow_pgno);
                    const overflow_count = overflow_page.overflowPageCount();
                    for (0..overflow_count) |offset| {
                        try out.append(allocator, overflow_pgno + offset);
                    }
                }
                if (include_named_subdbs and leaf.isSubdata() and !leaf.isDupdata() and !leaf.isBigData()) {
                    const value = leaf.inlineValue();
                    if (value.len < @sizeOf(format.Db)) return error.Corrupted;
                    const named_db = format.readStruct(format.Db, value[0..@sizeOf(format.Db)]);
                    try collectDbPageNumbersImpl(txn, named_db, false, allocator, out, use_dirty_pages);
                } else if (leaf.isDupdata() and leaf.isSubdata() and !leaf.isBigData()) {
                    const value = leaf.inlineValue();
                    if (value.len < @sizeOf(format.Db)) return error.Corrupted;
                    const dup_db = format.readStruct(format.Db, value[0..@sizeOf(format.Db)]);
                    try collectDbPageNumbersImpl(txn, dup_db, false, allocator, out, use_dirty_pages);
                }
            }
        },
        else => return error.Corrupted,
    }
}

pub fn loadFreeRecords(
    txn: anytype,
    db: format.Db,
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(FreeRecord),
) Error!void {
    if (db.md_root == format.invalid_pgno) return;
    try collectFreeRecordsRecursive(txn, db.md_root, allocator, out);
}

fn collectFreeRecordsRecursive(
    txn: anytype,
    pgno: format.Pgno,
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(FreeRecord),
) Error!void {
    const current_page = try txn.pageView(pgno);
    switch (current_page.kind()) {
        .branch => {
            for (0..current_page.nodeCount()) |i| {
                const child = try node.View.fromPage(current_page, i);
                try collectFreeRecordsRecursive(txn, child.branchPgno(), allocator, out);
            }
        },
        .leaf => {
            for (0..current_page.nodeCount()) |i| {
                const leaf = try node.View.fromPage(current_page, i);
                if (leaf.isSubdata() or leaf.isDupdata()) return error.UnsupportedNodeFlags;
                if (leaf.key().len != @sizeOf(format.Txnid)) return error.Corrupted;
                const txnid = format.readNativeInt(format.Txnid, leaf.key());
                const value = try read_support.readLeafValue(txn, leaf);
                try out.append(allocator, .{
                    .txnid = txnid,
                    .pages = try free_db.decodePgnoList(allocator, value),
                });
            }
        },
        else => return error.Corrupted,
    }
}
