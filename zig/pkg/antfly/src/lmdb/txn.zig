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
const readers = @import("readers.zig");
const writer_lock = @import("writer_lock.zig");
const mutate_leaf = @import("mutate_leaf.zig");
const rebalance_branch = @import("rebalance_branch.zig");
const free_db = @import("free_db.zig");
const support = @import("txn_support.zig");
const read_support = @import("read_support.zig");
const write_state_mod = @import("write_state.zig");
const write_mutation_support = @import("write_mutation_support.zig");
const dupsort_write_support = @import("dupsort_write_support.zig");
const write_page_state_support = @import("write_page_state_support.zig");
const write_path_support = @import("write_path_support.zig");
const commit_support = @import("commit_support.zig");
const materialize_support = @import("materialize_support.zig");
const prepare_commit_support = @import("prepare_commit_support.zig");
const txn_test_support = @import("txn_test_support.zig");

pub const Dbi = support.Dbi;

pub const free_dbi: Dbi = .{ .core = format.free_dbi };
pub const main_dbi: Dbi = .{ .core = format.main_dbi };

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

pub const Options = support.Options;
pub const DbOptions = support.DbOptions;
pub const PutOptions = support.PutOptions;
pub const ReserveOptions = support.ReserveOptions;

const SearchResult = support.SearchResult;
const RawEntry = write_state_mod.RawEntry;
const Entry = write_state_mod.Entry;
const DirtyPage = write_state_mod.DirtyPage;
const StagedOverflow = write_state_mod.StagedOverflow;
const PathStep = support.PathStep;
const LeafPath = support.LeafPath;
const dbHasDupSort = support.dbHasDupSort;
const dbHasDupSortFlags = support.dbHasDupSortFlags;
const dbFlagsFromOptions = support.dbFlagsFromOptions;
const dbFlagsSupportInlineDupsort = support.dbFlagsSupportInlineDupsort;
const dupSortSubdbFlags = support.dupSortSubdbFlags;
const emptyDb = support.emptyDb;
const findEntryIndex = support.findEntryIndex;
const findInsertIndex = support.findInsertIndex;
const findKeyRange = support.findKeyRange;
const findDupInsertIndex = support.findDupInsertIndex;
const findDupValueInsertIndex = support.findDupValueInsertIndex;
const findDupEntryIndex = support.findDupEntryIndex;
const validateDupsortValue = support.validateDupsortValue;
const appendUniquePgno = support.appendUniquePgno;
const compareEntryPair = support.compareEntryPair;
const compareKeys = support.compareKeys;
const searchPage = support.searchPage;
const findBranchChildIndex = support.findBranchChildIndex;
const writeOverflowPages = support.writeOverflowPages;
const writeLeaf2Page = support.writeLeaf2Page;
const writeMetaPage = support.writeMetaPage;
const findLeafNode = read_support.findLeafNode;
const readLeafValue = read_support.readLeafValue;
const readDupsortValue = read_support.readDupsortValue;
const readDupsortSubdbValueAt = read_support.readDupsortSubdbValueAt;
const collectDbEntries = write_state_mod.collectDbEntries;
const loadUserEntries = write_state_mod.loadUserEntries;
const collectDbPageNumbers = write_state_mod.collectDbPageNumbers;
const loadFreeRecords = write_state_mod.loadFreeRecords;
const DirtyDbState = write_state_mod.DirtyDbState;
const FreeRecord = write_state_mod.FreeRecord;
const PageImage = commit_support.PageImage;
const CommitPublishPhase = commit_support.CommitPublishPhase;
const PreparedCommit = commit_support.PreparedCommit;

const PutPageResult = write_page_state_support.PutPageResult;
const DeletePageResult = write_page_state_support.DeletePageResult;

const WriteState = struct {
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

    fn init(txn: *const Transaction) Error!WriteState {
        var state = WriteState{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .next_temp_pgno = txn.snapshot.mm_last_pg + 1,
        };
        errdefer state.deinit();

        state.main_db.base_meta = txn.snapshot.mm_dbs[format.main_dbi];
        state.main_db.meta = txn.snapshot.mm_dbs[format.main_dbi];
        try write_state_mod.loadFreeRecords(txn, txn.snapshot.mm_dbs[format.free_dbi], state.arena.allocator(), &state.free_records);
        return state;
    }

    fn clone(self: *const WriteState) Error!WriteState {
        var cloned = WriteState{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .next_temp_pgno = self.next_temp_pgno,
        };
        errdefer cloned.deinit();

        cloned.main_db = try cloneDirtyDbStateImpl(&cloned, self.main_db);
        for (self.named_dbs.items) |named_db| {
            try cloned.named_dbs.append(cloned.arena.allocator(), try cloneDirtyDbStateImpl(&cloned, named_db));
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

    fn deinit(self: *WriteState) void {
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

    fn openNamedDb(self: *WriteState, txn: *const Transaction, name: []const u8, opts: DbOptions) Error!Dbi {
        if (findNamedDbIndex(self.named_dbs.items, name)) |index| {
            return .{ .write_named = index };
        }
        if (txn.resolveNamedDb(name)) |named_db| {
            try self.named_dbs.append(self.arena.allocator(), .{
                .name = try self.arena.allocator().dupe(u8, name),
                .base_meta = named_db,
                .meta = named_db,
            });
            return .{ .write_named = self.named_dbs.items.len - 1 };
        } else |err| switch (err) {
            error.NotFound => {},
            else => return err,
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
        self.main_db.meta.md_entries += 1;
        return .{ .write_named = self.named_dbs.items.len - 1 };
    }

    fn get(self: *WriteState, txn: *const Transaction, dbi: Dbi, key: []const u8) Error![]const u8 {
        const db_state = try self.dbStateForWrite(dbi);
        if (!db_state.entries_loaded and !db_state.rebuild_required) {
            return read_support.get(txn, dbi, key);
        }
        try self.ensureEntriesLoaded(txn, dbi, db_state);
        if (dbHasDupSort(db_state.meta)) {
            const range = findKeyRange(db_state.entries.items, db_state.meta.md_flags, key) orelse return error.NotFound;
            return db_state.entries.items[range.start].value;
        }
        const index = findEntryIndex(db_state.entries.items, db_state.meta.md_flags, key) orelse return error.NotFound;
        return db_state.entries.items[index].value;
    }

    fn dbStateForRead(self: *const WriteState, dbi: Dbi) Error!*const DirtyDbState {
        return switch (dbi) {
            .core => |slot| switch (slot) {
                format.main_dbi => &self.main_db,
                else => error.InvalidDbi,
            },
            .write_named => |index| blk: {
                if (index >= self.named_dbs.items.len) return error.InvalidDbi;
                break :blk &self.named_dbs.items[index];
            },
            .named => error.InvalidDbi,
        };
    }

    pub fn dbStateForWrite(self: *WriteState, dbi: Dbi) Error!*DirtyDbState {
        return switch (dbi) {
            .core => |slot| switch (slot) {
                format.main_dbi => &self.main_db,
                else => error.InvalidDbi,
            },
            .write_named => |index| blk: {
                if (index >= self.named_dbs.items.len) return error.InvalidDbi;
                break :blk &self.named_dbs.items[index];
            },
            .named => error.InvalidDbi,
        };
    }

    fn cloneDirtyDbState(self: *WriteState, src: DirtyDbState) Error!DirtyDbState {
        return cloneDirtyDbStateImpl(self, src);
    }

    pub fn ensureEntriesLoaded(self: *WriteState, txn: *const Transaction, dbi: Dbi, db_state: *DirtyDbState) Error!void {
        if (db_state.entries_loaded) return;

        switch (dbi) {
            .core => {
                var raw_entries: std.ArrayListUnmanaged(RawEntry) = .empty;
                defer raw_entries.deinit(self.arena.allocator());
                try collectDbEntries(txn, db_state.meta, self.arena.allocator(), &raw_entries);
                for (raw_entries.items) |raw_entry| {
                    if ((raw_entry.flags & format.NodeFlags.subdata) != 0 and (raw_entry.flags & format.NodeFlags.dupdata) == 0) {
                        if (raw_entry.value.len < @sizeOf(format.Db)) return error.Corrupted;
                        const named_db = format.readStruct(format.Db, raw_entry.value[0..@sizeOf(format.Db)]);
                        if (findNamedDbIndex(self.named_dbs.items, raw_entry.key) == null) {
                            try self.named_dbs.append(self.arena.allocator(), .{
                                .name = try self.arena.allocator().dupe(u8, raw_entry.key),
                                .base_meta = named_db,
                                .meta = named_db,
                            });
                        }
                        continue;
                    }
                    try db_state.entries.append(self.arena.allocator(), .{
                        .key = raw_entry.key,
                        .value = raw_entry.value,
                        .flags = raw_entry.flags,
                        .data_size = raw_entry.data_size,
                    });
                }
            },
            .write_named => {
                try loadUserEntries(txn, db_state.meta, self.arena.allocator(), &db_state.entries);
            },
            .named => return error.InvalidDbi,
        }

        db_state.entries_loaded = true;
    }

    pub fn tryFastAppendToRightmostLeaf(
        self: *WriteState,
        txn: *const Transaction,
        db_state: *DirtyDbState,
        key: []const u8,
        value: []const u8,
    ) Error!bool {
        const hint = db_state.append_hint orelse return false;
        if (format.compareDbKeys(db_state.meta.md_flags, key, hint.last_key) != .gt) return false;
        if (hint.next_key) |next_key| {
            if (format.compareDbKeys(db_state.meta.md_flags, key, next_key) != .lt) return false;
        }

        const page_bytes = try self.mutablePageBytes(txn, hint.leaf_pgno);
        const leaf_page = try page.View.init(page_bytes);
        if (leaf_page.kind() != .leaf or leaf_page.nodeCount() == 0) {
            db_state.append_hint = null;
            return false;
        }

        const last_leaf = try node.View.fromPage(leaf_page, leaf_page.nodeCount() - 1);
        if (!std.mem.eql(u8, last_leaf.key(), hint.last_key)) {
            db_state.append_hint = null;
            return false;
        }

        const entry = if (needsOverflow(.{ .key = key, .value = value }, leaf_page.bytes.len))
            try self.stageOverflowEntry(txn, key, value)
        else
            SerializedLeafEntry{
                .key = key,
                .value = value,
                .flags = 0,
                .data_size = value.len,
            };

        mutate_leaf.appendEntryInPlace(page_bytes, entry) catch |err| switch (err) {
            error.MapFull => return false,
            else => return err,
        };
        db_state.append_hint = .{
            .leaf_pgno = hint.leaf_pgno,
            .parents = hint.parents,
            .last_key = try self.arena.allocator().dupe(u8, key),
            .next_key = hint.next_key,
        };
        return true;
    }

    pub fn nextLeafLowerBound(self: *WriteState, txn: *const Transaction, parents: []const PathStep) Error!?[]const u8 {
        return write_path_support.nextLeafLowerBound(self, txn, parents);
    }

    fn put(self: *WriteState, txn: *const Transaction, dbi: Dbi, key: []const u8, value: []const u8, opts: PutOptions) Error!void {
        return write_mutation_support.put(self, txn, dbi, key, value, opts);
    }

    fn reserve(self: *WriteState, txn: *const Transaction, dbi: Dbi, key: []const u8, size: usize, opts: ReserveOptions) Error![]u8 {
        return write_mutation_support.reserve(self, txn, dbi, key, size, opts);
    }

    fn delete(self: *WriteState, txn: *const Transaction, dbi: Dbi, key: []const u8) Error!void {
        return write_mutation_support.delete(self, txn, dbi, key);
    }

    fn deleteValue(self: *WriteState, txn: *const Transaction, dbi: Dbi, key: []const u8, value: []const u8) Error!void {
        return write_mutation_support.deleteValue(self, txn, dbi, key, value);
    }

    fn commit(self: *WriteState, txn: *const Transaction) Error!void {
        var prepared = try self.prepareCommit(txn);
        defer prepared.deinit();
        try commit_support.publishPreparedCommit(@constCast(txn.env), &prepared, .fully_published);
    }

    fn prepareCommit(self: *WriteState, txn: *const Transaction) Error!PreparedCommit {
        return prepare_commit_support.prepare(self, txn);
    }

    pub fn stageMainNamedDbDefs(self: *WriteState, txn: *const Transaction, named_db_defs: []const format.Db) Error!bool {
        for (self.named_dbs.items, named_db_defs) |named_db, named_db_def| {
            if (!try self.tryPutMainSubdataPageLevel(txn, named_db.name.?, named_db_def)) return false;
        }
        return true;
    }

    pub fn tryPutDupSortPageLevel(self: *WriteState, txn: *const Transaction, dbi: Dbi, key: []const u8, value: []const u8) Error!bool {
        return dupsort_write_support.tryPutPageLevel(self, txn, dbi, key, value);
    }

    pub fn tryDeleteDupSortPageLevel(self: *WriteState, txn: *const Transaction, dbi: Dbi, key: []const u8) Error!bool {
        return dupsort_write_support.tryDeletePageLevel(self, txn, dbi, key);
    }

    pub fn tryDeleteDupSortValuePageLevel(
        self: *WriteState,
        txn: *const Transaction,
        dbi: Dbi,
        key: []const u8,
        value: []const u8,
    ) Error!bool {
        return dupsort_write_support.tryDeleteValuePageLevel(self, txn, dbi, key, value);
    }

    fn dirtyPageCount(self: *const WriteState) usize {
        return self.dirty_pages.count();
    }

    pub fn materializeDbFromPageState(
        self: *WriteState,
        builder: *ImageBuilder,
        txn: *const Transaction,
        db: format.Db,
        entry_count: usize,
        db_flags: u16,
    ) Error!MaterializedDbBuild {
        const cloned = try self.cloneDbPageSubtree(builder, txn, db.md_root);
        var meta = db;
        meta.md_flags = db_flags;
        meta.md_entries = entry_count;
        meta.md_root = cloned.pgno;
        meta.md_depth = cloned.depth;
        meta.md_leaf_pages = cloned.leaf_pages;
        meta.md_branch_pages = cloned.branch_pages;
        meta.md_overflow_pages = cloned.overflow_pages;
        return .{
            .db = meta,
            .retired_pages = cloned.retired_pages,
        };
    }

    fn cloneDbPageSubtree(
        self: *WriteState,
        builder: *ImageBuilder,
        txn: *const Transaction,
        pgno: format.Pgno,
    ) Error!struct {
        pgno: format.Pgno,
        changed: bool,
        retired_pages: []const format.Pgno,
        depth: u16,
        leaf_pages: format.Pgno,
        branch_pages: format.Pgno,
        overflow_pages: format.Pgno,
    } {
        if (pgno == format.invalid_pgno) {
            return .{
                .pgno = format.invalid_pgno,
                .changed = false,
                .retired_pages = &.{},
                .depth = 0,
                .leaf_pages = 0,
                .branch_pages = 0,
                .overflow_pages = 0,
            };
        }
        const current_page = try self.pageViewForMutation(txn, pgno);
        switch (current_page.kind()) {
            .leaf => {
                if (self.dirty_pages.get(pgno) == null) {
                    const existing_overflow_pages = try countOverflowPagesInLeaf(txn, current_page);
                    return .{
                        .pgno = pgno,
                        .changed = false,
                        .retired_pages = &.{},
                        .depth = 1,
                        .leaf_pages = 1,
                        .branch_pages = 0,
                        .overflow_pages = existing_overflow_pages,
                    };
                }
                const dirty_page = self.dirty_pages.get(pgno).?;
                const new_pgno = builder.allocPgno(1);
                var entries = try mutate_leaf.cloneEntries(self.arena.allocator(), txn, current_page);
                const materialized_overflow_pages = try self.materializeLeafOverflowRefs(builder, txn, &entries);
                const subdb_retired_pages = try self.materializeLeafDupsortSubdbRefs(builder, txn, &entries);
                try builder.pages.append(builder.allocator, .{
                    .leaf = .{
                        .pgno = new_pgno,
                        .entries = entries,
                    },
                });
                var retired_pages = std.ArrayListUnmanaged(format.Pgno).empty;
                try retired_pages.appendSlice(self.arena.allocator(), subdb_retired_pages);
                if (!dirty_page.staged) {
                    try retired_pages.append(self.arena.allocator(), pgno);
                }
                return .{
                    .pgno = new_pgno,
                    .changed = true,
                    .retired_pages = retired_pages.items,
                    .depth = 1,
                    .leaf_pages = 1,
                    .branch_pages = 0,
                    .overflow_pages = materialized_overflow_pages,
                };
            },
            .leaf2 => {
                const dirty_page = self.dirty_pages.get(pgno);
                if (dirty_page == null) {
                    return .{
                        .pgno = pgno,
                        .changed = false,
                        .retired_pages = &.{},
                        .depth = 1,
                        .leaf_pages = 1,
                        .branch_pages = 0,
                        .overflow_pages = 0,
                    };
                }
                const new_pgno = builder.allocPgno(1);
                const key_size = current_page.pad();
                const values = try cloneLeaf2Values(builder.allocator, current_page);
                try builder.pages.append(builder.allocator, .{
                    .leaf2 = .{
                        .pgno = new_pgno,
                        .key_size = key_size,
                        .values = values,
                    },
                });
                return .{
                    .pgno = new_pgno,
                    .changed = true,
                    .retired_pages = if (dirty_page.?.staged)
                        &.{}
                    else
                        try self.arena.allocator().dupe(format.Pgno, &[_]format.Pgno{pgno}),
                    .depth = 1,
                    .leaf_pages = 1,
                    .branch_pages = 0,
                    .overflow_pages = 0,
                };
            },
            .branch => {
                const dirty_page = self.dirty_pages.get(pgno);
                const page_dirty = dirty_page != null;
                var child_changes = false;
                var retired_pages = std.ArrayListUnmanaged(format.Pgno).empty;
                const entries = try rebalance_branch.cloneEntries(self.arena.allocator(), current_page);
                var depth: u16 = 0;
                var leaf_pages: format.Pgno = 0;
                var branch_pages: format.Pgno = 0;
                var overflow_pages: format.Pgno = 0;
                for (entries, 0..) |*entry, i| {
                    const child = try self.cloneDbPageSubtree(builder, txn, entry.child_pgno);
                    entry.child_pgno = child.pgno;
                    if (child.changed) child_changes = true;
                    try retired_pages.appendSlice(self.arena.allocator(), child.retired_pages);
                    if (child.depth > depth) depth = child.depth;
                    leaf_pages += child.leaf_pages;
                    branch_pages += child.branch_pages;
                    overflow_pages += child.overflow_pages;
                    _ = i;
                }
                if (!page_dirty and !child_changes) {
                    return .{
                        .pgno = pgno,
                        .changed = false,
                        .retired_pages = retired_pages.items,
                        .depth = depth + 1,
                        .leaf_pages = leaf_pages,
                        .branch_pages = branch_pages + 1,
                        .overflow_pages = overflow_pages,
                    };
                }

                const new_pgno = builder.allocPgno(1);
                try builder.pages.append(builder.allocator, .{
                    .branch = .{
                        .pgno = new_pgno,
                        .entries = entries,
                    },
                });
                if (dirty_page == null or !dirty_page.?.staged) {
                    try retired_pages.append(self.arena.allocator(), pgno);
                }
                return .{
                    .pgno = new_pgno,
                    .changed = true,
                    .retired_pages = retired_pages.items,
                    .depth = depth + 1,
                    .leaf_pages = leaf_pages,
                    .branch_pages = branch_pages + 1,
                    .overflow_pages = overflow_pages,
                };
            },
            else => return error.Corrupted,
        }
    }

    fn materializeLeafDupsortSubdbRefs(
        self: *WriteState,
        builder: *ImageBuilder,
        txn: *const Transaction,
        entries: *[]SerializedLeafEntry,
    ) Error![]const format.Pgno {
        var retired_pages = std.ArrayListUnmanaged(format.Pgno).empty;
        for (entries.*, 0..) |*entry, i| {
            _ = i;
            if ((entry.flags & (format.NodeFlags.dupdata | format.NodeFlags.subdata)) != (format.NodeFlags.dupdata | format.NodeFlags.subdata)) continue;
            if (entry.value.len < @sizeOf(format.Db)) return error.Corrupted;
            var dup_db = format.readStruct(format.Db, entry.value[0..@sizeOf(format.Db)]);
            const cloned = try self.cloneDbPageSubtree(builder, txn, dup_db.md_root);
            dup_db.md_root = cloned.pgno;
            dup_db.md_depth = cloned.depth;
            dup_db.md_leaf_pages = cloned.leaf_pages;
            dup_db.md_branch_pages = cloned.branch_pages;
            dup_db.md_overflow_pages = cloned.overflow_pages;
            entry.value = try self.arena.allocator().dupe(u8, std.mem.asBytes(&dup_db));
            try retired_pages.appendSlice(self.arena.allocator(), cloned.retired_pages);
        }
        return retired_pages.items;
    }

    pub fn tryPutPageLevel(self: *WriteState, txn: *const Transaction, dbi: Dbi, key: []const u8, value: []const u8) Error!PutPageResult {
        return write_page_state_support.tryPutPageLevel(self, txn, dbi, key, value);
    }

    fn tryPutMainSubdataPageLevel(
        self: *WriteState,
        txn: *const Transaction,
        key: []const u8,
        named_db_def: format.Db,
    ) Error!bool {
        const db_state = &self.main_db;
        const encoded = try self.arena.allocator().dupe(u8, std.mem.asBytes(&named_db_def));
        const entry = SerializedLeafEntry{
            .key = try self.arena.allocator().dupe(u8, key),
            .value = encoded,
            .flags = format.NodeFlags.subdata,
            .data_size = @sizeOf(format.Db),
        };

        if (db_state.meta.md_root == format.invalid_pgno) {
            try self.initializeLeafRoot(txn, db_state, &.{entry});
            return true;
        }

        const path = try self.findLeafPath(txn, db_state, key);
        const leaf_page = try self.pageViewForMutation(txn, path.leaf_pgno);
        var entries = try mutate_leaf.cloneEntries(self.arena.allocator(), txn, leaf_page);
        const old_first_key = if (entries.len > 0) entries[0].key else "";

        if (path.exact) {
            if ((entries[path.leaf_index].flags & format.NodeFlags.dupdata) != 0) return false;
            if ((entries[path.leaf_index].flags & format.NodeFlags.subdata) == 0) return false;
            if ((entries[path.leaf_index].flags & format.NodeFlags.bigdata) != 0) return false;
            entries[path.leaf_index] = entry;
        } else {
            var inserted = std.ArrayListUnmanaged(SerializedLeafEntry).empty;
            for (entries[0..path.leaf_index]) |existing| {
                try inserted.append(self.arena.allocator(), existing);
            }
            try inserted.append(self.arena.allocator(), entry);
            for (entries[path.leaf_index..]) |existing| {
                try inserted.append(self.arena.allocator(), existing);
            }
            entries = inserted.items;
        }

        const new_first_key = entries[0].key;
        const staged_leaf = try self.arena.allocator().alloc(u8, leaf_page.bytes.len);
        mutate_leaf.writePage(staged_leaf, path.leaf_pgno, entries) catch |err| {
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

    pub fn initializeLeafRoot(
        self: *WriteState,
        txn: *const Transaction,
        db_state: *DirtyDbState,
        entries: []const SerializedLeafEntry,
    ) Error!void {
        const root_pgno = try self.allocateTempPage(try txn.pageSize(), .leaf, txn.env.opts.no_mem_init);
        try self.writeLeafEntriesToPgno(txn, root_pgno, entries);
        db_state.meta.md_root = root_pgno;
        db_state.meta.md_depth = 1;
        db_state.meta.md_leaf_pages = 1;
        db_state.meta.md_branch_pages = 0;
        db_state.meta.md_overflow_pages = 0;
    }

    pub fn stageOverflowEntry(self: *WriteState, txn: *const Transaction, key: []const u8, value: []const u8) Error!SerializedLeafEntry {
        const page_size = try txn.pageSize();
        const bytes_per_overflow = page_size - format.page_header_size;
        const page_count = std.math.divCeil(usize, value.len, bytes_per_overflow) catch return error.MapFull;
        const pgno = self.next_temp_pgno;
        self.next_temp_pgno += @intCast(page_count);
        try self.staged_overflows.append(self.arena.allocator(), .{
            .pgno = pgno,
            .page_count = @intCast(page_count),
            .data = try self.arena.allocator().dupe(u8, value),
        });

        const ref_bytes = try self.arena.allocator().alloc(u8, @sizeOf(format.Pgno));
        format.writeNativeInt(format.Pgno, ref_bytes, pgno);
        return .{
            .key = try self.arena.allocator().dupe(u8, key),
            .value = ref_bytes,
            .flags = format.NodeFlags.bigdata,
            .data_size = value.len,
        };
    }

    fn materializeLeafOverflowRefs(
        self: *WriteState,
        builder: *ImageBuilder,
        txn: *const Transaction,
        entries: *[]SerializedLeafEntry,
    ) Error!format.Pgno {
        var overflow_pages: format.Pgno = 0;
        for (entries.*, 0..) |*entry, i| {
            _ = i;
            if ((entry.flags & format.NodeFlags.bigdata) == 0) continue;
            const maybe_staged = self.findStagedOverflowRef(entry.value) orelse {
                overflow_pages += try overflowPageCountFromRef(txn, entry.value);
                continue;
            };
            const new_pgno = builder.allocPgno(maybe_staged.page_count);
            try builder.pages.append(builder.allocator, .{
                .overflow = .{
                    .pgno = new_pgno,
                    .page_count = @intCast(maybe_staged.page_count),
                    .data = maybe_staged.data,
                },
            });
            const ref_bytes = try self.arena.allocator().alloc(u8, @sizeOf(format.Pgno));
            format.writeNativeInt(format.Pgno, ref_bytes, new_pgno);
            entry.value = ref_bytes;
            overflow_pages += maybe_staged.page_count;
        }
        return overflow_pages;
    }

    pub fn findStagedOverflowRef(self: *const WriteState, value: []const u8) ?StagedOverflow {
        if (value.len < @sizeOf(format.Pgno)) return null;
        const pgno = format.readNativeInt(format.Pgno, value[0..@sizeOf(format.Pgno)]);
        for (self.staged_overflows.items) |overflow| {
            if (overflow.pgno == pgno) return overflow;
        }
        return null;
    }

    pub fn tryDeletePageLevel(self: *WriteState, txn: *const Transaction, dbi: Dbi, key: []const u8) Error!DeletePageResult {
        return write_page_state_support.tryDeletePageLevel(self, txn, dbi, key);
    }

    pub fn rebalanceLeafAfterDelete(self: *WriteState, txn: *const Transaction, db_state: *DirtyDbState, path: LeafPath, old_first_key: []const u8, entries: []SerializedLeafEntry) Error!bool {
        return write_page_state_support.rebalanceLeafAfterDelete(self, txn, db_state, path, old_first_key, entries);
    }

    pub fn splitLeafAfterInsert(self: *WriteState, txn: *const Transaction, db_state: *DirtyDbState, path: LeafPath, old_first_key: []const u8, entries: []SerializedLeafEntry) Error!bool {
        return write_page_state_support.splitLeafAfterInsert(self, txn, db_state, path, old_first_key, entries);
    }

    pub fn removeEmptyLeaf(self: *WriteState, txn: *const Transaction, db_state: *DirtyDbState, path: LeafPath, old_first_key: []const u8) Error!bool {
        return write_page_state_support.removeEmptyLeaf(self, txn, db_state, path, old_first_key);
    }

    pub fn findLeafPath(self: *WriteState, txn: *const Transaction, db_state: *DirtyDbState, key: []const u8) Error!LeafPath {
        return write_path_support.findLeafPath(self, txn, db_state, key);
    }

    pub fn canPropagateFirstKeyChange(self: *WriteState, parents: []const PathStep, txn: *const Transaction, old_first_key: []const u8, new_first_key: []const u8) Error!bool {
        return write_path_support.canPropagateFirstKeyChange(self, txn, parents, old_first_key, new_first_key);
    }

    pub fn applyFirstKeyChange(self: *WriteState, parents: []const PathStep, txn: *const Transaction, old_first_key: []const u8, new_first_key: []const u8) Error!void {
        return write_path_support.applyFirstKeyChange(self, txn, parents, old_first_key, new_first_key);
    }

    pub fn subtreeFirstKey(self: *WriteState, txn: *const Transaction, pgno: format.Pgno) Error![]const u8 {
        return write_path_support.subtreeFirstKey(self, txn, pgno);
    }

    pub fn pageViewForMutation(self: *WriteState, txn: *const Transaction, pgno: format.Pgno) Error!page.View {
        return write_path_support.pageViewForMutation(self, txn, pgno);
    }

    pub fn mutablePageBytes(self: *WriteState, txn: *const Transaction, pgno: format.Pgno) Error![]u8 {
        return write_path_support.mutablePageBytes(self, txn, pgno);
    }

    pub fn allocateTempPage(self: *WriteState, page_size: usize, kind: page.Kind, no_mem_init: bool) Error!format.Pgno {
        return write_path_support.allocateTempPage(self, null, page_size, kind, no_mem_init);
    }

    pub fn ensureTempPage(self: *WriteState, pgno: format.Pgno, page_size: usize, kind: page.Kind, no_mem_init: bool) Error!void {
        return write_path_support.ensureTempPage(self, page_size, pgno, kind, no_mem_init);
    }

    pub fn writeLeafEntriesToPgno(self: *WriteState, txn: *const Transaction, pgno: format.Pgno, entries: []const SerializedLeafEntry) Error!void {
        return write_path_support.writeLeafEntriesToPgno(self, txn, pgno, entries);
    }

    pub fn writeBranchEntriesToPgno(self: *WriteState, txn: *const Transaction, pgno: format.Pgno, entries: []const BranchPageEntry) Error!void {
        return write_path_support.writeBranchEntriesToPgno(self, txn, pgno, entries);
    }

    pub fn writeLeaf2ValuesToPgno(
        self: *WriteState,
        txn: *const Transaction,
        pgno: format.Pgno,
        key_size: u16,
        values: []const []const u8,
    ) Error!void {
        return write_path_support.writeLeaf2ValuesToPgno(self, txn, pgno, key_size, values);
    }

    pub fn mutableOrTempPageBytes(self: *WriteState, txn: *const Transaction, pgno: format.Pgno, kind: page.Kind) Error![]u8 {
        return write_path_support.mutableOrTempPageBytes(self, txn, pgno, kind);
    }
};

fn findNamedDbIndex(named_dbs: []const DirtyDbState, name: []const u8) ?usize {
    for (named_dbs, 0..) |named_db, i| {
        if (named_db.name != null and std.mem.eql(u8, named_db.name.?, name)) return i;
    }
    return null;
}

fn cloneDirtyDbStateImpl(self: *WriteState, src: DirtyDbState) Error!DirtyDbState {
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
            .parents = try self.arena.allocator().dupe(PathStep, hint.parents),
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

const LeafWriteEntry = materialize_support.LeafWriteEntry;
const SerializedLeafEntry = materialize_support.SerializedLeafEntry;
const BranchPageEntry = materialize_support.BranchPageEntry;
const DbImage = materialize_support.DbImage;
const ImageBuilder = materialize_support.ImageBuilder;
const KeySortContext = materialize_support.KeySortContext;
const DupSortSortContext = materialize_support.DupSortSortContext;
const leafWriteEntryLessThan = materialize_support.leafWriteEntryLessThan;
const leafWriteEntryDupSortLessThan = materialize_support.leafWriteEntryDupSortLessThan;
const leafEntryStorageSize = materialize_support.leafEntryStorageSize;
const needsOverflow = materialize_support.needsOverflow;
const countOverflowPagesInLeaf = materialize_support.countOverflowPagesInLeaf;
const appendOverflowPagesForSerializedEntry = materialize_support.appendOverflowPagesForSerializedEntry;
const overflowPageCountFromRef = materialize_support.overflowPageCountFromRef;
const cloneLeaf2Values = materialize_support.cloneLeaf2Values;

const FreeDbBuild = struct {
    builder: ImageBuilder,
    db: format.Db,
};

const MaterializedDbBuild = struct {
    db: format.Db,
    retired_pages: []const format.Pgno,
};

pub const Transaction = struct {
    const NamedDbCacheEntry = struct {
        name: []u8,
        db: format.Db,
    };

    env: *env_mod.Environment,
    snapshot: format.Meta,
    defer_page_mutation: bool = false,
    closed: bool = false,
    deferred_abort: bool = false,
    has_child: bool = false,
    parent: ?*Transaction = null,
    write_state: ?WriteState = null,
    named_db_cache: std.ArrayListUnmanaged(NamedDbCacheEntry) = .empty,
    read_slot: ?readers.SlotId = null,
    read_slot_ephemeral: bool = false,
    local_reader_active: bool = false,
    mapped_snapshot: ?env_mod.MappedBytes = null,
    write_lock: ?writer_lock.WriterLock = null,

    pub fn begin(env: *env_mod.Environment, opts: Options) Error!Transaction {
        var txn = Transaction{
            .env = env,
            .snapshot = env.activeMeta().meta,
            .defer_page_mutation = opts.defer_page_mutation or env.opts.defer_page_mutation,
        };
        if (opts.read_only) {
            env.localReaderEnter();
            txn.local_reader_active = true;
            txn.mapped_snapshot = env.mapped;
            errdefer {
                if (txn.local_reader_active) env.localReaderLeave();
            }
            if (!env.opts.no_lock) {
                if (env.reader_registry) |*registry| {
                    txn.read_slot = try registry.activate(txn.snapshot.mm_txnid);
                } else {
                    txn.read_slot = try readers.register(env.data_path, txn.snapshot.mm_txnid);
                    txn.read_slot_ephemeral = true;
                }
            }
        } else {
            if (!env.opts.no_lock) {
                txn.write_lock = try writer_lock.acquire(env.data_path);
                errdefer if (txn.write_lock) |*lock| lock.release();
            }
            txn.snapshot = env.activeMeta().meta;
            txn.write_state = try WriteState.init(&txn);
        }
        return txn;
    }

    pub fn beginChild(parent: *Transaction) Error!Transaction {
        try parent.ensureOpen();
        if (parent.has_child) return error.ChildTransactionActive;
        const parent_write_state = &(parent.write_state orelse return error.WriteTransactionsUnsupported);

        const child = Transaction{
            .env = parent.env,
            .snapshot = parent.snapshot,
            .parent = parent,
            .write_state = try parent_write_state.clone(),
        };
        parent.has_child = true;
        return child;
    }

    pub fn abort(self: *Transaction) void {
        if (self.has_child) {
            self.closed = true;
            self.deferred_abort = true;
            return;
        }
        self.finishAbort();
    }

    pub fn commit(self: *Transaction) Error!void {
        try self.ensureOpen();
        if (self.has_child) return error.ChildTransactionActive;

        if (self.parent) |parent| {
            if (parent.deferred_abort or parent.closed) {
                self.abort();
                return error.TransactionClosed;
            }

            const child_state = self.write_state.?;
            if (parent.write_state) |*parent_state| {
                parent_state.deinit();
            }
            parent.write_state = child_state;
            self.write_state = null;
            self.closed = true;
            self.notifyParentClosed();
            return;
        }

        if (self.write_state) |*write_state| {
            defer {
                write_state.deinit();
                self.write_state = null;
                if (self.write_lock) |*lock| {
                    lock.release();
                    self.write_lock = null;
                }
                self.closed = true;
            }
            try write_state.commit(self);
            return;
        }
        self.abort();
    }

    pub fn readOnly(self: *const Transaction) bool {
        return self.write_state == null;
    }

    pub fn rebindEnv(self: *Transaction, env: *env_mod.Environment) void {
        self.env = env;
    }

    pub fn pageSize(self: *const Transaction) Error!usize {
        try self.ensureUsable();
        if (self.write_state == null) return self.snapshot.mm_dbs[format.free_dbi].md_pad;
        return self.env.pageSize();
    }

    pub fn meta(self: *const Transaction) Error!format.Meta {
        try self.ensureUsable();
        return self.snapshot;
    }

    pub fn openDb(self: *Transaction, name: ?[]const u8, opts: DbOptions) Error!Dbi {
        try self.ensureUsable();

        if (name == null) {
            if ((opts.dup_fixed or opts.integer_dup or opts.reverse_dup) and !opts.dup_sort) return error.Incompatible;
            const requested_flags = dbFlagsFromOptions(opts);
            const main_db = if (self.write_state) |*write_state|
                write_state.main_db.meta
            else
                self.snapshot.mm_dbs[format.main_dbi];
            if (opts.create and requested_flags != main_db.md_flags) {
                if (main_db.md_entries > 0 or main_db.md_root != format.invalid_pgno) return error.Incompatible;
                if (self.write_state) |*write_state| {
                    write_state.main_db.meta.md_flags = requested_flags;
                    write_state.main_db.dirty = true;
                } else {
                    return error.Incompatible;
                }
            }
            return main_dbi;
        }
        if (self.write_state) |*write_state| {
            return write_state.openNamedDb(self, name.?, opts);
        }

        if (opts.create) return error.CreateUnsupported;
        if (self.lookupNamedDbCache(name.?)) |resolved_db| {
            return .{ .named = resolved_db };
        }
        const resolved_db = try self.resolveNamedDb(name.?);
        try self.cacheNamedDb(name.?, resolved_db);
        return .{ .named = resolved_db };
    }

    pub fn db(self: *const Transaction, dbi: Dbi) Error!format.Db {
        try self.ensureUsable();
        if (self.write_state) |*write_state| {
            return switch (dbi) {
                .core, .write_named => (try write_state.dbStateForRead(dbi)).meta,
                .named => |resolved_db| resolved_db,
            };
        }
        return switch (dbi) {
            .core => |slot| blk: {
                if (slot >= format.core_dbs) return error.InvalidDbi;
                break :blk self.snapshot.mm_dbs[slot];
            },
            .named => |resolved_db| resolved_db,
            .write_named => error.InvalidDbi,
        };
    }

    pub fn get(self: *const Transaction, dbi: Dbi, key: []const u8) Error![]const u8 {
        try self.ensureUsable();
        if (self.write_state) |*write_state_ptr| {
            const write_state = @constCast(write_state_ptr);
            return write_state.get(self, dbi, key);
        }
        return getReadOnly(self, dbi, key);
    }

    pub fn put(self: *Transaction, dbi: Dbi, key: []const u8, value: []const u8, opts: PutOptions) Error!void {
        try self.ensureUsable();
        const write_state = &(self.write_state orelse return error.WriteTransactionsUnsupported);
        return write_state.put(self, dbi, key, value, opts);
    }

    pub fn reserve(self: *Transaction, dbi: Dbi, key: []const u8, size: usize, opts: ReserveOptions) Error![]u8 {
        try self.ensureUsable();
        const write_state = &(self.write_state orelse return error.WriteTransactionsUnsupported);
        return write_state.reserve(self, dbi, key, size, opts);
    }

    pub fn delete(self: *Transaction, dbi: Dbi, key: []const u8) Error!void {
        try self.ensureUsable();
        const write_state = &(self.write_state orelse return error.WriteTransactionsUnsupported);
        return write_state.delete(self, dbi, key);
    }

    pub fn deleteValue(self: *Transaction, dbi: Dbi, key: []const u8, value: []const u8) Error!void {
        try self.ensureUsable();
        const write_state = &(self.write_state orelse return error.WriteTransactionsUnsupported);
        return write_state.deleteValue(self, dbi, key, value);
    }

    pub fn pageBytes(self: *const Transaction, pgno: format.Pgno) Error![]const u8 {
        try self.ensureUsable();
        if (self.write_state) |*write_state| {
            if (write_state.dirty_pages.get(pgno)) |dirty_page| return dirty_page.bytes;
        }
        if (self.mapped_snapshot) |mapped| {
            const page_size = self.snapshot.mm_dbs[format.free_dbi].md_pad;
            const offset = std.math.mul(usize, pgno, page_size) catch return error.PageOutOfBounds;
            const end = std.math.add(usize, offset, page_size) catch return error.PageOutOfBounds;
            if (end > mapped.len) return error.PageOutOfBounds;
            return mapped[offset..end];
        }
        return self.env.pageBytes(pgno);
    }

    pub fn pageView(self: *const Transaction, pgno: format.Pgno) Error!page.View {
        try self.ensureUsable();
        if (self.write_state) |*write_state| {
            if (write_state.dirty_pages.get(pgno)) |dirty_page| return page.View.init(dirty_page.bytes);
        }
        return page.View.init(try self.pageBytes(pgno));
    }

    pub fn data(self: *const Transaction) Error![]const u8 {
        try self.ensureUsable();
        if (self.mapped_snapshot) |mapped| return mapped;
        return self.env.data();
    }

    pub fn stagedOverflowData(self: *const Transaction, value: []const u8) ?[]const u8 {
        if (self.write_state) |*write_state| {
            const staged = write_state.findStagedOverflowRef(value) orelse return null;
            return staged.data;
        }
        return null;
    }

    fn finishAbort(self: *Transaction) void {
        if (self.read_slot) |slot| {
            if (self.read_slot_ephemeral) {
                readers.unregister(self.env.data_path, slot);
            } else if (self.env.reader_registry) |*registry| {
                registry.deactivate();
            }
            self.read_slot = null;
            self.read_slot_ephemeral = false;
        }
        if (self.local_reader_active) {
            self.env.localReaderLeave();
            self.local_reader_active = false;
            self.mapped_snapshot = null;
        }
        if (self.write_state) |*write_state| {
            write_state.deinit();
            self.write_state = null;
        }
        self.clearNamedDbCache();
        if (self.write_lock) |*lock| {
            lock.release();
            self.write_lock = null;
        }
        self.closed = true;
        self.deferred_abort = false;
        self.notifyParentClosed();
    }

    fn ensureOpen(self: *const Transaction) Error!void {
        if (self.closed) return error.TransactionClosed;
    }

    fn ensureUsable(self: *const Transaction) Error!void {
        try self.ensureOpen();
        if (self.has_child) return error.ChildTransactionActive;
    }

    fn notifyParentClosed(self: *Transaction) void {
        const parent = self.parent orelse return;
        self.parent = null;
        parent.has_child = false;
        if (parent.deferred_abort) {
            parent.finishAbort();
        }
    }

    fn resolveNamedDb(self: *const Transaction, name: []const u8) Error!format.Db {
        const main_db = self.snapshot.mm_dbs[format.main_dbi];
        if (main_db.md_root == format.invalid_pgno) return error.NotFound;

        var current_pgno = main_db.md_root;
        while (true) {
            const current_page = try self.pageView(current_pgno);
            switch (current_page.kind()) {
                .branch => {
                    const child_index = try findBranchChildIndex(current_page, self.snapshot.mm_dbs[format.main_dbi].md_flags, name);
                    const child = try node.View.fromPage(current_page, child_index);
                    current_pgno = child.branchPgno();
                },
                .leaf => {
                    const result = try searchPage(current_page, self.snapshot.mm_dbs[format.main_dbi].md_flags, name, false);
                    if (!result.exact) return error.NotFound;
                    const leaf = result.node.?;
                    if ((leaf.flags() & (format.NodeFlags.dupdata | format.NodeFlags.subdata)) != format.NodeFlags.subdata) {
                        return error.Incompatible;
                    }
                    const value = leaf.inlineValue();
                    if (value.len < @sizeOf(format.Db)) return error.Corrupted;
                    return format.readStruct(format.Db, value[0..@sizeOf(format.Db)]);
                },
                else => return error.Corrupted,
            }
        }
    }

    fn lookupNamedDbCache(self: *const Transaction, name: []const u8) ?format.Db {
        for (self.named_db_cache.items) |entry| {
            if (std.mem.eql(u8, entry.name, name)) return entry.db;
        }
        return null;
    }

    fn cacheNamedDb(self: *Transaction, name: []const u8, resolved_db: format.Db) Error!void {
        try self.named_db_cache.append(std.heap.page_allocator, .{
            .name = try std.heap.page_allocator.dupe(u8, name),
            .db = resolved_db,
        });
    }

    fn clearNamedDbCache(self: *Transaction) void {
        for (self.named_db_cache.items) |entry| {
            std.heap.page_allocator.free(entry.name);
        }
        self.named_db_cache.deinit(std.heap.page_allocator);
        self.named_db_cache = .empty;
    }
};

fn getReadOnly(txn: *const Transaction, dbi: Dbi, key: []const u8) Error![]const u8 {
    return read_support.get(txn, dbi, key);
}

fn beginTxnForTest(env: *env_mod.Environment, read_only: bool) Error!Transaction {
    return Transaction.begin(env, .{ .read_only = read_only });
}

pub fn publishCommitPhaseForTest(txn: *Transaction, phase: CommitPublishPhase) Error!void {
    const write_state = &(txn.write_state orelse return error.WriteTransactionsUnsupported);
    var prepared = try write_state.prepareCommit(txn);
    defer prepared.deinit();

    try commit_support.publishPreparedCommit(@constCast(txn.env), &prepared, phase);
}

fn countEntriesForTest(txn: *const Transaction, dbi: Dbi) Error!usize {
    return txn_test_support.countEntriesForTest(std.testing.allocator, txn, dbi, RawEntry, collectDbEntries);
}

fn initCrashTestFile(dir: anytype, sub_path: []const u8, page_size: usize, map_size: usize) !void {
    try txn_test_support.initCrashTestFile(dir, sub_path, page_size, map_size, emptyDb(0), writeMetaPage);
}

fn expectCrashPhaseReopenSnapshot(
    file_path: []const u8,
    opts: env_mod.EnvironmentOptions,
    phase: CommitPublishPhase,
    expect_beta: ?bool,
) anyerror!void {
    try txn_test_support.expectCrashPhaseReopenSnapshot(
        env_mod.Environment,
        file_path,
        opts,
        phase,
        expect_beta,
        beginTxnForTest,
        publishCommitPhaseForTest,
    );
}

test "read-only transaction snapshots active meta" {
    var fake_env = env_mod.Environment{
        .fd = 0,
        .mapped = &[_]u8{},
        .metas = .{
            .meta0 = null,
            .meta1 = null,
            .active = .{
                .header = undefined,
                .meta = .{
                    .mm_magic = format.mdb_magic,
                    .mm_version = format.mdb_data_version,
                    .mm_address = null,
                    .mm_mapsize = 123,
                    .mm_dbs = undefined,
                    .mm_last_pg = 0,
                    .mm_txnid = 9,
                },
            },
            .inactive = null,
        },
        .data_path = @constCast("/tmp/fake.mdb"),
        .opts = .{},
    };

    var txn = try Transaction.begin(&fake_env, .{});
    try std.testing.expectEqual(@as(format.Txnid, 9), (try txn.meta()).mm_txnid);
    txn.abort();
    try std.testing.expectError(error.TransactionClosed, txn.meta());
}

test "openDb resolves named DB records from the main DB" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 3]u8 = undefined;
    writeMetaPage(
        bytes[0..page_size],
        0,
        .{
            .md_pad = page_size,
            .md_flags = 0,
            .md_depth = 0,
            .md_branch_pages = 0,
            .md_leaf_pages = 0,
            .md_overflow_pages = 0,
            .md_entries = 0,
            .md_root = format.invalid_pgno,
        },
        .{
            .md_pad = 0,
            .md_flags = 0,
            .md_depth = 1,
            .md_branch_pages = 0,
            .md_leaf_pages = 1,
            .md_overflow_pages = 0,
            .md_entries = 1,
            .md_root = 2,
        },
        bytes.len,
        2,
        1,
    );
    writeMetaPage(
        bytes[page_size .. page_size * 2],
        1,
        .{
            .md_pad = page_size,
            .md_flags = 0,
            .md_depth = 0,
            .md_branch_pages = 0,
            .md_leaf_pages = 0,
            .md_overflow_pages = 0,
            .md_entries = 0,
            .md_root = format.invalid_pgno,
        },
        .{
            .md_pad = 0,
            .md_flags = 0,
            .md_depth = 1,
            .md_branch_pages = 0,
            .md_leaf_pages = 1,
            .md_overflow_pages = 0,
            .md_entries = 1,
            .md_root = 2,
        },
        bytes.len,
        2,
        2,
    );

    const named_db = format.Db{
        .md_pad = 0,
        .md_flags = 0,
        .md_depth = 1,
        .md_branch_pages = 0,
        .md_leaf_pages = 1,
        .md_overflow_pages = 0,
        .md_entries = 5,
        .md_root = 77,
    };
    try mutate_leaf.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        SerializedLeafEntry{
            .key = "docs",
            .value = std.mem.asBytes(&named_db),
            .flags = format.NodeFlags.subdata,
            .data_size = @sizeOf(format.Db),
        },
    });

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "named_db.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/named_db.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
    defer env.close();
    var txn = try Transaction.begin(&env, .{});
    defer txn.abort();

    const main = try txn.openDb(null, .{});
    try std.testing.expectEqual(@as(format.Pgno, 2), (try txn.db(main)).md_root);

    const resolved = try txn.openDb("docs", .{});
    try std.testing.expectEqual(@as(format.Pgno, 77), (try txn.db(resolved)).md_root);
    try std.testing.expectEqual(@as(usize, 5), (try txn.db(resolved)).md_entries);
}

test "write transaction commits unnamed and named databases" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = format.Db{
        .md_pad = 0,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 8, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 8, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "write_txn.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/write_txn.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();

        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        const docs = try txn.openDb("docs", .{ .create = true });
        try txn.put(main, "alpha", "1", .{});
        try txn.put(docs, "doc1", "content1", .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        const docs = try txn.openDb("docs", .{});
        try std.testing.expectEqualStrings("1", try txn.get(main, "alpha"));
        try std.testing.expectEqualStrings("content1", try txn.get(docs, "doc1"));
    }
}

test "write transaction reserve stores caller-filled value" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 8, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 8, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "reserve_txn.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/reserve_txn.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();

        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        const reserved = try txn.reserve(main, "blob", 5, .{});
        @memcpy(reserved, "hello");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("hello", try txn.get(main, "blob"));
    }
}

test "opening a named DB for creation does not force main-db rebuild" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 8, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 8, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "named_db_create_no_rebuild.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/named_db_create_no_rebuild.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
    defer env.close();
    var txn = try Transaction.begin(&env, .{ .read_only = false });
    defer txn.abort();

    _ = try txn.openDb("docs", .{ .create = true });

    const write_state = &txn.write_state.?;
    try std.testing.expect(!write_state.main_db.rebuild_required);
    try std.testing.expect(!write_state.named_dbs.items[0].rebuild_required);
}

test "named DB metadata updates persist without forcing main-db rebuild" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 64, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 64, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "named_db_structural_page_state.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/named_db_structural_page_state.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        const docs = try txn.openDb("docs", .{ .create = true });
        try txn.put(main, "alpha", "1", .{});
        try txn.put(docs, "seed", "0", .{});
        try txn.commit();
    }

    var large_value: [1400]u8 = undefined;
    @memset(&large_value, 'd');

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const docs = try txn.openDb("docs", .{});
        var key_buf: [16]u8 = undefined;
        for (0..4) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "doc-{d}", .{i});
            try txn.put(docs, key, &large_value, .{});
        }

        try std.testing.expect(!txn.write_state.?.main_db.rebuild_required);
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        const docs = try txn.openDb("docs", .{});
        try std.testing.expectEqualStrings("1", try txn.get(main, "alpha"));
        try std.testing.expectEqualStrings("0", try txn.get(docs, "seed"));

        var key_buf: [16]u8 = undefined;
        for (0..4) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "doc-{d}", .{i});
            try std.testing.expectEqualStrings(&large_value, try txn.get(docs, key));
        }
        try std.testing.expect((try txn.db(docs)).md_depth >= 2);
    }
}

test "raw subdata entries in named DBs round-trip through read and later page-state commits" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    var free_db_meta = emptyDb(format.DbFlags.integer_key);
    free_db_meta.md_pad = page_size;
    const main_db_meta = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, free_db_meta, main_db_meta, bytes.len, 1, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, free_db_meta, main_db_meta, bytes.len, 1, 2);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "raw_subdata.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/raw_subdata.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
    defer env.close();

    const raw_db = format.Db{
        .md_pad = 0,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const raw_bytes = std.mem.asBytes(&raw_db);

    {
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const docs = try txn.openDb("docs", .{ .create = true });
        _ = docs;
        const write_state = &(txn.write_state orelse unreachable);
        const named = &write_state.named_dbs.items[0];
        try named.entries.append(write_state.arena.allocator(), .{
            .key = try write_state.arena.allocator().dupe(u8, "opaque"),
            .value = try write_state.arena.allocator().dupe(u8, raw_bytes),
            .flags = format.NodeFlags.subdata,
            .data_size = @sizeOf(format.Db),
        });
        named.dirty = true;
        try txn.commit();
    }

    env.close();
    env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });

    {
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const docs = try txn.openDb("docs", .{});
        try std.testing.expectEqualSlices(u8, raw_bytes, try txn.get(docs, "opaque"));
    }

    {
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const docs = try txn.openDb("docs", .{});
        try txn.put(docs, "later", "value", .{});
        try txn.commit();
    }

    env.close();
    env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });

    {
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const docs = try txn.openDb("docs", .{});
        try std.testing.expectEqualSlices(u8, raw_bytes, try txn.get(docs, "opaque"));
        try std.testing.expectEqualStrings("value", try txn.get(docs, "later"));
    }
}

test "read-only get returns first value from dupsort leaf entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 3]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const main_db = format.Db{
        .md_pad = 0,
        .md_flags = format.DbFlags.dup_sort,
        .md_depth = 1,
        .md_branch_pages = 0,
        .md_leaf_pages = 1,
        .md_overflow_pages = 0,
        .md_entries = 1,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 2, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 2, 2);
    var dup_bytes = std.mem.zeroes([64]u8);
    try dupdata.writeSubpage(&dup_bytes, 0, &.{ "a", "b" });
    try mutate_leaf.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "dup", .value = &dup_bytes, .flags = format.NodeFlags.dupdata, .data_size = dup_bytes.len },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "dupdata_readonly.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/dupdata_readonly.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
    defer env.close();
    var txn = try Transaction.begin(&env, .{});
    defer txn.abort();

    const main = try txn.openDb(null, .{});
    try std.testing.expectEqualStrings("a", try txn.get(main, "dup"));
}

test "write transaction loads dupsort-backed main DB" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 3]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const main_db = format.Db{
        .md_pad = 0,
        .md_flags = format.DbFlags.dup_sort,
        .md_depth = 1,
        .md_branch_pages = 0,
        .md_leaf_pages = 1,
        .md_overflow_pages = 0,
        .md_entries = 2,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 2, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 2, 2);
    var dup_bytes = std.mem.zeroes([64]u8);
    try dupdata.writeSubpage(&dup_bytes, 0, &.{ "a", "b" });
    try mutate_leaf.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "dup", .value = &dup_bytes, .flags = format.NodeFlags.dupdata, .data_size = dup_bytes.len },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "dupdata_write.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/dupdata_write.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
    defer env.close();
    var txn = try Transaction.begin(&env, .{ .read_only = false });

    const main = try txn.openDb(null, .{ .dup_sort = true });
    try std.testing.expectEqualStrings("a", try txn.get(main, "dup"));
    try txn.put(main, "dup", "c", .{});
    try std.testing.expect(!txn.write_state.?.main_db.rebuild_required);
    try txn.commit();

    var reopened_env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
    defer reopened_env.close();
    var reopened_txn = try Transaction.begin(&reopened_env, .{});
    defer reopened_txn.abort();

    const reopened_main = try reopened_txn.openDb(null, .{});
    try std.testing.expectEqualStrings("a", try reopened_txn.get(reopened_main, "dup"));
    try std.testing.expectEqual(@as(usize, 3), try countEntriesForTest(&reopened_txn, reopened_main));
}

test "dupsort delete removes the whole key without rebuild" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    var free_db_meta = emptyDb(format.DbFlags.integer_key);
    free_db_meta.md_pad = page_size;
    const main_db_meta = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, free_db_meta, main_db_meta, bytes.len, 1, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, free_db_meta, main_db_meta, bytes.len, 1, 2);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "dupdata_delete.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/dupdata_delete.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true, .dup_sort = true });
        try txn.put(main, "dup", "a", .{});
        try txn.put(main, "dup", "b", .{});
        try txn.put(main, "tail", "z", .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .dup_sort = true });
        try txn.delete(main, "dup");
        try std.testing.expect(!txn.write_state.?.main_db.rebuild_required);
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectError(error.NotFound, txn.get(main, "dup"));
        try std.testing.expectEqualStrings("z", try txn.get(main, "tail"));
        try std.testing.expectEqual(@as(usize, 1), try countEntriesForTest(&txn, main));
    }
}

test "second write transaction is rejected while the first writer is active" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 16, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 16, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "writer_lock_txn.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/writer_lock_txn.mdb", .{tmp.sub_path});

    var env1 = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
    defer env1.close();
    var writer = try Transaction.begin(&env1, .{ .read_only = false });
    defer writer.abort();

    var env2 = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
    defer env2.close();
    try std.testing.expectError(error.WriterLocked, Transaction.begin(&env2, .{ .read_only = false }));
}

test "reader snapshots and single-writer lock coordinate across environments" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 128, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 128, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "reader_writer_coord.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/reader_writer_coord.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'a');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    const baseline_last_pg = blk: {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        break :blk env.activeMeta().meta.mm_last_pg;
    };

    var reader_env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
    defer reader_env.close();
    var reader_txn = try Transaction.begin(&reader_env, .{});

    var writer_env1 = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
    defer writer_env1.close();
    var writer1 = try Transaction.begin(&writer_env1, .{ .read_only = false });
    errdefer writer1.abort();

    var writer_env2 = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
    defer writer_env2.close();
    try std.testing.expectError(error.WriterLocked, Transaction.begin(&writer_env2, .{ .read_only = false }));

    {
        const main = try writer1.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try writer1.delete(main, key);
        }
        try writer1.commit();
    }

    {
        var writer2 = try Transaction.begin(&writer_env2, .{ .read_only = false });
        errdefer writer2.abort();
        const main = try writer2.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'b');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try writer2.put(main, key, &value_buf, .{});
        }
        try writer2.commit();
    }

    const with_reader_last_pg = blk: {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        break :blk env.activeMeta().meta.mm_last_pg;
    };

    reader_txn.abort();

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.delete(main, key);
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'c');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        const after_reader_last_pg = env.activeMeta().meta.mm_last_pg;
        try std.testing.expect(with_reader_last_pg > baseline_last_pg);
        try std.testing.expect(after_reader_last_pg <= with_reader_last_pg + 1);
    }
}

test "page-level put initializes an empty main DB without rebuild" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 16, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 16, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "empty_main_page_state.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/empty_main_page_state.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        try txn.put(main, "alpha", "1", .{});

        const write_state = &txn.write_state.?;
        try std.testing.expect(!write_state.main_db.rebuild_required);
        try std.testing.expect(write_state.main_db.meta.md_root != format.invalid_pgno);
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("1", try txn.get(main, "alpha"));
    }
}

test "page-level put initializes a newly created named DB without rebuild" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 16, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 16, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "empty_named_page_state.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/empty_named_page_state.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const docs = try txn.openDb("docs", .{ .create = true });
        try txn.put(docs, "doc1", "content1", .{});

        const write_state = &txn.write_state.?;
        try std.testing.expect(!write_state.named_dbs.items[0].rebuild_required);
        try std.testing.expect(write_state.named_dbs.items[0].meta.md_root != format.invalid_pgno);
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const docs = try txn.openDb("docs", .{});
        try std.testing.expectEqualStrings("content1", try txn.get(docs, "doc1"));
    }
}

test "named DB with empty values can be retired" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 16, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 16, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "named_db_empty_values.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/named_db_empty_values.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const docs = try txn.openDb("docs", .{ .create = true });
        try txn.put(docs, "a", "hdr", .{});
        try txn.put(docs, "b", &.{}, .{});
        try txn.put(docs, "c", &.{}, .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const docs = try txn.openDb("docs", .{});
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var retired_pages: std.ArrayListUnmanaged(format.Pgno) = .empty;
        defer retired_pages.deinit(arena.allocator());
        try collectDbPageNumbers(&txn, try txn.db(docs), false, arena.allocator(), &retired_pages);
        try std.testing.expect(retired_pages.items.len > 0);
    }
}

const FreeRecordSummary = struct {
    newest_txnid: format.Txnid,
    total_pages: usize,
};

fn freeRecordSummary(file_path: []const u8) !FreeRecordSummary {
    var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
    defer env.close();
    var txn = try Transaction.begin(&env, .{});
    defer txn.abort();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var records: std.ArrayListUnmanaged(FreeRecord) = .empty;
    try loadFreeRecords(&txn, (try txn.meta()).mm_dbs[format.free_dbi], arena.allocator(), &records);

    var newest_txnid: format.Txnid = 0;
    var total_pages: usize = 0;
    for (records.items) |record| {
        if (record.txnid > newest_txnid) newest_txnid = record.txnid;
        total_pages += record.pages.len;
    }
    return .{
        .newest_txnid = newest_txnid,
        .total_pages = total_pages,
    };
}

test "free pages are recorded across later commits" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = format.Db{
        .md_pad = 0,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 64, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 64, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "reuse_txn.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/reuse_txn.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'a');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.delete(main, key);
        }
        try txn.put(main, "small", "1", .{});
        try txn.commit();
    }

    const after_delete_summary = try freeRecordSummary(file_path);
    try std.testing.expect(after_delete_summary.total_pages > 0);

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "small");
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'b');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    const after_reinsert_summary = try freeRecordSummary(file_path);
    try std.testing.expect(after_reinsert_summary.total_pages > 0);
    try std.testing.expect(after_reinsert_summary.newest_txnid > after_delete_summary.newest_txnid);
}

test "active readers delay free-page reuse across environments" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 64, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 64, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "active_reader_reuse.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/active_reader_reuse.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'a');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    const baseline_last_pg = blk: {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        break :blk env.activeMeta().meta.mm_last_pg;
    };

    var reader_env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
    defer reader_env.close();
    var reader_txn = try Transaction.begin(&reader_env, .{});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.delete(main, key);
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'b');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    const with_reader_last_pg = blk: {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        break :blk env.activeMeta().meta.mm_last_pg;
    };

    reader_txn.abort();

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.delete(main, key);
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'c');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        const after_reader_last_pg = env.activeMeta().meta.mm_last_pg;
        const growth_with_reader = with_reader_last_pg - baseline_last_pg;
        const growth_after_reader = after_reader_last_pg - with_reader_last_pg;
        try std.testing.expect(with_reader_last_pg > baseline_last_pg);
        try std.testing.expect(growth_after_reader < growth_with_reader);
    }
}

test "read transaction keeps big borrowed values valid across remap" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 8, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 8, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "reader_remap_big_value.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/reader_remap_big_value.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
    defer env.close();

    {
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        const seed_value = try std.testing.allocator.alloc(u8, page_size * 3);
        defer std.testing.allocator.free(seed_value);
        @memset(seed_value, 's');
        try txn.put(main, "seed", seed_value, .{});
        try txn.commit();
    }

    var reader_txn = try Transaction.begin(&env, .{ .read_only = true });
    defer reader_txn.abort();
    const main = try reader_txn.openDb(null, .{});
    const borrowed = try reader_txn.get(main, "seed");
    const borrowed_copy = try std.testing.allocator.dupe(u8, borrowed);
    defer std.testing.allocator.free(borrowed_copy);
    const initial_map_len = reader_txn.mapped_snapshot.?.len;

    {
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const writer_main = try txn.openDb(null, .{});
        var key_buf: [24]u8 = undefined;
        const growth_value = try std.testing.allocator.alloc(u8, page_size * 3);
        defer std.testing.allocator.free(growth_value);
        @memset(growth_value, 'g');
        for (0..32) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "grow-{d:0>2}", .{i});
            try txn.put(writer_main, key, growth_value, .{});
        }
        try txn.commit();
    }

    try std.testing.expect(env.data().len > initial_map_len);
    try std.testing.expectEqualSlices(u8, borrowed_copy, borrowed);
}

test "oldest reader across multiple snapshots controls reclaim across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 128, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 128, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "multi_reader_reclaim.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/multi_reader_reclaim.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'a');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    var oldest_env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
    defer oldest_env.close();
    var oldest_reader = try Transaction.begin(&oldest_env, .{});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.delete(main, key);
        }
        try txn.commit();
    }

    var newer_env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
    defer newer_env.close();
    var newer_reader = try Transaction.begin(&newer_env, .{});

    const last_pg_with_two_readers = blk: {
        {
            var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
            defer env.close();
            var txn = try Transaction.begin(&env, .{ .read_only = false });
            errdefer txn.abort();

            const main = try txn.openDb(null, .{});
            var key_buf: [16]u8 = undefined;
            var value_buf: [96]u8 = undefined;
            @memset(&value_buf, 'b');
            for (0..256) |i| {
                const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
                try txn.put(main, key, &value_buf, .{});
            }
            try txn.commit();
        }

        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        break :blk env.activeMeta().meta.mm_last_pg;
    };

    newer_reader.abort();

    const last_pg_with_oldest_only = blk: {
        {
            var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
            defer env.close();
            var txn = try Transaction.begin(&env, .{ .read_only = false });
            errdefer txn.abort();

            const main = try txn.openDb(null, .{});
            var key_buf: [16]u8 = undefined;
            for (0..256) |i| {
                const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
                try txn.delete(main, key);
            }
            try txn.commit();
        }

        {
            var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
            defer env.close();
            var txn = try Transaction.begin(&env, .{ .read_only = false });
            errdefer txn.abort();

            const main = try txn.openDb(null, .{});
            var key_buf: [16]u8 = undefined;
            var value_buf: [96]u8 = undefined;
            @memset(&value_buf, 'c');
            for (0..256) |i| {
                const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
                try txn.put(main, key, &value_buf, .{});
            }
            try txn.commit();
        }

        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        break :blk env.activeMeta().meta.mm_last_pg;
    };

    oldest_reader.abort();

    const last_pg_after_oldest_released = blk: {
        {
            var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
            defer env.close();
            var txn = try Transaction.begin(&env, .{ .read_only = false });
            errdefer txn.abort();

            const main = try txn.openDb(null, .{});
            var key_buf: [16]u8 = undefined;
            for (0..256) |i| {
                const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
                try txn.delete(main, key);
            }
            try txn.commit();
        }

        {
            var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
            defer env.close();
            var txn = try Transaction.begin(&env, .{ .read_only = false });
            errdefer txn.abort();

            const main = try txn.openDb(null, .{});
            var key_buf: [16]u8 = undefined;
            var value_buf: [96]u8 = undefined;
            @memset(&value_buf, 'd');
            for (0..256) |i| {
                const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
                try txn.put(main, key, &value_buf, .{});
            }
            try txn.commit();
        }

        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        break :blk env.activeMeta().meta.mm_last_pg;
    };

    const growth_with_oldest_only = last_pg_with_oldest_only - last_pg_with_two_readers;
    const growth_after_oldest_released = last_pg_after_oldest_released - last_pg_with_oldest_only;
    try std.testing.expect(last_pg_with_oldest_only > last_pg_with_two_readers);
    try std.testing.expect(growth_after_oldest_released < growth_with_oldest_only);
}

test "reopen after data sync before meta publish preserves old committed snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 64, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 64, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "crash_before_meta.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/crash_before_meta.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'a');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.delete(main, key);
        }
        try publishCommitPhaseForTest(&txn, .after_data_sync_before_meta);
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqual(@as(usize, 256), try countEntriesForTest(&txn, main));
        try std.testing.expectEqualStrings(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            try txn.get(main, "k-0000"),
        );
    }
}

test "reopen before data sync preserves the old committed snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 64, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 64, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "crash_before_data_sync.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/crash_before_data_sync.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        try txn.put(main, "alpha", "one", .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.put(main, "beta", "two", .{});
        try publishCommitPhaseForTest(&txn, .before_data_sync);
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("one", try txn.get(main, "alpha"));
        try std.testing.expectError(error.NotFound, txn.get(main, "beta"));
    }
}

test "reopen after meta write before meta sync yields a complete snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 64, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 64, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "crash_after_meta_write.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/crash_after_meta_write.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'a');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.delete(main, key);
        }
        try publishCommitPhaseForTest(&txn, .after_meta_write_before_meta_sync);
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        const count = try countEntriesForTest(&txn, main);
        try std.testing.expect(count == 0 or count == 256);
        if (count == 0) {
            try std.testing.expectError(error.NotFound, txn.get(main, "k-0000"));
        } else {
            try std.testing.expectEqualStrings(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                try txn.get(main, "k-0000"),
            );
        }
    }
}

test "structural delete reopen after meta write publishes a complete merged snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 128, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 128, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "crash_structural_delete.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/crash_structural_delete.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'm');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        for (0..255) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.delete(main, key);
        }
        try publishCommitPhaseForTest(&txn, .after_meta_write_before_meta_sync);
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqual(@as(usize, 1), try countEntriesForTest(&txn, main));
        try std.testing.expectError(error.NotFound, txn.get(main, "k-0000"));
        try std.testing.expectEqualStrings(
            "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm",
            try txn.get(main, "k-0255"),
        );
    }
}

test "structural delete reopen before data sync preserves the pre-delete snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 128, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 128, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "crash_structural_before_data_sync.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/crash_structural_before_data_sync.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'p');
        for (0..128) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        var key_buf: [16]u8 = undefined;
        for (0..127) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.delete(main, key);
        }
        try publishCommitPhaseForTest(&txn, .before_data_sync);
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqual(@as(usize, 128), try countEntriesForTest(&txn, main));
        try std.testing.expectEqualStrings(
            "pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp",
            try txn.get(main, "k-0000"),
        );
        try std.testing.expectEqualStrings(
            "pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp",
            try txn.get(main, "k-0127"),
        );
    }
}

test "write_map reopen before data sync preserves the old committed snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try initCrashTestFile(tmp.dir, "write_map_before_data_sync.mdb", 4096, 4096 * 64);

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/write_map_before_data_sync.mdb", .{tmp.sub_path});

    try expectCrashPhaseReopenSnapshot(file_path, .{
        .write_map = true,
    }, .before_data_sync, false);
}

test "write_map reopen after data sync before meta publish preserves the old committed snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try initCrashTestFile(tmp.dir, "write_map_before_meta.mdb", 4096, 4096 * 64);

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/write_map_before_meta.mdb", .{tmp.sub_path});

    try expectCrashPhaseReopenSnapshot(file_path, .{
        .write_map = true,
    }, .after_data_sync_before_meta, false);
}

test "write_map map_async reopen after meta write before meta sync yields a complete snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try initCrashTestFile(tmp.dir, "write_map_map_async_after_meta.mdb", 4096, 4096 * 64);

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/write_map_map_async_after_meta.mdb", .{tmp.sub_path});

    try expectCrashPhaseReopenSnapshot(file_path, .{
        .write_map = true,
        .map_async = true,
    }, .after_meta_write_before_meta_sync, null);
}

test "fixed_map reopen after data sync before meta publish preserves the old committed snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try initCrashTestFile(tmp.dir, "fixed_map_before_meta.mdb", 4096, 4096 * 64);

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/fixed_map_before_meta.mdb", .{tmp.sub_path});

    expectCrashPhaseReopenSnapshot(file_path, .{
        .fixed_map = true,
    }, .after_data_sync_before_meta, false) catch |err| switch (err) {
        error.Incompatible => return,
        else => return err,
    };
}

test "nested child commit reopen after meta write publishes the merged snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try initCrashTestFile(tmp.dir, "nested_child_after_meta.mdb", 4096, 4096 * 64);

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/nested_child_after_meta.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();

        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        try txn.put(main, "alpha", "one", .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();

        var parent = try Transaction.begin(&env, .{ .read_only = false });
        defer parent.abort();

        const main = try parent.openDb(null, .{});
        var child = try parent.beginChild();
        try child.put(main, "beta", "two", .{});
        try child.commit();
        try parent.put(main, "gamma", "three", .{});
        try publishCommitPhaseForTest(&parent, .after_meta_write_before_meta_sync);
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();

        var txn = try Transaction.begin(&env, .{ .read_only = false });
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("one", try txn.get(main, "alpha"));
        try std.testing.expectEqualStrings("two", try txn.get(main, "beta"));
        try std.testing.expectEqualStrings("three", try txn.get(main, "gamma"));
    }
}

test "promoted dupsort reopen after meta write publishes the promoted duplicate set" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try initCrashTestFile(tmp.dir, "promoted_dups_after_meta.mdb", 4096, 4096 * 64);

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/promoted_dups_after_meta.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();

        var txn = try Transaction.begin(&env, .{ .read_only = false });
        defer txn.abort();

        const promoted = try txn.openDb("promoted", .{ .create = true, .dup_sort = true });
        var value: [96]u8 = undefined;
        for (0..40) |i| {
            @memset(&value, @as(u8, @intCast('A' + (i % 26))));
            try txn.put(promoted, "k", &value, .{});
        }
        try publishCommitPhaseForTest(&txn, .after_meta_write_before_meta_sync);
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();

        var txn = try Transaction.begin(&env, .{ .read_only = false });
        defer txn.abort();

        const promoted = try txn.openDb("promoted", .{ .dup_sort = true });
        try std.testing.expectEqual(@as(usize, 40), try countEntriesForTest(&txn, promoted));
        const first = try txn.get(promoted, "k");
        try std.testing.expectEqual(@as(usize, 96), first.len);
    }
}

test "free DB allocation reuses shared allocator pages" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const free_db_build = try free_db.buildDb(
        ImageBuilder,
        PageImage,
        LeafWriteEntry,
        arena.allocator(),
        4096,
        &[_]PageImage{},
        10,
        &[_]format.Pgno{ 5, 6, 7 },
        &[_]FreeRecord{},
        &[_]format.Pgno{},
        11,
    );

    try std.testing.expectEqual(@as(format.Pgno, 5), free_db_build.db.md_root);
    try std.testing.expectEqual(@as(format.Pgno, 10), free_db_build.builder.next_pgno);
    try std.testing.expectEqual(@as(usize, 2), free_db_build.builder.reusable_pages.len);
    try std.testing.expectEqual(@as(format.Pgno, 6), free_db_build.builder.reusable_pages[0]);
    try std.testing.expectEqual(@as(format.Pgno, 7), free_db_build.builder.reusable_pages[1]);
}

test "write transactions record touched tree pages" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 64, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 64, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "dirty_path.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/dirty_path.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'x');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}", .{i});
            try txn.put(main, key, &value_buf, .{});
        }
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.put(main, "k-0128", "updated", .{});

        const write_state = &txn.write_state.?;
        const main_db = try write_state.dbStateForRead(main);
        try std.testing.expect(main_db.touched_pages.items.len >= 2);
        try std.testing.expect(write_state.dirtyPageCount() >= 2);
    }
}

test "page-level delete updates parent branch separator key" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 5]u8 = undefined;
    const large_value = @as([1200]u8, @splat('p'));
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_depth = 2,
        .md_branch_pages = 1,
        .md_leaf_pages = 2,
        .md_overflow_pages = 0,
        .md_entries = 4,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len, 4, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len, 4, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try mutate_leaf.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "a", .value = &large_value, .data_size = large_value.len },
        .{ .key = "b", .value = &large_value, .data_size = large_value.len },
    });
    try mutate_leaf.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "m", .value = &large_value, .data_size = large_value.len },
        .{ .key = "n", .value = &large_value, .data_size = large_value.len },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "branch_propagation.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/branch_propagation.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
    defer env.close();
    var txn = try Transaction.begin(&env, .{ .read_only = false });
    defer txn.abort();

    const main = try txn.openDb(null, .{});
    try txn.delete(main, "m");

    const write_state = &txn.write_state.?;
    const dirty_branch = write_state.dirty_pages.get(2).?;
    const dirty_leaf = write_state.dirty_pages.get(4).?;

    const branch_view = try page.View.init(dirty_branch.bytes);
    const leaf_view = try page.View.init(dirty_leaf.bytes);
    const branch_key = (try node.View.fromPage(branch_view, 1)).key();
    const leaf_key = (try node.View.fromPage(leaf_view, 0)).key();

    try std.testing.expectEqualStrings("n", branch_key);
    try std.testing.expectEqualStrings("n", leaf_key);
}

test "commit serializes page-state branch updates" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 5]u8 = undefined;
    const large_value = @as([1200]u8, @splat('q'));
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_depth = 2,
        .md_branch_pages = 1,
        .md_leaf_pages = 2,
        .md_overflow_pages = 0,
        .md_entries = 4,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 4, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 4, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try mutate_leaf.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "a", .value = &large_value, .data_size = large_value.len },
        .{ .key = "b", .value = &large_value, .data_size = large_value.len },
    });
    try mutate_leaf.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "m", .value = &large_value, .data_size = large_value.len },
        .{ .key = "n", .value = &large_value, .data_size = large_value.len },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "commit_page_state.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/commit_page_state.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "m");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "n"));
        try std.testing.expectError(error.NotFound, txn.get(main, "m"));

        const root_page = try txn.pageView((try txn.db(main)).md_root);
        const branch_key = (try node.View.fromPage(root_page, 1)).key();
        try std.testing.expectEqualStrings("n", branch_key);
    }
}

test "page-level insert splits a leaf and persists the new root" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 3]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_entries = 2,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, page_size * 32, 2, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, page_size * 32, 2, 2);
    var large_value: [1500]u8 = undefined;
    @memset(&large_value, 'x');
    try mutate_leaf.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "a", .value = &large_value, .data_size = large_value.len },
        .{ .key = "m", .value = &large_value, .data_size = large_value.len },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "leaf_split.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/leaf_split.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.put(main, "z", &large_value, .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "a"));
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "m"));
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "z"));
        const main_meta = try txn.db(main);
        try std.testing.expectEqual(@as(u16, 2), main_meta.md_depth);
        try std.testing.expect(main_meta.md_leaf_pages >= 2);
        try std.testing.expect(main_meta.md_branch_pages >= 1);
    }
}

test "page-level delete removes an empty leaf from its parent" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 5]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_depth = 2,
        .md_branch_pages = 1,
        .md_leaf_pages = 2,
        .md_overflow_pages = 0,
        .md_entries = 2,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 4, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 4, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "z", .child_pgno = 4 },
    });
    try mutate_leaf.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "a", .value = "1", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "z", .value = "2", .data_size = 1 },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "leaf_merge.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/leaf_merge.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "z");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("1", try txn.get(main, "a"));
        try std.testing.expectError(error.NotFound, txn.get(main, "z"));
    }
}

test "page-level delete collapses a one-child root branch" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 5]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_depth = 2,
        .md_branch_pages = 1,
        .md_leaf_pages = 2,
        .md_overflow_pages = 0,
        .md_entries = 2,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 4, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 4, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "z", .child_pgno = 4 },
    });
    try mutate_leaf.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "a", .value = "1", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "z", .value = "2", .data_size = 1 },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "root_collapse.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/root_collapse.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "z");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("1", try txn.get(main, "a"));
        try std.testing.expectError(error.NotFound, txn.get(main, "z"));

        const main_meta = try txn.db(main);
        try std.testing.expectEqual(@as(u16, 1), main_meta.md_depth);

        const root_page = try txn.pageView(main_meta.md_root);
        try std.testing.expectEqual(page.Kind.leaf, root_page.kind());
    }
}

test "page-level delete updates ancestor separator after removing leftmost child" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 8]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_depth = 3,
        .md_branch_pages = 3,
        .md_leaf_pages = 3,
        .md_overflow_pages = 0,
        .md_entries = 3,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 7, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 7, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try rebalance_branch.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "", .child_pgno = 5 },
    });
    try rebalance_branch.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "", .child_pgno = 6 },
        .{ .key = "z", .child_pgno = 7 },
    });
    try mutate_leaf.writePage(bytes[page_size * 5 .. page_size * 6], 5, &.{
        .{ .key = "a", .value = "0", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 6 .. page_size * 7], 6, &.{
        .{ .key = "m", .value = "1", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 7 .. page_size * 8], 7, &.{
        .{ .key = "z", .value = "2", .data_size = 1 },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "ancestor_merge.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/ancestor_merge.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "m");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("0", try txn.get(main, "a"));
        try std.testing.expectEqualStrings("2", try txn.get(main, "z"));
        try std.testing.expectError(error.NotFound, txn.get(main, "m"));

        const main_meta = try txn.db(main);
        const root_page = try txn.pageView(main_meta.md_root);
        const second_child = try node.View.fromPage(root_page, 1);
        try std.testing.expectEqualStrings("z", second_child.key());
    }
}

test "page-level delete merges a non-root one-child branch into its sibling and collapses upward" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 9]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_depth = 3,
        .md_branch_pages = 3,
        .md_leaf_pages = 4,
        .md_overflow_pages = 0,
        .md_entries = 4,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 8, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 8, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try rebalance_branch.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "", .child_pgno = 5 },
        .{ .key = "d", .child_pgno = 6 },
    });
    try rebalance_branch.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "", .child_pgno = 7 },
        .{ .key = "z", .child_pgno = 8 },
    });
    try mutate_leaf.writePage(bytes[page_size * 5 .. page_size * 6], 5, &.{
        .{ .key = "a", .value = "0", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 6 .. page_size * 7], 6, &.{
        .{ .key = "d", .value = "1", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 7 .. page_size * 8], 7, &.{
        .{ .key = "m", .value = "2", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 8 .. page_size * 9], 8, &.{
        .{ .key = "z", .value = "3", .data_size = 1 },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "branch_merge_then_root_collapse.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/branch_merge_then_root_collapse.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "d");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("0", try txn.get(main, "a"));
        try std.testing.expectEqualStrings("2", try txn.get(main, "m"));
        try std.testing.expectEqualStrings("3", try txn.get(main, "z"));
        try std.testing.expectError(error.NotFound, txn.get(main, "d"));

        const main_meta = try txn.db(main);
        try std.testing.expectEqual(@as(u16, 2), main_meta.md_depth);

        const root_page = try txn.pageView(main_meta.md_root);
        try std.testing.expectEqual(page.Kind.branch, root_page.kind());
        try std.testing.expectEqual(@as(usize, 3), root_page.nodeCount());

        const middle_child = try node.View.fromPage(root_page, 1);
        try std.testing.expectEqualStrings("m", middle_child.key());
    }
}

test "page-level delete borrows from left branch sibling" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 10]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_depth = 3,
        .md_branch_pages = 3,
        .md_leaf_pages = 5,
        .md_overflow_pages = 0,
        .md_entries = 5,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 9, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 9, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try rebalance_branch.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "", .child_pgno = 5 },
        .{ .key = "c", .child_pgno = 6 },
        .{ .key = "g", .child_pgno = 7 },
    });
    try rebalance_branch.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "", .child_pgno = 8 },
        .{ .key = "z", .child_pgno = 9 },
    });
    try mutate_leaf.writePage(bytes[page_size * 5 .. page_size * 6], 5, &.{
        .{ .key = "a", .value = "0", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 6 .. page_size * 7], 6, &.{
        .{ .key = "c", .value = "1", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 7 .. page_size * 8], 7, &.{
        .{ .key = "g", .value = "2", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 8 .. page_size * 9], 8, &.{
        .{ .key = "m", .value = "3", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 9 .. page_size * 10], 9, &.{
        .{ .key = "z", .value = "4", .data_size = 1 },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "branch_borrow_left.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/branch_borrow_left.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "z");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("0", try txn.get(main, "a"));
        try std.testing.expectEqualStrings("1", try txn.get(main, "c"));
        try std.testing.expectEqualStrings("2", try txn.get(main, "g"));
        try std.testing.expectEqualStrings("3", try txn.get(main, "m"));
        try std.testing.expectError(error.NotFound, txn.get(main, "z"));

        const root_page = try txn.pageView((try txn.db(main)).md_root);
        const second_child = try node.View.fromPage(root_page, 1);
        try std.testing.expectEqualStrings("g", second_child.key());
    }
}

test "page-level delete borrows from right branch sibling" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 10]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_depth = 3,
        .md_branch_pages = 3,
        .md_leaf_pages = 5,
        .md_overflow_pages = 0,
        .md_entries = 5,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 9, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 9, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try rebalance_branch.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "", .child_pgno = 5 },
        .{ .key = "d", .child_pgno = 6 },
    });
    try rebalance_branch.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "", .child_pgno = 7 },
        .{ .key = "t", .child_pgno = 8 },
        .{ .key = "z", .child_pgno = 9 },
    });
    try mutate_leaf.writePage(bytes[page_size * 5 .. page_size * 6], 5, &.{
        .{ .key = "a", .value = "0", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 6 .. page_size * 7], 6, &.{
        .{ .key = "d", .value = "1", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 7 .. page_size * 8], 7, &.{
        .{ .key = "m", .value = "2", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 8 .. page_size * 9], 8, &.{
        .{ .key = "t", .value = "3", .data_size = 1 },
    });
    try mutate_leaf.writePage(bytes[page_size * 9 .. page_size * 10], 9, &.{
        .{ .key = "z", .value = "4", .data_size = 1 },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "branch_borrow_right.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/branch_borrow_right.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "a");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("1", try txn.get(main, "d"));
        try std.testing.expectEqualStrings("2", try txn.get(main, "m"));
        try std.testing.expectEqualStrings("3", try txn.get(main, "t"));
        try std.testing.expectEqualStrings("4", try txn.get(main, "z"));
        try std.testing.expectError(error.NotFound, txn.get(main, "a"));

        const root_page = try txn.pageView((try txn.db(main)).md_root);
        const second_child = try node.View.fromPage(root_page, 1);
        try std.testing.expectEqualStrings("t", second_child.key());
    }
}

test "page-level delete borrows from left leaf sibling" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 5]u8 = undefined;
    const large_value = @as([700]u8, @splat('x'));
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_depth = 2,
        .md_branch_pages = 1,
        .md_leaf_pages = 2,
        .md_overflow_pages = 0,
        .md_entries = 4,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 4, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 4, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try mutate_leaf.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "a", .value = &large_value, .data_size = large_value.len },
        .{ .key = "g", .value = &large_value, .data_size = large_value.len },
    });
    try mutate_leaf.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "m", .value = &large_value, .data_size = large_value.len },
        .{ .key = "z", .value = &large_value, .data_size = large_value.len },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "leaf_borrow_left.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/leaf_borrow_left.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "m");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "a"));
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "g"));
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "z"));
        try std.testing.expectError(error.NotFound, txn.get(main, "m"));

        const main_meta = try txn.db(main);
        const root_page = try txn.pageView(main_meta.md_root);
        const second_child = try node.View.fromPage(root_page, 1);
        try std.testing.expectEqualStrings("g", second_child.key());
    }
}

test "page-level delete borrows from right leaf sibling" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 5]u8 = undefined;
    const large_value = @as([700]u8, @splat('y'));
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_depth = 2,
        .md_branch_pages = 1,
        .md_leaf_pages = 2,
        .md_overflow_pages = 0,
        .md_entries = 4,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 4, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 4, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try mutate_leaf.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "a", .value = &large_value, .data_size = large_value.len },
        .{ .key = "d", .value = &large_value, .data_size = large_value.len },
    });
    try mutate_leaf.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "m", .value = &large_value, .data_size = large_value.len },
        .{ .key = "z", .value = &large_value, .data_size = large_value.len },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "leaf_borrow_right.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/leaf_borrow_right.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "a");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "d"));
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "m"));
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "z"));
        try std.testing.expectError(error.NotFound, txn.get(main, "a"));

        const main_meta = try txn.db(main);
        const root_page = try txn.pageView(main_meta.md_root);
        const second_child = try node.View.fromPage(root_page, 1);
        try std.testing.expectEqualStrings("z", second_child.key());
    }
}

test "page-level delete removes an overflow-valued entry" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = format.Db{
        .md_pad = 0,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, bytes.len * 8, 1, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, bytes.len * 8, 1, 2);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "overflow_delete.mdb", .data = &bytes });
    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/overflow_delete.mdb", .{tmp.sub_path});

    var large_value: [9000]u8 = undefined;
    @memset(&large_value, 'o');

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        try txn.put(main, "blob", &large_value, .{});
        try txn.put(main, "keep", "1", .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.delete(main, "blob");
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectError(error.NotFound, txn.get(main, "blob"));
        try std.testing.expectEqualStrings("1", try txn.get(main, "keep"));
        try std.testing.expectEqual(@as(format.Pgno, 0), (try txn.db(main)).md_overflow_pages);
    }
}

test "page-level put inserts an overflow-valued entry" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 3]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
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
        .md_entries = 1,
        .md_root = 2,
    };
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 2, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 2, 2);
    try mutate_leaf.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "a", .value = "1", .data_size = 1 },
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "overflow_insert.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/overflow_insert.mdb", .{tmp.sub_path});

    var large_value: [9000]u8 = undefined;
    @memset(&large_value, 'i');

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.put(main, "blob", &large_value, .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("1", try txn.get(main, "a"));
        try std.testing.expectEqualStrings(&large_value, try txn.get(main, "blob"));
        try std.testing.expect((try txn.db(main)).md_overflow_pages > 0);
    }
}

test "page-level put replaces with a new overflow-valued entry" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/overflow_replace.mdb", .{tmp.sub_path});

    const first_value = @as([9000]u8, @splat('r'));
    const second_value = @as([9500]u8, @splat('s'));

    {
        const page_size = 4096;
        var bytes: [page_size * 2]u8 = undefined;
        const empty_free_db = format.Db{
            .md_pad = page_size,
            .md_flags = format.DbFlags.integer_key,
            .md_depth = 0,
            .md_branch_pages = 0,
            .md_leaf_pages = 0,
            .md_overflow_pages = 0,
            .md_entries = 0,
            .md_root = format.invalid_pgno,
        };
        const empty_main_db = format.Db{
            .md_pad = 0,
            .md_flags = 0,
            .md_depth = 0,
            .md_branch_pages = 0,
            .md_leaf_pages = 0,
            .md_overflow_pages = 0,
            .md_entries = 0,
            .md_root = format.invalid_pgno,
        };
        writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, bytes.len * 8, 1, 1);
        writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, bytes.len * 8, 1, 2);
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "overflow_replace.mdb", .data = &bytes });
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        try txn.put(main, "blob", &first_value, .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
        defer env.close();
        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.put(main, "blob", &second_value, .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();
        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings(&second_value, try txn.get(main, "blob"));
        try std.testing.expect((try txn.db(main)).md_overflow_pages > 0);
    }
}

test "nested child commit merges into parent and persists only after parent commit" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 8, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 8, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "nested_commit.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/nested_commit.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
    defer env.close();

    var parent = try Transaction.begin(&env, .{ .read_only = false });
    errdefer parent.abort();

    const main = try parent.openDb(null, .{ .create = true });
    try parent.put(main, "alpha", "1", .{});

    var child = try Transaction.beginChild(&parent);
    errdefer child.abort();
    try child.put(main, "beta", "2", .{});

    try std.testing.expectError(error.ChildTransactionActive, parent.get(main, "alpha"));
    try child.commit();

    try std.testing.expectEqualStrings("1", try parent.get(main, "alpha"));
    try std.testing.expectEqualStrings("2", try parent.get(main, "beta"));

    {
        var precommit_env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer precommit_env.close();
        var precommit_txn = try Transaction.begin(&precommit_env, .{});
        defer precommit_txn.abort();

        const precommit_main = try precommit_txn.openDb(null, .{});
        try std.testing.expectError(error.NotFound, precommit_txn.get(precommit_main, "beta"));
    }

    try parent.commit();

    {
        var reopened_env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer reopened_env.close();
        var reopened_txn = try Transaction.begin(&reopened_env, .{});
        defer reopened_txn.abort();

        const reopened_main = try reopened_txn.openDb(null, .{});
        try std.testing.expectEqualStrings("1", try reopened_txn.get(reopened_main, "alpha"));
        try std.testing.expectEqualStrings("2", try reopened_txn.get(reopened_main, "beta"));
    }
}

test "nested child abort discards child changes and preserves parent state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 8, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 8, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "nested_abort.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/nested_abort.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true, .read_only = false });
    defer env.close();

    var parent = try Transaction.begin(&env, .{ .read_only = false });
    errdefer parent.abort();

    const main = try parent.openDb(null, .{ .create = true });
    try parent.put(main, "alpha", "1", .{});

    var child = try Transaction.beginChild(&parent);
    try child.put(main, "beta", "2", .{});
    child.abort();

    try std.testing.expectEqualStrings("1", try parent.get(main, "alpha"));
    try std.testing.expectError(error.NotFound, parent.get(main, "beta"));

    try parent.put(main, "gamma", "3", .{});
    try parent.commit();

    {
        var reopened_env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer reopened_env.close();
        var reopened_txn = try Transaction.begin(&reopened_env, .{});
        defer reopened_txn.abort();

        const reopened_main = try reopened_txn.openDb(null, .{});
        try std.testing.expectEqualStrings("1", try reopened_txn.get(reopened_main, "alpha"));
        try std.testing.expectEqualStrings("3", try reopened_txn.get(reopened_main, "gamma"));
        try std.testing.expectError(error.NotFound, reopened_txn.get(reopened_main, "beta"));
    }
}

test "async_io commit backend writes and reopens a basic transaction" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 2]u8 = undefined;
    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const empty_main_db = emptyDb(0);
    writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, page_size * 8, 1, 0);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, page_size * 8, 1, 1);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "async_io_basic.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/async_io_basic.mdb", .{tmp.sub_path});

    {
        var env = try env_mod.Environment.open(file_path, .{
            .no_subdir = true,
            .read_only = false,
            .commit_backend = .async_io,
        });
        defer env.close();

        var txn = try Transaction.begin(&env, .{ .read_only = false });
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        try txn.put(main, "alpha", "1", .{});
        try txn.put(main, "beta", "2", .{});
        try txn.commit();
    }

    {
        var env = try env_mod.Environment.open(file_path, .{ .no_subdir = true });
        defer env.close();

        var txn = try Transaction.begin(&env, .{});
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("1", try txn.get(main, "alpha"));
        try std.testing.expectEqualStrings("2", try txn.get(main, "beta"));
    }
}
