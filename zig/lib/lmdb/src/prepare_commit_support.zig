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
const format = @import("format.zig");
const readers = @import("readers.zig");
const writer_lock = @import("writer_lock.zig");
const free_db = @import("free_db.zig");
const commit_support = @import("commit_support.zig");
const materialize_support = @import("materialize_support.zig");
const support = @import("txn_support.zig");
const write_state_mod = @import("write_state.zig");

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

const PreparedCommit = commit_support.PreparedCommit;
const PageImage = commit_support.PageImage;
const ImageBuilder = materialize_support.ImageBuilder;
const LeafWriteEntry = materialize_support.LeafWriteEntry;
const KeySortContext = materialize_support.KeySortContext;
const DupSortSortContext = materialize_support.DupSortSortContext;
const leafWriteEntryLessThan = materialize_support.leafWriteEntryLessThan;
const leafWriteEntryDupSortLessThan = materialize_support.leafWriteEntryDupSortLessThan;
const FreeRecord = write_state_mod.FreeRecord;
const collectDbPageNumbers = write_state_mod.collectDbPageNumbers;
const collectDbPageNumbersSnapshot = write_state_mod.collectDbPageNumbersSnapshot;

pub fn prepare(self: anytype, txn: anytype) Error!PreparedCommit {
    const min_page_size = format.page_header_size + format.meta_body_size;
    var page_size = txn.env.pageSize();
    if (page_size < min_page_size or !std.math.isPowerOfTwo(page_size)) {
        page_size = 4096;
    }

    var prepared = PreparedCommit{
        .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
    };
    errdefer prepared.deinit();
    const allocator = prepared.arena.allocator();
    const oldest_reader = if (txn.env.opts.no_lock)
        txn.snapshot.mm_txnid
    else
        (try readers.oldest(txn.env.data_path)) orelse txn.snapshot.mm_txnid;

    var reusable_pages: std.ArrayListUnmanaged(format.Pgno) = .empty;
    var retained_free_records: std.ArrayListUnmanaged(FreeRecord) = .empty;
    for (self.free_records.items) |record| {
        if (record.txnid <= oldest_reader) {
            for (record.pages) |pgno| {
                try reusable_pages.append(allocator, pgno);
            }
        } else {
            try retained_free_records.append(allocator, record);
        }
    }
    std.sort.insertion(format.Pgno, reusable_pages.items, {}, free_db.lessThanPgno);

    var builder = ImageBuilder{
        .allocator = allocator,
        .page_size = page_size,
        .next_pgno = @max(format.num_metas, txn.snapshot.mm_last_pg + 1),
        .reusable_pages = reusable_pages.items,
    };

    var retired_pages: std.ArrayListUnmanaged(format.Pgno) = .empty;
    try collectDbPageNumbersSnapshot(txn, txn.snapshot.mm_dbs[format.free_dbi], false, allocator, &retired_pages);

    const named_db_defs = try allocator.alloc(format.Db, self.named_dbs.items.len);
    var any_named_dirty = false;
    for (self.named_dbs.items, 0..) |named_db, i| {
        if (named_db.dirty) any_named_dirty = true;
        if (!named_db.dirty) {
            named_db_defs[i] = named_db.meta;
            continue;
        }

        if (!named_db.created and !named_db.rebuild_required) {
            const materialized = try self.materializeDbFromPageState(
                &builder,
                txn,
                named_db.meta,
                named_db.meta.md_entries,
                named_db.meta.md_flags,
            );
            named_db_defs[i] = materialized.db;
            try retired_pages.appendSlice(allocator, materialized.retired_pages);
            try retired_pages.appendSlice(allocator, named_db.retired_pages.items);
            continue;
        }

        if (!named_db.created) {
            try collectDbPageNumbersSnapshot(txn, named_db.base_meta, false, allocator, &retired_pages);
        }
        var leaf_entries: std.ArrayListUnmanaged(LeafWriteEntry) = .empty;
        for (named_db.entries.items) |entry| {
            try leaf_entries.append(allocator, .{
                .key = entry.key,
                .value = entry.value,
                .flags = entry.flags,
                .data_size = entry.data_size,
            });
        }
        if (support.dbHasDupSort(named_db.meta)) {
            std.sort.insertion(LeafWriteEntry, leaf_entries.items, DupSortSortContext{ .db_flags = named_db.meta.md_flags }, leafWriteEntryDupSortLessThan);
        } else {
            std.sort.insertion(LeafWriteEntry, leaf_entries.items, KeySortContext{ .db_flags = named_db.meta.md_flags }, leafWriteEntryLessThan);
        }
        named_db_defs[i] = (try builder.buildDb(leaf_entries.items, named_db.meta.md_flags)).db;
    }

    if (any_named_dirty) {
        self.main_db.dirty = true;
        if (!self.main_db.rebuild_required) {
            const staged = try self.stageMainNamedDbDefs(txn, named_db_defs);
            if (!staged) self.main_db.rebuild_required = true;
        }
    }

    const next_txnid = txn.snapshot.mm_txnid + 1;
    const main_db = blk: {
        if (self.main_db.dirty and !self.main_db.rebuild_required) {
            const materialized = try self.materializeDbFromPageState(
                &builder,
                txn,
                self.main_db.meta,
                self.main_db.meta.md_entries,
                self.main_db.meta.md_flags,
            );
            try retired_pages.appendSlice(allocator, materialized.retired_pages);
            try retired_pages.appendSlice(allocator, self.main_db.retired_pages.items);
            break :blk materialized.db;
        }

        if (!self.main_db.dirty) break :blk self.main_db.meta;

        if (self.main_db.base_meta.md_root != format.invalid_pgno) {
            try collectDbPageNumbersSnapshot(txn, self.main_db.base_meta, false, allocator, &retired_pages);
        }

        var main_page_entries: std.ArrayListUnmanaged(LeafWriteEntry) = .empty;
        for (self.main_db.entries.items) |entry| {
            try main_page_entries.append(allocator, .{
                .key = entry.key,
                .value = entry.value,
                .flags = entry.flags,
                .data_size = entry.data_size,
            });
        }
        for (self.named_dbs.items, 0..) |named_db, i| {
            const db_bytes = try allocator.dupe(u8, std.mem.asBytes(&named_db_defs[i]));
            try main_page_entries.append(allocator, .{
                .key = named_db.name.?,
                .value = db_bytes,
                .flags = format.NodeFlags.subdata,
                .data_size = @sizeOf(format.Db),
            });
        }
        if (support.dbHasDupSort(self.main_db.meta)) {
            std.sort.insertion(LeafWriteEntry, main_page_entries.items, DupSortSortContext{ .db_flags = self.main_db.meta.md_flags }, leafWriteEntryDupSortLessThan);
        } else {
            std.sort.insertion(LeafWriteEntry, main_page_entries.items, KeySortContext{ .db_flags = self.main_db.meta.md_flags }, leafWriteEntryLessThan);
        }
        break :blk (try builder.buildDb(main_page_entries.items, self.main_db.meta.md_flags)).db;
    };

    retired_pages.items = retired_pages.items[0..free_db.sortAndUniquePgnoList(retired_pages.items)];
    const free_db_build = try free_db.buildDb(
        ImageBuilder,
        PageImage,
        LeafWriteEntry,
        allocator,
        page_size,
        builder.pages.items,
        builder.next_pgno,
        builder.reusable_pages,
        retained_free_records.items,
        retired_pages.items,
        next_txnid,
    );
    builder = free_db_build.builder;
    const free_db_meta_raw = free_db_build.db;

    var last_pg = builder.next_pgno - 1;
    for (builder.pages.items) |image| {
        last_pg = @max(last_pg, pageImageLastPgno(image));
    }
    const file_last_pg = @max(txn.snapshot.mm_last_pg, last_pg);
    const total_size = (file_last_pg + 1) * page_size;
    const mapsize = @max(txn.snapshot.mm_mapsize, total_size);

    var free_db_meta = free_db_meta_raw;
    free_db_meta.md_pad = @intCast(page_size);
    free_db_meta.md_flags = format.DbFlags.integer_key;

    const meta_pgno: format.Pgno = next_txnid & 1;
    const meta_page = try allocator.alloc(u8, page_size);
    support.writeMetaPageOptions(
        meta_page,
        meta_pgno,
        free_db_meta,
        main_db,
        mapsize,
        file_last_pg,
        next_txnid,
        if (txn.env.opts.fixed_map) @ptrCast(txn.env.mapped.ptr) else null,
        !txn.env.opts.no_mem_init,
    );

    prepared.page_size = page_size;
    prepared.total_size = total_size;
    prepared.page_images = builder.pages.items;
    prepared.serialized_pages = try commit_support.serializePageImages(allocator, page_size, txn.env.opts, builder.pages.items);
    prepared.serialized_spans = try commit_support.coalesceSerializedPages(allocator, prepared.serialized_pages);
    prepared.meta_pgno = meta_pgno;
    prepared.meta_page = meta_page;
    return prepared;
}

fn pageImageLastPgno(image: PageImage) format.Pgno {
    return switch (image) {
        .leaf => |leaf| leaf.pgno,
        .leaf2 => |leaf2| leaf2.pgno,
        .branch => |branch| branch.pgno,
        .overflow => |overflow| overflow.pgno + overflow.page_count - 1,
    };
}
