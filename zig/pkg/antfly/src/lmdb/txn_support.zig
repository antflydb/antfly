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
const dupdata = @import("dupdata.zig");

pub const Dbi = union(enum) {
    core: usize,
    named: format.Db,
    write_named: usize,
};

pub const Options = struct {
    read_only: bool = true,
    defer_page_mutation: bool = false,
};

pub const DbOptions = struct {
    create: bool = false,
    reverse_key: bool = false,
    integer_key: bool = false,
    dup_sort: bool = false,
    dup_fixed: bool = false,
    integer_dup: bool = false,
    reverse_dup: bool = false,
};

pub const PutOptions = struct {
    no_overwrite: bool = false,
    no_dup_data: bool = false,
    append: bool = false,
    append_dup: bool = false,
};

pub const ReserveOptions = struct {
    no_overwrite: bool = false,
    append: bool = false,
};

pub const SearchResult = struct {
    node: ?node.View,
    index: usize,
    exact: bool,
};

pub const RawEntry = struct {
    key: []u8,
    value: []u8,
    flags: u16,
    data_size: usize,
};

pub const Entry = struct {
    key: []u8,
    value: []u8,
    flags: u16 = 0,
    data_size: usize,
};

pub const DirtyPage = struct {
    pgno: format.Pgno,
    kind: page.Kind,
    staged: bool,
    bytes: []u8,
};

pub const StagedOverflow = struct {
    pgno: format.Pgno,
    page_count: format.Pgno,
    data: []const u8,
};

pub const PathStep = struct {
    pgno: format.Pgno,
    child_index: usize,
};

pub const LeafPath = struct {
    parents: []const PathStep,
    leaf_pgno: format.Pgno,
    leaf_index: usize,
    exact: bool,
};

pub const KeyRange = struct {
    start: usize,
    end: usize,
};

pub fn searchPage(page_view: page.View, db_flags: u16, key: []const u8, is_branch: bool) !SearchResult {
    const nkeys = page_view.nodeCount();
    if (nkeys == 0) {
        return .{
            .node = null,
            .index = 0,
            .exact = false,
        };
    }

    var low: usize = if (is_branch) 1 else 0;
    var high: usize = nkeys - 1;
    var found_node: ?node.View = null;
    var found_index: usize = low;
    var exact = false;
    var cmp: i8 = -1;

    if (low > high) {
        return .{
            .node = null,
            .index = nkeys,
            .exact = false,
        };
    }

    while (low <= high) {
        const mid = (low + high) >> 1;
        const candidate = try node.View.fromPage(page_view, mid);
        found_node = candidate;
        found_index = mid;
        cmp = compareKeys(db_flags, key, candidate.key());
        if (cmp == 0) {
            exact = true;
            break;
        }
        if (cmp > 0) {
            low = mid + 1;
        } else {
            if (mid == 0) break;
            high = mid - 1;
        }
    }

    if (!exact and cmp > 0) {
        found_index += 1;
        if (found_index >= nkeys) {
            return .{
                .node = null,
                .index = found_index,
                .exact = false,
            };
        }
        found_node = try node.View.fromPage(page_view, found_index);
    }

    return .{
        .node = found_node,
        .index = found_index,
        .exact = exact,
    };
}

pub fn compareKeys(db_flags: u16, left: []const u8, right: []const u8) i8 {
    return switch (format.compareDbKeys(db_flags, left, right)) {
        .lt => -1,
        .eq => 0,
        .gt => 1,
    };
}

pub fn compareEntryPair(db_flags: u16, left_key: []const u8, left_value: []const u8, right_key: []const u8, right_value: []const u8) std.math.Order {
    const key_cmp = format.compareDbKeys(db_flags, left_key, right_key);
    if (key_cmp != .eq) return key_cmp;
    return dupdata.compareValues(db_flags, left_value, right_value);
}

pub fn dbHasDupSort(db: format.Db) bool {
    return dbHasDupSortFlags(db.md_flags);
}

pub fn dbHasDupSortFlags(flags: u16) bool {
    return (flags & format.DbFlags.dup_sort) != 0;
}

pub fn dbFlagsFromOptions(opts: DbOptions) u16 {
    var flags: u16 = 0;
    if (opts.reverse_key) flags |= format.DbFlags.reverse_key;
    if (opts.integer_key) flags |= format.DbFlags.integer_key;
    if (opts.dup_sort) flags |= format.DbFlags.dup_sort;
    if (opts.dup_fixed) flags |= format.DbFlags.dup_fixed;
    if (opts.integer_dup) flags |= format.DbFlags.integer_dup;
    if (opts.reverse_dup) flags |= format.DbFlags.reverse_dup;
    return flags;
}

pub fn dbFlagsSupportInlineDupsort(flags: u16) bool {
    return dbHasDupSortFlags(flags);
}

pub fn dupSortSubdbFlags(main_flags: u16) u16 {
    var flags: u16 = 0;
    if ((main_flags & format.DbFlags.dup_fixed) != 0) flags |= format.DbFlags.dup_fixed;
    if ((main_flags & format.DbFlags.integer_dup) != 0) flags |= format.DbFlags.integer_key;
    if ((main_flags & format.DbFlags.reverse_dup) != 0) flags |= format.DbFlags.reverse_key;
    return flags;
}

pub fn emptyDb(flags: u16) format.Db {
    return .{
        .md_pad = 0,
        .md_flags = flags,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
}

pub fn findEntryIndex(entries: []const Entry, db_flags: u16, key: []const u8) ?usize {
    const insert_at = findInsertIndex(entries, db_flags, key);
    if (insert_at < entries.len and format.compareDbKeys(db_flags, entries[insert_at].key, key) == .eq) {
        return insert_at;
    }
    return null;
}

pub fn findInsertIndex(entries: []const Entry, db_flags: u16, key: []const u8) usize {
    var low: usize = 0;
    var high: usize = entries.len;
    while (low < high) {
        const mid = (low + high) >> 1;
        switch (format.compareDbKeys(db_flags, entries[mid].key, key)) {
            .lt => low = mid + 1,
            else => high = mid,
        }
    }
    return low;
}

pub fn findKeyRange(entries: []const Entry, db_flags: u16, key: []const u8) ?KeyRange {
    const start = findInsertIndex(entries, db_flags, key);
    if (start >= entries.len or format.compareDbKeys(db_flags, entries[start].key, key) != .eq) return null;

    var end = start;
    while (end < entries.len and format.compareDbKeys(db_flags, entries[end].key, key) == .eq) : (end += 1) {}
    return .{ .start = start, .end = end };
}

pub fn findDupInsertIndex(entries: []const Entry, db_flags: u16, key: []const u8, value: []const u8) usize {
    var low: usize = 0;
    var high: usize = entries.len;
    while (low < high) {
        const mid = (low + high) >> 1;
        switch (compareEntryPair(db_flags, entries[mid].key, entries[mid].value, key, value)) {
            .lt => low = mid + 1,
            .eq, .gt => high = mid,
        }
    }
    return low;
}

pub fn findDupValueInsertIndex(values: []const []u8, db_flags: u16, value: []const u8) usize {
    var low: usize = 0;
    var high: usize = values.len;
    while (low < high) {
        const mid = (low + high) >> 1;
        switch (dupdata.compareValues(db_flags, values[mid], value)) {
            .lt => low = mid + 1,
            .eq, .gt => high = mid,
        }
    }
    return low;
}

pub fn findDupEntryIndex(entries: []const Entry, db_flags: u16, key: []const u8, value: []const u8) ?usize {
    const index = findDupInsertIndex(entries, db_flags, key, value);
    if (index < entries.len and std.mem.eql(u8, entries[index].key, key) and std.mem.eql(u8, entries[index].value, value)) {
        return index;
    }
    return null;
}

pub fn validateDupsortValue(entries: []const Entry, db_flags: u16, value: []const u8) !void {
    if ((db_flags & format.DbFlags.dup_fixed) == 0) return;
    if (value.len == 0) return error.Incompatible;
    for (entries) |entry| {
        if (entry.value.len != value.len) return error.Incompatible;
    }
}

pub fn appendUniquePgno(
    allocator: std.mem.Allocator,
    list: *std.ArrayListUnmanaged(format.Pgno),
    pgno: format.Pgno,
) !void {
    for (list.items) |existing| {
        if (existing == pgno) return;
    }
    try list.append(allocator, pgno);
}

pub fn findBranchChildIndex(branch_page: page.View, db_flags: u16, key: []const u8) !usize {
    const nkeys = branch_page.nodeCount();
    if (nkeys == 0) return error.Corrupted;
    if (nkeys == 1) return 0;

    const result = try searchPage(branch_page, db_flags, key, true);
    if (result.node == null) return nkeys - 1;

    var index = result.index;
    if (!result.exact) {
        if (index == 0) return error.Corrupted;
        index -= 1;
    }
    return index;
}

pub fn writeOverflowPages(page_bytes: []u8, pgno: format.Pgno, page_count: u32, data: []const u8) void {
    writeOverflowPagesOptions(page_bytes, pgno, page_count, data, true);
}

pub fn writeOverflowPagesOptions(page_bytes: []u8, pgno: format.Pgno, page_count: u32, data: []const u8, zero_fill: bool) void {
    if (zero_fill) @memset(page_bytes, 0);
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

pub fn writeLeaf2Page(page_bytes: []u8, pgno: format.Pgno, key_size: u16, values: []const []const u8) !void {
    return writeLeaf2PageOptions(page_bytes, pgno, key_size, values, true);
}

pub fn writeLeaf2PageOptions(page_bytes: []u8, pgno: format.Pgno, key_size: u16, values: []const []const u8, zero_fill: bool) !void {
    if (zero_fill) @memset(page_bytes, 0);
    for (values, 0..) |value, i| {
        if (value.len != key_size) return error.Incompatible;
        const start = format.page_header_size + i * key_size;
        const end = start + key_size;
        if (end > page_bytes.len) return error.MapFull;
        @memcpy(page_bytes[start..end], value);
    }
    const lower = format.page_header_size + values.len * @sizeOf(format.Indx);
    const upper = page_bytes.len - values.len * (key_size - @sizeOf(format.Indx));
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = key_size,
        .mp_flags = format.PageFlags.leaf | format.PageFlags.leaf2,
        .mp_lower = @intCast(lower),
        .mp_upper = @intCast(upper),
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);
}

pub fn writeMetaPage(
    page_bytes: []u8,
    pgno: format.Pgno,
    free_db_meta: format.Db,
    main_db: format.Db,
    mapsize: usize,
    last_pg: format.Pgno,
    txnid: format.Txnid,
) void {
    writeMetaPageOptions(page_bytes, pgno, free_db_meta, main_db, mapsize, last_pg, txnid, null, true);
}

pub fn writeMetaPageOptions(
    page_bytes: []u8,
    pgno: format.Pgno,
    free_db_meta: format.Db,
    main_db: format.Db,
    mapsize: usize,
    last_pg: format.Pgno,
    txnid: format.Txnid,
    map_address: ?*anyopaque,
    zero_fill: bool,
) void {
    if (zero_fill) @memset(page_bytes, 0);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.meta,
        .mp_lower = 0,
        .mp_upper = 0,
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);

    const meta_value = format.Meta{
        .mm_magic = format.mdb_magic,
        .mm_version = format.mdb_data_version,
        .mm_address = map_address,
        .mm_mapsize = mapsize,
        .mm_dbs = .{ free_db_meta, main_db },
        .mm_last_pg = last_pg,
        .mm_txnid = txnid,
    };
    format.writeStruct(format.Meta, page_bytes[format.page_header_size..][0..format.meta_body_size], meta_value);
}
