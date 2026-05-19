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
const support = @import("txn_support.zig");

const findEntryIndex = support.findEntryIndex;
const findInsertIndex = support.findInsertIndex;
const findKeyRange = support.findKeyRange;
const findDupInsertIndex = support.findDupInsertIndex;
const findDupValueInsertIndex = support.findDupValueInsertIndex;
const findDupEntryIndex = support.findDupEntryIndex;
const validateDupsortValue = support.validateDupsortValue;
const dbHasDupSort = support.dbHasDupSort;

pub fn put(self: anytype, txn: anytype, dbi: support.Dbi, key: []const u8, value: []const u8, opts: support.PutOptions) !void {
    const db_state = try self.dbStateForWrite(dbi);
    if (dbHasDupSort(db_state.meta)) {
        return putDupSort(self, txn, db_state, dbi, key, value, opts);
    }
    if (opts.no_dup_data or opts.append_dup) return error.Incompatible;
    const defer_page_mutation = txn.defer_page_mutation;

    if (!defer_page_mutation and !opts.no_overwrite) {
        if (try self.tryFastAppendToRightmostLeaf(txn, db_state, key, value)) {
            db_state.dirty = true;
            db_state.meta.md_entries += 1;
            return;
        }
    }

    if (!defer_page_mutation and !db_state.entries_loaded and !db_state.rebuild_required) {
        switch (try self.tryPutPageLevel(txn, dbi, key, value)) {
            .inserted => {
                db_state.dirty = true;
                db_state.meta.md_entries += 1;
                return;
            },
            .replaced => {
                db_state.dirty = true;
                db_state.append_hint = null;
                return;
            },
            .fallback => {},
        }
    }

    try self.ensureEntriesLoaded(txn, dbi, db_state);
    const entries = &db_state.entries;
    const existing_index = findEntryIndex(entries.items, db_state.meta.md_flags, key);
    if (existing_index) |index| {
        if (opts.no_overwrite) return error.KeyExists;
        const page_level = if (defer_page_mutation)
            false
        else
            try self.tryPutPageLevel(txn, dbi, key, value) != .fallback;
        db_state.dirty = true;
        db_state.append_hint = null;
        if (!page_level) db_state.rebuild_required = true;
        entries.items[index].value = try self.arena.allocator().dupe(u8, value);
        entries.items[index].flags = 0;
        entries.items[index].data_size = value.len;
        return;
    }

    const page_level = if (defer_page_mutation)
        false
    else
        try self.tryPutPageLevel(txn, dbi, key, value) != .fallback;
    db_state.dirty = true;
    db_state.meta.md_entries += 1;
    if (!page_level) db_state.rebuild_required = true;
    const insert_at = findInsertIndex(entries.items, db_state.meta.md_flags, key);
    try entries.insert(self.arena.allocator(), insert_at, .{
        .key = try self.arena.allocator().dupe(u8, key),
        .value = try self.arena.allocator().dupe(u8, value),
        .flags = 0,
        .data_size = value.len,
    });
    db_state.append_hint = null;
}

pub fn reserve(self: anytype, txn: anytype, dbi: support.Dbi, key: []const u8, size: usize, opts: support.ReserveOptions) ![]u8 {
    const db_state = try self.dbStateForWrite(dbi);
    if (dbHasDupSort(db_state.meta)) return error.Incompatible;
    try self.ensureEntriesLoaded(txn, dbi, db_state);

    const entries = &db_state.entries;
    const existing_index = findEntryIndex(entries.items, db_state.meta.md_flags, key);
    if (existing_index != null and opts.append) return error.KeyExists;
    const insert_at = findInsertIndex(entries.items, db_state.meta.md_flags, key);
    if (opts.append and insert_at != entries.items.len) return error.KeyExists;

    const reserved = try self.arena.allocator().alloc(u8, size);

    db_state.dirty = true;
    db_state.rebuild_required = true;
    db_state.append_hint = null;

    if (existing_index) |index| {
        if (opts.no_overwrite) return error.KeyExists;
        entries.items[index].value = reserved;
        entries.items[index].flags = 0;
        entries.items[index].data_size = size;
        return reserved;
    }

    try entries.insert(self.arena.allocator(), insert_at, .{
        .key = try self.arena.allocator().dupe(u8, key),
        .value = reserved,
        .flags = 0,
        .data_size = size,
    });
    db_state.meta.md_entries += 1;
    return reserved;
}

pub fn delete(self: anytype, txn: anytype, dbi: support.Dbi, key: []const u8) !void {
    const db_state = try self.dbStateForWrite(dbi);
    if (dbHasDupSort(db_state.meta)) {
        return deleteDupSort(self, txn, db_state, dbi, key);
    }
    const defer_page_mutation = txn.defer_page_mutation;

    if (!defer_page_mutation and !db_state.entries_loaded and !db_state.rebuild_required) {
        switch (try self.tryDeletePageLevel(txn, dbi, key)) {
            .deleted => {
                db_state.dirty = true;
                db_state.meta.md_entries -= 1;
                db_state.append_hint = null;
                return;
            },
            .not_found => return error.NotFound,
            .fallback => {},
        }
    }

    try self.ensureEntriesLoaded(txn, dbi, db_state);
    const entries = &db_state.entries;
    const index = findEntryIndex(entries.items, db_state.meta.md_flags, key) orelse return error.NotFound;

    const page_level = if (defer_page_mutation)
        false
    else
        try self.tryDeletePageLevel(txn, dbi, key) != .fallback;
    db_state.dirty = true;
    db_state.meta.md_entries -= 1;
    db_state.append_hint = null;
    if (!page_level) db_state.rebuild_required = true;
    _ = entries.orderedRemove(index);
}

pub fn deleteValue(self: anytype, txn: anytype, dbi: support.Dbi, key: []const u8, value: []const u8) !void {
    const db_state = try self.dbStateForWrite(dbi);
    if (!dbHasDupSort(db_state.meta)) {
        try self.ensureEntriesLoaded(txn, dbi, db_state);
        const index = findEntryIndex(db_state.entries.items, db_state.meta.md_flags, key) orelse return error.NotFound;
        if (!std.mem.eql(u8, db_state.entries.items[index].value, value)) return error.NotFound;
        return delete(self, txn, dbi, key);
    }

    try self.ensureEntriesLoaded(txn, dbi, db_state);
    const pair_index = findDupEntryIndex(db_state.entries.items, db_state.meta.md_flags, key, value) orelse return error.NotFound;
    db_state.dirty = true;
    const page_level = if (txn.defer_page_mutation)
        false
    else
        try self.tryDeleteDupSortValuePageLevel(txn, dbi, key, value);
    if (!page_level) db_state.rebuild_required = true;
    db_state.meta.md_entries -= 1;
    db_state.append_hint = null;
    _ = db_state.entries.orderedRemove(pair_index);
}

fn putDupSort(self: anytype, txn: anytype, db_state: anytype, dbi: support.Dbi, key: []const u8, value: []const u8, opts: support.PutOptions) !void {
    try self.ensureEntriesLoaded(txn, dbi, db_state);
    const range = findKeyRange(db_state.entries.items, db_state.meta.md_flags, key);
    if (opts.no_overwrite and range != null) return error.KeyExists;

    const pair_index = findDupEntryIndex(db_state.entries.items, db_state.meta.md_flags, key, value);
    if (opts.no_dup_data and pair_index != null) return error.KeyExists;
    try validateDupsortValue(db_state.entries.items, db_state.meta.md_flags, value);

    const insert_at = findDupInsertIndex(db_state.entries.items, db_state.meta.md_flags, key, value);
    if (opts.append) {
        if (range != null) return error.KeyExists;
        if (insert_at != db_state.entries.items.len) return error.KeyExists;
    }
    if (opts.append_dup) {
        if (range) |existing_range| {
            if (insert_at != existing_range.end) return error.KeyExists;
        } else if (insert_at != db_state.entries.items.len) {
            return error.KeyExists;
        }
    }

    db_state.dirty = true;
    const page_level = if (txn.defer_page_mutation)
        false
    else
        try self.tryPutDupSortPageLevel(txn, dbi, key, value);
    if (!page_level) db_state.rebuild_required = true;
    db_state.meta.md_entries += 1;
    db_state.append_hint = null;

    try db_state.entries.insert(self.arena.allocator(), insert_at, .{
        .key = try self.arena.allocator().dupe(u8, key),
        .value = try self.arena.allocator().dupe(u8, value),
        .flags = 0,
        .data_size = value.len,
    });
}

fn deleteDupSort(self: anytype, txn: anytype, db_state: anytype, dbi: support.Dbi, key: []const u8) !void {
    try self.ensureEntriesLoaded(txn, dbi, db_state);
    const range = findKeyRange(db_state.entries.items, db_state.meta.md_flags, key) orelse return error.NotFound;
    db_state.dirty = true;
    const page_level = if (txn.defer_page_mutation)
        false
    else
        try self.tryDeleteDupSortPageLevel(txn, dbi, key);
    if (!page_level) db_state.rebuild_required = true;
    db_state.meta.md_entries -= range.end - range.start;
    db_state.append_hint = null;

    var i = range.end;
    while (i > range.start) {
        i -= 1;
        _ = db_state.entries.orderedRemove(i);
    }
}
