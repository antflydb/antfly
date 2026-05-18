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
const txn_mod = @import("txn.zig");
const read_support = @import("read_support.zig");

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

pub const PlainEntry = Entry;

pub const PlainLeafBatch = struct {
    txn: *txn_mod.Transaction,
    leaf_page: page.View,
    start_index: usize,
    end_index: usize,

    pub fn len(self: PlainLeafBatch) usize {
        return self.end_index - self.start_index;
    }

    pub fn entryAt(self: PlainLeafBatch, batch_index: usize) Error!PlainEntry {
        const absolute_index = self.start_index + batch_index;
        if (absolute_index >= self.end_index) return error.NotFound;
        return decodePlainLeafEntry(self.txn, self.leaf_page, absolute_index);
    }
};

const max_depth = 32;

pub const Error = txn_mod.Error || node.Error || error{
    NotFound,
    Corrupted,
    UnsupportedPageType,
    UnsupportedNodeFlags,
    CursorStackOverflow,
};

const SearchResult = read_support.SearchResult;

const Frame = struct {
    pgno: format.Pgno,
    index: usize,
};

const DupState = struct {
    index: usize = 0,
    count: usize = 0,
    subdb: ?format.Db = null,
};

pub const PlainScanner = struct {
    txn: *txn_mod.Transaction,
    dbi: txn_mod.Dbi,
    db_flags: u16,
    initialized: bool = false,
    eof: bool = false,
    depth: usize = 0,
    current_leaf: ?page.View = null,
    frames: [max_depth]Frame = undefined,

    pub fn init(txn: *txn_mod.Transaction, dbi: txn_mod.Dbi) Error!PlainScanner {
        const db = try txn.db(dbi);
        if ((db.md_flags & format.DbFlags.dup_sort) != 0) return error.Incompatible;
        return .{
            .txn = txn,
            .dbi = dbi,
            .db_flags = db.md_flags,
        };
    }

    pub fn seekRange(self: *PlainScanner, key: []const u8) Error!void {
        self.reset();

        const db = try self.txn.db(self.dbi);
        if (db.md_root == format.invalid_pgno) return error.NotFound;

        var current_pgno = db.md_root;
        while (true) {
            const current_page = try self.txn.pageView(current_pgno);
            switch (current_page.kind()) {
                .branch => {
                    const child_index = try findBranchChildIndex(current_page, self.db_flags, key);
                    try self.push(.{ .pgno = current_pgno, .index = child_index });
                    const child = try node.View.fromPage(current_page, child_index);
                    current_pgno = child.branchPgno();
                },
                .leaf => {
                    const result = try searchPage(current_page, self.db_flags, key, false);
                    const node_count = current_page.nodeCount();
                    if (node_count == 0) return error.NotFound;

                    if (result.exact or result.node != null) {
                        try self.push(.{ .pgno = current_pgno, .index = result.index });
                        self.current_leaf = current_page;
                        self.initialized = true;
                        self.eof = false;
                        return;
                    }

                    try self.push(.{ .pgno = current_pgno, .index = node_count - 1 });
                    self.current_leaf = current_page;
                    self.initialized = true;
                    self.eof = false;
                    try self.advanceToNextLeaf();
                    return;
                },
                .leaf2 => return error.UnsupportedPageType,
                else => return error.Corrupted,
            }
        }
    }

    pub fn nextBatch(self: *PlainScanner, out: []PlainEntry) Error!usize {
        if (!self.initialized or self.eof) return error.NotFound;
        if (out.len == 0) return 0;

        var written: usize = 0;
        while (written < out.len and !self.eof) {
            const leaf_page = try self.currentLeafPage();
            var index = self.frames[self.depth - 1].index;
            const count = try fillPlainLeafBatch(self.txn, leaf_page, &index, out[written..]);
            written += count;

            if (written == out.len) {
                self.frames[self.depth - 1].index = index;
                if (index >= leaf_page.nodeCount()) {
                    self.advanceToNextLeaf() catch |err| switch (err) {
                        error.NotFound => {},
                        else => return err,
                    };
                }
                break;
            }

            self.advanceToNextLeaf() catch |err| switch (err) {
                error.NotFound => break,
                else => return err,
            };
        }

        if (written == 0) return error.NotFound;
        return written;
    }

    pub fn nextLeafBatch(self: *PlainScanner) Error!PlainLeafBatch {
        if (!self.initialized or self.eof) return error.NotFound;

        while (!self.eof) {
            const leaf_page = try self.currentLeafPage();
            const start_index = self.frames[self.depth - 1].index;
            const end_index = leaf_page.nodeCount();
            if (start_index < end_index) {
                const batch = PlainLeafBatch{
                    .txn = self.txn,
                    .leaf_page = leaf_page,
                    .start_index = start_index,
                    .end_index = end_index,
                };
                self.advanceToNextLeaf() catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                return batch;
            }
            self.advanceToNextLeaf() catch |err| switch (err) {
                error.NotFound => break,
                else => return err,
            };
        }

        return error.NotFound;
    }

    fn descendLeftmost(self: *PlainScanner, start_pgno: format.Pgno) Error!void {
        var current_pgno = start_pgno;
        while (true) {
            const current_page = try self.txn.pageView(current_pgno);
            switch (current_page.kind()) {
                .branch => {
                    if (current_page.nodeCount() == 0) return error.Corrupted;
                    try self.push(.{ .pgno = current_pgno, .index = 0 });
                    const child = try node.View.fromPage(current_page, 0);
                    current_pgno = child.branchPgno();
                },
                .leaf => {
                    if (current_page.nodeCount() == 0) return error.NotFound;
                    try self.push(.{ .pgno = current_pgno, .index = 0 });
                    self.current_leaf = current_page;
                    return;
                },
                .leaf2 => return error.UnsupportedPageType,
                else => return error.Corrupted,
            }
        }
    }

    fn advanceToNextLeaf(self: *PlainScanner) Error!void {
        if (self.depth == 0) return error.NotFound;

        self.depth -= 1;
        while (self.depth > 0) {
            const parent_pos = self.depth - 1;
            const parent_page = try self.txn.pageView(self.frames[parent_pos].pgno);
            if (self.frames[parent_pos].index + 1 < parent_page.nodeCount()) {
                self.frames[parent_pos].index += 1;
                self.depth = parent_pos + 1;
                const sibling = try node.View.fromPage(parent_page, self.frames[parent_pos].index);
                try self.descendLeftmost(sibling.branchPgno());
                self.initialized = true;
                self.eof = false;
                return;
            }
            self.depth -= 1;
        }

        self.eof = true;
        return error.NotFound;
    }

    fn currentLeafPage(self: *const PlainScanner) Error!page.View {
        if (self.depth == 0) return error.NotFound;
        const leaf_page = self.current_leaf orelse try self.txn.pageView(self.frames[self.depth - 1].pgno);
        if (leaf_page.kind() != .leaf) return error.Corrupted;
        return leaf_page;
    }

    fn push(self: *PlainScanner, frame: Frame) Error!void {
        if (self.depth >= max_depth) return error.CursorStackOverflow;
        self.frames[self.depth] = frame;
        self.depth += 1;
    }

    fn reset(self: *PlainScanner) void {
        self.initialized = false;
        self.eof = false;
        self.depth = 0;
        self.current_leaf = null;
    }
};

fn fillPlainLeafBatch(
    txn: *txn_mod.Transaction,
    leaf_page: page.View,
    index: *usize,
    out: []PlainEntry,
) Error!usize {
    if (out.len == 0) return 0;

    const node_count = leaf_page.nodeCount();

    var written: usize = 0;
    var i = index.*;
    while (i < node_count and written < out.len) : (i += 1) {
        out[written] = try decodePlainLeafEntry(txn, leaf_page, i);
        written += 1;
    }

    index.* = i;
    return written;
}

fn decodePlainLeafEntry(txn: *txn_mod.Transaction, leaf_page: page.View, index: usize) Error!PlainEntry {
    if (index >= leaf_page.nodeCount()) return error.NotFound;

    const bytes = leaf_page.bytes;
    const ptr_size = @sizeOf(format.Indx);
    const ptr_offset = format.page_header_size + index * ptr_size;
    if (ptr_offset + ptr_size > bytes.len) return error.Corrupted;

    const node_offset = format.readNativeInt(format.Indx, bytes[ptr_offset..][0..ptr_size]);
    if (node_offset + node.header_size > bytes.len) return error.Corrupted;

    const hdr_bytes = bytes[node_offset..][0..node.header_size];
    const flags = format.readNativeInt(u16, hdr_bytes[4..6]);
    if (flags == 0) {
        const key_size = format.readNativeInt(u16, hdr_bytes[6..8]);
        const value_size: usize = @as(usize, format.readNativeInt(u16, hdr_bytes[0..2])) |
            (@as(usize, format.readNativeInt(u16, hdr_bytes[2..4])) << 16);
        const key_start = node_offset + node.header_size;
        const key_end = key_start + key_size;
        const value_end = key_end + value_size;
        if (value_end > bytes.len) return error.Corrupted;

        return .{
            .key = bytes[key_start..key_end],
            .value = bytes[key_end..value_end],
        };
    }

    const leaf = try node.View.fromPage(leaf_page, index);
    if (leaf.isSubdata() or leaf.isDupdata()) return error.UnsupportedNodeFlags;
    return .{
        .key = leaf.key(),
        .value = try read_support.readLeafValue(txn, leaf),
    };
}

pub const Cursor = struct {
    txn: *txn_mod.Transaction,
    dbi: txn_mod.Dbi,
    initialized: bool = false,
    eof: bool = false,
    depth: usize = 0,
    dup: ?DupState = null,
    db_flags: ?u16 = null,
    current_leaf: ?page.View = null,
    current_leaf_entry: ?node.View = null,
    frames: [max_depth]Frame = undefined,

    pub fn init(txn: *txn_mod.Transaction, dbi: txn_mod.Dbi) Cursor {
        return .{
            .txn = txn,
            .dbi = dbi,
        };
    }

    pub fn first(self: *Cursor) Error!Entry {
        self.reset();
        const db = try self.txn.db(self.dbi);
        if (db.md_root == format.invalid_pgno) return error.NotFound;
        try self.descendLeftmost(db.md_root);
        self.initialized = true;
        self.eof = false;
        return self.getCurrent();
    }

    pub fn last(self: *Cursor) Error!Entry {
        self.reset();
        const db = try self.txn.db(self.dbi);
        if (db.md_root == format.invalid_pgno) return error.NotFound;
        try self.descendRightmost(db.md_root);
        self.initialized = true;
        self.eof = false;
        return self.getCurrent();
    }

    pub fn firstDup(self: *Cursor) Error!Entry {
        if (!self.initialized) return self.first();
        const dup_state = self.dup orelse return error.NotFound;
        _ = dup_state;
        self.dup.?.index = 0;
        return self.getCurrent();
    }

    pub fn lastDup(self: *Cursor) Error!Entry {
        if (!self.initialized) return self.first();
        const dup_state = self.dup orelse return error.NotFound;
        if (dup_state.count == 0) return error.NotFound;
        self.dup.?.index = dup_state.count - 1;
        return self.getCurrent();
    }

    pub fn next(self: *Cursor) Error!Entry {
        if (!self.initialized) return self.first();
        if (self.eof) return error.NotFound;
        if ((try self.dbFlags() & format.DbFlags.dup_sort) == 0) return self.nextPlain();

        if (self.dup) |*dup_state| {
            if (dup_state.index + 1 < dup_state.count) {
                dup_state.index += 1;
                return self.getCurrent();
            }
            self.dup = null;
        }

        const leaf_page = try self.currentLeafPage();
        const top = self.depth - 1;
        if (self.frames[top].index + 1 < leaf_page.nodeCount()) {
            self.frames[top].index += 1;
            self.current_leaf_entry = null;
            try self.initDupState();
            return self.getCurrent();
        }

        try self.advanceToNextLeaf();
        return self.getCurrent();
    }

    pub fn prev(self: *Cursor) Error!Entry {
        if (!self.initialized) return self.last();
        if (self.eof) return error.NotFound;

        if (self.dup) |*dup_state| {
            if (dup_state.index > 0) {
                dup_state.index -= 1;
                return self.getCurrent();
            }
            self.dup = null;
        }

        _ = try self.currentLeafPage();
        const top = self.depth - 1;
        if (self.frames[top].index > 0) {
            self.frames[top].index -= 1;
            self.current_leaf_entry = null;
            try self.initDupState();
            self.selectLastDup();
            return self.getCurrent();
        }

        try self.advanceToPrevLeaf();
        return self.getCurrent();
    }

    pub fn nextDup(self: *Cursor) Error!Entry {
        if (!self.initialized) return self.first();
        const dup_state = self.dup orelse return error.NotFound;
        if (dup_state.index + 1 >= dup_state.count) return error.NotFound;
        self.dup.?.index += 1;
        return self.getCurrent();
    }

    pub fn prevDup(self: *Cursor) Error!Entry {
        if (!self.initialized) return error.NotFound;
        const dup_state = self.dup orelse return error.NotFound;
        if (dup_state.index == 0) return error.NotFound;
        self.dup.?.index -= 1;
        return self.getCurrent();
    }

    pub fn nextNoDup(self: *Cursor) Error!Entry {
        if (!self.initialized) return self.first();
        if (self.eof) return error.NotFound;
        if ((try self.dbFlags() & format.DbFlags.dup_sort) == 0) return self.nextPlain();

        self.dup = null;
        const leaf_page = try self.currentLeafPage();
        const top = self.depth - 1;
        if (self.frames[top].index + 1 < leaf_page.nodeCount()) {
            self.frames[top].index += 1;
            self.current_leaf_entry = null;
            try self.initDupState();
            return self.getCurrent();
        }

        try self.advanceToNextLeaf();
        return self.getCurrent();
    }

    pub fn prevNoDup(self: *Cursor) Error!Entry {
        if (!self.initialized) return self.last();
        if (self.eof) return error.NotFound;

        self.dup = null;
        _ = try self.currentLeafPage();
        const top = self.depth - 1;
        if (self.frames[top].index > 0) {
            self.frames[top].index -= 1;
            self.current_leaf_entry = null;
            try self.initDupState();
            self.selectLastDup();
            return self.getCurrent();
        }

        try self.advanceToPrevLeaf();
        return self.getCurrent();
    }

    pub fn set(self: *Cursor, key: []const u8) Error!Entry {
        return self.seekInternal(key, true);
    }

    pub fn setRange(self: *Cursor, key: []const u8) Error!Entry {
        return self.seekInternal(key, false);
    }

    pub fn getBoth(self: *Cursor, key: []const u8, value: []const u8) Error!Entry {
        return self.seekDupValue(key, value, true);
    }

    pub fn getBothRange(self: *Cursor, key: []const u8, value: []const u8) Error!Entry {
        return self.seekDupValue(key, value, false);
    }

    pub fn getCurrent(self: *Cursor) Error!Entry {
        if (!self.initialized or self.eof or self.depth == 0) return error.NotFound;

        if ((try self.dbFlags() & format.DbFlags.dup_sort) == 0) {
            const leaf_page = try self.currentLeafPage();
            const index = self.frames[self.depth - 1].index;
            if (index >= leaf_page.nodeCount()) return error.NotFound;
            if (try fastInlineLeafEntry(leaf_page, index)) |entry| return entry;

            const leaf = try self.currentLeafEntryView();
            if (leaf.isSubdata() or leaf.isDupdata()) return error.UnsupportedNodeFlags;
            return .{
                .key = leaf.key(),
                .value = try read_support.readLeafValue(self.txn, leaf),
            };
        }

        const leaf = try self.currentLeafEntryView();
        if (leaf.isSubdata() and !leaf.isDupdata()) return error.UnsupportedNodeFlags;
        if (leaf.isDupdata()) {
            const dup_index = if (self.dup) |dup_state| dup_state.index else 0;
            return .{
                .key = leaf.key(),
                .value = try read_support.readDupsortValue(self.txn, leaf, dup_index),
            };
        }

        return .{
            .key = leaf.key(),
            .value = try read_support.readLeafValue(self.txn, leaf),
        };
    }

    fn nextPlain(self: *Cursor) Error!Entry {
        const leaf_page = try self.currentLeafPage();
        const top = self.depth - 1;
        if (self.frames[top].index + 1 < leaf_page.nodeCount()) {
            self.frames[top].index += 1;
            self.current_leaf_entry = null;
            if (try fastInlineLeafEntry(leaf_page, self.frames[top].index)) |entry| return entry;

            const leaf = try self.currentLeafEntryView();
            if (leaf.isSubdata() or leaf.isDupdata()) return error.UnsupportedNodeFlags;
            return .{
                .key = leaf.key(),
                .value = try read_support.readLeafValue(self.txn, leaf),
            };
        }

        try self.advanceToNextLeaf();
        const next_leaf_page = try self.currentLeafPage();
        const next_index = self.frames[self.depth - 1].index;
        if (try fastInlineLeafEntry(next_leaf_page, next_index)) |entry| return entry;

        const leaf = try self.currentLeafEntryView();
        if (leaf.isSubdata() or leaf.isDupdata()) return error.UnsupportedNodeFlags;
        return .{
            .key = leaf.key(),
            .value = try read_support.readLeafValue(self.txn, leaf),
        };
    }

    pub fn put(self: *Cursor, key: []const u8, value: []const u8, opts: txn_mod.PutOptions) Error!void {
        try self.txn.put(self.dbi, key, value, opts);
        const db = try self.txn.db(self.dbi);
        if ((db.md_flags & format.DbFlags.dup_sort) != 0) {
            _ = try self.getBoth(key, value);
            return;
        }
        _ = try self.set(key);
    }

    pub fn reserve(self: *Cursor, key: []const u8, size: usize, opts: txn_mod.ReserveOptions) Error![]u8 {
        const reserved = try self.txn.reserve(self.dbi, key, size, opts);
        _ = self.set(key) catch |err| switch (err) {
            error.NotFound => reserved,
            else => return err,
        };
        return reserved;
    }

    pub fn deleteCurrent(self: *Cursor) Error!void {
        if (!self.initialized or self.eof or self.depth == 0) return error.NotFound;

        const current = try self.getCurrent();
        const db = try self.txn.db(self.dbi);
        const dup_index_before = if (self.dup) |dup_state| dup_state.index else 0;

        if ((db.md_flags & format.DbFlags.dup_sort) != 0) {
            try self.txn.deleteValue(self.dbi, current.key, current.value);
        } else {
            try self.txn.delete(self.dbi, current.key);
        }

        self.reset();

        if ((db.md_flags & format.DbFlags.dup_sort) == 0) {
            _ = self.setRange(current.key) catch |err| switch (err) {
                error.NotFound => {
                    self.eof = true;
                    return;
                },
                else => return err,
            };
            return;
        }

        _ = self.set(current.key) catch |err| switch (err) {
            error.NotFound => {
                _ = self.setRange(current.key) catch |next_err| switch (next_err) {
                    error.NotFound => {
                        self.eof = true;
                        return;
                    },
                    else => return next_err,
                };
                return;
            },
            else => return err,
        };

        if (self.dup) |*dup_state| {
            if (dup_index_before < dup_state.count) {
                dup_state.index = dup_index_before;
                return;
            }
            _ = self.nextNoDup() catch |err| switch (err) {
                error.NotFound => {
                    self.eof = true;
                    return;
                },
                else => return err,
            };
            return;
        }

        if (dup_index_before == 0) return;
        _ = self.nextNoDup() catch |err| switch (err) {
            error.NotFound => {
                self.eof = true;
                return;
            },
            else => return err,
        };
    }

    fn seekInternal(self: *Cursor, key: []const u8, exact_only: bool) Error!Entry {
        self.reset();

        const db = try self.txn.db(self.dbi);
        if (db.md_root == format.invalid_pgno) return error.NotFound;

        var current_pgno = db.md_root;
        while (true) {
            const current_page = try self.txn.pageView(current_pgno);
            switch (current_page.kind()) {
                .branch => {
                    const child_index = try findBranchChildIndex(current_page, db.md_flags, key);
                    try self.push(.{ .pgno = current_pgno, .index = child_index });
                    const child = try node.View.fromPage(current_page, child_index);
                    current_pgno = child.branchPgno();
                },
                .leaf => {
                    const result = try searchPage(current_page, db.md_flags, key, false);
                    const node_count = current_page.nodeCount();
                    if (node_count == 0) return error.NotFound;

                    if (result.exact) {
                        try self.push(.{ .pgno = current_pgno, .index = result.index });
                        self.current_leaf = current_page;
                        self.current_leaf_entry = null;
                        self.initialized = true;
                        self.eof = false;
                        try self.initDupState();
                        return self.getCurrent();
                    }

                    if (exact_only) return error.NotFound;

                    if (result.node) |_| {
                        try self.push(.{ .pgno = current_pgno, .index = result.index });
                        self.current_leaf = current_page;
                        self.current_leaf_entry = null;
                        self.initialized = true;
                        self.eof = false;
                        try self.initDupState();
                        return self.getCurrent();
                    }

                    try self.push(.{ .pgno = current_pgno, .index = node_count - 1 });
                    self.initialized = true;
                    self.eof = false;
                    self.dup = null;
                    try self.advanceToNextLeaf();
                    return self.getCurrent();
                },
                .leaf2 => return error.UnsupportedPageType,
                else => return error.Corrupted,
            }
        }
    }

    fn descendLeftmost(self: *Cursor, start_pgno: format.Pgno) Error!void {
        var current_pgno = start_pgno;
        while (true) {
            const current_page = try self.txn.pageView(current_pgno);
            switch (current_page.kind()) {
                .branch => {
                    if (current_page.nodeCount() == 0) return error.Corrupted;
                    try self.push(.{ .pgno = current_pgno, .index = 0 });
                    const child = try node.View.fromPage(current_page, 0);
                    current_pgno = child.branchPgno();
                },
                .leaf => {
                    if (current_page.nodeCount() == 0) return error.NotFound;
                    try self.push(.{ .pgno = current_pgno, .index = 0 });
                    self.current_leaf = current_page;
                    self.current_leaf_entry = null;
                    try self.initDupState();
                    return;
                },
                .leaf2 => return error.UnsupportedPageType,
                else => return error.Corrupted,
            }
        }
    }

    fn descendRightmost(self: *Cursor, start_pgno: format.Pgno) Error!void {
        var current_pgno = start_pgno;
        while (true) {
            const current_page = try self.txn.pageView(current_pgno);
            switch (current_page.kind()) {
                .branch => {
                    if (current_page.nodeCount() == 0) return error.Corrupted;
                    const index = current_page.nodeCount() - 1;
                    try self.push(.{ .pgno = current_pgno, .index = index });
                    const child = try node.View.fromPage(current_page, index);
                    current_pgno = child.branchPgno();
                },
                .leaf => {
                    if (current_page.nodeCount() == 0) return error.NotFound;
                    try self.push(.{ .pgno = current_pgno, .index = current_page.nodeCount() - 1 });
                    self.current_leaf = current_page;
                    self.current_leaf_entry = null;
                    try self.initDupState();
                    self.selectLastDup();
                    return;
                },
                .leaf2 => return error.UnsupportedPageType,
                else => return error.Corrupted,
            }
        }
    }

    fn advanceToNextLeaf(self: *Cursor) Error!void {
        if (self.depth == 0) return error.NotFound;

        self.depth -= 1;
        while (self.depth > 0) {
            const parent_pos = self.depth - 1;
            const parent_page = try self.txn.pageView(self.frames[parent_pos].pgno);
            if (self.frames[parent_pos].index + 1 < parent_page.nodeCount()) {
                self.frames[parent_pos].index += 1;
                self.depth = parent_pos + 1;
                const sibling = try node.View.fromPage(parent_page, self.frames[parent_pos].index);
                try self.descendLeftmost(sibling.branchPgno());
                self.initialized = true;
                self.eof = false;
                try self.initDupState();
                return;
            }
            self.depth -= 1;
        }

        self.eof = true;
        return error.NotFound;
    }

    fn advanceToPrevLeaf(self: *Cursor) Error!void {
        if (self.depth == 0) return error.NotFound;

        self.depth -= 1;
        while (self.depth > 0) {
            const parent_pos = self.depth - 1;
            const parent_page = try self.txn.pageView(self.frames[parent_pos].pgno);
            if (self.frames[parent_pos].index > 0) {
                self.frames[parent_pos].index -= 1;
                self.depth = parent_pos + 1;
                const sibling = try node.View.fromPage(parent_page, self.frames[parent_pos].index);
                try self.descendRightmost(sibling.branchPgno());
                self.initialized = true;
                self.eof = false;
                return;
            }
            self.depth -= 1;
        }

        self.eof = true;
        return error.NotFound;
    }

    fn currentLeafPage(self: *const Cursor) Error!page.View {
        if (self.depth == 0) return error.NotFound;
        const leaf_page = self.current_leaf orelse try self.txn.pageView(self.frames[self.depth - 1].pgno);
        if (leaf_page.kind() != .leaf) return error.Corrupted;
        return leaf_page;
    }

    fn currentLeafEntryView(self: *Cursor) Error!node.View {
        if (self.current_leaf_entry) |leaf| return leaf;
        const leaf_page = try self.currentLeafPage();
        const leaf = try node.View.fromPage(leaf_page, self.frames[self.depth - 1].index);
        self.current_leaf_entry = leaf;
        return leaf;
    }

    fn push(self: *Cursor, frame: Frame) Error!void {
        if (self.depth >= max_depth) return error.CursorStackOverflow;
        self.frames[self.depth] = frame;
        self.depth += 1;
    }

    fn reset(self: *Cursor) void {
        self.initialized = false;
        self.eof = false;
        self.depth = 0;
        self.dup = null;
        self.current_leaf = null;
        self.current_leaf_entry = null;
    }

    fn seekDupValue(self: *Cursor, key: []const u8, value: []const u8, exact_only: bool) Error!Entry {
        _ = try self.set(key);
        const dup_state = self.dup orelse {
            const current = try self.getCurrent();
            const db = try self.txn.db(self.dbi);
            const cmp = dupdata.compareValues(db.md_flags, current.value, value);
            if (exact_only and cmp != .eq) return error.NotFound;
            if (!exact_only and cmp == .lt) return error.NotFound;
            return current;
        };

        var index: usize = 0;
        const db = try self.txn.db(self.dbi);
        while (index < dup_state.count) : (index += 1) {
            const candidate = try self.dupValueAt(index);
            switch (dupdata.compareValues(db.md_flags, candidate, value)) {
                .lt => continue,
                .eq => {
                    self.dup.?.index = index;
                    return self.getCurrent();
                },
                .gt => {
                    if (exact_only) return error.NotFound;
                    self.dup.?.index = index;
                    return self.getCurrent();
                },
            }
        }
        return error.NotFound;
    }

    fn dupValueAt(self: *const Cursor, dup_index: usize) Error![]const u8 {
        const mutable_self: *Cursor = @constCast(self);
        const leaf = try mutable_self.currentLeafEntryView();
        return read_support.readDupsortValue(self.txn, leaf, dup_index);
    }

    fn initDupState(self: *Cursor) Error!void {
        if (self.depth == 0) {
            self.dup = null;
            return;
        }

        if ((try self.dbFlags() & format.DbFlags.dup_sort) == 0) {
            self.dup = null;
            return;
        }

        const leaf = try self.currentLeafEntryView();
        if (!leaf.isDupdata()) {
            self.dup = null;
            return;
        }
        const dup_db = if (leaf.isSubdata()) blk: {
            const value = leaf.inlineValue();
            if (value.len < @sizeOf(format.Db)) return error.Corrupted;
            break :blk format.readStruct(format.Db, value[0..@sizeOf(format.Db)]);
        } else null;
        self.dup = .{
            .index = 0,
            .count = if (dup_db) |db| db.md_entries else try dupdata.count(leaf),
            .subdb = dup_db,
        };
    }

    fn selectLastDup(self: *Cursor) void {
        if (self.dup) |*dup_state| {
            if (dup_state.count > 0) dup_state.index = dup_state.count - 1;
        }
    }

    fn dbFlags(self: *Cursor) Error!u16 {
        if (self.db_flags) |flags| return flags;
        const flags = (try self.txn.db(self.dbi)).md_flags;
        self.db_flags = flags;
        return flags;
    }
};

const findBranchChildIndex = read_support.findBranchChildIndex;
const searchPage = read_support.searchPage;

const LeafEntry = struct {
    key: []const u8,
    value: []const u8,
};

const BranchEntry = struct {
    key: []const u8,
    child_pgno: format.Pgno,
};

fn fastInlineLeafEntry(leaf_page: page.View, index: usize) Error!?Entry {
    if (index >= leaf_page.nodeCount()) return error.NotFound;

    const ptr_offset = format.page_header_size + index * @sizeOf(format.Indx);
    if (ptr_offset + @sizeOf(format.Indx) > leaf_page.bytes.len) return error.Corrupted;
    const node_offset = format.readNativeInt(format.Indx, leaf_page.bytes[ptr_offset..][0..@sizeOf(format.Indx)]);
    if (node_offset + node.header_size > leaf_page.bytes.len) return error.Corrupted;

    const hdr = format.readStruct(node.Header, leaf_page.bytes[node_offset..][0..node.header_size]);
    if (hdr.mn_flags != 0) return null;

    const key_size = hdr.mn_ksize;
    const value_size: usize = @as(usize, hdr.mn_lo) | (@as(usize, hdr.mn_hi) << 16);
    const key_start = node_offset + node.header_size;
    const key_end = key_start + key_size;
    const value_end = key_end + value_size;
    if (value_end > leaf_page.bytes.len) return error.Corrupted;

    return .{
        .key = leaf_page.bytes[key_start..key_end],
        .value = leaf_page.bytes[key_end..value_end],
    };
}

fn writeMetaPage(page_bytes: []u8, pgno: format.Pgno, page_size: u32, root_pgno: format.Pgno, txnid: format.Txnid) void {
    @memset(page_bytes, 0);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.meta,
        .mp_lower = 0,
        .mp_upper = 0,
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);

    const free_db = format.Db{
        .md_pad = page_size,
        .md_flags = 0,
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
        .md_root = root_pgno,
    };
    const meta_value = format.Meta{
        .mm_magic = format.mdb_magic,
        .mm_version = format.mdb_data_version,
        .mm_address = null,
        .mm_mapsize = page_bytes.len * format.num_metas,
        .mm_dbs = .{ free_db, main_db },
        .mm_last_pg = root_pgno,
        .mm_txnid = txnid,
    };
    format.writeStruct(format.Meta, page_bytes[format.page_header_size..][0..format.meta_body_size], meta_value);
}

fn writeLeafPage(page_bytes: []u8, pgno: format.Pgno, entries: []const LeafEntry) !void {
    @memset(page_bytes, 0);

    var upper = page_bytes.len;
    for (entries, 0..) |entry, i| {
        const node_len = node.header_size + entry.key.len + entry.value.len;
        upper -= node_len;

        const hdr = node.Header{
            .mn_lo = @intCast(entry.value.len & 0xffff),
            .mn_hi = @intCast((entry.value.len >> 16) & 0xffff),
            .mn_flags = 0,
            .mn_ksize = @intCast(entry.key.len),
        };
        format.writeStruct(node.Header, page_bytes[upper..][0..node.header_size], hdr);
        @memcpy(page_bytes[upper + node.header_size .. upper + node.header_size + entry.key.len], entry.key);
        @memcpy(
            page_bytes[upper + node.header_size + entry.key.len .. upper + node.header_size + entry.key.len + entry.value.len],
            entry.value,
        );

        const ptr_offset = format.page_header_size + i * @sizeOf(format.Indx);
        format.writeNativeInt(format.Indx, page_bytes[ptr_offset..][0..@sizeOf(format.Indx)], @intCast(upper));
    }

    const lower = format.page_header_size + entries.len * @sizeOf(format.Indx);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.leaf,
        .mp_lower = @intCast(lower),
        .mp_upper = @intCast(upper),
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);
}

fn writeBranchPage(page_bytes: []u8, pgno: format.Pgno, entries: []const BranchEntry) !void {
    @memset(page_bytes, 0);

    var upper = page_bytes.len;
    for (entries, 0..) |entry, i| {
        const node_len = node.header_size + entry.key.len;
        upper -= node_len;

        const hdr = node.Header{
            .mn_lo = @intCast(entry.child_pgno & 0xffff),
            .mn_hi = @intCast((entry.child_pgno >> 16) & 0xffff),
            .mn_flags = @intCast((entry.child_pgno >> 32) & 0xffff),
            .mn_ksize = @intCast(entry.key.len),
        };
        format.writeStruct(node.Header, page_bytes[upper..][0..node.header_size], hdr);
        @memcpy(page_bytes[upper + node.header_size .. upper + node.header_size + entry.key.len], entry.key);

        const ptr_offset = format.page_header_size + i * @sizeOf(format.Indx);
        format.writeNativeInt(format.Indx, page_bytes[ptr_offset..][0..@sizeOf(format.Indx)], @intCast(upper));
    }

    const lower = format.page_header_size + entries.len * @sizeOf(format.Indx);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.branch,
        .mp_lower = @intCast(lower),
        .mp_upper = @intCast(upper),
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);
}

test "cursor first and next iterate across sibling leaves" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 5]u8 = undefined;
    writeMetaPage(bytes[0..page_size], 0, page_size, 2, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, page_size, 2, 2);
    try writeBranchPage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try writeLeafPage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "apple", .value = "1" },
        .{ .key = "banana", .value = "2" },
    });
    try writeLeafPage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "orange", .value = "3" },
        .{ .key = "pear", .value = "4" },
    });

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "cursor_iter.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/cursor_iter.mdb", .{tmp.sub_path});

    var env = try @import("env.zig").Environment.open(file_path, .{ .no_subdir = true });
    defer env.close();
    var txn = try txn_mod.Transaction.begin(&env, .{});
    defer txn.abort();

    var cursor = Cursor.init(&txn, txn_mod.main_dbi);
    try std.testing.expectEqualStrings("apple", (try cursor.first()).key);
    try std.testing.expectEqualStrings("banana", (try cursor.next()).key);
    try std.testing.expectEqualStrings("orange", (try cursor.next()).key);
    try std.testing.expectEqualStrings("pear", (try cursor.next()).key);
    try std.testing.expectError(error.NotFound, cursor.next());
}

test "cursor set, set_range, and get_current work across leaves" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const page_size = 4096;
    var bytes: [page_size * 5]u8 = undefined;
    writeMetaPage(bytes[0..page_size], 0, page_size, 2, 1);
    writeMetaPage(bytes[page_size .. page_size * 2], 1, page_size, 2, 2);
    try writeBranchPage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "m", .child_pgno = 4 },
    });
    try writeLeafPage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "apple", .value = "1" },
        .{ .key = "banana", .value = "2" },
    });
    try writeLeafPage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "orange", .value = "3" },
        .{ .key = "pear", .value = "4" },
    });

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "cursor_seek.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/cursor_seek.mdb", .{tmp.sub_path});

    var env = try @import("env.zig").Environment.open(file_path, .{ .no_subdir = true });
    defer env.close();
    var txn = try txn_mod.Transaction.begin(&env, .{});
    defer txn.abort();

    var cursor = Cursor.init(&txn, txn_mod.main_dbi);
    try std.testing.expectEqualStrings("orange", (try cursor.set("orange")).key);
    try std.testing.expectEqualStrings("orange", (try cursor.getCurrent()).key);
    try std.testing.expectEqualStrings("orange", (try cursor.setRange("blueberry")).key);
    try std.testing.expectEqualStrings("pear", (try cursor.setRange("pear")).key);
    try std.testing.expectError(error.NotFound, cursor.set("blueberry"));
    try std.testing.expectError(error.NotFound, cursor.setRange("zulu"));
}
