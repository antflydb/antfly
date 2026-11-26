// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Request-private full and sliding-window KV page tables. The backend owns
//! payload writes and fences them before committing or aborting an append.
const std = @import("std");
const block = @import("block.zig");
const table = @import("block_table.zig");
const storage = @import("storage.zig");
const writable = @import("writable_blocks.zig");

pub const Group = enum { full, sliding };

pub const Layout = struct {
    page_tokens: u16,
    window_tokens: usize,
    max_tokens: usize,
    max_append_tokens: usize,

    fn pageEnd(self: Layout, tokens: usize) usize {
        const page: usize = self.page_tokens;
        return tokens / page + @intFromBool(tokens % page != 0);
    }

    fn firstPage(self: Layout, group: Group, committed_tokens: usize) usize {
        return switch (group) {
            .full => 0,
            .sliding => (committed_tokens -| self.window_tokens) / self.page_tokens,
        };
    }

    /// Bound the live page IDs, including the previous committed window and
    /// the staged suffix. The local span can begin partway through a page.
    pub fn pageCapacity(self: Layout, group: Group) !usize {
        if (self.page_tokens == 0 or self.window_tokens == 0 or
            self.max_tokens == 0 or self.max_append_tokens == 0) return error.InvalidKvShape;
        const full = self.pageEnd(self.max_tokens);
        if (group == .full) return full;
        const page: usize = self.page_tokens;
        const live = @min(self.max_tokens, self.window_tokens +| self.max_append_tokens);
        return @min(full, live / page + self.pageEnd(live % page + page - 1));
    }
};

pub const PlaneFormat = enum {
    bf16,
    q4_head128,

    pub fn rowBytes(self: PlaneFormat, kv_heads: u32, head_dim: u32) !usize {
        if (kv_heads == 0 or head_dim == 0) return error.InvalidKvShape;
        const head_bytes: usize = switch (self) {
            .bf16 => @as(usize, head_dim) * @sizeOf(u16),
            .q4_head128 => if (head_dim == 128) 32 * @sizeOf(u16) + @sizeOf(f16) else return error.InvalidKvShape,
        };
        return std.math.mul(usize, kv_heads, head_bytes);
    }
};

/// Resident bank geometry for independently stored full and sliding visits.
pub const BankConfig = struct {
    layout: Layout,
    full_slots: u16,
    sliding_slots: u16,
    kv_heads: u32,
    head_dim: u32,
    full_format: PlaneFormat = .bf16,

    pub fn slots(self: BankConfig, group: Group) u16 {
        return switch (group) {
            .full => self.full_slots,
            .sliding => self.sliding_slots,
        };
    }

    pub fn rowBytes(self: BankConfig) !usize {
        return PlaneFormat.bf16.rowBytes(self.kv_heads, self.head_dim);
    }

    pub fn format(self: BankConfig, group: Group) PlaneFormat {
        return if (group == .full) self.full_format else .bf16;
    }

    pub fn deviceBytes(self: BankConfig, group: Group) !usize {
        const slot_count = self.slots(group);
        if (slot_count == 0) return error.InvalidKvShape;
        return std.math.mul(usize, try self.planeBytes(group), @as(usize, slot_count) * 2);
    }

    pub fn planeBytes(self: BankConfig, group: Group) !usize {
        const rows = try std.math.mul(usize, try self.layout.pageCapacity(group), self.layout.page_tokens);
        return std.math.mul(usize, rows, try self.format(group).rowBytes(self.kv_heads, self.head_dim));
    }
};

pub const Visit = struct {
    group: Group,
    slot: u16,
};

/// A staged logical range and the physical geometry of its payload planes.
pub const StagedRange = struct {
    first_page: usize,
    page_count: usize,
    physical_pages: usize,
    page_tokens: u16,
    visible_start: usize,
    visible_end: usize,

    pub fn physicalRows(self: StagedRange) !usize {
        if (self.page_tokens == 0 or self.physical_pages == 0 or self.physical_pages > std.math.maxInt(u32) or
            self.visible_start >= self.visible_end or
            self.first_page != self.visible_start / self.page_tokens or
            self.page_count != (self.visible_end - 1) / self.page_tokens + 1 - self.first_page)
            return error.InvalidAttentionGeometry;
        return std.math.mul(usize, self.physical_pages, self.page_tokens);
    }
};

const PageGroup = struct {
    pool: *storage.KvStorage,
    pages: table.SequenceBlockTable = .{},
    reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty,

    fn deinit(self: *PageGroup, allocator: std.mem.Allocator) void {
        for (self.pages.blocks.items) |id|
            _ = self.pool.releaseRef(allocator, id) catch unreachable;
        std.debug.assert(self.reserved.items.len == 0);
        self.pages.deinit(allocator);
        self.reserved.deinit(allocator);
    }

    fn prepare(self: *PageGroup, allocator: std.mem.Allocator, row_count: usize, new_pages: usize, window: ?usize) !writable.PreparedAppend {
        // Do not count pages retired at commit toward acquisition headroom.
        if (self.pool.hostPool().?.availableBlocks()) |available| {
            if (new_pages > available) return error.KvCapacityTooSmall;
        }
        return writable.prepareAppend(self.pool, &self.pages, &self.reserved, allocator, row_count, 0, window, 0, false, null);
    }
};

/// The caller serializes access and keeps the sequence and both canonical
/// metadata pools at stable addresses for the lifetime of a prepared append.
pub const Sequence = struct {
    allocator: std.mem.Allocator,
    layout: Layout,
    full: PageGroup,
    sliding: PageGroup,
    committed_tokens: usize = 0,
    append_pending: bool = false,

    pub fn init(allocator: std.mem.Allocator, layout: Layout, full: *storage.KvStorage, sliding: *storage.KvStorage) !Sequence {
        _ = try layout.pageCapacity(.sliding);
        if (full.ptr == sliding.ptr or !full.hasCanonicalHostBlockOwnership() or
            !sliding.hasCanonicalHostBlockOwnership() or full.config.page_size_tokens != layout.page_tokens or
            sliding.config.page_size_tokens != layout.page_tokens) return error.InvalidKvShape;
        return .{ .allocator = allocator, .layout = layout, .full = .{ .pool = full }, .sliding = .{ .pool = sliding } };
    }

    pub fn deinit(self: *Sequence) void {
        std.debug.assert(!self.append_pending);
        self.full.deinit(self.allocator);
        self.sliding.deinit(self.allocator);
    }

    pub fn pageId(self: *const Sequence, group: Group, logical_page: usize) ?block.KvBlockId {
        const pages = switch (group) {
            .full => self.full.pages.blocks.items,
            .sliding => self.sliding.pages.blocks.items,
        };
        const first = self.layout.firstPage(group, self.committed_tokens);
        if (logical_page < first or logical_page - first >= pages.len) return null;
        return pages[logical_page - first];
    }

    pub fn prepareAppend(self: *Sequence, row_count: usize) !Append {
        if (self.append_pending) return error.KvAppendPending;
        if (row_count == 0 or row_count > self.layout.max_append_tokens or
            row_count > self.layout.max_tokens - self.committed_tokens) return error.KvCapacityTooSmall;

        const next_tokens = self.committed_tokens + row_count;
        const new_pages = self.layout.pageEnd(next_tokens) - self.layout.pageEnd(self.committed_tokens);
        var full = try self.full.prepare(self.allocator, row_count, new_pages, null);
        errdefer full.abort();
        const sliding = try self.sliding.prepare(self.allocator, row_count, new_pages, self.layout.window_tokens);
        self.append_pending = true;
        return .{ .sequence = self, .next_tokens = next_tokens, .full = full, .sliding = sliding };
    }
};

pub const Append = struct {
    sequence: *Sequence,
    next_tokens: usize,
    full: writable.PreparedAppend,
    sliding: writable.PreparedAppend,
    active: bool = true,

    pub fn stagedRange(self: *const Append, group: Group) !StagedRange {
        if (!self.active) return error.KvAppendMissing;
        const layout = self.sequence.layout;
        const start = if (group == .sliding) self.sequence.committed_tokens -| layout.window_tokens else 0;
        const first = layout.firstPage(group, self.sequence.committed_tokens);
        return .{
            .first_page = first,
            .page_count = layout.pageEnd(self.next_tokens) - first,
            .physical_pages = try layout.pageCapacity(group),
            .page_tokens = layout.page_tokens,
            .visible_start = start,
            .visible_end = self.next_tokens,
        };
    }

    pub fn stagedPageIds(self: *Append, group: Group, ids: []u32) !usize {
        const range = try self.stagedRange(group);
        if (ids.len < range.page_count) return error.KvCapacityTooSmall;
        for (ids[0..range.page_count], range.first_page..) |*id, logical|
            id.* = self.pageId(group, logical) orelse return error.InvalidKvRange;
        return range.page_count;
    }

    /// Old rows are readable during the append. Payload writes may touch only
    /// rows at or beyond the committed cursor and before next_tokens.
    pub fn pageId(self: *Append, group: Group, logical_page: usize) ?block.KvBlockId {
        if (!self.active) return null;
        const first = self.sequence.layout.firstPage(group, self.sequence.committed_tokens);
        if (logical_page < first) return null;
        const staged = switch (group) {
            .full => &self.full,
            .sliding => &self.sliding,
        };
        return staged.stagedPageId(logical_page - first);
    }

    /// Call after successful completion of any payload work. The caller may
    /// then publish other request state before releasing staging metadata.
    pub fn commit(self: *Append) void {
        std.debug.assert(self.active);
        self.full.commit();
        self.sliding.commit();
        self.sequence.committed_tokens = self.next_tokens;
        self.sequence.append_pending = false;
        self.active = false;
    }

    pub fn commitPrefix(self: *Append, rows: usize) !void {
        if (!self.active or rows == 0 or rows > self.next_tokens - self.sequence.committed_tokens)
            return error.InvalidKvRange;
        if (rows < self.next_tokens - self.sequence.committed_tokens) {
            try self.full.retainPrivatePrefix(rows, null);
            try self.sliding.retainPrivatePrefix(rows, self.sequence.layout.window_tokens);
            self.next_tokens = self.sequence.committed_tokens + rows;
        }
        self.commit();
    }

    /// Defer after prepare, including on commit. Submitted writes must be
    /// drained before an uncommitted append returns its pages to the pools.
    pub fn abort(self: *Append) void {
        self.sliding.abort();
        self.full.abort();
        if (self.active) self.sequence.append_pending = false;
        self.active = false;
    }
};

/// Maps a linear token range to contiguous physical rows within each page.
pub const RowSpans = struct {
    sequence: *const Sequence,
    append: ?*Append,
    group: Group,
    start: usize,
    end: usize,
    position: usize,
    physical_pages: usize,

    pub const Span = struct {
        linear_row: usize,
        physical_row: usize,
        rows: usize,
    };

    pub fn init(sequence: *const Sequence, append: ?*Append, group: Group, start: usize, end: usize) !RowSpans {
        const oldest = if (group == .sliding) sequence.committed_tokens -| sequence.layout.window_tokens else 0;
        const available_end = if (append) |staged| blk: {
            if (!staged.active or staged.sequence != sequence) return error.InvalidKvRange;
            break :blk staged.next_tokens;
        } else sequence.committed_tokens;
        if (start < oldest or end < start or end > available_end) return error.InvalidKvRange;
        return .{
            .sequence = sequence,
            .append = append,
            .group = group,
            .start = start,
            .end = end,
            .position = start,
            .physical_pages = try sequence.layout.pageCapacity(group),
        };
    }

    fn physicalPage(self: *const RowSpans, logical: usize) !usize {
        const page = (if (self.append) |staged| staged.pageId(self.group, logical) else self.sequence.pageId(self.group, logical)) orelse
            return error.InvalidKvRange;
        if (page >= self.physical_pages) return error.InvalidKvRange;
        return page;
    }

    /// Extend the copy only across consecutive physical pages. Partial pages
    /// and recycled slots retain the same logical-to-physical boundaries.
    pub fn next(self: *RowSpans) !?Span {
        if (self.position == self.end) return null;
        const tokens = self.sequence.layout.page_tokens;
        const within = self.position % tokens;
        const page = try self.physicalPage(self.position / tokens);
        var span: Span = .{
            .linear_row = self.position - self.start,
            .physical_row = page * tokens + within,
            .rows = @min(self.end - self.position, tokens - within),
        };
        var frontier = self.position + span.rows;
        while (frontier < self.end) {
            const next_page = try self.physicalPage(frontier / tokens);
            if (next_page * tokens != span.physical_row + span.rows) break;
            const count = @min(self.end - frontier, tokens);
            span.rows += count;
            frontier += count;
        }
        self.position = frontier;
        return span;
    }
};

const inkling_test_layout: Layout = .{ .page_tokens = 16, .window_tokens = 1024, .max_tokens = 32768, .max_append_tokens = 256 };

fn testPool(layer_count: u16) !storage.KvStorage {
    return storage.initHost(std.testing.allocator, .{
        .backend = .cuda,
        .dtype = .bf16,
        .page_size_tokens = inkling_test_layout.page_tokens,
        .num_layers_packed = layer_count,
        .num_kv_heads = 8,
        .head_dim = 128,
        .store_cpu_bytes = false,
    });
}

test "hybrid paged KV coalesces contiguous rows and splits at a physical gap" {
    const a = std.testing.allocator;
    var full = try testPool(8);
    defer full.deinit(a);
    var sliding = try testPool(40);
    defer sliding.deinit(a);
    var sequence = try Sequence.init(a, inkling_test_layout, &full, &sliding);
    defer sequence.deinit();
    var initial = try sequence.prepareAppend(16);
    defer initial.abort();
    initial.commit();

    const pool = full.hostPool().?;
    const gap = try pool.acquire(a);
    defer _ = pool.releaseRef(a, gap) catch unreachable;
    var suffix = try sequence.prepareAppend(31);
    defer suffix.abort();
    var spans = try RowSpans.init(&sequence, &suffix, .full, 3, 47);
    try std.testing.expectEqual(RowSpans.Span{ .linear_row = 0, .physical_row = 3, .rows = 13 }, (try spans.next()).?);
    try std.testing.expectEqual(RowSpans.Span{ .linear_row = 13, .physical_row = 32, .rows = 31 }, (try spans.next()).?);
    try std.testing.expectEqual(null, try spans.next());
}

test "hybrid paged KV bounds sliding pages through long and misaligned prefills" {
    const allocator = std.testing.allocator;
    const layout = inkling_test_layout;
    var full = try testPool(8);
    defer full.deinit(allocator);
    var sliding = try testPool(40);
    defer sliding.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2048), try layout.pageCapacity(.full));
    try std.testing.expectEqual(@as(usize, 81), try layout.pageCapacity(.sliding));
    sliding.hostPool().?.setTargetMaxBlocks(try layout.pageCapacity(.sliding));
    var sequence = try Sequence.init(allocator, layout, &full, &sliding);
    defer sequence.deinit();

    while (sequence.committed_tokens < layout.max_tokens) {
        const before = sequence.committed_tokens;
        const count = @min(if (before == 0) 1 else layout.max_append_tokens, layout.max_tokens - before);
        var append = try sequence.prepareAppend(count);
        defer append.abort();
        const first_local = (before -| 1024) / 16;
        const last = (before + count - 1) / 16;
        try std.testing.expect(append.pageId(.sliding, first_local) != null);
        if (first_local > 0) try std.testing.expectEqual(null, append.pageId(.sliding, first_local - 1));
        try std.testing.expect(append.pageId(.sliding, last) != null);
        try std.testing.expectEqual(null, append.pageId(.sliding, last + 1));
        const range = try append.stagedRange(.sliding);
        try std.testing.expectEqual(before -| layout.window_tokens, range.visible_start);
        try std.testing.expectEqual(before + count, range.visible_end);
        try std.testing.expectEqual(first_local, range.first_page);
        try std.testing.expectEqual(last + 1 - first_local, range.page_count);
        var ids: [81]u32 = undefined;
        try std.testing.expectEqual(range.page_count, try append.stagedPageIds(.sliding, &ids));
        for (ids[0..range.page_count], first_local..) |id, logical|
            try std.testing.expectEqual(append.pageId(.sliding, logical).?, id);
        try std.testing.expectEqual(@as(usize, 81 * 16), try range.physicalRows());
        try std.testing.expectEqual(before, sequence.committed_tokens);
        append.commit();
    }
    try std.testing.expectEqual(@as(usize, 81), sliding.hostPool().?.liveBlocks());
    try std.testing.expect(sequence.pageId(.full, 0) != null);
    try std.testing.expectEqual(null, sequence.pageId(.sliding, 0));
    try std.testing.expectError(error.KvCapacityTooSmall, sequence.prepareAppend(1));
}

test "hybrid paged KV keeps an accepted prefix at a page and sliding-window boundary" {
    const allocator = std.testing.allocator;
    var full = try testPool(8);
    defer full.deinit(allocator);
    var sliding = try testPool(40);
    defer sliding.deinit(allocator);
    var sequence = try Sequence.init(allocator, inkling_test_layout, &full, &sliding);
    defer sequence.deinit();
    for ([_]usize{ 256, 256, 256, 256, 14 }) |rows| {
        var append = try sequence.prepareAppend(rows);
        defer append.abort();
        append.commit();
    }

    const oldest = sequence.pageId(.sliding, 0);
    const committed_tail = sequence.pageId(.full, 64);
    {
        var speculative = try sequence.prepareAppend(8);
        defer speculative.abort();
        const rejected_page = speculative.pageId(.full, 65);
        try std.testing.expect(rejected_page != null);
        try speculative.commitPrefix(1);
        try std.testing.expectEqual(@as(usize, 1039), sequence.committed_tokens);
        try std.testing.expectEqual(oldest, sequence.pageId(.sliding, 0));
        try std.testing.expectEqual(committed_tail, sequence.pageId(.full, 64));
        try std.testing.expectEqual(null, sequence.pageId(.full, 65));
    }
    {
        var speculative = try sequence.prepareAppend(8);
        defer speculative.abort();
        const accepted_page = speculative.pageId(.full, 65);
        try speculative.commitPrefix(3);
        try std.testing.expectEqual(@as(usize, 1042), sequence.committed_tokens);
        try std.testing.expectEqual(null, sequence.pageId(.sliding, 0));
        try std.testing.expectEqual(accepted_page, sequence.pageId(.full, 65));
    }
}

test "hybrid paged KV preserves both groups when prepare or append aborts" {
    const allocator = std.testing.allocator;
    var full = try testPool(8);
    defer full.deinit(allocator);
    var sliding = try testPool(40);
    defer sliding.deinit(allocator);
    var sequence = try Sequence.init(allocator, inkling_test_layout, &full, &sliding);
    defer sequence.deinit();
    for (0..4) |_| {
        var append = try sequence.prepareAppend(256);
        defer append.abort();
        append.commit();
    }
    const old_sliding = sequence.pageId(.sliding, 0).?;
    sliding.hostPool().?.setTargetMaxBlocks(64);
    try std.testing.expectError(error.KvCapacityTooSmall, sequence.prepareAppend(16));
    try std.testing.expectEqual(@as(usize, 1), full.hostPool().?.freeListBlocks());
    try std.testing.expectEqual(@as(usize, 1024), sequence.committed_tokens);
    try std.testing.expectEqual(null, sequence.pageId(.full, 64));
    try std.testing.expectEqual(old_sliding, sequence.pageId(.sliding, 0).?);

    sliding.hostPool().?.setTargetMaxBlocks(65);
    {
        var append = try sequence.prepareAppend(16);
        defer append.abort();
        try std.testing.expectError(error.KvAppendPending, sequence.prepareAppend(1));
        try std.testing.expectEqual(old_sliding, append.pageId(.sliding, 0).?);
        try std.testing.expect(append.pageId(.sliding, 64) != null);
    }
    try std.testing.expectEqual(@as(usize, 1024), sequence.committed_tokens);
    try std.testing.expectEqual(old_sliding, sequence.pageId(.sliding, 0).?);
    try std.testing.expectEqual(null, sequence.pageId(.sliding, 64));

    var retry = try sequence.prepareAppend(16);
    defer retry.abort();
    retry.commit();
    try std.testing.expectEqual(@as(usize, 1040), sequence.committed_tokens);
    try std.testing.expectEqual(null, sequence.pageId(.sliding, 0));
    try std.testing.expectEqual(@as(usize, 1), sliding.hostPool().?.freeListBlocks());
}
