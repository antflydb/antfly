// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Checkpoint-owned ownership ledger. Commits append changed counters and
//! retirement events; periodic metadata checkpoints bound replay and write
//! amplification. Data and values are never rewritten to replenish free space.
//! A hierarchical bitmap locates free runs without scanning the page table.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const free: u32 = 0x80000000;
pub const max_references: u32 = free - 1;
pub const max_pending_objects = 1024 * 1024;
pub const Kind = enum(u8) { record, index, value, metadata, page };
pub const Retirement = struct {
    id: u64 = 0,
    epoch: u64,
    page: u64,
    length: u64 = 0,
    kind: Kind,
};
pub const Root = struct {
    snapshot: u64 = 0,
    pending: u64 = 0,
    deltas: u64 = 0,
    next_id: u64 = 1,
    covered_pages: u64 = 1,
    delta_bytes: u64 = 0,
};
pub const IO = struct {
    context: *anyopaque,
    allocate: *const fn (*anyopaque) anyerror!u64,
    read: *const fn (*anyopaque, Allocator, u64) anyerror![]u8,
    write: *const fn (*anyopaque, u64, []const u8) anyerror!void,
    payload_bytes: usize,
};

const Heap = std.PriorityQueue(Retirement, void, order);
fn order(_: void, a: Retirement, b: Retirement) std.math.Order {
    const epoch = std.math.order(a.epoch, b.epoch);
    return if (epoch == .eq) std.math.order(a.id, b.id) else epoch;
}

pub const State = struct {
    allocator: Allocator,
    counts: std.ArrayList(u32) = .empty,
    // Level 0 covers 64 physical pages; higher levels summarize nonempty words.
    bitmap: [8]std.ArrayList(u64) = @splat(.empty),
    free_pages: u64 = 0,
    data_pending: u64 = 0,
    pending: std.AutoHashMapUnmanaged(u64, Retirement) = .empty,
    heap: Heap,
    changes: std.AutoHashMapUnmanaged(u64, u32) = .empty,
    added: std.ArrayList(Retirement) = .empty,
    completed: std.ArrayList(u64) = .empty,
    metadata: std.ArrayList(u64) = .empty,
    root: Root = .{},
    root_page: u64 = 0,
    allocations: u64 = 0,
    collected: u64 = 0,
    checkpointing: bool = false,
    force_checkpoint: bool = false,

    pub fn init(a: Allocator) State {
        return .{ .allocator = a, .heap = Heap.initContext({}) };
    }
    pub fn deinit(self: *State) void {
        self.counts.deinit(self.allocator);
        for (&self.bitmap) |*level| level.deinit(self.allocator);
        self.pending.deinit(self.allocator);
        self.heap.deinit(self.allocator);
        self.changes.deinit(self.allocator);
        self.added.deinit(self.allocator);
        self.completed.deinit(self.allocator);
        self.metadata.deinit(self.allocator);
        self.* = undefined;
    }
    fn ensure(self: *State, page: u64) !void {
        const needed = std.math.cast(usize, std.math.add(u64, page, 1) catch return error.InvalidNativeAllocator) orelse return error.InvalidNativeAllocator;
        if (needed <= self.counts.items.len) return;
        const old = self.counts.items.len;
        try self.counts.resize(self.allocator, needed);
        @memset(self.counts.items[old..], 0);
        var words = (needed + 63) / 64;
        for (&self.bitmap) |*level| {
            const before = level.items.len;
            if (words > before) {
                try level.resize(self.allocator, words);
                @memset(level.items[before..], 0);
            }
            words = (words + 63) / 64;
        }
    }
    pub fn reserveTail(self: *State, page: u64) !void {
        try self.ensure(page);
        if (self.count(page) != 0) return error.InvalidNativeAllocator;
    }
    fn setBitmap(self: *State, page: u64, available: bool) void {
        var index: usize = @intCast(page);
        var present = available;
        for (&self.bitmap) |*level| {
            const word = &level.items[index / 64];
            const mask = @as(u64, 1) << @as(u6, @intCast(index % 64));
            const before = word.* != 0;
            if (present) word.* |= mask else word.* &= ~mask;
            present = word.* != 0;
            if (before == present) break;
            index /= 64;
        }
    }
    fn set(self: *State, page: u64, references: u32, journal: bool) !void {
        if (page == 0 or (references >= free and references != free)) return error.InvalidNativeAllocator;
        try self.ensure(page);
        const previous = self.counts.items[@intCast(page)];
        if (journal) try self.changes.put(self.allocator, page, references);
        if (previous == free) self.free_pages -= 1;
        if (references == free) self.free_pages += 1;
        self.counts.items[@intCast(page)] = references;
        self.setBitmap(page, references == free);
    }
    pub fn count(self: *const State, page: u64) u32 {
        return if (page < self.counts.items.len) self.counts.items[@intCast(page)] else 0;
    }
    pub fn retain(self: *State, page: u64) !void {
        const current = self.count(page);
        if (current == free or current == max_references) return error.InvalidNativeAllocator;
        try self.set(page, current + 1, true);
    }
    /// Only the last removed ownership can retire children of a shared value.
    pub fn release(self: *State, page: u64) !bool {
        const current = self.count(page);
        if (current == 0 or current >= free) return error.InvalidNativeAllocator;
        try self.set(page, if (current == 1) free else current - 1, true);
        return current == 1;
    }
    pub fn releaseMetadata(self: *State, page: u64) !void {
        if (self.count(page) != 0) return error.InvalidNativeAllocator;
        try self.set(page, free, true);
    }
    pub fn allocate(self: *State) !?u64 {
        if (self.free_pages == 0) return null;
        var index: usize = 0;
        var level: usize = self.bitmap.len;
        while (level > 0) {
            level -= 1;
            if (index >= self.bitmap[level].items.len) return error.InvalidNativeAllocator;
            const word = self.bitmap[level].items[index];
            if (word == 0) return error.InvalidNativeAllocator;
            index = index * 64 + @as(usize, @intCast(@ctz(word)));
        }
        if (self.count(index) != free) return error.InvalidNativeAllocator;
        try self.set(index, 0, true);
        self.allocations +|= 1;
        return index;
    }
    pub fn retire(self: *State, item: Retirement) !void {
        // Bound owner metadata debt independently of reader lifetime. The caller
        // can keep reads available and report pressure when this limit is hit.
        if (self.pending.count() >= max_pending_objects) return error.LiteRetirementBacklogExceeded;
        var owned = item;
        owned.id = self.root.next_id;
        self.root.next_id = try std.math.add(u64, owned.id, 1);
        try self.added.append(self.allocator, owned);
        try self.pending.put(self.allocator, owned.id, owned);
        try self.heap.push(self.allocator, owned);
        if (owned.kind != .metadata) self.data_pending += 1;
    }
    pub fn oldest(self: *State, frontier: u64) ?Retirement {
        const item = self.heap.peek() orelse return null;
        return if (item.epoch <= frontier) item else null;
    }
    pub fn complete(self: *State, item: Retirement) !void {
        try self.completed.append(self.allocator, item.id);
        const first = self.heap.pop() orelse return error.InvalidNativeAllocator;
        if (first.id != item.id or !self.pending.remove(item.id)) return error.InvalidNativeAllocator;
        self.collected +|= 1;
        if (item.kind != .metadata) self.data_pending -= 1;
    }
    /// Private-image publication discards all earlier private recovery roots.
    /// Rebase pending epochs and checkpoint the queue at that final boundary.
    pub fn rebaseEpochs(self: *State, epoch: u64) !void {
        self.heap.clearRetainingCapacity();
        var items = self.pending.valueIterator();
        while (items.next()) |item| {
            item.epoch = @min(item.epoch, epoch);
            try self.heap.push(self.allocator, item.*);
        }
        self.force_checkpoint = true;
    }

    pub fn clean(self: *State) void {
        self.changes.clearRetainingCapacity();
        self.added.clearRetainingCapacity();
        self.completed.clearRetainingCapacity();
    }

    pub fn encodeRoot(root: Root, out: *[64]u8) void {
        @memset(out, 0);
        @memcpy(out[0..8], "AFL4ALOC");
        put64(out, 8, root.snapshot);
        put64(out, 16, root.pending);
        put64(out, 24, root.deltas);
        put64(out, 32, root.next_id);
        put64(out, 40, root.covered_pages);
        put64(out, 48, root.delta_bytes);
        put64(out, 56, 1);
    }
    pub fn decodeRoot(raw: []const u8) !Root {
        if (raw.len != 64 or !std.mem.eql(u8, raw[0..8], "AFL4ALOC") or get64(raw, 56) != 1) return error.InvalidNativeAllocator;
        const root: Root = .{ .snapshot = get64(raw, 8), .pending = get64(raw, 16), .deltas = get64(raw, 24), .next_id = get64(raw, 32), .covered_pages = get64(raw, 40), .delta_bytes = get64(raw, 48) };
        if (root.next_id == 0 or root.covered_pages == 0 or root.snapshot == 0) return error.InvalidNativeAllocator;
        return root;
    }

    pub fn load(a: Allocator, io: IO, root_page: u64) !State {
        var self = State.init(a);
        errdefer self.deinit();
        const raw = try io.read(io.context, a, root_page);
        defer a.free(raw);
        self.root = try decodeRoot(raw);
        self.root_page = root_page;
        try self.ensure(self.root.covered_pages - 1);
        var visited: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer visited.deinit(a);
        var snapshot_first: u64 = 1;
        var page = self.root.snapshot;
        while (page != 0) {
            try self.visit(page, &visited);
            const payload = try io.read(io.context, a, page);
            defer a.free(payload);
            if (payload.len < 24 or !std.mem.eql(u8, payload[0..4], "L4SS")) return error.InvalidNativeAllocator;
            const first = get64(payload, 16);
            const n = (payload.len - 24) / 4;
            if ((payload.len - 24) % 4 != 0 or first != snapshot_first or first > self.root.covered_pages or n > self.root.covered_pages - first) return error.InvalidNativeAllocator;
            for (0..n) |i| {
                const value = std.mem.readInt(u32, payload[24 + i * 4 ..][0..4], .little);
                try self.set(first + i, value, false);
            }
            snapshot_first = first + n;
            page = get64(payload, 8);
        }
        page = self.root.pending;
        while (page != 0) {
            try self.visit(page, &visited);
            const payload = try io.read(io.context, a, page);
            defer a.free(payload);
            if (payload.len < 16 or !std.mem.eql(u8, payload[0..4], "L4RQ") or (payload.len - 16) % 48 != 0) return error.InvalidNativeAllocator;
            var offset: usize = 16;
            while (offset < payload.len) : (offset += 48) {
                const item = try decodeRetirement(payload[offset..][0..48]);
                if (self.pending.contains(item.id) or self.pending.count() >= max_pending_objects) return error.InvalidNativeAllocator;
                try self.pending.put(a, item.id, item);
            }
            page = get64(payload, 8);
        }
        var deltas: std.ArrayList(u64) = .empty;
        defer deltas.deinit(a);
        page = self.root.deltas;
        while (page != 0) {
            try self.visit(page, &visited);
            try deltas.append(a, page);
            const payload = try io.read(io.context, a, page);
            defer a.free(payload);
            if (payload.len < 16 or !std.mem.eql(u8, payload[0..4], "L4DL") or (payload.len - 16) % 56 != 0) return error.InvalidNativeAllocator;
            page = get64(payload, 8);
        }
        var i = deltas.items.len;
        while (i > 0) {
            i -= 1;
            const payload = try io.read(io.context, a, deltas.items[i]);
            defer a.free(payload);
            var offset: usize = 16;
            while (offset < payload.len) : (offset += 56) {
                const entry = payload[offset..][0..56];
                switch (entry[0]) {
                    1 => {
                        const changed_page = get64(entry, 8);
                        if (changed_page >= self.root.covered_pages) return error.InvalidNativeAllocator;
                        try self.set(changed_page, std.math.cast(u32, get64(entry, 16)) orelse return error.InvalidNativeAllocator, false);
                    },
                    2 => {
                        const item = try decodeRetirement(entry[8..][0..48]);
                        if (self.pending.contains(item.id) or self.pending.count() >= max_pending_objects) return error.InvalidNativeAllocator;
                        try self.pending.put(a, item.id, item);
                    },
                    3 => if (!self.pending.remove(get64(entry, 8))) return error.InvalidNativeAllocator,
                    else => return error.InvalidNativeAllocator,
                }
            }
        }
        var items = self.pending.valueIterator();
        while (items.next()) |item| {
            if (item.id >= self.root.next_id or item.page == 0) return error.InvalidNativeAllocator;
            try self.heap.push(a, item.*);
            if (item.kind != .metadata) self.data_pending += 1;
        }
        for (self.metadata.items) |metadata_page| if (self.count(metadata_page) == free) return error.InvalidNativeAllocator;
        if (self.count(root_page) == free) return error.InvalidNativeAllocator;
        self.clean();
        return self;
    }
    fn visit(self: *State, page: u64, visited: *std.AutoHashMapUnmanaged(u64, void)) !void {
        if (page == 0 or page >= self.root.covered_pages or visited.contains(page)) return error.InvalidNativeAllocator;
        try visited.put(self.allocator, page, {});
        try self.metadata.append(self.allocator, page);
    }

    /// Metadata snapshots amortize O(page count) work over at least twice their
    /// encoded size in counter changes. Snapshotting never touches value bytes.
    pub fn preparePersist(self: *State, epoch: u64) !void {
        if (self.root_page != 0) try self.retire(.{ .epoch = epoch, .page = self.root_page, .kind = .metadata });
        const delta_size = (self.changes.count() + self.added.items.len + self.completed.items.len) * 56;
        const threshold = @max(@as(u64, 256 * 1024), @as(u64, self.counts.items.len) * 8 + self.pending.count() * 96);
        self.checkpointing = self.force_checkpoint or self.root.snapshot == 0 or self.root.delta_bytes +| delta_size >= threshold;
        if (self.checkpointing) {
            // These pages remain reachable from the fallback allocator root.
            for (self.metadata.items) |old| try self.retire(.{ .epoch = epoch, .page = old, .kind = .metadata });
        }
    }
    /// Reserve this many metadata pages BEFORE encoding counters. Allocation
    /// consumes free bits; serializing first would advertise its own pages free.
    /// Re-evaluate after reserving until the small fixed point is reached.
    pub fn pagesRequired(self: *State, payload_bytes: usize) usize {
        if (self.checkpointing) return @max(@as(usize, 1), std.math.divCeil(usize, self.counts.items.len -| 1, (payload_bytes - 24) / 4) catch unreachable) +
            (std.math.divCeil(usize, self.pending.count(), (payload_bytes - 16) / 48) catch unreachable);
        return std.math.divCeil(usize, self.changes.count() + self.added.items.len + self.completed.items.len, (payload_bytes - 16) / 56) catch unreachable;
    }
    pub fn persist(self: *State, io: IO, root_page: u64, covered: *u64) !void {
        const delta_size = (self.changes.count() + self.added.items.len + self.completed.items.len) * 56;
        if (self.checkpointing) {
            self.metadata.clearRetainingCapacity();
            self.root.snapshot = try self.writeCounts(io);
            self.root.pending = try self.writePending(io);
            self.root.deltas = 0;
            self.root.delta_bytes = 0;
        } else {
            self.root.deltas = try self.writeDelta(io, self.root.deltas);
            self.root.delta_bytes +|= delta_size;
        }
        self.force_checkpoint = false;
        self.root.covered_pages = covered.*;
        var payload: [64]u8 = undefined;
        encodeRoot(self.root, &payload);
        try io.write(io.context, root_page, &payload);
        self.root_page = root_page;
        self.clean();
    }
    fn writeCounts(self: *State, io: IO) !u64 {
        const per_page = (io.payload_bytes - 24) / 4;
        if (per_page == 0) return error.InvalidNativeAllocator;
        var end = self.counts.items.len;
        var next: u64 = 0;
        while (end > 1) {
            const start = @max(@as(usize, 1), end -| per_page);
            const payload = try self.allocator.alloc(u8, 24 + (end - start) * 4);
            defer self.allocator.free(payload);
            @memset(payload, 0);
            @memcpy(payload[0..4], "L4SS");
            put64(payload, 8, next);
            put64(payload, 16, start);
            for (self.counts.items[start..end], 0..) |value, i| std.mem.writeInt(u32, payload[24 + i * 4 ..][0..4], value, .little);
            const page = try io.allocate(io.context);
            try io.write(io.context, page, payload);
            try self.metadata.append(self.allocator, page);
            next = page;
            end = start;
        }
        // Empty files still have a canonical, nonzero snapshot identity.
        if (next == 0) {
            var payload: [28]u8 = @splat(0);
            @memcpy(payload[0..4], "L4SS");
            put64(&payload, 16, 1);
            const page = try io.allocate(io.context);
            try io.write(io.context, page, &payload);
            try self.metadata.append(self.allocator, page);
            next = page;
        }
        return next;
    }
    fn writePending(self: *State, io: IO) !u64 {
        const per_page = (io.payload_bytes - 16) / 48;
        var it = self.pending.valueIterator();
        var next: u64 = 0;
        const payload = try self.allocator.alloc(u8, 16 + per_page * 48);
        defer self.allocator.free(payload);
        while (it.next()) |first| {
            @memset(payload, 0);
            @memcpy(payload[0..4], "L4RQ");
            put64(payload, 8, next);
            encodeRetirement(first.*, payload[16..][0..48]);
            var n: usize = 1;
            while (n < per_page) : (n += 1) {
                const item = it.next() orelse break;
                encodeRetirement(item.*, payload[16 + n * 48 ..][0..48]);
            }
            const page = try io.allocate(io.context);
            try io.write(io.context, page, payload[0 .. 16 + n * 48]);
            try self.metadata.append(self.allocator, page);
            next = page;
        }
        return next;
    }
    fn writeDelta(self: *State, io: IO, previous: u64) !u64 {
        const per_page = (io.payload_bytes - 16) / 56;
        const payload = try self.allocator.alloc(u8, 16 + per_page * 56);
        defer self.allocator.free(payload);
        var next = previous;
        var used: usize = 0;
        var changes = self.changes.iterator();
        while (changes.next()) |entry| {
            var item: [56]u8 = @splat(0);
            item[0] = 1;
            put64(&item, 8, entry.key_ptr.*);
            put64(&item, 16, entry.value_ptr.*);
            try self.appendDelta(io, payload, &used, &next, &item);
        }
        for (self.added.items) |event| {
            var item: [56]u8 = @splat(0);
            item[0] = 2;
            encodeRetirement(event, item[8..][0..48]);
            try self.appendDelta(io, payload, &used, &next, &item);
        }
        for (self.completed.items) |id| {
            var item: [56]u8 = @splat(0);
            item[0] = 3;
            put64(&item, 8, id);
            try self.appendDelta(io, payload, &used, &next, &item);
        }
        if (used != 0) try self.flushDelta(io, payload, used, &next);
        return next;
    }
    fn appendDelta(self: *State, io: IO, payload: []u8, used: *usize, next: *u64, item: *const [56]u8) !void {
        if (16 + (used.* + 1) * 56 > payload.len) {
            try self.flushDelta(io, payload, used.*, next);
            used.* = 0;
        }
        @memcpy(payload[16 + used.* * 56 ..][0..56], item);
        used.* += 1;
    }
    fn flushDelta(self: *State, io: IO, payload: []u8, used: usize, next: *u64) !void {
        @memset(payload[0..16], 0);
        @memcpy(payload[0..4], "L4DL");
        put64(payload, 8, next.*);
        const page = try io.allocate(io.context);
        try io.write(io.context, page, payload[0 .. 16 + used * 56]);
        try self.metadata.append(self.allocator, page);
        next.* = page;
    }
};
fn put64(raw: []u8, offset: usize, value: u64) void {
    std.mem.writeInt(u64, raw[offset..][0..8], value, .little);
}
fn get64(raw: []const u8, offset: usize) u64 {
    return std.mem.readInt(u64, raw[offset..][0..8], .little);
}
fn encodeRetirement(item: Retirement, raw: []u8) void {
    @memset(raw, 0);
    put64(raw, 0, item.id);
    put64(raw, 8, item.epoch);
    put64(raw, 16, item.page);
    put64(raw, 24, item.length);
    raw[32] = @intFromEnum(item.kind);
}
fn decodeRetirement(raw: []const u8) !Retirement {
    if (get64(raw, 0) == 0) return error.InvalidNativeAllocator;
    for (raw[33..48]) |byte| if (byte != 0) return error.InvalidNativeAllocator;
    return .{ .id = get64(raw, 0), .epoch = get64(raw, 8), .page = get64(raw, 16), .length = get64(raw, 24), .kind = std.enums.fromInt(Kind, raw[32]) orelse return error.InvalidNativeAllocator };
}

test "lite allocator v4 free bitmap scales beyond one free-map page" {
    var state = State.init(std.testing.allocator);
    defer state.deinit();
    for (1..16385) |page| {
        try state.retain(page);
        try std.testing.expect(try state.release(page));
    }
    try std.testing.expectEqual(@as(u64, 16384), state.free_pages);
    for (1..16385) |page| try std.testing.expectEqual(@as(?u64, page), try state.allocate());
    try std.testing.expectEqual(@as(?u64, null), try state.allocate());
    try std.testing.expectEqual(@as(u64, 0), state.free_pages);
}
test "lite allocator v4 retirement preserves shared ownership and oldest epoch" {
    var state = State.init(std.testing.allocator);
    defer state.deinit();
    try state.retain(1);
    try state.retain(1);
    try std.testing.expect(!try state.release(1));
    try std.testing.expectEqual(@as(u32, 1), state.count(1));
    try state.retire(.{ .epoch = 10, .page = 1, .kind = .value });
    try state.retire(.{ .epoch = 3, .page = 2, .kind = .metadata });
    try std.testing.expect(state.oldest(2) == null);
    const oldest = state.oldest(3).?;
    try std.testing.expectEqual(@as(u64, 2), oldest.page);
    try state.complete(oldest);
    try std.testing.expect(state.oldest(9) == null);
    try std.testing.expectEqual(@as(u64, 1), state.oldest(10).?.page);
}
