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
pub const collector_reserve = 4096; // 64 levels × 63 siblings, plus publication metadata.

fn ceil(n: u64, divisor: usize) u64 {
    return n / divisor + @intFromBool(n % divisor != 0);
}

pub fn snapshotPages(payload: usize, covered: u64, pending: usize) u64 {
    return 1 +| @max(@as(u64, 1), ceil(covered -| 1, (payload - 24) / 4)) +| ceil(pending, (payload - 16) / 48);
}

/// Capacity for a full metadata checkpoint, bounded DFS expansion, and two
/// recovery-slot advances. Uses scalar ledger sizes, never a graph/page scan.
/// Include counters for reserve pages themselves to reach the small fixed point.
pub fn retirementReservePages(payload: usize, covered: u64, pending: usize) u64 {
    const counters = (payload - 24) / 4;
    const events = (payload - 16) / 48;
    const advance = serviceJournalPages(payload, 1);
    const queue_pages = ceil(@as(u64, pending) +| collector_reserve, events);
    var reserve: u64 = 0;
    while (true) {
        const next = 1 +| ceil((covered -| 1) +| reserve, counters) +| queue_pages +| (2 *| advance);
        if (next == reserve) return reserve;
        reserve = next;
    }
}

/// Upper bound for a service journal: one index may release every key, a value
/// may discover 64 children, and housekeeping promotes the preceding batch's
/// completed pages. Allocation of journal pages also changes free counters.
pub fn serviceJournalPages(payload: usize, work: usize) u64 {
    const per_page = (payload - 16) / 56;
    const entries = @as(u64, work) *| (4 * (payload / 18 + 2) + 64 + 16) +| 32;
    var pages: u64 = 1;
    while (true) {
        const next = 1 +| ceil(entries +| pages, per_page);
        if (next == pages) return pages;
        pages = next;
    }
}
pub const Kind = enum(u8) { record, index, value, metadata, page, allocator_chain };
pub const Retirement = struct {
    id: u64 = 0,
    epoch: u64,
    page: u64,
    length: u64 = 0,
    kind: Kind,
    previous: u64 = 0,
    next: u64 = 0,
};
pub const Root = struct {
    snapshot: u64 = 0,
    pending: u64 = 0,
    deltas: u64 = 0,
    next_id: u64 = 1,
    covered_pages: u64 = 1,
    delta_bytes: u64 = 0,
    delta_stop: u64 = 0,
    build_snapshot: u64 = 0,
    build_pending: u64 = 0,
    build_end: u64 = 0,
    build_cursor: u64 = 0,
    build_limit: u64 = 0,
    build_boundary: u64 = 0,
    build_delta_bytes: u64 = 0,
};
pub const IO = struct {
    context: *anyopaque,
    allocate: *const fn (*anyopaque) anyerror!u64,
    read: *const fn (*anyopaque, Allocator, u64) anyerror![]u8,
    write: *const fn (*anyopaque, u64, []const u8) anyerror!void,
    payload_bytes: usize,
    cancel_requested: ?*const std.atomic.Value(bool) = null,
    fn checkCancel(self: IO) !void {
        if (self.cancel_requested) |flag| if (flag.load(.acquire)) return error.MaintenanceCanceled;
    }
};

const Heap = std.PriorityQueue(Retirement, void, order);
fn order(_: void, a: Retirement, b: Retirement) std.math.Order {
    const epoch = std.math.order(a.epoch, b.epoch);
    return if (epoch == .eq) std.math.order(b.id, a.id) else epoch;
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
    metadata_heap: Heap,
    chains_heap: Heap,
    pending_limit: usize = max_pending_objects,
    changes: std.AutoHashMapUnmanaged(u64, u32) = .empty,
    added: std.ArrayList(Retirement) = .empty,
    completed: std.ArrayList(u64) = .empty,
    metadata: std.ArrayList(u64) = .empty,
    builder_metadata: std.ArrayList(u64) = .empty,
    first_pending: u64 = 0,
    last_pending: u64 = 0,
    incremental: bool = false,
    root: Root = .{},
    root_page: u64 = 0,
    allocations: u64 = 0,
    collected: u64 = 0,
    /// Unpublished data-page releases; harvested only at owner publication.
    released_data_pages: u64 = 0,
    retired_inline_bytes: u64 = 0,
    checkpointing: bool = false,
    force_checkpoint: bool = false,

    pub fn init(a: Allocator) State {
        return .{ .allocator = a, .heap = Heap.initContext({}), .metadata_heap = Heap.initContext({}), .chains_heap = Heap.initContext({}) };
    }
    pub fn deinit(self: *State) void {
        self.counts.deinit(self.allocator);
        for (&self.bitmap) |*level| level.deinit(self.allocator);
        self.pending.deinit(self.allocator);
        self.heap.deinit(self.allocator);
        self.metadata_heap.deinit(self.allocator);
        self.chains_heap.deinit(self.allocator);
        self.changes.deinit(self.allocator);
        self.added.deinit(self.allocator);
        self.completed.deinit(self.allocator);
        self.metadata.deinit(self.allocator);
        self.builder_metadata.deinit(self.allocator);
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
    /// A retired object's bytes can still be required by the rollback/fallback
    /// allocator queue. Removing ownership does not make that object reusable
    /// until its completion has crossed both recovery slots.
    pub fn releaseDeferred(self: *State, page: u64) !bool {
        const current = self.count(page);
        if (current == 0 or current >= free) return error.InvalidNativeAllocator;
        try self.set(page, current - 1, true);
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
    fn queue(self: *State, kind: Kind) *Heap {
        return switch (kind) {
            .metadata => &self.metadata_heap,
            .allocator_chain => &self.chains_heap,
            else => &self.heap,
        };
    }
    pub fn retire(self: *State, item: Retirement) !void {
        if (self.pending.count() >= self.pending_limit) return error.LiteRetirementBacklogExceeded;
        return self.retireCollected(item);
    }
    pub fn retireCollected(self: *State, item: Retirement) !void {
        // Bound owner metadata debt independently of reader lifetime. The caller
        // can keep reads available and report pressure when this limit is hit.
        if (self.pending.count() >= self.pending_limit + collector_reserve) return error.LiteRetirementBacklogExceeded;
        var owned = item;
        owned.id = self.root.next_id;
        self.root.next_id = try std.math.add(u64, owned.id, 1);
        try self.added.append(self.allocator, owned);
        owned.previous = self.last_pending;
        try self.pending.put(self.allocator, owned.id, owned);
        if (self.last_pending != 0) self.pending.getPtr(self.last_pending).?.next = owned.id else self.first_pending = owned.id;
        self.last_pending = owned.id;
        try self.queue(owned.kind).push(self.allocator, owned);
        if (owned.kind != .metadata and owned.kind != .allocator_chain) self.data_pending += 1;
    }
    pub fn oldest(self: *State, frontier: u64) ?Retirement {
        return self.eligible(frontier, frontier);
    }
    pub fn eligible(self: *State, data_frontier: u64, metadata_frontier: u64) ?Retirement {
        for ([_]*Heap{ &self.metadata_heap, &self.chains_heap, &self.heap }, [_]u64{ metadata_frontier, metadata_frontier, data_frontier }) |q, frontier| {
            if (q.peek()) |item| if (item.epoch <= frontier) return item;
        }
        return null;
    }
    pub fn eligibleWork(self: *State, data_frontier: u64, metadata_frontier: u64) ?Retirement {
        if (self.chains_heap.peek()) |item| if (item.epoch <= metadata_frontier) return item;
        if (self.heap.peek()) |item| if (item.epoch <= data_frontier) return item;
        return null;
    }
    pub fn complete(self: *State, item: Retirement) !void {
        try self.completed.append(self.allocator, item.id);
        const first = self.queue(item.kind).pop() orelse return error.InvalidNativeAllocator;
        if (first.id != item.id) return error.InvalidNativeAllocator;
        const linked = self.pending.get(item.id) orelse return error.InvalidNativeAllocator;
        if (linked.previous != 0) self.pending.getPtr(linked.previous).?.next = linked.next else self.first_pending = linked.next;
        if (linked.next != 0) self.pending.getPtr(linked.next).?.previous = linked.previous else self.last_pending = linked.previous;
        if (self.root.build_cursor == item.id) self.root.build_cursor = linked.next;
        _ = self.pending.remove(item.id);
        self.collected +|= 1;
        if (item.kind != .metadata and item.kind != .allocator_chain) self.data_pending -= 1;
    }
    /// Private-image publication discards all earlier private recovery roots.
    /// Rebase pending epochs and checkpoint the queue at that final boundary.
    pub fn rebaseEpochs(self: *State, epoch: u64, cancel_requested: ?*const std.atomic.Value(bool)) !void {
        self.heap.clearRetainingCapacity();
        self.metadata_heap.clearRetainingCapacity();
        self.chains_heap.clearRetainingCapacity();
        var items = self.pending.valueIterator();
        while (items.next()) |item| {
            if (cancel_requested) |flag| if (flag.load(.acquire)) return error.MaintenanceCanceled;
            item.epoch = @min(item.epoch, epoch);
            try self.queue(item.kind).push(self.allocator, item.*);
        }
        self.force_checkpoint = true;
    }

    pub fn clean(self: *State) void {
        self.changes.clearRetainingCapacity();
        self.added.clearRetainingCapacity();
        self.completed.clearRetainingCapacity();
    }

    pub fn encodeRoot(root: Root, out: *[128]u8) void {
        @memset(out, 0);
        @memcpy(out[0..8], "AFL4ALOC");
        put64(out, 8, root.snapshot);
        put64(out, 16, root.pending);
        put64(out, 24, root.deltas);
        put64(out, 32, root.next_id);
        put64(out, 40, root.covered_pages);
        put64(out, 48, root.delta_bytes);
        put64(out, 56, 2);
        inline for (.{ "delta_stop", "build_snapshot", "build_pending", "build_end", "build_cursor", "build_limit", "build_boundary", "build_delta_bytes" }, 0..) |name, i| put64(out, 64 + i * 8, @field(root, name));
    }
    pub fn decodeRoot(raw: []const u8) !Root {
        if ((raw.len != 64 and raw.len != 128) or !std.mem.eql(u8, raw[0..8], "AFL4ALOC") or (get64(raw, 56) != 1 and get64(raw, 56) != 2) or (get64(raw, 56) == 2 and raw.len != 128)) return error.InvalidNativeAllocator;
        var root: Root = .{ .snapshot = get64(raw, 8), .pending = get64(raw, 16), .deltas = get64(raw, 24), .next_id = get64(raw, 32), .covered_pages = get64(raw, 40), .delta_bytes = get64(raw, 48) };
        if (get64(raw, 56) == 2) inline for (.{ "delta_stop", "build_snapshot", "build_pending", "build_end", "build_cursor", "build_limit", "build_boundary", "build_delta_bytes" }, 0..) |name, i| {
            @field(root, name) = get64(raw, 64 + i * 8);
        };
        if (root.next_id == 0 or root.covered_pages == 0 or root.snapshot == 0) return error.InvalidNativeAllocator;
        if (root.delta_stop >= root.covered_pages or root.build_boundary >= root.covered_pages) return error.InvalidNativeAllocator;
        if (root.build_limit == 0) {
            if (root.build_snapshot != 0 or root.build_pending != 0 or root.build_end != 0 or root.build_cursor != 0 or root.build_boundary != 0 or root.build_delta_bytes != 0) return error.InvalidNativeAllocator;
        } else if (root.build_limit > root.next_id or root.build_end == 0 or root.build_end > root.covered_pages or root.build_cursor >= root.next_id or root.build_snapshot == 0) return error.InvalidNativeAllocator;
        return root;
    }

    pub fn load(a: Allocator, io: IO, root_page: u64) !State {
        try io.checkCancel();
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
            try io.checkCancel();
            try self.visit(page, &visited);
            const payload = try io.read(io.context, a, page);
            defer a.free(payload);
            if (payload.len < 24 or !std.mem.eql(u8, payload[0..4], "L4SS")) return error.InvalidNativeAllocator;
            const first = get64(payload, 16);
            const n = (payload.len - 24) / 4;
            if ((payload.len - 24) % 4 != 0 or first != snapshot_first or first > self.root.covered_pages or n > self.root.covered_pages - first) return error.InvalidNativeAllocator;
            for (0..n) |i| {
                try io.checkCancel();
                const value = std.mem.readInt(u32, payload[24 + i * 4 ..][0..4], .little);
                try self.set(first + i, value, false);
            }
            snapshot_first = first + n;
            page = get64(payload, 8);
        }
        page = self.root.pending;
        while (page != 0) {
            try io.checkCancel();
            try self.visit(page, &visited);
            const payload = try io.read(io.context, a, page);
            defer a.free(payload);
            if (payload.len < 16 or !std.mem.eql(u8, payload[0..4], "L4RQ") or (payload.len - 16) % 48 != 0) return error.InvalidNativeAllocator;
            var offset: usize = 16;
            while (offset < payload.len) : (offset += 48) {
                try io.checkCancel();
                const item = try decodeRetirement(payload[offset..][0..48]);
                if (self.pending.contains(item.id) or self.pending.count() >= max_pending_objects + collector_reserve) return error.InvalidNativeAllocator;
                try self.pending.put(a, item.id, item);
            }
            page = get64(payload, 8);
        }
        var deltas: std.ArrayList(u64) = .empty;
        defer deltas.deinit(a);
        page = self.root.deltas;
        var before_build_boundary = true;
        while (page != self.root.delta_stop) {
            if (page == 0) return error.InvalidNativeAllocator;
            try io.checkCancel();
            try self.visit(page, &visited);
            if (page == self.root.build_boundary) before_build_boundary = false;
            if (self.root.build_limit != 0 and before_build_boundary) try self.builder_metadata.append(a, page);
            try deltas.append(a, page);
            const payload = try io.read(io.context, a, page);
            defer a.free(payload);
            if (payload.len < 16 or !std.mem.eql(u8, payload[0..4], "L4DL") or (payload.len - 16) % 56 != 0) return error.InvalidNativeAllocator;
            page = get64(payload, 8);
        }
        var i = deltas.items.len;
        while (i > 0) {
            try io.checkCancel();
            i -= 1;
            const payload = try io.read(io.context, a, deltas.items[i]);
            defer a.free(payload);
            var offset: usize = 16;
            while (offset < payload.len) : (offset += 56) {
                try io.checkCancel();
                const entry = payload[offset..][0..56];
                switch (entry[0]) {
                    1 => {
                        const changed_page = get64(entry, 8);
                        if (changed_page >= self.root.covered_pages) return error.InvalidNativeAllocator;
                        try self.set(changed_page, std.math.cast(u32, get64(entry, 16)) orelse return error.InvalidNativeAllocator, false);
                    },
                    2 => {
                        const item = try decodeRetirement(entry[8..][0..48]);
                        if (self.pending.get(item.id)) |existing| {
                            // The first builder batch can sample additions
                            // whose journal is published in that same batch.
                            if (get64(raw, 56) == 1 or existing.epoch != item.epoch or existing.page != item.page or existing.length != item.length or existing.kind != item.kind) return error.InvalidNativeAllocator;
                        }
                        try self.pending.put(a, item.id, item);
                    },
                    3 => {
                        const id = get64(entry, 8);
                        if (id == 0 or id >= self.root.next_id) return error.InvalidNativeAllocator;
                        if (!self.pending.remove(id) and get64(raw, 56) == 1) return error.InvalidNativeAllocator;
                    },
                    else => return error.InvalidNativeAllocator,
                }
            }
        }
        if (self.pending.count() > max_pending_objects + collector_reserve) return error.InvalidNativeAllocator;
        var ids: std.ArrayList(u64) = .empty;
        defer ids.deinit(a);
        var keys = self.pending.keyIterator();
        while (keys.next()) |id| try ids.append(a, id.*);
        std.mem.sort(u64, ids.items, {}, std.sort.asc(u64));
        for (ids.items) |id| {
            try io.checkCancel();
            const item = self.pending.getPtr(id).?;
            item.previous = self.last_pending;
            if (self.last_pending != 0) self.pending.getPtr(self.last_pending).?.next = id else self.first_pending = id;
            self.last_pending = id;
            if (item.id >= self.root.next_id or item.page == 0) return error.InvalidNativeAllocator;
            try self.queue(item.kind).push(a, item.*);
            if (item.kind != .metadata and item.kind != .allocator_chain) self.data_pending += 1;
        }
        // The stop page is a live sentinel, even though replay excludes its
        // payload. Reusing its ID could otherwise stop at an unrelated new page.
        if (self.root.delta_stop != 0) try self.visit(self.root.delta_stop, &visited);
        for ([_]u64{ self.root.build_snapshot, self.root.build_pending }, [_][]const u8{ "L4SS", "L4RQ" }) |head, magic| {
            page = head;
            while (page != 0) {
                try io.checkCancel();
                try self.visit(page, &visited);
                try self.builder_metadata.append(a, page);
                const payload = try io.read(io.context, a, page);
                defer a.free(payload);
                if (payload.len < 16 or !std.mem.eql(u8, payload[0..4], magic)) return error.InvalidNativeAllocator;
                page = get64(payload, 8);
            }
        }
        if (self.root.build_cursor != 0 and !self.pending.contains(self.root.build_cursor)) return error.InvalidNativeAllocator;
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
    pub fn preparePersist(self: *State, epoch: u64, payload_bytes: usize) !void {
        if (self.root_page != 0) try self.retireCollected(.{ .epoch = epoch, .page = self.root_page, .kind = .metadata });
        const delta_size = self.deltaFootprint(payload_bytes);
        const threshold = @max(@as(u64, 256 * 1024), @as(u64, self.counts.items.len) * 8 + self.pending.count() * 96);
        self.checkpointing = self.root.snapshot == 0 or (!self.incremental and self.root.build_limit == 0 and self.root.delta_bytes +| delta_size >= threshold);
        // Small tables can checkpoint in one bounded publication without an
        // intermediate builder/journal generation. Larger owner tables always
        // use the resumable builder; private images may normalize in full.
        if (self.force_checkpoint and (!self.incremental or (self.root.build_limit == 0 and snapshotPages(payload_bytes, self.counts.items.len +| 8, self.pending.count() +| 4) <= 5))) self.checkpointing = true;
        if (self.checkpointing) {
            if (self.root.delta_stop != 0) try self.retireCollected(.{ .epoch = epoch, .page = self.root.delta_stop, .kind = .metadata });
            // These pages remain reachable from the fallback allocator root.
            for ([_]u64{ self.root.snapshot, self.root.pending, self.root.deltas }) |old| {
                if (old != 0 and old != self.root.delta_stop) try self.retireCollected(.{ .epoch = epoch, .page = old, .length = if (old == self.root.deltas) self.root.delta_stop else 0, .kind = .allocator_chain });
            }
        }
    }
    /// Reserve this many metadata pages BEFORE encoding counters. Allocation
    /// consumes free bits; serializing first would advertise its own pages free.
    /// Re-evaluate after reserving until the small fixed point is reached.
    fn deltaFootprint(self: *State, payload_bytes: usize) u64 {
        const entries = self.changes.count() + self.added.items.len + self.completed.items.len;
        const pages = std.math.divCeil(usize, entries, (payload_bytes - 16) / 56) catch unreachable;
        return @as(u64, pages) * payload_bytes;
    }
    pub fn pagesRequired(self: *State, payload_bytes: usize) usize {
        if (self.checkpointing) return @max(@as(usize, 1), std.math.divCeil(usize, self.counts.items.len -| 1, (payload_bytes - 24) / 4) catch unreachable) +
            (std.math.divCeil(usize, self.pending.count(), (payload_bytes - 16) / 48) catch unreachable);
        return std.math.divCeil(usize, self.changes.count() + self.added.items.len + self.completed.items.len, (payload_bytes - 16) / 56) catch unreachable;
    }
    pub fn persist(self: *State, io: IO, root_page: u64, covered: *u64) !void {
        try io.checkCancel();
        const delta_size = self.deltaFootprint(io.payload_bytes);
        if (self.checkpointing) {
            self.metadata.clearRetainingCapacity();
            self.root.snapshot = try self.writeCounts(io);
            self.root.pending = try self.writePending(io);
            self.root.deltas = 0;
            self.root.delta_bytes = 0;
            self.root.delta_stop = 0;
        } else {
            self.root.deltas = try self.writeDelta(io, self.root.deltas);
            self.root.delta_bytes +|= delta_size;
        }
        self.force_checkpoint = false;
        self.root.covered_pages = covered.*;
        try io.checkCancel();
        var payload: [128]u8 = undefined;
        encodeRoot(self.root, &payload);
        try io.write(io.context, root_page, &payload);
        self.root_page = root_page;
        self.clean();
    }
    pub fn checkpointDue(self: *const State) bool {
        const threshold = @max(@as(u64, 256 * 1024), @as(u64, self.counts.items.len) * 8 + self.pending.count() * 96);
        return self.root.build_limit != 0 or self.force_checkpoint or self.root.delta_bytes >= threshold;
    }

    /// One immutable snapshot page per call. Allocation must already have
    /// consumed its free bit. The caller publishes the builder roots together
    /// with the normal journal; foreground writers can run between calls.
    pub fn checkpointStep(self: *State, io: IO, page: u64, epoch: u64) !void {
        try io.checkCancel();
        if (self.root.build_limit == 0) {
            self.root.build_end = self.counts.items.len;
            self.root.build_cursor = self.first_pending;
            self.root.build_limit = self.root.next_id;
            self.root.build_boundary = self.root.deltas;
            self.root.build_delta_bytes = self.root.delta_bytes;
            self.builder_metadata.clearRetainingCapacity();
        }
        const payload = try self.allocator.alloc(u8, io.payload_bytes);
        defer self.allocator.free(payload);
        @memset(payload, 0);
        var used: usize = 0;
        if (self.root.build_end > 1) {
            const end: usize = @intCast(self.root.build_end);
            const start = @max(@as(usize, 1), end -| ((io.payload_bytes - 24) / 4));
            @memcpy(payload[0..4], "L4SS");
            put64(payload, 8, self.root.build_snapshot);
            put64(payload, 16, start);
            for (self.counts.items[start..end], 0..) |value, i| std.mem.writeInt(u32, payload[24 + i * 4 ..][0..4], value, .little);
            used = 24 + (end - start) * 4;
            try io.write(io.context, page, payload[0..used]);
            self.root.build_snapshot = page;
            self.root.build_end = start;
        } else {
            @memcpy(payload[0..4], "L4RQ");
            put64(payload, 8, self.root.build_pending);
            used = 16;
            while (self.root.build_cursor != 0 and self.root.build_cursor < self.root.build_limit and used + 48 <= payload.len) {
                const item = self.pending.get(self.root.build_cursor) orelse return error.InvalidNativeAllocator;
                encodeRetirement(item, payload[used..][0..48]);
                used += 48;
                self.root.build_cursor = item.next;
            }
            try io.write(io.context, page, payload[0..used]);
            self.root.build_pending = page;
        }
        try self.metadata.append(self.allocator, page);
        try self.builder_metadata.append(self.allocator, page);
        if (self.root.build_end == 1 and (self.root.build_cursor == 0 or self.root.build_cursor >= self.root.build_limit)) {
            for ([_]u64{ self.root.snapshot, self.root.pending }) |old| {
                if (old != 0) try self.retireCollected(.{ .epoch = epoch, .page = old, .kind = .allocator_chain });
            }
            if (self.root.build_boundary != 0) {
                const boundary = try io.read(io.context, self.allocator, self.root.build_boundary);
                defer self.allocator.free(boundary);
                const next = get64(boundary, 8);
                if (next != 0 and next != self.root.delta_stop) try self.retireCollected(.{ .epoch = epoch, .page = next, .length = self.root.delta_stop, .kind = .allocator_chain });
                try self.builder_metadata.append(self.allocator, self.root.build_boundary);
            }
            if (self.root.delta_stop != 0 and self.root.delta_stop != self.root.build_boundary) try self.retireCollected(.{ .epoch = epoch, .page = self.root.delta_stop, .kind = .metadata });
            self.root.snapshot = self.root.build_snapshot;
            self.root.pending = self.root.build_pending;
            self.root.delta_stop = self.root.build_boundary;
            self.root.delta_bytes -|= self.root.build_delta_bytes;
            self.root.build_snapshot = 0;
            self.root.build_pending = 0;
            self.root.build_end = 0;
            self.root.build_cursor = 0;
            self.root.build_limit = 0;
            self.root.build_boundary = 0;
            self.root.build_delta_bytes = 0;
            std.mem.swap(std.ArrayList(u64), &self.metadata, &self.builder_metadata);
            self.builder_metadata.clearRetainingCapacity();
            self.force_checkpoint = false;
        }
    }

    fn writeCounts(self: *State, io: IO) !u64 {
        const per_page = (io.payload_bytes - 24) / 4;
        if (per_page == 0) return error.InvalidNativeAllocator;
        var end = self.counts.items.len;
        var next: u64 = 0;
        while (end > 1) {
            try io.checkCancel();
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
            try io.checkCancel();
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
            try io.checkCancel();
            var item: [56]u8 = @splat(0);
            item[0] = 1;
            put64(&item, 8, entry.key_ptr.*);
            put64(&item, 16, entry.value_ptr.*);
            try self.appendDelta(io, payload, &used, &next, &item);
        }
        for (self.added.items) |event| {
            try io.checkCancel();
            var item: [56]u8 = @splat(0);
            item[0] = 2;
            encodeRetirement(event, item[8..][0..48]);
            try self.appendDelta(io, payload, &used, &next, &item);
        }
        for (self.completed.items) |id| {
            try io.checkCancel();
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
        try io.checkCancel();
        @memset(payload[0..16], 0);
        @memcpy(payload[0..4], "L4DL");
        put64(payload, 8, next.*);
        const page = try io.allocate(io.context);
        try io.write(io.context, page, payload[0 .. 16 + used * 56]);
        try self.metadata.append(self.allocator, page);
        if (self.root.build_limit != 0) try self.builder_metadata.append(self.allocator, page);
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
    raw[32] = @backingInt(item.kind);
}
fn decodeRetirement(raw: []const u8) !Retirement {
    if (get64(raw, 0) == 0) return error.InvalidNativeAllocator;
    for (raw[33..48]) |byte| if (byte != 0) return error.InvalidNativeAllocator;
    return .{ .id = get64(raw, 0), .epoch = get64(raw, 8), .page = get64(raw, 16), .length = get64(raw, 24), .kind = std.enums.fromInt(Kind, raw[32]) orelse return error.InvalidNativeAllocator };
}

const TestLedger = struct {
    state: *State,
    next: u64,
    pages: std.AutoHashMapUnmanaged(u64, []u8) = .empty,
    writes: usize = 0,
    cancel: ?*std.atomic.Value(bool) = null,
    cancel_after: ?usize = null,
    fn io(self: *TestLedger) IO {
        return .{ .context = self, .allocate = allocate, .read = read, .write = write, .payload_bytes = 128, .cancel_requested = self.cancel };
    }
    fn allocate(context: *anyopaque) !u64 {
        const self: *TestLedger = @ptrCast(@alignCast(context));
        const page = self.next;
        self.next += 1;
        try self.state.reserveTail(page);
        return page;
    }
    pub fn read(context: *anyopaque, a: Allocator, page: u64) ![]u8 {
        const self: *TestLedger = @ptrCast(@alignCast(context));
        return a.dupe(u8, self.pages.get(page) orelse return error.InvalidNativeAllocator);
    }
    fn write(context: *anyopaque, page: u64, payload: []const u8) !void {
        const self: *TestLedger = @ptrCast(@alignCast(context));
        const owned = try std.testing.allocator.dupe(u8, payload);
        errdefer std.testing.allocator.free(owned);
        if (try self.pages.fetchPut(std.testing.allocator, page, owned)) |old| std.testing.allocator.free(old.value);
        self.writes += 1;
        if (self.cancel_after) |limit| if (self.writes >= limit) self.cancel.?.store(true, .release);
    }
    fn publish(self: *TestLedger, epoch: u64) !void {
        const root = try allocate(self);
        try self.state.preparePersist(epoch, 128);
        try self.state.persist(self.io(), root, &self.next);
    }
    pub fn deinit(self: *TestLedger) void {
        var values = self.pages.valueIterator();
        while (values.next()) |bytes| std.testing.allocator.free(bytes.*);
        self.pages.deinit(std.testing.allocator);
    }
};

test "lite allocator v4 incremental checkpoints reconcile mutations and resume after reopen" {
    const a = std.testing.allocator;
    var state = State.init(a);
    defer state.deinit();
    var disk = TestLedger{ .state = &state, .next = 1001 };
    defer disk.deinit();
    for (1..1001) |page| try state.retain(page);
    try state.retire(.{ .epoch = 1, .page = 10, .kind = .page });
    try disk.publish(1);
    state.incremental = true;
    try state.retain(500);
    try disk.publish(2);
    state.force_checkpoint = true;
    var rounds: usize = 0;
    while (state.checkpointDue()) : (rounds += 1) {
        try std.testing.expect(rounds < 100);
        const before = disk.writes;
        const page = try TestLedger.allocate(&disk);
        try state.checkpointStep(disk.io(), page, rounds + 2);
        try std.testing.expectEqual(before + 1, disk.writes);
        if (rounds == 0) {
            const item = state.heap.peek().?;
            try state.complete(item);
            _ = try state.releaseDeferred(item.page);
            // The first counter chunk has already sampled this page.
            try state.retain(999);
            try state.retire(.{ .epoch = 1, .page = 20, .kind = .page });
        }
        try disk.publish(rounds + 2);
        const root = state.root_page;
        state.deinit();
        state = try State.load(a, disk.io(), root);
        state.incremental = true;
    }
    try std.testing.expect(rounds > 30);
    try std.testing.expectEqual(@as(u32, 2), state.count(999));
    try std.testing.expectEqual(@as(u32, 0), state.count(10));
    try std.testing.expectEqual(@as(u64, 1), state.data_pending);
    try std.testing.expect(state.root.delta_stop != 0);
}

test "lite allocator v4 canceled persistence leaves the durable root unchanged" {
    const a = std.testing.allocator;
    var state = State.init(a);
    defer state.deinit();
    var disk = TestLedger{ .state = &state, .next = 101 };
    defer disk.deinit();
    for (1..101) |page| try state.retain(page);
    try disk.publish(1);
    const root = state.root_page;
    var cancel = std.atomic.Value(bool).init(true);
    disk.cancel = &cancel;
    const writes = disk.writes;
    state.checkpointing = true;
    try std.testing.expectError(error.MaintenanceCanceled, state.persist(disk.io(), 999, &disk.next));
    try std.testing.expectEqual(writes, disk.writes);
    cancel.store(false, .release);
    disk.cancel_after = writes + 2;
    try std.testing.expectError(error.MaintenanceCanceled, state.persist(disk.io(), 999, &disk.next));
    try std.testing.expectEqual(writes + 2, disk.writes);
    cancel.store(false, .release);
    disk.cancel_after = null;
    var restored = try State.load(a, disk.io(), root);
    defer restored.deinit();
    try std.testing.expectEqual(@as(u32, 1), restored.count(100));
}

test "lite allocator v4 accepts version one roots and rejects unknown builder encodings" {
    const expected: Root = .{ .snapshot = 2, .next_id = 5, .covered_pages = 10 };
    var encoded: [128]u8 = undefined;
    State.encodeRoot(expected, &encoded);
    put64(&encoded, 56, 1);
    try std.testing.expectEqualDeep(expected, try State.decodeRoot(encoded[0..64]));
    put64(&encoded, 56, 3);
    try std.testing.expectError(error.InvalidNativeAllocator, State.decodeRoot(&encoded));
    State.encodeRoot(expected, &encoded);
    put64(&encoded, 88, 11); // A builder cannot scan outside the covered table.
    put64(&encoded, 104, 5);
    try std.testing.expectError(error.InvalidNativeAllocator, State.decodeRoot(&encoded));
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

test "lite allocator v4 saturated admission preserves depth first collector space" {
    var state = State.init(std.testing.allocator);
    defer state.deinit();
    state.pending_limit = 8;
    for (1..9) |page| try state.retire(.{ .epoch = 1, .page = page, .kind = .value });
    try std.testing.expectError(error.LiteRetirementBacklogExceeded, state.retire(.{ .epoch = 1, .page = 9, .kind = .value }));
    const parent = state.oldest(1).?;
    try state.complete(parent);
    // Expand one full extent at a time. Newly discovered children must be
    // serviced before older siblings, including at the admission limit.
    for (0..63) |depth| {
        for (0..64) |child| try state.retireCollected(.{ .epoch = 1, .page = 1000 + depth * 64 + child, .kind = .value });
        const next = state.oldest(1).?;
        try std.testing.expectEqual(@as(u64, 1000 + depth * 64 + 63), next.page);
        try state.complete(next);
    }
    try std.testing.expect(state.pending.count() < state.pending_limit + collector_reserve);
    while (state.oldest(1)) |item| try state.complete(item);
    try std.testing.expectEqual(@as(u64, 0), state.data_pending);
    try state.retire(.{ .epoch = 2, .page = 1, .kind = .value });
}

test "lite allocator v4 checkpoints retire chains without queue bursts" {
    var state = State.init(std.testing.allocator);
    defer state.deinit();
    state.root_page = 1;
    state.root.snapshot = 2;
    state.root.pending = 3;
    state.root.deltas = 4;
    state.force_checkpoint = true;
    for (2..10000) |page| try state.metadata.append(std.testing.allocator, page);
    try state.preparePersist(5, 4080);
    try std.testing.expectEqual(@as(usize, 4), state.pending.count());
    try std.testing.expectEqual(@as(u64, 0), state.data_pending);
    try state.retire(.{ .epoch = 3, .page = 10000, .kind = .value });
    // A data reader at epoch 2 does not inspect allocator metadata. Recovery
    // slots at epoch 5 still protect every page that can be reused.
    const root = state.eligible(2, 5).?;
    try std.testing.expectEqual(Kind.metadata, root.kind);
    try state.complete(root);
    for (0..3) |_| {
        const chain = state.eligible(2, 5).?;
        try std.testing.expectEqual(Kind.allocator_chain, chain.kind);
        try state.complete(chain);
    }
    try std.testing.expect(state.eligible(2, 5) == null);
    try std.testing.expectEqual(Kind.value, state.eligible(3, 5).?.kind);
}

test "lite allocator v4 sparse journal pages count toward checkpoint threshold" {
    var state = State.init(std.testing.allocator);
    defer state.deinit();
    state.root.snapshot = 1;
    state.root.delta_bytes = 64 * 4080;
    try state.retain(2);
    try state.preparePersist(1, 4080);
    try std.testing.expect(state.checkpointing);
}

test "lite allocator v4 capacity reserve scales with snapshots and queue width" {
    const payload = 4080;
    const small = retirementReservePages(payload, 3, 0);
    const million = retirementReservePages(payload, 1_000_000, 0);
    const wide = retirementReservePages(payload, 1_000_000, max_pending_objects);
    try std.testing.expect(small < 128);
    try std.testing.expect(million < 1200);
    try std.testing.expect(wide < 15000);
    // The reserve covers a checkpoint even after the entire DFS reserve is
    // occupied and allocating its own pages grows the counter table.
    try std.testing.expect(wide >= snapshotPages(payload, 1_000_000 + wide, max_pending_objects + collector_reserve) + 2 * serviceJournalPages(payload, 1));
}
