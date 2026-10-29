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

//! Bounded native reclamation over the existing Antfly transactional engine.
//! HEAD owns the active job; a captured metadata root owns phase and marks.
const std = @import("std");
const objects = @import("objectstore");
const tree = @import("../graph_segment/page_tree.zig");
const Wal = @import("indexed_object_store.zig").IndexedObjectStore;
const kv = objects.transactional_store;
const journal = objects.immutable_log;
const A = std.mem.Allocator;
const max_inventory: u32 = 16;
const child_chunk = 60;
const max_cursor_bytes = 12 * 1024;

const Cursor = struct {
    after: []const u8 = "",
    token: ?[]const u8 = null,
    key: ?[]const u8 = null,
    version: ?[]const u8 = null,
    fn validate(self: @This(), versions: bool, prefix: []const u8) !void {
        for ([_]?[]const u8{ self.after, self.token, self.key, self.version }) |text| if (text) |value| {
            if (value.len > 4096) return error.CollectionCursorTooLarge;
        };
        if (versions) {
            if (self.after.len != 0 or (self.token != null and self.key != null) or (self.version != null and self.key == null)) return error.InvalidCollectionCursor;
            if (self.key) |key| if (!std.mem.startsWith(u8, key, prefix)) return error.InvalidCollectionCursor;
        } else if (self.token != null or self.key != null or self.version != null or
            (self.after.len != 0 and !std.mem.startsWith(u8, self.after, prefix))) return error.InvalidCollectionCursor;
    }
};
const Meta = struct {
    format: u8 = 1,
    id: [16]u8,
    retired: u64,
    phase: enum { mark, pages, heads, complete } = .mark,
    next: u64 = 0,
    done: u64 = 0,
    child_offset: u32 = 0,
    versions: bool,
    cursor: Cursor = .{},
    marked: u64 = 0,
    deleted: u64 = 0,
};

pub const Collector = struct {
    alloc: A,
    wal: *Wal,
    client: objects.Client,
    io: std.Io,
    owner: [16]u8,
    versions: ?bool = null,
    hints: [64]Hint = @splat(.{}),
    hint_hand: usize = 0,
    last_now_ns: u64 = 0,
    next_work_ns: u64 = 0,
    step_interval_ns: u64 = 250 * std.time.ns_per_ms,
    mutex: std.atomic.Mutex = .unlocked,

    /// Restart/takeover waits again. No cross-host timestamp authorizes delete.
    pub const grace_ns = 60 * std.time.ns_per_s;
    pub const Result = struct {
        phase: enum { idle, deferred, cleaning, compacting, marking, waiting, sweeping, finished } = .idle,
        deleted_objects: usize = 0,
        marked_pages: usize = 0,
    };
    const Hint = struct {
        namespace: ?[32]u8 = null,
        active: bool = false,
        finishing: bool = false,
        id: [16]u8 = @splat(0),
        retired: u64 = 0,
        observed_ns: u64 = 0,
        idle_until_ns: u64 = 0,
        dirty: bool = false,
        cleanup_done: bool = false,
        cleanup_cursor: [max_cursor_bytes]u8 = undefined,
        cleanup_len: usize = 0,
        state: ?journal.State = null,
        root: [journal.max_root_bytes]u8 = undefined,

        fn remember(self: *@This(), value: journal.State) !void {
            if (value.root.len > self.root.len) return error.InvalidCollectionState;
            @memcpy(self.root[0..value.root.len], value.root);
            self.state = value;
            self.state.?.root = self.root[0..value.root.len];
        }
        fn job(self: *@This(), value: Wal.ActiveCollection, now: u64) void {
            if (self.active and self.retired == value.roots.retired_before and std.mem.eql(u8, &self.id, &value.id)) return;
            self.active = true;
            self.finishing = false;
            self.id = value.id;
            self.retired = value.roots.retired_before;
            self.observed_ns = now;
            self.state = null;
            self.dirty = false;
        }
        fn rememberCursor(self: *@This(), a: A, cursor: Cursor) !void {
            const bytes = try std.json.Stringify.valueAlloc(a, cursor, .{});
            defer a.free(bytes);
            if (bytes.len > self.cleanup_cursor.len) return error.CollectionCursorTooLarge;
            @memcpy(self.cleanup_cursor[0..bytes.len], bytes);
            self.cleanup_len = bytes.len;
        }
    };

    /// Keep the collector at a stable address after its first operation.
    pub fn init(a: A, wal: *Wal, io: std.Io) Collector {
        var owner: [16]u8 = undefined;
        io.random(&owner);
        if (std.mem.allEqual(u8, &owner, 0)) owner[0] = 1;
        var client = wal.client;
        client.allocator = a;
        return .{ .alloc = a, .wal = wal, .client = client, .io = io, .owner = owner };
    }
    pub fn wake(self: *@This(), namespace: []const u8) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const digest = hash(namespace);
        for (&self.hints) |*hint| if (hint.namespace) |scope| if (std.mem.eql(u8, &scope, &digest)) {
            hint.dirty = true;
            hint.idle_until_ns = 0;
            return;
        };
    }
    pub fn isDue(self: *@This()) bool {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const now = self.wal.read_clock.read();
        return now == 0 or now == std.math.maxInt(u64) or now < self.last_now_ns or now >= self.next_work_ns;
    }
    pub fn namespaceDue(self: *@This(), namespace: []const u8) bool {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const now = self.wal.read_clock.read();
        const digest = hash(namespace);
        var available = false;
        for (&self.hints) |*hint| {
            if (!hint.active) available = true;
            if (hint.namespace) |scope| if (std.mem.eql(u8, &scope, &digest)) return hint.active or hint.finishing or hint.dirty or now >= hint.idle_until_ns;
        }
        return available;
    }
    pub fn deferWork(self: *@This()) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.next_work_ns = self.wal.read_clock.read() +| self.step_interval_ns;
    }
    fn hintFor(self: *@This(), namespace: []const u8) ?*Hint {
        const digest = hash(namespace);
        for (&self.hints) |*hint| if (hint.namespace) |scope| if (std.mem.eql(u8, &scope, &digest)) return hint;
        for (0..self.hints.len) |n| {
            const index = (self.hint_hand + n) % self.hints.len;
            if (self.hints[index].active) continue;
            self.hint_hand = (index + 1) % self.hints.len;
            self.hints[index] = .{ .namespace = digest };
            return &self.hints[index];
        }
        return null;
    }
    pub fn step(self: *@This(), namespace: []const u8, cancellation: ?objects.CancellationToken) !Result {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (!self.wal.managed_reads) return error.ManagedCollectionRequired;
        const now = self.wal.read_clock.read();
        if (now == 0 or now == std.math.maxInt(u64)) return error.CollectionClockUnavailable;
        if (now < self.last_now_ns) {
            for (&self.hints) |*hint| hint.observed_ns = now;
            self.last_now_ns = now;
            self.next_work_ns = now;
            return error.CollectionClockRollback;
        }
        self.last_now_ns = now;
        if (cancellation) |token| try token.check();
        if (now < self.next_work_ns) return .{};
        const hint = self.hintFor(namespace) orelse return .{ .phase = .deferred };
        if (!hint.active and !hint.finishing and !hint.dirty and now < hint.idle_until_ns) return .{};
        self.next_work_ns = now +| self.step_interval_ns;
        var clock_run = ClockRun{ .collector = self, .opened = now, .upstream = cancellation };
        const token = clock_run.token();
        const result = self.run(namespace, hint, token) catch |err| {
            // A new owner requires fresh recovery, never repeated stale writes.
            if (err == error.ImmutableLogFenced) hint.state = null;
            if (err == error.Canceled and !(if (cancellation) |signal| signal.isCancelled() else false) and ClockRun.canceled(&clock_run)) return error.CollectionStepExpired;
            return err;
        };
        return result;
    }
    fn run(self: *@This(), namespace: []const u8, hint: *Hint, token: objects.CancellationToken) !Result {
        if (self.versions == null) self.versions = try self.client.supportsObjectVersions(self.wal.bucket, token);
        var active = try self.wal.collectionStatus(namespace, token);
        if (active == null) {
            if (hint.active) {
                // A lost finish ACK or another collector's completion cannot
                // resurrect the cached old job or lose its metadata cleanup.
                self.finished(hint);
                return .{ .phase = .finished };
            }
            hint.active = false;
            hint.state = null;
            if (!hint.cleanup_done) {
                const cleaned = try self.cleanup(namespace, hint, token);
                if (cleaned.deleted_objects != 0 or !hint.cleanup_done or cleaned.phase == .deferred) return cleaned;
            }
            if (hint.finishing) {
                hint.finishing = false;
                return .{ .phase = .cleaning };
            }
            if (try self.wal.compactPrefixStepWithCancellation(namespace, 256, token)) return .{ .phase = .compacting };
            var id: [16]u8 = undefined;
            self.io.random(&id);
            if (std.mem.allEqual(u8, &id, 0)) id[0] = 1;
            const roots = self.wal.beginCollectionWithCancellation(namespace, id, token) catch |err| switch (err) {
                error.CollectionBusy => return .{ .phase = .deferred },
                else => return err,
            };
            active = .{ .id = id, .roots = roots };
        }
        hint.job(active.?, self.wal.read_clock.read()); // sampled AFTER the barrier was observed
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        var run_ctx = try Run.open(self, arena.allocator(), namespace, hint, active.?, token);
        if (run_ctx.has_more) return .{ .phase = .deferred };
        var meta = try run_ctx.load();
        switch (meta.phase) {
            .mark => {
                var progress = Result{ .phase = .marking };
                // One atomic commit amortizes native metadata path copies
                // across eight nodes. Eight full 60-child expansions need at
                // most 505 commands, including the job row. A wide branch
                // still yields at its explicit child continuation.
                var changes = std.ArrayList(kv.Command).empty;
                for (0..8) |_| {
                    const part = try run_ctx.mark(&meta, &changes);
                    progress.marked_pages += part.marked_pages;
                    if (meta.phase != .mark or meta.child_offset != 0) break;
                }
                try run_ctx.commit(meta, changes.items);
                return progress;
            },
            .pages, .heads => {
                const observed_now = self.wal.read_clock.read();
                if (observed_now < hint.observed_ns or observed_now - hint.observed_ns < grace_ns) return .{ .phase = .waiting };
                return run_ctx.sweep(&meta);
            },
            .complete => {
                const released = self.wal.finishCollection(namespace, active.?.id, active.?.roots.retired_before, token) catch |err| blk: {
                    // Completion may have committed before its response failed.
                    const current = try self.wal.collectionStatus(namespace, token);
                    if (current != null and sameJob(current.?, active.?)) return err;
                    break :blk false;
                };
                if (!released) if (try self.wal.collectionStatus(namespace, token)) |current| {
                    if (sameJob(current, active.?)) return error.CollectionConflict;
                    hint.job(current, self.wal.read_clock.read());
                    return .{ .phase = .deferred };
                };
                self.finished(hint);
                return .{ .phase = .finished };
            },
        }
    }
    fn finished(self: *@This(), hint: *Hint) void {
        hint.active = false;
        hint.finishing = true;
        hint.state = null;
        hint.cleanup_done = false;
        hint.cleanup_len = 0;
        hint.idle_until_ns = if (hint.dirty) 0 else self.wal.read_clock.read() +| 5 * std.time.ns_per_min;
    }

    const ClockRun = struct {
        collector: *Collector,
        opened: u64,
        upstream: ?objects.CancellationToken,
        fn token(self: *@This()) objects.CancellationToken {
            return .{ .ptr = self, .is_cancelled_fn = canceled };
        }
        fn canceled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.upstream) |signal| if (signal.isCancelled()) return true;
            const now = self.collector.wal.read_clock.read();
            return now == 0 or now == std.math.maxInt(u64) or now < self.opened or now - self.opened >= 30 * std.time.ns_per_s;
        }
    };

    const Run = struct {
        collector: *Collector,
        arena: A,
        namespace: []const u8,
        hint: *Hint,
        active: Wal.ActiveCollection,
        context: kv.Context,
        state: journal.State,
        has_more: bool,
        token: objects.CancellationToken,
        const Row = struct { key: []const u8, value: []const u8 };
        const Header = struct { state: journal.State, has_more: bool = false };
        fn json(self: *@This(), comptime T: type, req: kv.Request) !T {
            const bytes = try self.context.execute(req);
            defer self.collector.alloc.free(bytes);
            if (bytes.len > 8 * 1024 * 1024) return error.CollectionReadBudgetExceeded;
            // The serialized response is freed below; unescaped keys/cursors
            // must not retain borrowed slices into that transport buffer.
            return (try std.json.parseFromSlice(T, self.arena, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always })).value;
        }
        fn open(collector: *Collector, arena: A, namespace: []const u8, hint: *Hint, active: Wal.ActiveCollection, token: objects.CancellationToken) !Run {
            const prefix = try jobPrefix(arena, collector.wal.prefix, namespace, active);
            var self = Run{ .collector = collector, .arena = arena, .namespace = namespace, .hint = hint, .active = active, .token = token, .has_more = false, .state = undefined, .context = .{ .alloc = collector.alloc, .client = collector.client, .bucket = collector.wal.bucket, .prefix = prefix, .namespace = namespace, .io = collector.io, .cancellation = token } };
            const header = try self.json(Header, .{ .action = .begin, .owner = collector.owner, .state = hint.state, .checkpoint_commits = 8 });
            try hint.remember(header.state);
            self.state = header.state;
            self.has_more = header.has_more;
            if (!self.has_more and !std.mem.eql(u8, &self.state.owner, &collector.owner)) {
                hint.state = null;
                return error.ImmutableLogFenced;
            }
            return self;
        }
        fn read(self: *@This(), keys: []const []const u8) ![]const []const Row {
            if (keys.len == 0) return &.{};
            const commands = try self.arena.alloc(kv.Command, keys.len);
            for (commands, keys) |*command, key| command.* = .{ .kind = .get, .key = key };
            const value = try self.json(struct { results: []const []const Row }, .{ .action = .read, .owner = self.collector.owner, .state = self.state, .commands = commands, .checkpoint_commits = 8, .read_limits = .{ .max_encoded_bytes = 64 * 1024, .max_value_bytes = 16 * 1024 } });
            if (value.results.len != keys.len) return error.InvalidCollectionState;
            for (value.results, keys) |rows, key| {
                if (rows.len > 1 or (rows.len == 1 and !std.mem.eql(u8, rows[0].key, key))) return error.InvalidCollectionState;
            }
            return value.results;
        }
        fn commit(self: *@This(), meta: Meta, changes: []const kv.Command) !void {
            try self.assertActive();
            const commands = try self.arena.alloc(kv.Command, changes.len + 1);
            @memcpy(commands[0..changes.len], changes);
            commands[changes.len] = .{ .kind = .put, .key = "job", .value = try std.json.Stringify.valueAlloc(self.arena, meta, .{}) };
            const hot: []const []const u8 = if (meta.phase == .mark and meta.done < meta.next) &.{ "job", try frontierKey(self.arena, meta.done) } else &.{"job"};
            const header = try self.json(Header, .{ .action = .commit, .owner = self.collector.owner, .state = self.state, .commands = commands, .checkpoint_commits = 8, .inline_read_set = .{ .keys = hot } });
            self.state = header.state;
            try self.validateCounts(meta);
            try self.hint.remember(header.state);
        }
        fn load(self: *@This()) !Meta {
            const rows = try self.read(&.{"job"});
            if (rows[0].len == 0) {
                if (self.state.root.len != 0) return error.CollectionStateMissing;
                var meta = Meta{ .id = self.active.id, .retired = self.active.roots.retired_before, .versions = self.collector.versions.? };
                var commands = std.ArrayList(kv.Command).empty;
                for ([_]?tree.Ref{ self.active.roots.records, self.active.roots.operations, self.active.roots.documents }) |ref| if (ref) |value| {
                    try commands.append(self.arena, .{ .kind = .put, .key = try frontierKey(self.arena, meta.next), .value = try std.json.Stringify.valueAlloc(self.arena, value, .{}) });
                    meta.next += 1;
                };
                if (meta.next == 0) meta.phase = .pages;
                try self.commit(meta, commands.items);
                return meta;
            }
            if (rows[0][0].value.len > 16 * 1024) return error.InvalidCollectionState;
            const meta = (try std.json.parseFromSlice(Meta, self.arena, rows[0][0].value, .{})).value;
            if (meta.format != 1 or !std.mem.eql(u8, &meta.id, &self.active.id) or meta.retired != self.active.roots.retired_before or meta.versions != self.collector.versions.? or meta.marked > meta.done or
                meta.done > meta.next or (meta.phase == .mark and meta.done == meta.next) or (meta.phase != .mark and (meta.done != meta.next or meta.child_offset != 0))) return error.InvalidCollectionState;
            try self.validateCounts(meta);
            return meta;
        }
        fn validateCounts(self: *@This(), meta: Meta) !void {
            const value = (try std.json.parseFromSlice(std.json.Value, self.arena, self.state.root, .{})).value;
            if (value != .object) return error.InvalidCollectionState;
            const page_value = if (value.object.contains("format")) value.object.get("metadata_tree") orelse return error.InvalidCollectionState else value;
            if (page_value == .null) return error.CollectionStateMissing;
            const page = (try std.json.parseFromValue(tree.Ref, self.arena, page_value, .{})).value;
            try page.validate();
            // Every completed unique page owns one logical seen key and one
            // physical mark. Frontier rows and an unfinished parent's mark are
            // accounted separately. A copied sweep phase over a reset/missing
            // mark map cannot pass this authenticated tree-summary check.
            var expected = std.math.mul(u64, meta.marked, 2) catch return error.CollectionCounterExhausted;
            expected = std.math.add(u64, expected, meta.next - meta.done) catch return error.CollectionCounterExhausted;
            expected = std.math.add(u64, expected, 1 + @as(u64, @intFromBool(meta.child_offset != 0))) catch return error.CollectionCounterExhausted;
            if (page.records != expected) return error.CollectionStateMissing;
        }
        fn assertActive(self: *@This()) !void {
            const active = try self.collector.wal.collectionStatus(self.namespace, self.token);
            if (active == null or !sameJob(active.?, self.active)) return error.CollectionSuperseded;
        }
        /// The durable snapshot stays fixed for the whole batch. Consult
        /// queued mutations before reading it so newly enqueued children and
        /// shared subtrees are observed without exposing tentative progress.
        fn pendingRow(self: *@This(), changes: []const kv.Command, key: []const u8) !?Row {
            for (changes) |change| if (std.mem.eql(u8, change.key, key)) return switch (change.kind) {
                .put => .{ .key = change.key, .value = change.value orelse return error.InvalidCollectionState },
                .delete => null,
                else => error.InvalidCollectionState,
            };
            const rows = try self.read(&.{key});
            return if (rows[0].len == 0) null else rows[0][0];
        }
        fn queueChange(self: *@This(), changes: *std.ArrayList(kv.Command), change: kv.Command) !void {
            for (changes.items) |*pending| if (std.mem.eql(u8, pending.key, change.key)) {
                pending.* = change;
                return;
            };
            // Reserve one command for the canonical job row. Coalescing also
            // removes duplicate tree mutations for children consumed in-step.
            if (changes.items.len >= 511) return error.CoordinatorBatchExceeded;
            try changes.append(self.arena, change);
        }
        fn mark(self: *@This(), meta: *Meta, changes: *std.ArrayList(kv.Command)) !Result {
            const frontier = try frontierKey(self.arena, meta.done);
            const pending = (try self.pendingRow(changes.items, frontier)) orelse return error.CollectionStateMissing;
            if (pending.value.len > 2048) return error.CollectionStateMissing;
            const ref = (try std.json.parseFromSlice(tree.Ref, self.arena, pending.value, .{})).value;
            try ref.validate();
            if (ref.pack != null) return error.PackedWalCollectionUnsupported;
            const encoded = try std.json.Stringify.valueAlloc(self.arena, ref, .{});
            const seen = try std.fmt.allocPrint(self.arena, "s/{s}", .{std.fmt.bytesToHex(hash(encoded), .lower)});
            const seen_row = try self.pendingRow(changes.items, seen);
            var newly_marked = false;
            if (seen_row) |row| {
                if (!std.mem.eql(u8, row.value, "1") or meta.child_offset != 0) return error.InvalidCollectionState;
            } else {
                var page = try self.collector.wal.traceCollectionPage(self.collector.alloc, self.namespace, self.active.id, self.active.roots.retired_before, ref, self.token);
                defer page.deinit(self.collector.alloc);
                var children = std.ArrayList(tree.Ref).empty;
                for (page.entries) |entry| if (entry.child) |child| try children.append(self.arena, child.*);
                if (meta.child_offset > children.items.len) return error.InvalidCollectionState;
                const end = @min(children.items.len, @as(usize, meta.child_offset) + child_chunk);
                for (children.items[meta.child_offset..end]) |child| {
                    try self.queueChange(changes, .{ .kind = .put, .key = try frontierKey(self.arena, meta.next), .value = try std.json.Stringify.valueAlloc(self.arena, child, .{}) });
                    meta.next = std.math.add(u64, meta.next, 1) catch return error.CollectionFrontierExhausted;
                }
                const key = try self.collector.wal.pageKeyAlloc(self.arena, self.namespace, ref);
                const prefix = try pagesPrefix(self.arena, self.collector.wal.prefix, self.namespace);
                if (!std.mem.startsWith(u8, key, prefix)) return error.InvalidCollectionState;
                try self.queueChange(changes, .{ .kind = .put, .key = try std.fmt.allocPrint(self.arena, "m/{s}", .{key[prefix.len..]}), .value = "1" });
                if (end != children.items.len) {
                    meta.child_offset = @intCast(end);
                    return .{ .phase = .marking };
                }
                try self.queueChange(changes, .{ .kind = .put, .key = seen, .value = "1" });
                meta.marked += 1;
                newly_marked = true;
            }
            try self.queueChange(changes, .{ .kind = .delete, .key = frontier });
            meta.done += 1;
            meta.child_offset = 0;
            if (meta.done == meta.next) meta.phase = .pages;
            return .{ .phase = .marking, .marked_pages = @intFromBool(newly_marked) };
        }
        fn sweep(self: *@This(), meta: *Meta) !Result {
            const prefix = if (meta.phase == .pages) try pagesPrefix(self.arena, self.collector.wal.prefix, self.namespace) else try headKey(self.arena, self.collector.wal.prefix, self.namespace);
            var inventory = try Inventory.load(self.collector, prefix, meta.versions, meta.cursor, self.token);
            defer inventory.deinit(self.collector.alloc);
            var deleted: usize = 0;
            var eligible_rows: [max_inventory]bool = @splat(false);
            if (meta.phase == .pages) {
                var keys = std.ArrayList([]const u8).empty;
                var indices = std.ArrayList(usize).empty;
                for (inventory.items, 0..) |item, index| {
                    if (!std.mem.startsWith(u8, item.key, prefix)) return error.InvalidCollectionListing;
                    const epoch = pageEpoch(item.key[prefix.len..]) orelse continue;
                    if (epoch >= meta.retired) continue;
                    try keys.append(self.arena, try std.fmt.allocPrint(self.arena, "m/{s}", .{item.key[prefix.len..]}));
                    try indices.append(self.arena, index);
                }
                // One native read shares authenticated navigation pages across
                // the whole inventory batch instead of reopening for each key.
                const marks = try self.read(keys.items);
                for (marks, indices.items) |rows, index| {
                    if (rows.len != 0 and !std.mem.eql(u8, rows[0].value, "1")) return error.InvalidCollectionState;
                    eligible_rows[index] = rows.len == 0;
                }
            }
            try self.assertActive();
            // Phase and mark membership use precisely self.state. Never refresh
            // this root and retain a cached sweep phase over an empty mark map.
            for (inventory.items, 0..) |item, index| {
                if (!std.mem.startsWith(u8, item.key, prefix)) return error.InvalidCollectionListing;
                var eligible = false;
                if (meta.phase == .pages) {
                    eligible = eligible_rows[index];
                } else {
                    if (!std.mem.eql(u8, item.key, prefix) or item.version == null or std.mem.eql(u8, item.version.?, "null") or item.delete_marker) continue;
                    var value = self.collector.client.getObject(self.collector.wal.bucket, item.key, .{ .version_id = item.version, .max_response_bytes = 4096, .skip_metadata_probe = true, .cancellation = self.token }) catch |err| switch (err) {
                        error.FileNotFound => continue,
                        else => return err,
                    };
                    defer value.deinit(self.collector.alloc);
                    const head = try std.json.parseFromSlice(Wal.Head, self.arena, value.body, .{});
                    if (head.value.root_lifetime_ms == null or head.value.root_lifetime_ms.? != Wal.root_lifetime_ms) return error.WalReaderContractMissing;
                    eligible = head.value.write_epoch != 0 and head.value.write_epoch < meta.retired;
                }
                if (eligible) {
                    // Every old-epoch candidate was classified from the same
                    // closed mark snapshot. Later writers can only retain
                    // marked old pages or create pages in newer epochs; a
                    // takeover cannot make an unmarked retired page reachable.
                    if (try deleteItem(self.collector, item, self.token)) deleted += 1;
                }
            }
            meta.deleted = std.math.add(u64, meta.deleted, deleted) catch return error.CollectionCounterExhausted;
            meta.cursor = inventory.next;
            if (!inventory.more) {
                meta.cursor = .{};
                meta.phase = if (meta.phase == .pages and meta.versions) .heads else .complete;
            }
            try self.commit(meta.*, &.{});
            return .{ .phase = .sweeping, .deleted_objects = deleted };
        }
    };

    fn cleanup(self: *@This(), namespace: []const u8, hint: *Hint, token: objects.CancellationToken) !Result {
        const a = self.alloc;
        const prefix = try jobsPrefix(a, self.wal.prefix, namespace);
        defer a.free(prefix);
        var parsed: ?std.json.Parsed(Cursor) = if (hint.cleanup_len != 0) try std.json.parseFromSlice(Cursor, a, hint.cleanup_cursor[0..hint.cleanup_len], .{}) else null;
        defer if (parsed) |*value| value.deinit();
        var inventory = try Inventory.load(self, prefix, self.versions.?, if (parsed) |value| value.value else .{}, token);
        defer inventory.deinit(a);
        const active = try self.wal.collectionStatus(namespace, token);
        var deleted: usize = 0;
        for (inventory.items) |item| {
            if (!std.mem.startsWith(u8, item.key, prefix)) return error.InvalidCollectionListing;
            const identity = ownedJobObject(item.key[prefix.len..], hash(namespace), prefix) orelse continue;
            if (active) |current| if (identity.epoch == current.roots.retired_before and std.mem.eql(u8, &identity.id, &current.id)) continue;
            if (try deleteItem(self, item, token)) deleted += 1;
        }
        if (inventory.more) try hint.rememberCursor(a, inventory.next) else {
            hint.cleanup_len = 0;
            hint.cleanup_done = true;
        }
        return .{ .phase = if (active != null) .deferred else .cleaning, .deleted_objects = deleted };
    }
};

const Item = struct { key: []const u8, version: ?[]const u8 = null, delete_marker: bool = false };
const Inventory = struct {
    normal: ?objects.types.ListResult = null,
    versions: ?objects.types.ListObjectVersionsResult = null,
    items: []Item,
    next: Cursor = .{},
    more: bool,
    fn deinit(self: *@This(), a: A) void {
        if (self.normal) |*value| value.deinit(a);
        if (self.versions) |*value| value.deinit(a);
        a.free(self.items);
    }
    fn load(collector: *Collector, prefix: []const u8, versions: bool, cursor: Cursor, token: objects.CancellationToken) !Inventory {
        try cursor.validate(versions, prefix);
        var result = Inventory{ .items = undefined, .more = false };
        if (versions) {
            var page = try collector.client.listObjectVersions(collector.wal.bucket, .{ .prefix = prefix, .max_keys = max_inventory, .continuation_token = cursor.token, .key_marker = cursor.key, .version_id_marker = cursor.version, .cancellation = token });
            errdefer page.deinit(collector.alloc);
            if (page.entries.len > max_inventory) return error.InvalidCollectionListing;
            result.items = try collector.alloc.alloc(Item, page.entries.len);
            result.versions = page;
            result.more = page.is_truncated;
            result.next = .{ .token = page.next_continuation_token, .key = page.next_key_marker, .version = page.next_version_id_marker };
            for (result.items, page.entries) |*item, entry| item.* = .{ .key = entry.key, .version = entry.version_id, .delete_marker = entry.is_delete_marker };
        } else {
            var page = try collector.client.listObjects(collector.wal.bucket, .{ .prefix = prefix, .max_keys = max_inventory, .start_after = if (cursor.after.len != 0) cursor.after else null, .cancellation = token });
            errdefer page.deinit(collector.alloc);
            if (page.entries.len > max_inventory) return error.InvalidCollectionListing;
            result.items = try collector.alloc.alloc(Item, page.entries.len);
            result.normal = page;
            result.more = page.next_continuation_token != null;
            if (result.more and page.entries.len != 0) result.next.after = page.entries[page.entries.len - 1].key;
            var previous = cursor.after;
            for (result.items, page.entries) |*item, entry| {
                if (std.mem.order(u8, previous, entry.key) != .lt) {
                    collector.alloc.free(result.items);
                    return error.InvalidCollectionListing;
                }
                previous = entry.key;
                item.* = .{ .key = entry.key };
            }
        }
        errdefer result.deinit(collector.alloc);
        try result.next.validate(versions, prefix);
        if (result.more and (result.items.len == 0 or cursorEqual(result.next, .{}) or cursorEqual(cursor, result.next))) return error.InvalidCollectionListing;
        return result;
    }
};

fn deleteItem(collector: *Collector, item: Item, token: objects.CancellationToken) !bool {
    if (item.version) |version| {
        if (version.len == 0 or version.len > 4096) return error.InvalidObjectVersionId;
        // Only strictly retired immutable page/job names reach this branch.
        // Mutable WAL HEAD null versions are explicitly excluded by sweep().
        collector.client.deleteObject(collector.wal.bucket, item.key, .{ .version_id = version, .cancellation = token }) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
    } else {
        var meta = collector.client.statObjectWithOptions(collector.wal.bucket, item.key, .{ .cancellation = token }) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        defer meta.deinit(collector.alloc);
        const revision = meta.writeRevision() orelse return error.MissingObjectRevision;
        try collector.client.compareAndDelete(collector.wal.bucket, item.key, revision, .{ .cancellation = token });
    }
    return true;
}
fn sameJob(a: Wal.ActiveCollection, b: Wal.ActiveCollection) bool {
    return a.roots.retired_before == b.roots.retired_before and std.mem.eql(u8, &a.id, &b.id) and std.mem.eql(u8, &a.roots.domain, &b.roots.domain);
}
fn hash(text: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(text, &digest, .{});
    return digest;
}
fn frontierKey(a: A, index: u64) ![]u8 {
    return std.fmt.allocPrint(a, "f/{d:0>20}", .{index});
}
fn pagesPrefix(a: A, prefix: []const u8, namespace: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}/wal-v1/{s}/pages/", .{ prefix, std.fmt.bytesToHex(hash(namespace), .lower) });
}
fn headKey(a: A, prefix: []const u8, namespace: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}/wal-v1/{s}/HEAD", .{ prefix, std.fmt.bytesToHex(hash(namespace), .lower) });
}
fn jobsPrefix(a: A, prefix: []const u8, namespace: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}/gc-jobs/{s}/", .{ prefix, std.fmt.bytesToHex(hash(namespace), .lower) });
}
fn jobPrefix(a: A, prefix: []const u8, namespace: []const u8, active: Wal.ActiveCollection) ![]u8 {
    return std.fmt.allocPrint(a, "{s}/gc-jobs/{s}/{d:0>20}-{s}", .{ prefix, std.fmt.bytesToHex(hash(namespace), .lower), active.roots.retired_before, std.fmt.bytesToHex(active.id, .lower) });
}
fn isHex(text: []const u8, len: usize) bool {
    if (text.len != len) return false;
    for (text) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return false;
    return true;
}
fn pageEpoch(key: []const u8) ?u64 {
    if (isHex(key, 64)) return 0;
    if (key.len != 97 or key[32] != '/' or !isHex(key[0..32], 32) or !isHex(key[33..], 64)) return null;
    var attempt: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&attempt, key[0..32]) catch return null;
    return std.mem.readInt(u64, attempt[0..8], .big);
}
fn ownedJobObject(key: []const u8, namespace: [32]u8, parent: []const u8) ?struct { epoch: u64, id: [16]u8 } {
    if (key.len < 54 or key[20] != '-' or key[53] != '/' or !isHex(key[21..53], 32)) return null;
    for (key[0..20]) |c| if (!std.ascii.isDigit(c)) return null;
    const epoch = std.fmt.parseInt(u64, key[0..20], 10) catch return null;
    if (epoch < 2) return null;
    var id: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&id, key[21..53]) catch return null;
    var expected: [74]u8 = undefined;
    const prefix = std.fmt.bufPrint(&expected, "coord-v1/{s}/", .{std.fmt.bytesToHex(namespace, .lower)}) catch return null;
    if (!std.mem.startsWith(u8, key[54..], prefix)) return null;
    const tail = key[54 + prefix.len ..];
    if (std.mem.eql(u8, tail, "CHECKPOINT") or std.mem.eql(u8, tail, "CHECKPOINT_REQUIRED")) return .{ .epoch = epoch, .id = id };
    if (std.mem.startsWith(u8, tail, "epochs/") and tail.len >= 40 and isHex(tail[7..39], 32) and tail[39] == '/') {
        const name = tail[40..];
        if (std.mem.eql(u8, name, "START") or std.mem.eql(u8, name, "SEALED")) return .{ .epoch = epoch, .id = id };
        if (name.len == 25 and std.mem.endsWith(u8, name, ".json")) {
            for (name[0..20]) |c| if (!std.ascii.isDigit(c)) return null;
            return .{ .epoch = epoch, .id = id };
        }
    }
    if (std.mem.startsWith(u8, tail, "artifacts/graph/") and tail.len == 16 + 64 + 1 + 32 + 1 + 64 and
        isHex(tail[16..80], 64) and tail[80] == '/' and isHex(tail[81..113], 32) and tail[113] == '/' and isHex(tail[114..], 64))
    {
        var base_buffer: [4096]u8 = undefined;
        const base = std.fmt.bufPrint(&base_buffer, "{s}{s}", .{ parent, key[0 .. 54 + prefix.len - 1] }) catch return null;
        const domain = objects.ordered_pages.Pages.domainForNamespace(base);
        if (!std.mem.eql(u8, tail[16..80], &std.fmt.bytesToHex(domain, .lower))) return null;
        return .{ .epoch = epoch, .id = id };
    }
    return null;
}
fn cursorEqual(a: Cursor, b: Cursor) bool {
    if (!std.mem.eql(u8, a.after, b.after)) return false;
    for ([_]?[]const u8{ a.token, a.key, a.version }, [_]?[]const u8{ b.token, b.key, b.version }) |x, y| {
        if ((x == null) != (y == null)) return false;
        if (x) |value| if (!std.mem.eql(u8, value, y.?)) return false;
    }
    return true;
}

test "serverless multiprovider native collector reclaims retired pages and retains original receipt replay" {
    const a = std.testing.allocator;
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var wal = try Wal.init(a, client, "db", "gc");
    defer wal.deinit();
    wal.managed_reads = true;
    var now: u64 = std.time.ns_per_s;
    const Clock = struct {
        fn read(raw: ?*const anyopaque) u64 {
            return @as(*const u64, @ptrCast(@alignCast(raw.?))).*;
        }
    };
    wal.read_clock = .{ .ptr = &now, .now_fn = Clock.read };
    _ = try wal.appendIdempotent("docs", 1, "alpha", "op-a");
    _ = try wal.appendIdempotent("docs", 2, "beta", "op-b");
    _ = try wal.truncatePrefix("docs", 2);
    const prefix = try pagesPrefix(a, wal.prefix, "docs");
    defer a.free(prefix);
    var before = try client.listObjects("db", .{ .prefix = prefix });
    defer before.deinit(a);
    const collector = try a.create(Collector);
    defer a.destroy(collector);
    collector.* = Collector.init(a, &wal, std.testing.io);
    collector.step_interval_ns = 0;
    var finished = false;
    var deleted: usize = 0;
    for (0..128) |_| {
        const result = try collector.step("docs", null);
        deleted += result.deleted_objects;
        if (result.phase == .waiting) now += Collector.grace_ns;
        if (result.phase == .finished) {
            finished = true;
            break;
        }
    }
    try std.testing.expect(finished and deleted > 0);
    try std.testing.expect(try wal.collectionStatus("docs", null) == null);
    var after = try client.listObjects("db", .{ .prefix = prefix });
    defer after.deinit(a);
    try std.testing.expect(after.entries.len < before.entries.len);
    try std.testing.expectEqual(@as(u64, 1), try wal.appendIdempotent("docs", 1, "alpha", "op-a"));
    const records = try wal.readRangeAlloc(a, "docs", .{ .start_lsn = 2, .end_lsn = 2 });
    defer @import("types.zig").freeRecords(a, records);
    try std.testing.expectEqualStrings("beta", records[0].payload);
}

const TestFixture = struct {
    memory: objects.MemoryClient,
    faults: objects.ScriptedFaultClient,
    wal: Wal,
    collector: *Collector,
    now_ns: u64 = std.time.ns_per_s,
    rng: std.Random.DefaultPrng,
    io_vtable: std.Io.VTable,
    io: std.Io,
    const Entropy = struct {
        threadlocal var source: ?*std.Random.DefaultPrng = null;
        fn fill(_: ?*anyopaque, buffer: []u8) void {
            source.?.random().bytes(buffer);
        }
    };
    fn clock(raw: ?*const anyopaque) u64 {
        return @as(*const u64, @ptrCast(@alignCast(raw.?))).*;
    }
    fn init(self: *@This()) !void {
        const a = std.testing.allocator;
        self.now_ns = std.time.ns_per_s;
        self.rng = std.Random.DefaultPrng.init(17);
        Entropy.source = &self.rng;
        self.io_vtable = std.testing.io.vtable.*;
        self.io_vtable.random = Entropy.fill;
        self.io = .{ .userdata = std.testing.io.userdata, .vtable = &self.io_vtable };
        self.memory = objects.MemoryClient.init(a);
        errdefer self.memory.deinit();
        var backing = self.memory.client();
        try backing.makeBucket("db");
        self.faults = objects.ScriptedFaultClient.init(a, backing);
        errdefer self.faults.deinit();
        self.wal = try Wal.init(a, self.faults.client(), "db", "fixture");
        errdefer self.wal.deinit();
        self.wal.managed_reads = true;
        self.wal.read_clock = .{ .ptr = &self.now_ns, .now_fn = clock };
        self.collector = try a.create(Collector);
        self.collector.* = Collector.init(a, &self.wal, self.io);
        self.collector.step_interval_ns = 0;
    }
    fn deinit(self: *@This()) void {
        std.testing.allocator.destroy(self.collector);
        self.wal.deinit();
        self.faults.deinit();
        self.memory.deinit();
        Entropy.source = null;
    }
    fn reopen(self: *@This()) !void {
        std.testing.allocator.destroy(self.collector);
        self.collector = try std.testing.allocator.create(Collector);
        self.collector.* = Collector.init(std.testing.allocator, &self.wal, self.io);
        self.collector.step_interval_ns = 0;
    }
    fn waitForSweep(self: *@This()) !void {
        for (0..1024) |_| {
            if ((try self.collector.step("docs", null)).phase == .waiting) return;
        }
        return error.CollectionDidNotReachSweep;
    }
    fn finish(self: *@This()) !void {
        for (0..4096) |_| {
            const result = try self.collector.step("docs", null);
            if (result.phase == .waiting) self.now_ns += Collector.grace_ns;
            if (result.phase == .finished) return;
        }
        return error.CollectionDidNotFinish;
    }
    fn meta(self: *@This()) !Meta {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const active = (try self.wal.collectionStatus("docs", null)).?;
        const hint = self.collector.hintFor("docs").?;
        var run_ctx = try Collector.Run.open(self.collector, arena.allocator(), "docs", hint, active, .{ .ptr = &self.now_ns, .is_cancelled_fn = neverCanceled });
        var value = try run_ctx.load();
        value.cursor = .{}; // do not return slices owned by the temporary arena
        return value;
    }
    fn neverCanceled(_: *const anyopaque) bool {
        return false;
    }
    fn verify(self: *@This()) !void {
        const a = std.testing.allocator;
        try std.testing.expectEqual(@as(u64, 1), try self.wal.appendIdempotent("docs", 1, "alpha", "op-a"));
        const records = try self.wal.readRangeAlloc(a, "docs", .{ .start_lsn = 2, .end_lsn = 2 });
        defer @import("types.zig").freeRecords(a, records);
        try std.testing.expectEqualStrings("beta", records[0].payload);
    }
    fn seed(self: *@This()) !void {
        _ = try self.wal.appendIdempotent("docs", 1, "alpha", "op-a");
        _ = try self.wal.appendIdempotent("docs", 2, "beta", "op-b");
        _ = try self.wal.truncatePrefix("docs", 2);
    }
};

test "serverless multiprovider native collection resumes inside a 193-child node after owner replacement" {
    const a = std.testing.allocator;
    var fixture: TestFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    const body: [20 * 1024]u8 = @splat('x');
    const payloads: [193][]const u8 = @splat(&body);
    _ = try fixture.wal.appendBatch("docs", 1, &payloads);
    _ = try fixture.collector.step("docs", null);
    const active = (try fixture.wal.collectionStatus("docs", null)).?;
    var root = try fixture.wal.traceCollectionPage(a, "docs", active.id, active.roots.retired_before, active.roots.records.?, null);
    defer root.deinit(a);
    try std.testing.expectEqual(@as(usize, 193), root.entries.len);
    try std.testing.expectEqual(@as(u32, 60), (try fixture.meta()).child_offset);
    try fixture.reopen();
    _ = try fixture.collector.step("docs", null);
    try std.testing.expectEqual(@as(u32, 120), (try fixture.meta()).child_offset);
    try fixture.finish();
    const records = try fixture.wal.readRangeAlloc(a, "docs", .{ .start_lsn = 1, .end_lsn = 193 });
    defer @import("types.zig").freeRecords(a, records);
    try std.testing.expectEqual(@as(usize, 193), records.len);
    for (records) |record| try std.testing.expectEqualSlices(u8, &body, record.payload);
}

test "serverless multiprovider native collection waits a full new grace after restart and retries an ambiguous delete" {
    var fixture: TestFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    try fixture.seed();
    try fixture.waitForSweep();
    fixture.now_ns += Collector.grace_ns;
    try fixture.reopen();
    fixture.faults.delete_attempts = 0;
    try std.testing.expectEqual(Collector.Result{ .phase = .waiting }, try fixture.collector.step("docs", null));
    try std.testing.expectEqual(@as(u64, 0), fixture.faults.delete_attempts);
    fixture.now_ns += Collector.grace_ns;
    fixture.faults.commitNextDeleteThenFail(error.InjectedDeleteAckLoss);
    try std.testing.expectError(error.InjectedDeleteAckLoss, fixture.collector.step("docs", null));
    try fixture.verify();
    try fixture.reopen();
    try fixture.finish();
    try fixture.verify();
}

test "serverless multiprovider native collection rejects a sweep phase over a reset mark map" {
    const a = std.testing.allocator;
    var fixture: TestFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    try fixture.seed();
    try fixture.waitForSweep();
    fixture.now_ns += Collector.grace_ns;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    const active = (try fixture.wal.collectionStatus("docs", null)).?;
    var run_ctx = try Collector.Run.open(fixture.collector, m, "docs", fixture.collector.hintFor("docs").?, active, .{ .ptr = &fixture.now_ns, .is_cancelled_fn = TestFixture.neverCanceled });
    const meta = try run_ctx.load();
    const prefix = try pagesPrefix(m, fixture.wal.prefix, "docs");
    const root_key = try fixture.wal.pageKeyAlloc(m, "docs", active.roots.records.?);
    // Simulate a validly encoded metadata regression, rather than just a bad
    // page checksum. The unchanged completed count must detect the lost mark.
    _ = try run_ctx.json(Collector.Run.Header, .{ .action = .commit, .owner = fixture.collector.owner, .state = run_ctx.state, .checkpoint_commits = 8, .commands = &.{.{ .kind = .delete, .key = try std.fmt.allocPrint(m, "m/{s}", .{root_key[prefix.len..]}) }}, .inline_read_set = .{ .keys = &.{"job"} } });
    try std.testing.expect(meta.marked > 0);
    fixture.faults.delete_attempts = 0;
    try std.testing.expectError(error.CollectionStateMissing, fixture.collector.step("docs", null));
    try std.testing.expectEqual(@as(u64, 0), fixture.faults.delete_attempts);
    try fixture.verify();
}

test "serverless multiprovider native collection allocation failures preserve receipts and current pages" {
    const Check = struct {
        fn run(a: A, sweeping: bool) !void {
            var fixture: TestFixture = undefined;
            try fixture.init();
            defer fixture.deinit();
            try fixture.seed();
            if (sweeping) {
                try fixture.waitForSweep();
                fixture.now_ns += Collector.grace_ns;
            }
            fixture.collector.alloc = a;
            fixture.collector.client.allocator = a;
            _ = fixture.collector.step("docs", null) catch |err| {
                try fixture.verify();
                return err;
            };
            try fixture.verify();
        }
    };
    for ([_]bool{ false, true }) |sweeping| try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{sweeping});
}

test "serverless multiprovider native collection preserves current HEADs with opaque tuple and null versions" {
    const a = std.testing.allocator;
    const Provider = @import("../artifacts/cloud_generations_test_provider.zig").Provider;
    for ([_]u8{ 0, 1, 2 }) |mode| {
        var provider = Provider.init(a);
        defer provider.deinit();
        provider.stable_pagination = true;
        provider.marker_pagination = mode != 0;
        provider.null_versions = mode == 2;
        provider.until_ms = 0;
        var client = provider.client();
        try client.makeBucket("db");
        var faults = objects.ScriptedFaultClient.init(a, client);
        defer faults.deinit();
        var wal = try Wal.init(a, faults.client(), "db", "versioned");
        defer wal.deinit();
        wal.managed_reads = true;
        var now: u64 = std.time.ns_per_s;
        wal.read_clock = .{ .ptr = &now, .now_fn = TestFixture.clock };
        _ = try wal.appendIdempotent("docs", 1, "alpha", "op-a");
        _ = try wal.appendIdempotent("docs", 2, "beta", "op-b");
        _ = try wal.truncatePrefix("docs", 2);
        const collector = try a.create(Collector);
        defer a.destroy(collector);
        collector.* = Collector.init(a, &wal, std.testing.io);
        collector.step_interval_ns = 0;
        var finished = false;
        for (0..1024) |_| {
            const result = try collector.step("docs", null);
            if (result.phase == .waiting) now += Collector.grace_ns;
            if (result.phase == .finished) {
                finished = true;
                break;
            }
        }
        try std.testing.expect(finished);
        try std.testing.expect(collector.versions.?); // fault wrappers preserve capabilities
        const head_key = try headKey(a, wal.prefix, "docs");
        defer a.free(head_key);
        var head = try client.getObject("db", head_key, .{});
        defer head.deinit(a);
        const parsed = try std.json.parseFromSlice(Wal.Head, a, head.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(u64, 2), parsed.value.write_epoch);
        var history = try client.listObjectVersions("db", .{ .prefix = head_key });
        defer history.deinit(a);
        try std.testing.expect(history.entries.len > 0);
        for (history.entries) |entry| {
            var old = try client.getObject("db", entry.key, .{ .version_id = entry.version_id });
            defer old.deinit(a);
            const prior = try std.json.parseFromSlice(Wal.Head, a, old.body, .{});
            defer prior.deinit();
            try std.testing.expect(prior.value.write_epoch >= 2);
        }
        try std.testing.expectEqual(@as(u64, 1), try wal.appendIdempotent("docs", 1, "alpha", "op-a"));
        // Complete cleanup of the retired job's versioned metadata too.
        var idle = false;
        for (0..4096) |_| if ((try collector.step("docs", null)).phase == .idle) {
            idle = true;
            break;
        };
        try std.testing.expect(idle);
        const prefix = try jobsPrefix(a, wal.prefix, "docs");
        defer a.free(prefix);
        var metadata_versions = try client.listObjectVersions("db", .{ .prefix = prefix });
        defer metadata_versions.deinit(a);
        try std.testing.expectEqual(@as(usize, 0), metadata_versions.entries.len);
    }
}

test "serverless multiprovider managed activation rejects historical versions behind deleted current names" {
    const a = std.testing.allocator;
    const Provider = @import("../artifacts/cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    provider.until_ms = 0;
    var client = provider.client();
    try client.makeBucket("db");
    var old = try client.putObject("db", "old/wal-v1/HEAD", "unmanaged-old", .{});
    defer old.deinit(a);
    var current = try client.putObject("db", "old/wal-v1/HEAD", "unmanaged-current", .{});
    defer current.deinit(a);
    try client.deleteObject("db", "old/wal-v1/HEAD", .{ .version_id = current.version_id });
    var wrapper = try @import("object_store.zig").ObjectStore.initWithClient(a, client, "db", "old");
    defer wrapper.deinit();
    try std.testing.expectError(error.ManagedWalMigrationRequired, wrapper.enableIndexed(.{ .managed_collection = true }));
    try std.testing.expectError(error.FileNotFound, client.getObject("db", "old/.antfly/WAL_FORMAT", .{}));
}

test "serverless multiprovider native collection retains document roots and newer epoch writes" {
    const a = std.testing.allocator;
    const codec = @import("../api/codec.zig");
    var fixture: TestFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    fixture.wal.document_index_enabled = true;
    const first = try codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "a", .body = "alpha" });
    defer a.free(first);
    const second = try codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "b", .body = "beta" });
    defer a.free(second);
    _ = try fixture.wal.appendIdempotent("docs", 1, first, "op-a");
    _ = try fixture.wal.appendIdempotent("docs", 2, second, "op-b");
    _ = try fixture.collector.step("docs", null);
    const active = (try fixture.wal.collectionStatus("docs", null)).?;
    try std.testing.expect(active.roots.records != null and active.roots.operations != null and active.roots.documents != null);
    const newer = try codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "b", .body = "newer" });
    defer a.free(newer);
    try std.testing.expectEqual(@as(u64, 3), try fixture.wal.appendIdempotent("docs", 3, newer, "op-new"));
    try fixture.finish();
    var point = try fixture.wal.readPointAlloc(a, "docs", "b", .{ .start_lsn = 1, .end_lsn = 3 });
    defer if (point.record) |*record| {
        a.free(record.payload);
        if (record.operation_id) |id| a.free(id);
    };
    try std.testing.expect(point.indexed);
    try std.testing.expectEqualSlices(u8, newer, point.record.?.payload);
    try std.testing.expectEqual(@as(u64, 1), try fixture.wal.appendIdempotent("docs", 1, first, "op-a"));
}

test "serverless multiprovider native collection fails before deletion when an authenticated mark page is missing" {
    const a = std.testing.allocator;
    var fixture: TestFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    try fixture.seed();
    try fixture.waitForSweep();
    fixture.now_ns += Collector.grace_ns;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    const active = (try fixture.wal.collectionStatus("docs", null)).?;
    const root_value = (try std.json.parseFromSlice(std.json.Value, m, fixture.collector.hintFor("docs").?.state.?.root, .{})).value;
    const ref = (try std.json.parseFromValue(tree.Ref, m, root_value.object.get("metadata_tree").?, .{})).value;
    const job = try jobPrefix(m, fixture.wal.prefix, "docs", active);
    const base = try std.fmt.allocPrint(m, "{s}/coord-v1/{s}", .{ job, std.fmt.bytesToHex(hash("docs"), .lower) });
    const domain = objects.ordered_pages.Pages.domainForNamespace(base);
    const page_key = try std.fmt.allocPrint(m, "{s}/artifacts/graph/{s}/{s}/{s}", .{ base, std.fmt.bytesToHex(domain, .lower), std.fmt.bytesToHex(ref.attempt, .lower), std.fmt.bytesToHex(ref.digest, .lower) });
    var client = fixture.memory.client();
    try client.deleteObject("db", page_key, .{});
    fixture.faults.delete_attempts = 0;
    try std.testing.expectError(error.FileNotFound, fixture.collector.step("docs", null));
    try std.testing.expectEqual(@as(u64, 0), fixture.faults.delete_attempts);
    try fixture.verify();
}

test "serverless multiprovider native collection reclaims late orphan uploads and preserves unrelated names" {
    const a = std.testing.allocator;
    var fixture: TestFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    var client = fixture.memory.client();
    _ = try fixture.wal.appendIdempotent("docs", 1, "alpha", "op-a");
    const head_key = try headKey(a, fixture.wal.prefix, "docs");
    defer a.free(head_key);
    var old_head = try client.getObject("db", head_key, .{});
    defer old_head.deinit(a);
    const parsed = try std.json.parseFromSlice(Wal.Head, a, old_head.body, .{});
    defer parsed.deinit();
    const retired_key = try fixture.wal.pageKeyAlloc(a, "docs", parsed.value.records.?);
    defer a.free(retired_key);
    var old_page = try client.getObject("db", retired_key, .{});
    defer old_page.deinit(a);
    _ = try fixture.wal.appendIdempotent("docs", 2, "beta", "op-b");
    _ = try fixture.wal.truncatePrefix("docs", 2);
    const prefix = try pagesPrefix(a, fixture.wal.prefix, "docs");
    defer a.free(prefix);
    const unknown = try std.fmt.allocPrint(a, "{s}unrecognized-name", .{prefix});
    defer a.free(unknown);
    var untouched = try client.putObject("db", unknown, "keep", .{});
    untouched.deinit(a);
    try fixture.finish();
    try std.testing.expectError(error.FileNotFound, client.getObject("db", retired_key, .{}));
    var late = try client.putObject("db", retired_key, old_page.body, .{ .if_none_match = true });
    late.deinit(a);
    fixture.collector.wake("docs");
    try fixture.finish();
    try std.testing.expectError(error.FileNotFound, client.getObject("db", retired_key, .{}));
    var kept = try client.getObject("db", unknown, .{});
    defer kept.deinit(a);
    try std.testing.expectEqualStrings("keep", kept.body);
    try fixture.verify();
}

test "serverless multiprovider native collection scale preserves a retained tail and receipts" {
    const enabled = std.c.getenv("ANTFLY_NATIVE_GC_SCALE") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "50000")) return error.InvalidNativeCollectionScale;
    const a = std.testing.allocator;
    const codec = @import("../api/codec.zig");
    var fixture: TestFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    fixture.wal.document_index_enabled = true;
    const body: [1024]u8 = @splat('x');
    const rows = 50_000;
    var first: ?[]const []const u8 = null;
    defer if (first) |values| {
        for (values) |value| a.free(value);
        a.free(values);
    };
    var begin: usize = 0;
    while (begin < rows) : (begin += 128) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const temporary = arena.allocator();
        const end = @min(rows, begin + 128);
        const payloads = try temporary.alloc([]const u8, end - begin);
        for (payloads, begin..) |*payload, i| payload.* = try codec.encodeMutationAlloc(temporary, .{ .kind = .upsert, .doc_id = try std.fmt.allocPrint(temporary, "doc-{d:0>8}", .{i}), .body = &body });
        const receipt = try fixture.wal.appendBatchIdempotent("docs", 1, payloads, try std.fmt.allocPrint(temporary, "batch-{d}", .{begin}));
        try std.testing.expectEqual(@as(u64, end), receipt.end_lsn);
        if (begin == 0) {
            const saved = try a.alloc([]const u8, payloads.len);
            var initialized: usize = 0;
            errdefer {
                for (saved[0..initialized]) |value| a.free(value);
                a.free(saved);
            }
            for (saved, payloads) |*out, value| {
                out.* = try a.dupe(u8, value);
                initialized += 1;
            }
            first = saved;
        }
    }
    try std.testing.expectEqual(@as(u64, 25_000), try fixture.wal.truncatePrefix("docs", 25_001));
    const before_objects = fixture.memory.buckets.get("db").?.count();
    var marked: usize = 0;
    var deleted: usize = 0;
    var peak_operations: u64 = 0;
    var newer = false;
    var finished = false;
    var steps: usize = 0;
    for (0..20_000) |_| {
        fixture.memory.resetOperationCount();
        const result = try fixture.collector.step("docs", null);
        peak_operations = @max(peak_operations, fixture.memory.operationCount());
        marked += result.marked_pages;
        deleted += result.deleted_objects;
        steps += 1;
        if (!newer and try fixture.wal.collectionStatus("docs", null) != null) {
            const encoded = try codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "newer", .body = "newer" });
            defer a.free(encoded);
            try std.testing.expectEqual(@as(u64, 50_001), try fixture.wal.appendIdempotent("docs", 2, encoded, "newer"));
            newer = true;
        }
        // This is a deterministic CPU/I/O scale fixture, not a real-clock or
        // provider SLO. The separate live test uses the actual grace period.
        fixture.now_ns += 250 * std.time.ns_per_ms;
        if (result.phase == .waiting) fixture.now_ns += Collector.grace_ns;
        if (result.phase == .finished) {
            finished = true;
            break;
        }
    }
    try std.testing.expect(newer and finished and marked > 128 and deleted > 0);
    try std.testing.expect(peak_operations <= 1024);
    const job_end_objects = fixture.memory.buckets.get("db").?.count();
    const Cleanup = struct {
        fn run(value: *TestFixture, peak: *u64) !usize {
            var removed: usize = 0;
            for (0..20_000) |_| {
                value.memory.resetOperationCount();
                const result = try value.collector.step("docs", null);
                peak.* = @max(peak.*, value.memory.operationCount());
                removed += result.deleted_objects;
                if (result.phase == .idle) return removed;
            }
            return error.CollectionCleanupDidNotFinish;
        }
    };
    var cleanup_deleted = try Cleanup.run(&fixture, &peak_operations);
    const after_cleanup_first = fixture.memory.buckets.get("db").?.count();
    try std.testing.expect(after_cleanup_first < before_objects);
    fixture.collector.wake("docs");
    var second_finished = false;
    var second_steps: usize = 0;
    for (0..20_000) |_| {
        fixture.memory.resetOperationCount();
        const result = try fixture.collector.step("docs", null);
        peak_operations = @max(peak_operations, fixture.memory.operationCount());
        second_steps += 1;
        fixture.now_ns += 250 * std.time.ns_per_ms;
        if (result.phase == .waiting) fixture.now_ns += Collector.grace_ns;
        if (result.phase == .finished) {
            second_finished = true;
            break;
        }
    }
    try std.testing.expect(second_finished);
    cleanup_deleted += try Cleanup.run(&fixture, &peak_operations);
    const after_objects = fixture.memory.buckets.get("db").?.count();
    // The same retained roots cannot accumulate completed private job state
    // across cycles. A small fixed allowance covers newer-epoch path copies.
    try std.testing.expect(after_objects <= after_cleanup_first + 16);
    try std.testing.expect(peak_operations <= 1024);
    var after: u64 = 25_000;
    while (after < rows) {
        const end = @min(rows, after + 128);
        const records = try fixture.wal.readRangeAlloc(a, "docs", .{ .start_lsn = after + 1, .end_lsn = end, .max_records = 128 });
        defer @import("types.zig").freeRecords(a, records);
        for (records, @as(usize, @intCast(after))..) |record, i| {
            const id = try std.fmt.allocPrint(a, "doc-{d:0>8}", .{i});
            defer a.free(id);
            const expected = try codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = id, .body = &body });
            defer a.free(expected);
            try std.testing.expectEqual(@as(u64, i + 1), record.lsn);
            try std.testing.expectEqual(@as(u64, 1), record.timestamp_ns);
            try std.testing.expectEqualSlices(u8, expected, record.payload);
        }
        after = end;
    }
    const replay = try fixture.wal.appendBatchIdempotent("docs", 1, first.?, "batch-0");
    try std.testing.expectEqual(@as(u64, 1), replay.start_lsn);
    try std.testing.expectEqual(@as(u64, 128), replay.end_lsn);
    try std.testing.expectEqual(@as(u64, 50_001), try fixture.wal.latestLsn("docs"));
    const point = try fixture.wal.readPointAlloc(a, "docs", "newer", .{ .start_lsn = 25_001, .end_lsn = 50_001 });
    defer if (point.record) |record| {
        a.free(record.payload);
        if (record.operation_id) |id| a.free(id);
    };
    try std.testing.expect(point.indexed and point.record != null);
    try std.testing.expectEqual(@as(u64, 50_001), point.record.?.lsn);
    std.debug.print("native GC scale: rows=50000 retained=25000 cycles=2 steps={d} second_steps={d} marked={d} deleted={d} cleanup_deleted={d} peak_operations={d} before_objects={d} job_end_objects={d} after_cleanup_first={d} after_objects={d}\n", .{ steps, second_steps, marked, deleted, cleanup_deleted, peak_operations, before_objects, job_end_objects, after_cleanup_first, after_objects });
}

test "serverless multiprovider grouped frontier commit recovers all marks after a lost acknowledgement" {
    var fixture: TestFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    try fixture.seed();
    const Filter = struct {
        slots: usize = 0,
        fn matches(raw: *anyopaque, _: []const u8, key: []const u8, _: []const u8) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.containsAtLeast(u8, key, 1, "/gc-jobs/") or !std.mem.endsWith(u8, key, ".json")) return false;
            self.slots += 1;
            // The first transaction seeds the frontier. Lose the ACK for the
            // second, which closes both captured root pages in one commit.
            return self.slots == 2;
        }
    };
    var filter = Filter{};
    fixture.faults.put_filter = .{ .ptr = &filter, .matches = Filter.matches };
    fixture.faults.commitNextPutThenFail(error.InjectedMarkBatchAckLoss);
    try std.testing.expectError(error.InjectedMarkBatchAckLoss, fixture.collector.step("docs", null));
    try std.testing.expectEqual(@as(usize, 2), filter.slots);
    try fixture.reopen();
    try fixture.waitForSweep();
    const meta = try fixture.meta();
    try std.testing.expectEqual(@as(u64, 2), meta.marked);
    try std.testing.expectEqual(meta.next, meta.done);
    try fixture.finish();
    try fixture.verify();
}
