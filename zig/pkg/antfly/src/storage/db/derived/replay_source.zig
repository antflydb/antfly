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
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const change_journal_mod = @import("change_journal.zig");
const docstore_mod = @import("../../docstore.zig");
const internal_keys = @import("../../internal_keys.zig");
const mem_backend_mod = @import("../../mem_backend.zig");
const platform_time = @import("antfly_platform").time;

pub const TargetHint = change_journal_mod.TargetHint;

pub const PendingDocumentGroup = struct {
    sequence: u64,
    doc_key: []const u8,
};

pub const StopReplayChunk = error{StopReplayChunk};

const primary_store_fallback_scan_budget_min: usize = 256;
const primary_store_fallback_scan_budget_max: usize = 4096;

pub const MatchingRecordStats = struct {
    matched_entries: usize = 0,
    scanned_entries: usize = 0,
    hint_filter_skips: usize = 0,
    scan_batches: usize = 0,
    last_sequence: u64 = 0,

    fn add(self: *MatchingRecordStats, other: MatchingRecordStats) void {
        self.matched_entries += other.matched_entries;
        self.scanned_entries += other.scanned_entries;
        self.hint_filter_skips += other.hint_filter_skips;
        self.scan_batches += other.scan_batches;
        self.last_sequence = @max(self.last_sequence, other.last_sequence);
    }
};

pub const MatchingCursor = struct {
    state: union(enum) {
        journal: JournalMatchingCursor,
        primary_store: PrimaryStoreMatchingCursor,
    },

    pub fn canFollowTail(self: *const MatchingCursor) bool {
        return switch (self.state) {
            .journal => true,
            .primary_store => false,
        };
    }

    pub fn deinit(self: *MatchingCursor, alloc: Allocator) void {
        _ = alloc;
        switch (self.state) {
            .journal => {},
            .primary_store => |*cursor| cursor.deinit(),
        }
        self.* = undefined;
    }

    pub fn forEachNext(
        self: *MatchingCursor,
        max_matched_entries: usize,
        ctx: *anyopaque,
        consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
    ) !MatchingRecordStats {
        return switch (self.state) {
            .journal => |*cursor| journalMatchingCursorForEachNext(
                cursor,
                max_matched_entries,
                ctx,
                consume,
            ),
            .primary_store => |*cursor| primaryStoreMatchingCursorForEachNext(
                cursor,
                max_matched_entries,
                ctx,
                consume,
            ),
        };
    }
};

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        open_matching_cursor: *const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            from_sequence: u64,
            hint: TargetHint,
        ) anyerror!MatchingCursor,
        for_each_matching_record: *const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            from_sequence: u64,
            hint: TargetHint,
            max_matched_entries: usize,
            ctx: *anyopaque,
            consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
        ) anyerror!MatchingRecordStats,
        latest_matching_sequence: *const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            from_sequence: u64,
            hint: TargetHint,
        ) anyerror!u64,
        collect_enrichment_document_groups: *const fn (ptr: *anyopaque, alloc: Allocator, from_sequence: u64) anyerror![]PendingDocumentGroup,
        is_sequence_visible: *const fn (ptr: *anyopaque, sequence: u64) anyerror!bool,
    };

    pub fn fromJournal(journal: *change_journal_mod.Journal) Source {
        return .{
            .ptr = journal,
            .vtable = &journal_vtable,
        };
    }

    pub fn fromPrimaryStore(store: *docstore_mod.DocStore, fallback_journal: ?*change_journal_mod.Journal, resource_manager: anytype) Source {
        _ = resource_manager;
        _ = fallback_journal;
        return .{
            .ptr = store,
            .vtable = &primary_store_vtable,
        };
    }

    pub fn forEachMatchingRecord(
        self: Source,
        alloc: Allocator,
        from_sequence: u64,
        hint: TargetHint,
        max_matched_entries: usize,
        ctx: *anyopaque,
        consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
    ) !MatchingRecordStats {
        return try self.vtable.for_each_matching_record(
            self.ptr,
            alloc,
            from_sequence,
            hint,
            max_matched_entries,
            ctx,
            consume,
        );
    }

    pub fn openMatchingCursor(
        self: Source,
        alloc: Allocator,
        from_sequence: u64,
        hint: TargetHint,
    ) !MatchingCursor {
        return try self.vtable.open_matching_cursor(self.ptr, alloc, from_sequence, hint);
    }

    pub fn latestMatchingSequence(self: Source, alloc: Allocator, from_sequence: u64, hint: TargetHint) !u64 {
        return try self.vtable.latest_matching_sequence(self.ptr, alloc, from_sequence, hint);
    }

    pub fn collectEnrichmentDocumentGroups(self: Source, alloc: Allocator, from_sequence: u64) ![]PendingDocumentGroup {
        return try self.vtable.collect_enrichment_document_groups(self.ptr, alloc, from_sequence);
    }

    pub fn isSequenceVisible(self: Source, sequence: u64) !bool {
        return try self.vtable.is_sequence_visible(self.ptr, sequence);
    }
};

pub fn freePendingDocumentGroups(alloc: Allocator, groups: []PendingDocumentGroup) void {
    for (groups) |group| alloc.free(group.doc_key);
    alloc.free(groups);
}

const journal_vtable = Source.VTable{
    .open_matching_cursor = journalOpenMatchingCursor,
    .for_each_matching_record = journalForEachMatchingRecord,
    .latest_matching_sequence = journalLatestMatchingSequence,
    .collect_enrichment_document_groups = journalCollectEnrichmentDocumentGroups,
    .is_sequence_visible = journalIsSequenceVisible,
};

const primary_store_vtable = Source.VTable{
    .open_matching_cursor = primaryStoreOpenMatchingCursor,
    .for_each_matching_record = primaryStoreForEachMatchingRecord,
    .latest_matching_sequence = primaryStoreLatestMatchingSequence,
    .collect_enrichment_document_groups = primaryStoreCollectEnrichmentDocumentGroups,
    .is_sequence_visible = primaryStoreIsSequenceVisible,
};

const JournalMatchingCursor = struct {
    alloc: Allocator,
    journal: *change_journal_mod.Journal,
    next_sequence: u64,
    hint: TargetHint,
};

const PrimaryStoreMatchingCursor = struct {
    store: *docstore_mod.DocStore,
    kind_ordinal: u8,
    hint: TargetHint,
    next_sequence: u64,
    hint_exhausted: bool = false,

    fn deinit(self: *@This()) void {
        self.* = undefined;
    }
};

fn primaryStoreFallbackScanBudget(max_matched_entries: usize) usize {
    if (max_matched_entries == 0) return primary_store_fallback_scan_budget_max;
    const requested = max_matched_entries *| 32;
    return @min(@max(requested, primary_store_fallback_scan_budget_min), primary_store_fallback_scan_budget_max);
}

fn journalMatchingCursorForEachNext(
    cursor: *JournalMatchingCursor,
    max_matched_entries: usize,
    ctx: *anyopaque,
    consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
) !MatchingRecordStats {
    const entries = try cursor.journal.iterateOpaqueFrom(cursor.alloc, cursor.next_sequence + 1);
    defer {
        for (entries) |*entry| entry.deinit(cursor.alloc);
        cursor.alloc.free(entries);
    }

    var stats = MatchingRecordStats{};
    stats.scan_batches = 1;
    for (entries) |entry| {
        if (!try change_journal_mod.encodedRecordHasHint(entry.payload, cursor.hint)) {
            stats.scanned_entries += 1;
            stats.hint_filter_skips += 1;
            cursor.next_sequence = entry.sequence;
            continue;
        }
        consume(ctx, entry.sequence, entry.payload) catch |err| switch (err) {
            StopReplayChunk.StopReplayChunk => return stats,
            else => return err,
        };
        stats.scanned_entries += 1;
        cursor.next_sequence = entry.sequence;
        stats.matched_entries += 1;
        stats.last_sequence = entry.sequence;
        if (max_matched_entries != 0 and stats.matched_entries >= max_matched_entries) break;
    }
    return stats;
}

fn primaryStoreMatchingCursorForEachNext(
    cursor: *PrimaryStoreMatchingCursor,
    max_matched_entries: usize,
    ctx: *anyopaque,
    consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
) !MatchingRecordStats {
    if (cursor.hint_exhausted) return .{};

    const Context = struct {
        consumer_ctx: *anyopaque,
        consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
        stats: MatchingRecordStats = .{},

        fn handle(self: *@This(), sequence: u64, payload: []const u8) !void {
            self.stats.scanned_entries += 1;
            try self.consume(self.consumer_ctx, sequence, payload);
            self.stats.matched_entries += 1;
            self.stats.last_sequence = sequence;
        }
    };
    var callback_ctx = Context{
        .consumer_ctx = ctx,
        .consume = consume,
    };
    callback_ctx.stats.scan_batches = 1;
    const replay_stats = cursor.store.forEachReplayLaneFrom(
        cursor.kind_ordinal,
        cursor.next_sequence + 1,
        max_matched_entries,
        &callback_ctx,
        Context.handle,
    ) catch |err| switch (err) {
        error.ReplayIndexUnavailable => {
            const stats = try primaryStoreForEachMatchingRecordFallbackAll(
                cursor.store,
                cursor.next_sequence,
                cursor.hint,
                max_matched_entries,
                primaryStoreFallbackScanBudget(max_matched_entries),
                ctx,
                consume,
            );
            cursor.next_sequence = @max(cursor.next_sequence, stats.last_sequence);
            if (stats.matched_entries == 0 and stats.scanned_entries == 0) cursor.hint_exhausted = true;
            return stats;
        },
        StopReplayChunk.StopReplayChunk => {
            cursor.next_sequence = @max(cursor.next_sequence, callback_ctx.stats.last_sequence);
            return callback_ctx.stats;
        },
        else => return err,
    };
    cursor.next_sequence = @max(cursor.next_sequence, replay_stats.last_sequence);
    if (replay_stats.matched_entries == 0) {
        const fallback_stats = try primaryStoreForEachMatchingRecordFallbackAll(
            cursor.store,
            cursor.next_sequence,
            cursor.hint,
            max_matched_entries,
            primaryStoreFallbackScanBudget(max_matched_entries),
            ctx,
            consume,
        );
        cursor.next_sequence = @max(cursor.next_sequence, fallback_stats.last_sequence);
        if (fallback_stats.matched_entries == 0 and fallback_stats.scanned_entries == 0) cursor.hint_exhausted = true;
        return fallback_stats;
    }
    callback_ctx.stats.scanned_entries = @max(callback_ctx.stats.scanned_entries, replay_stats.scanned_entries);
    callback_ctx.stats.hint_filter_skips += replay_stats.hint_filter_skips;
    callback_ctx.stats.scan_batches = @max(callback_ctx.stats.scan_batches, replay_stats.scan_batches);
    return callback_ctx.stats;
}

fn primaryStoreForEachMatchingRecordFallbackAll(
    store: *docstore_mod.DocStore,
    from_sequence: u64,
    hint: TargetHint,
    max_matched_entries: usize,
    max_scanned_entries: usize,
    ctx: *anyopaque,
    consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
) !MatchingRecordStats {
    const Context = struct {
        consumer_ctx: *anyopaque,
        consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
        hint: TargetHint,
        max_matched_entries: usize,
        stats: MatchingRecordStats = .{},

        fn handle(self: *@This(), sequence: u64, payload: []const u8) !void {
            self.stats.scanned_entries += 1;
            if (!try change_journal_mod.encodedRecordHasHint(payload, self.hint)) {
                self.stats.hint_filter_skips += 1;
                return;
            }
            try self.consume(self.consumer_ctx, sequence, payload);
            self.stats.matched_entries += 1;
            self.stats.last_sequence = sequence;
            if (self.max_matched_entries != 0 and self.stats.matched_entries >= self.max_matched_entries) {
                return StopReplayChunk.StopReplayChunk;
            }
        }
    };

    var callback_ctx = Context{
        .consumer_ctx = ctx,
        .consume = consume,
        .hint = hint,
        .max_matched_entries = max_matched_entries,
    };
    callback_ctx.stats.scan_batches = 1;
    const replay_stats = store.forEachReplayLaneFrom(
        internal_keys.replay_all_kind,
        from_sequence + 1,
        max_scanned_entries,
        &callback_ctx,
        Context.handle,
    ) catch |err| switch (err) {
        error.ReplayIndexUnavailable => return .{},
        StopReplayChunk.StopReplayChunk => return callback_ctx.stats,
        else => return err,
    };
    callback_ctx.stats.scanned_entries = @max(callback_ctx.stats.scanned_entries, replay_stats.scanned_entries);
    callback_ctx.stats.hint_filter_skips += replay_stats.hint_filter_skips;
    callback_ctx.stats.scan_batches = @max(callback_ctx.stats.scan_batches, replay_stats.scan_batches);
    callback_ctx.stats.last_sequence = @max(callback_ctx.stats.last_sequence, replay_stats.last_sequence);
    return callback_ctx.stats;
}

fn journalOpenMatchingCursor(
    ptr: *anyopaque,
    alloc: Allocator,
    from_sequence: u64,
    hint: TargetHint,
) !MatchingCursor {
    const journal: *change_journal_mod.Journal = @ptrCast(@alignCast(ptr));
    return .{
        .state = .{
            .journal = .{
                .alloc = alloc,
                .journal = journal,
                .next_sequence = from_sequence,
                .hint = hint,
            },
        },
    };
}

fn primaryStoreOpenMatchingCursor(
    ptr: *anyopaque,
    alloc: Allocator,
    from_sequence: u64,
    hint: TargetHint,
) !MatchingCursor {
    _ = alloc;
    const store: *docstore_mod.DocStore = @ptrCast(@alignCast(ptr));

    const kind_ordinal: u8 = @intCast(@intFromEnum(hint));
    return .{
        .state = .{
            .primary_store = .{
                .store = store,
                .kind_ordinal = kind_ordinal,
                .hint = hint,
                .next_sequence = from_sequence,
            },
        },
    };
}

fn journalForEachMatchingRecord(
    ptr: *anyopaque,
    alloc: Allocator,
    from_sequence: u64,
    hint: TargetHint,
    max_matched_entries: usize,
    ctx: *anyopaque,
    consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
) !MatchingRecordStats {
    var cursor = try journalOpenMatchingCursor(ptr, alloc, from_sequence, hint);
    defer cursor.deinit(alloc);
    return try cursor.forEachNext(max_matched_entries, ctx, consume);
}

fn journalLatestMatchingSequence(
    ptr: *anyopaque,
    alloc: Allocator,
    from_sequence: u64,
    hint: TargetHint,
) !u64 {
    const journal: *change_journal_mod.Journal = @ptrCast(@alignCast(ptr));
    const entries = try journal.iterateOpaqueFrom(alloc, from_sequence + 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    var latest = from_sequence;
    for (entries) |entry| {
        if (!try change_journal_mod.encodedRecordHasHint(entry.payload, hint)) continue;
        latest = entry.sequence;
    }
    return latest;
}

fn journalCollectEnrichmentDocumentGroups(ptr: *anyopaque, alloc: Allocator, from_sequence: u64) ![]PendingDocumentGroup {
    const journal: *change_journal_mod.Journal = @ptrCast(@alignCast(ptr));
    const entries = try journal.iterateOpaqueFrom(alloc, from_sequence + 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    return try collectEnrichmentDocumentGroupsFromEntries(alloc, entries);
}

fn journalIsSequenceVisible(ptr: *anyopaque, sequence: u64) !bool {
    const journal: *change_journal_mod.Journal = @ptrCast(@alignCast(ptr));
    return sequence <= journal.lastSequence();
}

fn primaryStoreForEachMatchingRecord(
    ptr: *anyopaque,
    alloc: Allocator,
    from_sequence: u64,
    hint: TargetHint,
    max_matched_entries: usize,
    ctx: *anyopaque,
    consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
) !MatchingRecordStats {
    _ = alloc;
    const store: *docstore_mod.DocStore = @ptrCast(@alignCast(ptr));
    const Context = struct {
        consumer_ctx: *anyopaque,
        consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
        stats: MatchingRecordStats = .{},

        fn handle(self: *@This(), sequence: u64, payload: []const u8) !void {
            self.stats.scanned_entries += 1;
            try self.consume(self.consumer_ctx, sequence, payload);
            self.stats.matched_entries += 1;
            self.stats.last_sequence = sequence;
        }

        fn handleErased(erased_ctx: *anyopaque, sequence: u64, payload: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(erased_ctx));
            return try self.handle(sequence, payload);
        }
    };

    var callback_ctx = Context{
        .consumer_ctx = ctx,
        .consume = consume,
    };
    callback_ctx.stats.scan_batches = 1;
    const replay_stats = store.forEachReplayLaneFrom(
        @intCast(@intFromEnum(hint)),
        from_sequence + 1,
        max_matched_entries,
        &callback_ctx,
        Context.handle,
    ) catch |err| switch (err) {
        error.ReplayIndexUnavailable => return try primaryStoreForEachMatchingRecordFallbackAll(
            store,
            from_sequence,
            hint,
            max_matched_entries,
            primaryStoreFallbackScanBudget(max_matched_entries),
            ctx,
            consume,
        ),
        StopReplayChunk.StopReplayChunk => return callback_ctx.stats,
        else => return err,
    };
    if (replay_stats.matched_entries == 0) {
        return try primaryStoreForEachMatchingRecordFallbackAll(
            store,
            from_sequence,
            hint,
            max_matched_entries,
            primaryStoreFallbackScanBudget(max_matched_entries),
            ctx,
            consume,
        );
    }
    callback_ctx.stats.scanned_entries = @max(callback_ctx.stats.scanned_entries, replay_stats.scanned_entries);
    callback_ctx.stats.hint_filter_skips += replay_stats.hint_filter_skips;
    callback_ctx.stats.scan_batches = @max(callback_ctx.stats.scan_batches, replay_stats.scan_batches);
    callback_ctx.stats.last_sequence = @max(callback_ctx.stats.last_sequence, replay_stats.last_sequence);
    return callback_ctx.stats;
}

fn primaryStoreLatestMatchingSequence(
    ptr: *anyopaque,
    alloc: Allocator,
    from_sequence: u64,
    hint: TargetHint,
) !u64 {
    _ = alloc;
    const store: *docstore_mod.DocStore = @ptrCast(@alignCast(ptr));
    return try store.latestReplaySequenceForHint(hint, from_sequence);
}

fn primaryStoreCollectEnrichmentDocumentGroups(ptr: *anyopaque, alloc: Allocator, from_sequence: u64) ![]PendingDocumentGroup {
    var pending = std.StringHashMapUnmanaged(PendingDocumentGroup).empty;
    errdefer cleanupPendingDocumentGroupMap(alloc, &pending);

    const Context = struct {
        alloc: Allocator,
        pending: *std.StringHashMapUnmanaged(PendingDocumentGroup),

        fn consume(ctx_ptr: *anyopaque, sequence: u64, payload: []const u8) !void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            var record = try change_journal_mod.decodeRecord(ctx.alloc, payload);
            defer record.deinit();
            if (!recordHasEnrichmentHint(record.record)) return;

            for (record.record.changed_doc_keys) |doc_key| {
                try appendPendingDocumentGroup(ctx.alloc, ctx.pending, sequence, doc_key);
            }
        }
    };

    var ctx = Context{
        .alloc = alloc,
        .pending = &pending,
    };
    _ = try primaryStoreForEachMatchingRecord(ptr, alloc, from_sequence, .enrichment, 0, &ctx, Context.consume);
    return try pendingDocumentGroupsToOwnedSlice(alloc, &pending);
}

fn primaryStoreIsSequenceVisible(ptr: *anyopaque, sequence: u64) !bool {
    const store: *docstore_mod.DocStore = @ptrCast(@alignCast(ptr));
    const Context = struct {
        expected: u64,
        visible: bool = false,

        fn consume(self: *@This(), found_sequence: u64, payload: []const u8) !void {
            _ = payload;
            self.visible = found_sequence == self.expected;
        }
    };
    var ctx = Context{ .expected = sequence };
    _ = store.forEachReplayLaneFrom(
        internal_keys.replay_all_kind,
        sequence,
        1,
        &ctx,
        Context.consume,
    ) catch |err| switch (err) {
        error.ReplayIndexUnavailable => return try primaryStoreIsSequenceVisibleFallback(store, sequence),
        else => return err,
    };
    return ctx.visible;
}

fn primaryStoreIsSequenceVisibleFallback(store: *docstore_mod.DocStore, sequence: u64) !bool {
    var txn = try store.beginCurrentScanTxn();
    defer txn.abort();

    var cur = try txn.openCursor();
    defer cur.close();

    const key = internal_keys.replayEntryKey(internal_keys.replay_all_kind, sequence);
    const entry = try cur.seekAtOrAfter(key[0..]) orelse return false;
    return std.mem.eql(u8, entry.key, key[0..]);
}

fn collectEnrichmentDocumentGroupsFromEntries(alloc: Allocator, entries: anytype) ![]PendingDocumentGroup {
    var pending = std.StringHashMapUnmanaged(PendingDocumentGroup).empty;
    errdefer cleanupPendingDocumentGroupMap(alloc, &pending);

    for (entries) |entry| {
        var record = try change_journal_mod.decodeRecord(alloc, entry.payload);
        defer record.deinit();
        if (!recordHasEnrichmentHint(record.record)) continue;

        for (record.record.changed_doc_keys) |doc_key| {
            try appendPendingDocumentGroup(alloc, &pending, entry.sequence, doc_key);
        }
    }

    return try pendingDocumentGroupsToOwnedSlice(alloc, &pending);
}

fn cleanupPendingDocumentGroupMap(alloc: Allocator, pending: *std.StringHashMapUnmanaged(PendingDocumentGroup)) void {
    var it = pending.iterator();
    while (it.next()) |entry| alloc.free(entry.key_ptr.*);
    pending.deinit(alloc);
}

fn appendPendingDocumentGroup(
    alloc: Allocator,
    pending: *std.StringHashMapUnmanaged(PendingDocumentGroup),
    sequence: u64,
    doc_key: []const u8,
) !void {
    const owned_key = try alloc.dupe(u8, doc_key);
    errdefer alloc.free(owned_key);
    const gop = try pending.getOrPut(alloc, owned_key);
    if (gop.found_existing) {
        alloc.free(owned_key);
    } else {
        gop.key_ptr.* = owned_key;
    }
    gop.value_ptr.* = .{
        .sequence = sequence,
        .doc_key = gop.key_ptr.*,
    };
}

fn pendingDocumentGroupsToOwnedSlice(
    alloc: Allocator,
    pending: *std.StringHashMapUnmanaged(PendingDocumentGroup),
) ![]PendingDocumentGroup {
    var groups = try alloc.alloc(PendingDocumentGroup, pending.count());
    var index: usize = 0;
    var it = pending.iterator();
    while (it.next()) |entry| : (index += 1) {
        groups[index] = entry.value_ptr.*;
    }
    pending.deinit(alloc);

    if (groups.len > 1) {
        std.mem.sort(PendingDocumentGroup, groups, {}, struct {
            fn lessThan(_: void, lhs: PendingDocumentGroup, rhs: PendingDocumentGroup) bool {
                if (lhs.sequence != rhs.sequence) return lhs.sequence < rhs.sequence;
                return std.mem.order(u8, lhs.doc_key, rhs.doc_key) == .lt;
            }
        }.lessThan);
    }

    return groups;
}

fn recordHasEnrichmentHint(record: change_journal_mod.Record) bool {
    for (record.target_hints) |hint| {
        if (hint == .enrichment) return true;
    }
    return false;
}

test "replay source collects changed documents from thin change journal" {
    const alloc = std.testing.allocator;

    var temp_path_nonce: u64 = 0;
    var path_buf: [256]u8 = undefined;
    const path = blk: {
        const base = "/tmp/antfly-replay-source-journal-doc-test-";
        const ts = platform_time.monotonicNs();
        const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
        const path_fmt = std.fmt.bufPrint(&path_buf, "{s}{d}-{d}\x00", .{ base, ts, nonce }) catch unreachable;
        break :blk @as([*:0]const u8, @ptrCast(path_fmt.ptr));
    };
    defer {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
    }

    var journal = try change_journal_mod.Journal.open(path, .{});
    defer journal.close();

    const first_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{ "doc:a", "doc:b" },
        .target_hints = &.{.enrichment},
    });
    defer alloc.free(first_payload);
    _ = try journal.appendOpaque(first_payload);

    const second_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{ .enrichment, .dense_vector },
    });
    defer alloc.free(second_payload);
    _ = try journal.appendOpaque(second_payload);

    const source = Source.fromJournal(&journal);
    const groups = try source.collectEnrichmentDocumentGroups(alloc, 0);
    defer freePendingDocumentGroups(alloc, groups);

    try std.testing.expectEqual(@as(usize, 2), groups.len);
    try std.testing.expectEqual(@as(u64, 1), groups[0].sequence);
    try std.testing.expectEqualStrings("doc:b", groups[0].doc_key);
    try std.testing.expectEqual(@as(u64, 2), groups[1].sequence);
    try std.testing.expectEqualStrings("doc:a", groups[1].doc_key);
}

test "replay source collects changed documents from replay stream" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const first_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{ "doc:a", "doc:b" },
        .target_hints = &.{.enrichment},
    });
    defer alloc.free(first_payload);
    try store.appendReplayOpaque(alloc, 1, first_payload);

    const second_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{ .enrichment, .dense_vector },
    });
    defer alloc.free(second_payload);
    try store.appendReplayOpaque(alloc, 2, second_payload);

    const source = Source.fromPrimaryStore(&store, null, null);
    const groups = try source.collectEnrichmentDocumentGroups(alloc, 0);
    defer freePendingDocumentGroups(alloc, groups);

    try std.testing.expectEqual(@as(usize, 2), groups.len);
    try std.testing.expectEqual(@as(u64, 1), groups[0].sequence);
    try std.testing.expectEqualStrings("doc:b", groups[0].doc_key);
    try std.testing.expectEqual(@as(u64, 2), groups[1].sequence);
    try std.testing.expectEqualStrings("doc:a", groups[1].doc_key);
}

test "replay source stops after first matching record" {
    const alloc = std.testing.allocator;

    var temp_path_nonce: u64 = 0;
    var path_buf: [256]u8 = undefined;
    const path = blk: {
        const base = "/tmp/antfly-replay-source-journal-stop-test-";
        const ts = platform_time.monotonicNs();
        const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
        const path_fmt = std.fmt.bufPrint(&path_buf, "{s}{d}-{d}\x00", .{ base, ts, nonce }) catch unreachable;
        break :blk @as([*:0]const u8, @ptrCast(path_fmt.ptr));
    };
    defer {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
    }

    var journal = try change_journal_mod.Journal.open(path, .{});
    defer journal.close();

    const first_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(first_payload);
    _ = try journal.appendOpaque(first_payload);

    const second_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(second_payload);
    _ = try journal.appendOpaque(second_payload);

    const Context = struct {
        calls: usize = 0,
        last_sequence: u64 = 0,

        fn consume(ptr: *anyopaque, sequence: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.last_sequence = sequence;
            return StopReplayChunk.StopReplayChunk;
        }
    };

    var context = Context{};
    const stats = try Source.fromJournal(&journal).forEachMatchingRecord(
        alloc,
        0,
        .dense_vector,
        0,
        &context,
        Context.consume,
    );

    try std.testing.expectEqual(@as(usize, 1), context.calls);
    try std.testing.expectEqual(@as(u64, 1), context.last_sequence);
    try std.testing.expectEqual(@as(usize, 0), stats.matched_entries);
    try std.testing.expectEqual(@as(u64, 0), stats.last_sequence);
}

test "replay source primary store stops after first matching record" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const first_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(first_payload);
    try store.appendReplayOpaque(alloc, 1, first_payload);

    const second_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(second_payload);
    try store.appendReplayOpaque(alloc, 2, second_payload);

    const Context = struct {
        calls: usize = 0,
        last_sequence: u64 = 0,

        fn consume(ptr: *anyopaque, sequence: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.last_sequence = sequence;
            return StopReplayChunk.StopReplayChunk;
        }
    };

    var context = Context{};
    const stats = try Source.fromPrimaryStore(&store, null, null).forEachMatchingRecord(
        alloc,
        0,
        .dense_vector,
        0,
        &context,
        Context.consume,
    );

    try std.testing.expectEqual(@as(usize, 1), context.calls);
    try std.testing.expectEqual(@as(u64, 1), context.last_sequence);
    try std.testing.expectEqual(@as(usize, 0), stats.matched_entries);
    try std.testing.expectEqual(@as(u64, 0), stats.last_sequence);
}

test "replay source journal respects max matched entries" {
    const alloc = std.testing.allocator;

    var temp_path_nonce: u64 = 0;
    var path_buf: [256]u8 = undefined;
    const path = blk: {
        const base = "/tmp/antfly-replay-source-journal-limit-test-";
        const ts = platform_time.monotonicNs();
        const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
        const path_fmt = std.fmt.bufPrint(&path_buf, "{s}{d}-{d}\x00", .{ base, ts, nonce }) catch unreachable;
        break :blk @as([*:0]const u8, @ptrCast(path_fmt.ptr));
    };
    defer {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
    }

    var journal = try change_journal_mod.Journal.open(path, .{});
    defer journal.close();

    const first_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(first_payload);
    _ = try journal.appendOpaque(first_payload);

    const second_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(second_payload);
    _ = try journal.appendOpaque(second_payload);

    const Context = struct {
        calls: usize = 0,
        last_sequence: u64 = 0,

        fn consume(ptr: *anyopaque, sequence: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.last_sequence = sequence;
        }
    };

    var context = Context{};
    const stats = try Source.fromJournal(&journal).forEachMatchingRecord(
        alloc,
        0,
        .dense_vector,
        1,
        &context,
        Context.consume,
    );

    try std.testing.expectEqual(@as(usize, 1), context.calls);
    try std.testing.expectEqual(@as(u64, 1), context.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), stats.matched_entries);
    try std.testing.expectEqual(@as(u64, 1), stats.last_sequence);
}

test "replay source primary store respects max matched entries" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const first_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(first_payload);
    try store.appendReplayOpaque(alloc, 1, first_payload);

    const second_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(second_payload);
    try store.appendReplayOpaque(alloc, 2, second_payload);

    const Context = struct {
        calls: usize = 0,
        last_sequence: u64 = 0,

        fn consume(ptr: *anyopaque, sequence: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.last_sequence = sequence;
        }
    };

    var context = Context{};
    const stats = try Source.fromPrimaryStore(&store, null, null).forEachMatchingRecord(
        alloc,
        0,
        .dense_vector,
        1,
        &context,
        Context.consume,
    );

    try std.testing.expectEqual(@as(usize, 1), context.calls);
    try std.testing.expectEqual(@as(u64, 1), context.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), stats.matched_entries);
    try std.testing.expectEqual(@as(u64, 1), stats.last_sequence);
}

test "replay source primary store hinted replay skips non-matching records before callback" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const full_text_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:ft"},
        .target_hints = &.{.full_text},
    });
    defer alloc.free(full_text_payload);
    try store.appendReplayOpaque(alloc, 1, full_text_payload);

    const dense_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:dense"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(dense_payload);
    try store.appendReplayOpaque(alloc, 2, dense_payload);

    const Context = struct {
        calls: usize = 0,
        last_sequence: u64 = 0,

        fn consume(ptr: *anyopaque, sequence: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.last_sequence = sequence;
        }
    };

    var context = Context{};
    const stats = try Source.fromPrimaryStore(&store, null, null).forEachMatchingRecord(
        alloc,
        0,
        .dense_vector,
        0,
        &context,
        Context.consume,
    );

    try std.testing.expectEqual(@as(usize, 1), context.calls);
    try std.testing.expectEqual(@as(u64, 2), context.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), stats.matched_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 0), stats.hint_filter_skips);
    try std.testing.expectEqual(@as(usize, 1), stats.scan_batches);
    try std.testing.expectEqual(@as(u64, 2), stats.last_sequence);
}

test "replay source primary store falls back to all lane when hint lane is missing" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const full_text_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:ft"},
        .target_hints = &.{.full_text},
    });
    defer alloc.free(full_text_payload);

    const dense_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:dense"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(dense_payload);

    var batch = try store.beginWriteBatch();
    errdefer batch.abort();
    try batch.put(internal_keys.replay_meta_init_key[0..], "");
    const full_text_key = internal_keys.replayEntryKey(internal_keys.replay_all_kind, 1);
    try batch.put(full_text_key[0..], full_text_payload);
    const dense_key = internal_keys.replayEntryKey(internal_keys.replay_all_kind, 2);
    try batch.put(dense_key[0..], dense_payload);
    try batch.commit();

    const Context = struct {
        calls: usize = 0,
        last_sequence: u64 = 0,

        fn consume(ptr: *anyopaque, sequence: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.last_sequence = sequence;
        }
    };

    var context = Context{};
    const stats = try Source.fromPrimaryStore(&store, null, null).forEachMatchingRecord(
        alloc,
        0,
        .dense_vector,
        0,
        &context,
        Context.consume,
    );

    try std.testing.expectEqual(@as(usize, 1), context.calls);
    try std.testing.expectEqual(@as(u64, 2), context.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), stats.matched_entries);
    try std.testing.expectEqual(@as(usize, 2), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.hint_filter_skips);
    try std.testing.expectEqual(@as(usize, 1), stats.scan_batches);
    try std.testing.expectEqual(@as(u64, 2), stats.last_sequence);
}

test "replay source primary fallback cursor is bounded across unrelated rows" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    var batch = try store.beginWriteBatch();
    errdefer batch.abort();
    try batch.put(internal_keys.replay_meta_init_key[0..], "");
    var sequence: u64 = 1;
    while (sequence <= primary_store_fallback_scan_budget_min) : (sequence += 1) {
        const payload = try change_journal_mod.encodeRecord(alloc, .{
            .sequence = sequence,
            .changed_doc_keys = &.{"doc:ft"},
            .target_hints = &.{.full_text},
        });
        defer alloc.free(payload);
        const key = internal_keys.replayEntryKey(internal_keys.replay_all_kind, sequence);
        try batch.put(key[0..], payload);
    }
    const dense_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = sequence,
        .changed_doc_keys = &.{"doc:dense"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(dense_payload);
    const dense_key = internal_keys.replayEntryKey(internal_keys.replay_all_kind, sequence);
    try batch.put(dense_key[0..], dense_payload);
    try batch.commit();

    const Context = struct {
        calls: usize = 0,
        last_sequence: u64 = 0,

        fn consume(ptr: *anyopaque, sequence_value: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.last_sequence = sequence_value;
        }
    };

    var cursor = try Source.fromPrimaryStore(&store, null, null).openMatchingCursor(alloc, 0, .dense_vector);
    defer cursor.deinit(alloc);

    var first = Context{};
    const first_stats = try cursor.forEachNext(1, &first, Context.consume);
    try std.testing.expectEqual(@as(usize, 0), first.calls);
    try std.testing.expectEqual(@as(usize, 0), first_stats.matched_entries);
    try std.testing.expectEqual(primary_store_fallback_scan_budget_min, first_stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, primary_store_fallback_scan_budget_min), first_stats.hint_filter_skips);
    try std.testing.expectEqual(@as(u64, primary_store_fallback_scan_budget_min), first_stats.last_sequence);

    var second = Context{};
    const second_stats = try cursor.forEachNext(1, &second, Context.consume);
    try std.testing.expectEqual(@as(usize, 1), second.calls);
    try std.testing.expectEqual(@as(u64, primary_store_fallback_scan_budget_min + 1), second.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), second_stats.matched_entries);
    try std.testing.expectEqual(@as(usize, 1), second_stats.scanned_entries);
    try std.testing.expectEqual(@as(u64, primary_store_fallback_scan_budget_min + 1), second_stats.last_sequence);
}

test "replay source primary cursor resumes after stop chunk progress" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const first_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:first"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(first_payload);
    try store.appendReplayOpaque(alloc, 1, first_payload);

    const second_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:second"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(second_payload);
    try store.appendReplayOpaque(alloc, 2, second_payload);

    const Context = struct {
        calls: usize = 0,
        sequences: [4]u64 = .{ 0, 0, 0, 0 },

        fn stopAfterFirst(ptr: *anyopaque, sequence: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.sequences[self.calls] = sequence;
            self.calls += 1;
            if (self.calls > 1) return StopReplayChunk.StopReplayChunk;
        }

        fn consume(ptr: *anyopaque, sequence: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.sequences[self.calls] = sequence;
            self.calls += 1;
        }
    };

    var cursor = try Source.fromPrimaryStore(&store, null, null).openMatchingCursor(alloc, 0, .dense_vector);
    defer cursor.deinit(alloc);

    var context = Context{};
    const first = try cursor.forEachNext(0, &context, Context.stopAfterFirst);
    try std.testing.expectEqual(@as(usize, 2), context.calls);
    try std.testing.expectEqual(@as(u64, 1), context.sequences[0]);
    try std.testing.expectEqual(@as(u64, 2), context.sequences[1]);
    try std.testing.expectEqual(@as(u64, 1), first.last_sequence);

    const second = try cursor.forEachNext(0, &context, Context.consume);
    try std.testing.expectEqual(@as(usize, 3), context.calls);
    try std.testing.expectEqual(@as(u64, 2), context.sequences[2]);
    try std.testing.expectEqual(@as(u64, 2), second.last_sequence);
}

test "replay source primary store collects enrichment groups from hint lane" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const full_text_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:ft"},
        .target_hints = &.{.full_text},
    });
    defer alloc.free(full_text_payload);

    const enrichment_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:enriched"},
        .target_hints = &.{.enrichment},
    });
    defer alloc.free(enrichment_payload);

    try store.appendReplayOpaque(alloc, 1, full_text_payload);
    try store.appendReplayOpaque(alloc, 2, enrichment_payload);

    const groups = try Source.fromPrimaryStore(&store, null, null).collectEnrichmentDocumentGroups(alloc, 0);
    defer freePendingDocumentGroups(alloc, groups);

    try std.testing.expectEqual(@as(usize, 1), groups.len);
    try std.testing.expectEqual(@as(u64, 2), groups[0].sequence);
    try std.testing.expectEqualStrings("doc:enriched", groups[0].doc_key);
}

test "replay source primary store matching cursor resumes across bounded windows" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    inline for (.{ 1, 2, 3 }) |sequence| {
        const payload = try change_journal_mod.encodeRecord(alloc, .{
            .sequence = sequence,
            .changed_doc_keys = &.{"doc"},
            .target_hints = &.{.dense_vector},
        });
        defer alloc.free(payload);
        try store.appendReplayOpaque(alloc, sequence, payload);
    }

    const Context = struct {
        calls: usize = 0,
        last_sequence: u64 = 0,

        fn consume(ptr: *anyopaque, sequence: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.last_sequence = sequence;
        }
    };

    var cursor = try Source.fromPrimaryStore(&store, null, null).openMatchingCursor(alloc, 0, .dense_vector);
    defer cursor.deinit(alloc);

    var first = Context{};
    const first_stats = try cursor.forEachNext(1, &first, Context.consume);
    try std.testing.expectEqual(@as(usize, 1), first.calls);
    try std.testing.expectEqual(@as(u64, 1), first.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), first_stats.matched_entries);
    try std.testing.expectEqual(@as(u64, 1), first_stats.last_sequence);

    var second = Context{};
    const second_stats = try cursor.forEachNext(1, &second, Context.consume);
    try std.testing.expectEqual(@as(usize, 1), second.calls);
    try std.testing.expectEqual(@as(u64, 2), second.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), second_stats.matched_entries);
    try std.testing.expectEqual(@as(u64, 2), second_stats.last_sequence);
}

test "replay source primary store missing replay index behaves as empty stream" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const Context = struct {
        calls: usize = 0,

        fn consume(ptr: *anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
    };

    var cursor = try Source.fromPrimaryStore(&store, null, null).openMatchingCursor(alloc, 0, .dense_vector);
    defer cursor.deinit(alloc);

    var ctx = Context{};
    const stats = try cursor.forEachNext(1, &ctx, Context.consume);
    try std.testing.expectEqual(@as(usize, 0), ctx.calls);
    try std.testing.expectEqual(@as(usize, 0), stats.matched_entries);
    try std.testing.expectEqual(@as(u64, 0), stats.last_sequence);
}

test "replay source primary visibility checks the exact replay sequence" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const first_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:one"},
    });
    defer alloc.free(first_payload);
    try store.appendReplayOpaque(alloc, 1, first_payload);

    const third_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 3,
        .changed_doc_keys = &.{"doc:three"},
    });
    defer alloc.free(third_payload);
    try store.appendReplayOpaque(alloc, 3, third_payload);

    const source = Source.fromPrimaryStore(&store, null, null);
    try std.testing.expect(try source.isSequenceVisible(1));
    try std.testing.expect(!(try source.isSequenceVisible(2)));
    try std.testing.expect(try source.isSequenceVisible(3));
    try std.testing.expect(!(try source.isSequenceVisible(4)));
}
