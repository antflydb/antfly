// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Restartable validation of cross-document producer read sets. Primary and
//! artifact mutations invalidate a completed pass in their own transaction.
//! The cursor certifies only work performed against one unchanged mutation
//! epoch; producer completions may drain concurrently, but cannot evade it.
const std = @import("std");
const publication = @import("artifact_publication.zig");
pub const key = "\x00\x00__artifact_publication__:validation";
pub const max_cursor_bytes = 1024 * 1024;
pub const State = struct {
    namespace: publication.Namespace,
    authority_epoch: u64,
    mutation_epoch: u64 = 0,
    scan_epoch: u64 = 0,
    complete: bool = false,
    cursor: []const u8 = "",

    fn requireAuthority(self: State, authority: publication.Authority) !void {
        if (self.authority_epoch != authority.epoch or !std.mem.eql(u8, &self.namespace, &authority.namespace)) return error.ArtifactCatalogDrift;
    }
};

pub fn load(txn: anytype) !?State {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len < 89 or raw.len > 89 + max_cursor_bytes or !std.mem.eql(u8, raw[0..4], "APV1") or raw[52] > 1) return error.ArtifactCatalogCorrupt;
    const length = std.mem.readInt(u32, raw[53..57], .little);
    if (raw.len != 89 + @as(usize, length)) return error.ArtifactCatalogCorrupt;
    var digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    const state: State = .{ .namespace = raw[4..28].*, .authority_epoch = std.mem.readInt(u64, raw[28..36], .little), .mutation_epoch = std.mem.readInt(u64, raw[36..44], .little), .scan_epoch = std.mem.readInt(u64, raw[44..52], .little), .complete = raw[52] == 1, .cursor = raw[57 .. 57 + length] };
    if (state.authority_epoch == 0 or state.scan_epoch > state.mutation_epoch or
        (state.complete and (state.scan_epoch != state.mutation_epoch or state.cursor.len != 0))) return error.ArtifactCatalogCorrupt;
    return state;
}

fn store(alloc: std.mem.Allocator, txn: anytype, state: State) !void {
    if (state.cursor.len > max_cursor_bytes) return error.TransactionTooLarge;
    const raw = try alloc.alloc(u8, 89 + state.cursor.len);
    defer alloc.free(raw);
    @memcpy(raw[0..4], "APV1");
    @memcpy(raw[4..28], &state.namespace);
    std.mem.writeInt(u64, raw[28..36], state.authority_epoch, .little);
    std.mem.writeInt(u64, raw[36..44], state.mutation_epoch, .little);
    std.mem.writeInt(u64, raw[44..52], state.scan_epoch, .little);
    raw[52] = @intFromBool(state.complete);
    std.mem.writeInt(u32, raw[53..57], @intCast(state.cursor.len), .little);
    @memcpy(raw[57 .. raw.len - 32], state.cursor);
    std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
    try txn.put(key, raw);
}

pub fn begin(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority) !void {
    if (try load(txn)) |state| {
        if (state.authority_epoch == authority.epoch and std.mem.eql(u8, &state.namespace, &authority.namespace)) return;
    }
    try store(alloc, txn, .{ .namespace = authority.namespace, .authority_epoch = authority.epoch });
}

/// Once per physical mutation transaction, never once per field or read.
pub fn invalidate(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority) !void {
    var state = (try load(txn)) orelse return error.ArtifactCatalogDrift;
    try state.requireAuthority(authority);
    state.mutation_epoch = std.math.add(u64, state.mutation_epoch, 1) catch return error.ArtifactCatalogCorrupt;
    state.scan_epoch = state.mutation_epoch;
    state.complete = false;
    state.cursor = "";
    try store(alloc, txn, state);
}

/// Called only after a bounded page of accepted provenance is validated in
/// the same writer snapshot. A concurrent mutation makes the old page stale;
/// do not advance its cursor or interpret it as a successful empty page.
pub fn advance(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, expected: State, next_cursor: []const u8, at_end: bool) !void {
    try expected.requireAuthority(authority);
    var current = (try load(txn)) orelse return error.ArtifactCatalogDrift;
    try current.requireAuthority(authority);
    if (current.mutation_epoch != expected.mutation_epoch or current.scan_epoch != expected.scan_epoch or
        !std.mem.eql(u8, current.cursor, expected.cursor) or current.complete != expected.complete) return error.EnrichmentSourceChanged;
    if (!at_end and std.mem.order(u8, next_cursor, current.cursor) != .gt) return error.InvalidBatchRequest;
    current.cursor = if (at_end) "" else next_cursor;
    current.complete = at_end;
    try store(alloc, txn, current);
}

pub fn requireComplete(txn: anytype, authority: publication.Authority) !void {
    const state = (try load(txn)) orelse return error.ArtifactCatalogDrift;
    try state.requireAuthority(authority);
    if (!state.complete or state.scan_epoch != state.mutation_epoch) return error.ArtifactCatalogDrift;
}

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    command: publication.Command,
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
    }
};

/// Discover and validate a bounded page on an immutable, transient read.
/// Physical proof references, not artifact payloads, are the scan index.
/// Nothing is acknowledged here: even an empty page must pass ordered apply.
pub fn prepareRaft(alloc: std.mem.Allocator, store_handle: anytype) !?Prepared {
    const provenance = @import("artifact_producer_provenance.zig");
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var read = try store_handle.beginReadTxnWithBlockCacheAdmission(.transient);
    defer read.abort();
    const authority = (try publication.authority(&read)) orelse {
        arena.deinit();
        return null;
    };
    _ = try @import("../source_authority.zig").require(&read, .raft, authority.namespace);
    const state = (try load(&read)) orelse {
        var catalog = (try @import("artifact_inventory.zig").load(alloc, &read)) orelse return error.ArtifactCatalogCorrupt;
        defer catalog.deinit();
        if (catalog.value.command.binding.epoch != authority.epoch or
            !std.mem.eql(u8, &catalog.value.command.namespace, &authority.namespace) or
            !std.mem.eql(u8, &catalog.value.command.binding.digest, &authority.catalog_digest)) return error.ArtifactCatalogDrift;
        if (catalog.value.command.binding.effect_protocol != 14) return error.ArtifactCatalogCorrupt;
        arena.deinit();
        return null;
    };
    try state.requireAuthority(authority);
    if (state.complete) {
        arena.deinit();
        return null;
    }
    const prefix = provenance.referencePrefix(authority);
    if (state.cursor.len != 0 and (!std.mem.startsWith(u8, state.cursor, &prefix) or state.cursor.len != prefix.len + 32)) return error.ArtifactCatalogCorrupt;
    const expected = try owned.dupe(u8, state.cursor);
    var cursor = try read.openPhysicalCursorAdapter();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(if (expected.len == 0) &prefix else expected);
    if (entry) |item| if (std.mem.eql(u8, item.key, expected)) {
        entry = try cursor.next();
    };
    var next: []const u8 = expected;
    var repairs: std.ArrayList([]const u8) = .empty;
    var visited: usize = 0;
    var bytes: usize = 0;
    var repair_bytes: usize = 0;
    const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
    while (entry) |item| {
        if (!std.mem.startsWith(u8, item.key, &prefix)) {
            entry = null;
            break;
        }
        if (visited != 0 and (visited >= 128 or bytes >= 64 * 1024 or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
        if (item.key.len != prefix.len + 32 or item.value.len != 32) return error.ArtifactCatalogCorrupt;
        const digest: publication.Digest = item.value[0..32].*;
        const raw = read.get(&provenance.key(authority.namespace, digest)) catch |err| switch (err) {
            error.NotFound => return error.ArtifactCatalogCorrupt,
            else => return err,
        };
        var proof = try provenance.decodeAlloc(alloc, raw);
        defer proof.deinit();
        const source = for (proof.proof.sources) |source| {
            if (std.mem.eql(u8, item.key, &provenance.referenceKey(proof.proof.inputCommand(), source))) break source;
        } else return error.ArtifactCatalogCorrupt;
        // Reserve command space before doing work; leave this reference for
        // the next page rather than skipping it when a repair key is large.
        if (source.document_key.len > max_cursor_bytes) return error.ArtifactCatalogCorrupt;
        if (visited != 0 and source.document_key.len > 2 * 1024 * 1024 - repair_bytes) break;
        var accepted = provenance.readConvergedForSource(alloc, &read, proof.proof.inputCommand(), source) catch |err| switch (err) {
            error.EnrichmentSourceChanged => blk: {
                try repairs.append(owned, try owned.dupe(u8, source.document_key));
                repair_bytes += source.document_key.len;
                break :blk null;
            },
            else => return err,
        };
        if (accepted) |*value| value.deinit();
        next = try owned.dupe(u8, item.key);
        bytes +|= raw.len +| item.key.len;
        visited += 1;
        entry = try cursor.next();
    }
    var command: publication.Command = .{
        .mode = .validate_inputs,
        .namespace = authority.namespace,
        .authority_epoch = authority.epoch,
        .catalog_digest = authority.catalog_digest,
        .producer_name = "",
        .producer_generation = 0,
        .sources = &.{},
        .mutations = &.{},
        .publication_digest = @splat(0),
        .validation = .{ .mutation_epoch = state.mutation_epoch, .expected_cursor = expected, .next_cursor = next, .at_end = entry == null, .repair_documents = repairs.items },
    };
    command.publication_digest = command.digest();
    try command.validate(owned);
    return .{ .arena = arena, .command = command };
}

/// Apply the leader's selected result, never a follower-local scan. Every
/// source/artifact mutation invalidates the epoch in its own transaction.
/// Stale/replayed pages are inert, and repairs retain the CURRENT input stamp.
pub fn stageRaft(alloc: std.mem.Allocator, txn: anytype, command: publication.Command) !bool {
    if (command.mode != .validate_inputs) return error.InvalidBatchRequest;
    const page = command.validation orelse return error.InvalidBatchRequest;
    try page.validate();
    const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (authority.epoch != command.authority_epoch or !std.mem.eql(u8, &authority.namespace, &command.namespace) or
        !std.mem.eql(u8, &authority.catalog_digest, &command.catalog_digest)) return error.ArtifactCatalogDrift;
    const current = (try load(txn)) orelse return error.ArtifactCatalogCorrupt;
    try current.requireAuthority(authority);
    if (current.complete) return true;
    if (current.mutation_epoch != page.mutation_epoch or !std.mem.eql(u8, current.cursor, page.expected_cursor)) return false;
    const prefix = @import("artifact_producer_provenance.zig").referencePrefix(authority);
    for ([_][]const u8{ page.expected_cursor, page.next_cursor }) |cursor| {
        if (cursor.len != 0 and (cursor.len != prefix.len + 32 or !std.mem.startsWith(u8, cursor, &prefix))) return error.InvalidBatchRequest;
    }
    const obligations = @import("artifact_producer_obligations.zig");
    const pending = (try obligations.load(txn)) orelse return error.ArtifactCatalogCorrupt;
    try pending.requireAuthority(authority);
    if (pending.sealed_attempt != null) return error.ArtifactCatalogDrift;
    // Serialize the borrowed cursor before mark() replaces any store values.
    try advance(alloc, txn, authority, current, page.next_cursor, page.at_end);
    for (page.repair_documents) |document| {
        _ = try obligations.mark(alloc, txn, authority, document, try publication.inputRevision(txn, authority.namespace, document));
    }
    return page.at_end;
}

test "ordered artifact inventory producer proof cursor rejects concurrent mutation and survives reopen" {
    const Fake = struct {
        raw: ?[]u8 = null,
        pub fn get(self: *@This(), name: []const u8) anyerror![]const u8 {
            if (!std.mem.eql(u8, name, key)) return error.NotFound;
            return self.raw orelse error.NotFound;
        }
        pub fn put(self: *@This(), name: []const u8, value: []const u8) !void {
            try std.testing.expectEqualStrings(key, name);
            const copy = try std.testing.allocator.dupe(u8, value);
            if (self.raw) |old| std.testing.allocator.free(old);
            self.raw = copy;
        }
    };
    const alloc = std.testing.allocator;
    var txn: Fake = .{};
    defer if (txn.raw) |value| alloc.free(value);
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    try begin(alloc, &txn, authority);
    try std.testing.expectError(error.ArtifactCatalogDrift, requireComplete(&txn, authority));
    const stale = (try load(&txn)).?;
    try invalidate(alloc, &txn, authority);
    try std.testing.expectError(error.EnrichmentSourceChanged, advance(alloc, &txn, authority, stale, "proof-a", false));
    try advance(alloc, &txn, authority, (try load(&txn)).?, "proof-a", false);
    // Reopening observes only durable bytes; no in-memory traversal state is
    // required to continue at the committed cursor.
    var reopened: Fake = .{ .raw = try alloc.dupe(u8, txn.raw.?) };
    defer if (reopened.raw) |value| alloc.free(value);
    const resumed = (try load(&reopened)).?;
    try std.testing.expectEqualStrings("proof-a", resumed.cursor);
    try advance(alloc, &reopened, authority, resumed, "", true);
    try requireComplete(&reopened, authority);
    try invalidate(alloc, &reopened, authority);
    try std.testing.expectError(error.ArtifactCatalogDrift, requireComplete(&reopened, authority));
    try std.testing.expectEqualStrings("", (try load(&reopened)).?.cursor);
}
