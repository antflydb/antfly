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
const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const doc_set = @import("doc_set.zig");

pub const DocOrdinal = doc_set.DocOrdinal;

pub const Namespace = struct {
    table_id: u64 = 0,
    shard_id: u64 = 0,
    range_id: u64 = 0,

    pub fn eql(self: Namespace, other: Namespace) bool {
        return self.table_id == other.table_id and self.shard_id == other.shard_id and self.range_id == other.range_id;
    }
};

pub const default_namespace = Namespace{};

pub const NamespaceMismatchPolicy = enum {
    reject,
    use_existing,
};

pub const OrdinalState = struct {
    canonical_doc_id: u64,
    created_generation: u64,
    deleted_generation: ?u64 = null,

    pub fn isLive(self: OrdinalState) bool {
        return self.deleted_generation == null;
    }

    pub fn isVisibleAt(self: OrdinalState, generation: u64) bool {
        if (self.created_generation > generation) return false;
        if (self.deleted_generation) |deleted| return deleted > generation;
        return true;
    }

    pub fn validate(self: OrdinalState) !void {
        if (self.canonical_doc_id == 0) return error.InvalidDocIdentity;
        if (self.deleted_generation) |deleted| {
            if (deleted < self.created_generation) return error.InvalidDocIdentity;
        }
    }
};

pub const ResolvedIdentity = struct {
    ordinal: DocOrdinal,
    canonical_doc_id: u64,
    state: OrdinalState,
};

pub const Stats = struct {
    next_ordinal: DocOrdinal = 1,
    allocated_ordinals: u64 = 0,
    state_rows: u64 = 0,
    live_ordinals: u64 = 0,
    tombstone_ordinals: u64 = 0,
    min_created_generation: u64 = 0,
    max_created_generation: u64 = 0,
    min_deleted_generation: u64 = 0,
    max_deleted_generation: u64 = 0,
    complete: bool = false,
};

pub const VisibilitySummary = struct {
    live_ordinals: u64 = 0,
    tombstone_ordinals: u64 = 0,
    max_created_generation: u64 = 0,
    min_deleted_generation: u64 = 0,
    max_deleted_generation: u64 = 0,
};

pub const AllNewTrustedState = struct {
    next_ordinal: DocOrdinal = 1,
    visibility_summary: VisibilitySummary = .{},
    namespace_written: bool = false,
};

pub fn canonicalDocId(table_id: u64, shard_id: u64, doc_id: []const u8) u64 {
    return canonicalDocIdForNamespace(.{ .table_id = table_id, .shard_id = shard_id }, doc_id);
}

pub fn canonicalDocIdForNamespace(namespace: Namespace, doc_id: []const u8) u64 {
    var h = std.hash.XxHash64.init(0);
    var buf: [24]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], namespace.table_id, .little);
    std.mem.writeInt(u64, buf[8..16], namespace.shard_id, .little);
    std.mem.writeInt(u64, buf[16..24], namespace.range_id, .little);
    h.update(&buf);
    h.update(doc_id);
    const value = h.final();
    return if (value == 0) 1 else value;
}

pub fn loadNamespaceTxn(txn: anytype) !?Namespace {
    const mutable_txn = txn;
    const raw = mutable_txn.get(internal_keys.identity_namespace_key[0..]) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try decodeNamespace(raw);
}

pub fn loadNamespaceFromStore(store: *docstore_mod.DocStore) !?Namespace {
    var txn = try store.beginProbeTxn();
    defer txn.abort();
    return try loadNamespaceTxn(&txn);
}

pub fn loadOrInitNamespace(
    store: *docstore_mod.DocStore,
    configured_namespace: ?Namespace,
    persist_if_missing: bool,
) !Namespace {
    return try loadOrInitNamespaceWithPolicy(store, configured_namespace, persist_if_missing, .reject);
}

pub fn loadOrInitNamespaceWithPolicy(
    store: *docstore_mod.DocStore,
    configured_namespace: ?Namespace,
    persist_if_missing: bool,
    mismatch_policy: NamespaceMismatchPolicy,
) !Namespace {
    if (try loadNamespaceFromStore(store)) |stored| {
        if (configured_namespace) |configured| {
            if (!stored.eql(configured)) switch (mismatch_policy) {
                .reject => return error.IdentityNamespaceMismatch,
                .use_existing => return stored,
            };
        }
        return stored;
    }

    const namespace = configured_namespace orelse default_namespace;
    if (persist_if_missing) try writeNamespaceToStore(store, namespace);
    return namespace;
}

pub fn writeNamespaceToStore(store: *docstore_mod.DocStore, namespace: Namespace) !void {
    var buf: [24]u8 = undefined;
    encodeNamespace(buf[0..], namespace);
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    try txn.put(internal_keys.identity_namespace_key[0..], &buf);
    try txn.commit();
}

pub fn reassignNamespaceAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    namespace: Namespace,
) !void {
    try validateStoreAlloc(alloc, store);

    var ordinal_rows = OrdinalDocRows{};
    defer ordinal_rows.deinit(alloc);
    try collectOrdinalDocRowsAlloc(alloc, store, &ordinal_rows);

    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    var namespace_value: [24]u8 = undefined;
    encodeNamespace(namespace_value[0..], namespace);
    try txn.put(internal_keys.identity_namespace_key[0..], &namespace_value);

    for (ordinal_rows.items.items) |row| {
        var state = (try lookupStateTxn(&txn, row.ordinal)) orelse return error.InvalidDocIdentity;
        try deleteCanonicalOrdinalMappingTxn(&txn, state.canonical_doc_id);
        state.canonical_doc_id = canonicalDocIdForNamespace(namespace, row.doc_id);
        try writeOrdinalStateTxn(&txn, row.ordinal, state);
        try writeCanonicalOrdinalMappingTxn(&txn, state.canonical_doc_id, row.ordinal);
    }

    try txn.commit();
    try validateStoreAlloc(alloc, store);
}

fn encodeNamespace(buf: []u8, namespace: Namespace) void {
    std.debug.assert(buf.len == 24);
    std.mem.writeInt(u64, buf[0..8], namespace.table_id, .big);
    std.mem.writeInt(u64, buf[8..16], namespace.shard_id, .big);
    std.mem.writeInt(u64, buf[16..24], namespace.range_id, .big);
}

fn decodeNamespace(raw: []const u8) !Namespace {
    if (raw.len != 16 and raw.len != 24) return error.InvalidDocIdentityNamespace;
    return .{
        .table_id = std.mem.readInt(u64, raw[0..8], .big),
        .shard_id = std.mem.readInt(u64, raw[8..16], .big),
        .range_id = if (raw.len == 24) std.mem.readInt(u64, raw[16..24], .big) else 0,
    };
}

fn encodeVisibilitySummary(summary: VisibilitySummary) [40]u8 {
    var buf: [40]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], summary.live_ordinals, .big);
    std.mem.writeInt(u64, buf[8..16], summary.tombstone_ordinals, .big);
    std.mem.writeInt(u64, buf[16..24], summary.max_created_generation, .big);
    std.mem.writeInt(u64, buf[24..32], summary.min_deleted_generation, .big);
    std.mem.writeInt(u64, buf[32..40], summary.max_deleted_generation, .big);
    return buf;
}

fn decodeVisibilitySummary(raw: []const u8) !VisibilitySummary {
    if (raw.len != 40) return error.InvalidDocIdentity;
    return .{
        .live_ordinals = std.mem.readInt(u64, raw[0..8], .big),
        .tombstone_ordinals = std.mem.readInt(u64, raw[8..16], .big),
        .max_created_generation = std.mem.readInt(u64, raw[16..24], .big),
        .min_deleted_generation = std.mem.readInt(u64, raw[24..32], .big),
        .max_deleted_generation = std.mem.readInt(u64, raw[32..40], .big),
    };
}

fn readVisibilitySummaryTxn(txn: anytype) !?VisibilitySummary {
    const mutable_txn = txn;
    const raw = mutable_txn.get(internal_keys.identity_visibility_summary_key[0..]) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try decodeVisibilitySummary(raw);
}

pub fn lookupOrdinalTxn(alloc: Allocator, txn: anytype, doc_id: []const u8) !?DocOrdinal {
    const mutable_txn = txn;
    const key = try internal_keys.identityDocToOrdinalKeyAlloc(alloc, doc_id);
    defer alloc.free(key);

    const raw = mutable_txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != @sizeOf(u32)) return error.InvalidDocIdentity;
    return std.mem.readInt(u32, raw[0..4], .big);
}

pub fn lookupOrdinalsTxnAlloc(alloc: Allocator, txn: anytype, doc_ids: []const []const u8) ![]?DocOrdinal {
    const mutable_txn = txn;
    var ordinals = try alloc.alloc(?DocOrdinal, doc_ids.len);
    errdefer alloc.free(ordinals);
    @memset(ordinals, null);
    if (doc_ids.len == 0) return ordinals;

    const PendingOrdinalLookup = struct {
        source_index: usize,
        key: []u8,

        fn lessThan(_: void, lhs: @This(), rhs: @This()) bool {
            return std.mem.lessThan(u8, lhs.key, rhs.key);
        }
    };

    var pending = try alloc.alloc(PendingOrdinalLookup, doc_ids.len);
    defer {
        for (pending) |item| alloc.free(item.key);
        alloc.free(pending);
    }
    for (doc_ids, 0..) |doc_id, i| {
        pending[i] = .{
            .source_index = i,
            .key = try internal_keys.identityDocToOrdinalKeyAlloc(alloc, doc_id),
        };
    }
    std.sort.pdq(PendingOrdinalLookup, pending, {}, PendingOrdinalLookup.lessThan);

    var read_keys = try alloc.alloc([]const u8, pending.len);
    defer alloc.free(read_keys);
    var read_values = try alloc.alloc(?[]const u8, pending.len);
    defer alloc.free(read_values);
    for (pending, 0..) |item, i| {
        read_keys[i] = item.key;
        read_values[i] = null;
    }

    try mutable_txn.getManySorted(read_keys, read_values);
    for (pending, 0..) |item, i| {
        const raw = read_values[i] orelse continue;
        if (raw.len != @sizeOf(u32)) return error.InvalidDocIdentity;
        ordinals[item.source_index] = std.mem.readInt(u32, raw[0..4], .big);
    }
    return ordinals;
}

pub fn lookupDocIdTxn(alloc: Allocator, txn: anytype, ordinal: DocOrdinal) !?[]u8 {
    const mutable_txn = txn;
    const key = internal_keys.identityOrdinalToDocKey(ordinal);
    const raw = mutable_txn.get(key[0..]) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try alloc.dupe(u8, raw);
}

pub fn lookupStateTxn(txn: anytype, ordinal: DocOrdinal) !?OrdinalState {
    const mutable_txn = txn;
    const key = internal_keys.identityOrdinalStateKey(ordinal);
    const raw = mutable_txn.get(key[0..]) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try decodeOrdinalState(raw);
}

pub fn lookupCanonicalOrdinalTxn(txn: anytype, canonical_doc_id: u64) !?DocOrdinal {
    const mutable_txn = txn;
    const key = internal_keys.identityCanonicalToOrdinalKey(canonical_doc_id);
    const raw = mutable_txn.get(key[0..]) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != @sizeOf(u32)) return error.InvalidDocIdentity;
    return std.mem.readInt(u32, raw[0..4], .big);
}

pub fn ensureOrdinalTxn(
    alloc: Allocator,
    txn: anytype,
    table_id: u64,
    shard_id: u64,
    generation: u64,
    doc_id: []const u8,
) !ResolvedIdentity {
    return try ensureOrdinalForNamespaceTxn(alloc, txn, .{ .table_id = table_id, .shard_id = shard_id }, generation, doc_id);
}

pub fn ensureOrdinalForNamespaceTxn(
    alloc: Allocator,
    txn: anytype,
    namespace: Namespace,
    generation: u64,
    doc_id: []const u8,
) !ResolvedIdentity {
    const mutable_txn = txn;
    if (try lookupOrdinalTxn(alloc, mutable_txn, doc_id)) |ordinal| {
        const existing_state = try lookupStateTxn(mutable_txn, ordinal);
        var state = existing_state orelse OrdinalState{
            .canonical_doc_id = canonicalDocIdForNamespace(namespace, doc_id),
            .created_generation = generation,
        };
        if (existing_state == null or state.deleted_generation != null) {
            if (try readVisibilitySummaryTxn(mutable_txn)) |summary_before| {
                var summary = summary_before;
                if (state.deleted_generation != null) {
                    noteSummaryResurrect(&summary, generation);
                } else {
                    noteSummaryLiveCreate(&summary, generation);
                }
                try writeVisibilitySummaryTxn(mutable_txn, summary);
            }
            state.created_generation = generation;
            state.deleted_generation = null;
            try writeOrdinalStateTxn(mutable_txn, ordinal, state);
        }
        try ensureCanonicalOrdinalMappingTxn(mutable_txn, state.canonical_doc_id, ordinal);
        return .{
            .ordinal = ordinal,
            .canonical_doc_id = state.canonical_doc_id,
            .state = state,
        };
    }

    const canonical_doc_id = canonicalDocIdForNamespace(namespace, doc_id);
    if (try lookupCanonicalOrdinalTxn(mutable_txn, canonical_doc_id)) |_| return error.InvalidDocIdentity;

    const ordinal = try reserveOrdinalTxn(mutable_txn);
    const state = OrdinalState{
        .canonical_doc_id = canonical_doc_id,
        .created_generation = generation,
    };
    try writeDocOrdinalMappingTxn(alloc, mutable_txn, doc_id, ordinal);
    try writeOrdinalDocMappingTxn(mutable_txn, ordinal, doc_id);
    try writeOrdinalStateTxn(mutable_txn, ordinal, state);
    try writeCanonicalOrdinalMappingTxn(mutable_txn, state.canonical_doc_id, ordinal);
    if (try readVisibilitySummaryTxn(mutable_txn)) |summary_before| {
        var summary = summary_before;
        noteSummaryLiveCreate(&summary, generation);
        try writeVisibilitySummaryTxn(mutable_txn, summary);
    }
    return .{
        .ordinal = ordinal,
        .canonical_doc_id = state.canonical_doc_id,
        .state = state,
    };
}

pub fn markDeletedTxn(alloc: Allocator, txn: anytype, generation: u64, doc_id: []const u8) !void {
    const mutable_txn = txn;
    const ordinal = (try lookupOrdinalTxn(alloc, mutable_txn, doc_id)) orelse return;
    var state = (try lookupStateTxn(mutable_txn, ordinal)) orelse return;
    if (state.deleted_generation == null) {
        if (try readVisibilitySummaryTxn(mutable_txn)) |summary_before| {
            var summary = summary_before;
            noteSummaryDelete(&summary, generation);
            try writeVisibilitySummaryTxn(mutable_txn, summary);
        }
        state.deleted_generation = generation;
        try writeOrdinalStateTxn(mutable_txn, ordinal, state);
    }
}

pub fn resolvedDocSetForIdsTxn(alloc: Allocator, txn: anytype, doc_ids: []const []const u8) !doc_set.ResolvedDocSet {
    return try resolvedDocSetForIdsAtGenerationTxn(alloc, txn, doc_ids, null);
}

pub fn resolvedDocSetForIdsAtGenerationTxn(
    alloc: Allocator,
    txn: anytype,
    doc_ids: []const []const u8,
    generation: ?u64,
) !doc_set.ResolvedDocSet {
    const mutable_txn = txn;
    var ordinals = std.ArrayListUnmanaged(DocOrdinal).empty;
    defer ordinals.deinit(alloc);
    var fallback_doc_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer fallback_doc_ids.deinit(alloc);
    var missing_ordinal_coverage = false;

    for (doc_ids) |doc_id| {
        if (try lookupOrdinalTxn(alloc, mutable_txn, doc_id)) |ordinal| {
            const state = try lookupStateTxn(mutable_txn, ordinal);
            if (state) |ordinal_state| {
                const visible = if (generation) |at| ordinal_state.isVisibleAt(at) else ordinal_state.isLive();
                if (!visible) continue;
                try ordinals.append(alloc, ordinal);
                try fallback_doc_ids.append(alloc, doc_id);
            } else {
                missing_ordinal_coverage = true;
                try fallback_doc_ids.append(alloc, doc_id);
            }
        } else {
            missing_ordinal_coverage = true;
            try fallback_doc_ids.append(alloc, doc_id);
        }
    }

    if (missing_ordinal_coverage) {
        return try doc_set.cloneDocKeysAlloc(alloc, fallback_doc_ids.items);
    }
    if (ordinals.items.len == 0) return .none;
    return try doc_set.fromOrdinalsAlloc(alloc, ordinals.items);
}

pub fn fastStatsFromStore(store: *docstore_mod.DocStore) !Stats {
    var txn = try store.beginProbeTxn();
    defer txn.abort();
    const next = try readNextOrdinalTxn(&txn);
    return .{
        .next_ordinal = next,
        .allocated_ordinals = if (next > 0) next - 1 else 0,
    };
}

pub fn allVisibleFromSummaryFast(store: *docstore_mod.DocStore, generation: ?u64) !?bool {
    var txn = try store.beginProbeTxn();
    defer txn.abort();
    const summary = (try readVisibilitySummaryTxn(&txn)) orelse return null;
    return allVisibleFromSummary(summary, generation);
}

pub fn allVisibleFromSummary(summary: VisibilitySummary, generation: ?u64) bool {
    if (summary.tombstone_ordinals != 0) return false;
    if (generation) |at| {
        if (summary.max_created_generation > at) return false;
    }
    return true;
}

pub fn visibilitySummaryFromWrites(writes: []const docstore_mod.KVPair) !?VisibilitySummary {
    var idx = writes.len;
    while (idx > 0) {
        idx -= 1;
        const write = writes[idx];
        if (!std.mem.eql(u8, write.key, internal_keys.identity_visibility_summary_key[0..])) continue;
        return try decodeVisibilitySummary(write.value);
    }
    return null;
}

pub fn fullStatsFromStore(store: *docstore_mod.DocStore) !Stats {
    var stats = try fastStatsFromStore(store);
    const State = struct {
        stats: *Stats,

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            _ = internal_keys.parseIdentityOrdinalKey(key, internal_keys.identity_ordinal_state_kind) orelse return .@"continue";
            const ordinal_state = decodeOrdinalState(value) catch return .@"continue";
            state.stats.state_rows += 1;
            if (state.stats.min_created_generation == 0 or ordinal_state.created_generation < state.stats.min_created_generation) {
                state.stats.min_created_generation = ordinal_state.created_generation;
            }
            state.stats.max_created_generation = @max(state.stats.max_created_generation, ordinal_state.created_generation);
            if (ordinal_state.isLive()) {
                state.stats.live_ordinals += 1;
            } else {
                state.stats.tombstone_ordinals += 1;
                if (ordinal_state.deleted_generation) |deleted_generation| {
                    if (state.stats.min_deleted_generation == 0 or deleted_generation < state.stats.min_deleted_generation) {
                        state.stats.min_deleted_generation = deleted_generation;
                    }
                    state.stats.max_deleted_generation = @max(state.stats.max_deleted_generation, deleted_generation);
                }
            }
            return .@"continue";
        }
    };

    const lower = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_ordinal_state_kind };
    const upper = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_ordinal_state_kind + 1 };
    var state = State{ .stats = &stats };
    try store.scanWithContext(lower[0..], upper[0..], .{}, &state, State.scanEntry);
    stats.complete = true;
    return stats;
}

pub fn validateStoreAlloc(alloc: Allocator, store: *docstore_mod.DocStore) !void {
    const namespace = (try loadNamespaceFromStore(store)) orelse {
        if (try hasIdentityRows(store)) return error.InvalidDocIdentity;
        return;
    };

    var txn = try store.beginProbeTxn();
    defer txn.abort();
    const next_ordinal = try readNextOrdinalTxn(&txn);

    var doc_rows = DocOrdinalRows{};
    defer doc_rows.deinit(alloc);
    try collectDocOrdinalRowsAlloc(alloc, store, &doc_rows);

    var max_ordinal: DocOrdinal = 0;
    for (doc_rows.items.items) |row| {
        if (row.ordinal == 0 or row.ordinal >= next_ordinal) return error.InvalidDocIdentity;
        max_ordinal = @max(max_ordinal, row.ordinal);

        {
            const reverse_doc_id = (try lookupDocIdTxn(alloc, &txn, row.ordinal)) orelse return error.InvalidDocIdentity;
            defer alloc.free(reverse_doc_id);
            if (!std.mem.eql(u8, reverse_doc_id, row.doc_id)) return error.InvalidDocIdentity;
        }

        try validateStateForDocTxn(&txn, namespace, row.ordinal, row.doc_id);
        const expected_canonical = canonicalDocIdForNamespace(namespace, row.doc_id);
        if (try lookupCanonicalOrdinalTxn(&txn, expected_canonical)) |mapped_ordinal| {
            if (mapped_ordinal != row.ordinal) return error.InvalidDocIdentity;
        }
    }

    var ordinal_rows = OrdinalDocRows{};
    defer ordinal_rows.deinit(alloc);
    try collectOrdinalDocRowsAlloc(alloc, store, &ordinal_rows);
    for (ordinal_rows.items.items) |row| {
        if (row.ordinal == 0 or row.ordinal >= next_ordinal) return error.InvalidDocIdentity;
        max_ordinal = @max(max_ordinal, row.ordinal);

        const mapped_ordinal = (try lookupOrdinalTxn(alloc, &txn, row.doc_id)) orelse return error.InvalidDocIdentity;
        if (mapped_ordinal != row.ordinal) return error.InvalidDocIdentity;
        try validateStateForDocTxn(&txn, namespace, row.ordinal, row.doc_id);
    }

    var canonical_rows = CanonicalOrdinalRows{};
    defer canonical_rows.deinit(alloc);
    try collectCanonicalOrdinalRowsAlloc(alloc, store, &canonical_rows);
    for (canonical_rows.items.items) |row| {
        if (row.ordinal == 0 or row.ordinal >= next_ordinal) return error.InvalidDocIdentity;
        max_ordinal = @max(max_ordinal, row.ordinal);
        const state = (try lookupStateTxn(&txn, row.ordinal)) orelse return error.InvalidDocIdentity;
        if (state.canonical_doc_id != row.canonical_doc_id) return error.InvalidDocIdentity;
        {
            const doc_id = (try lookupDocIdTxn(alloc, &txn, row.ordinal)) orelse return error.InvalidDocIdentity;
            defer alloc.free(doc_id);
            try validateStateForDocTxn(&txn, namespace, row.ordinal, doc_id);
        }
    }

    var state_ordinals = std.ArrayListUnmanaged(DocOrdinal).empty;
    defer state_ordinals.deinit(alloc);
    try collectStateOrdinalsAlloc(alloc, store, &state_ordinals);
    for (state_ordinals.items) |ordinal| {
        if (ordinal == 0 or ordinal >= next_ordinal) return error.InvalidDocIdentity;
        max_ordinal = @max(max_ordinal, ordinal);

        {
            const doc_id = (try lookupDocIdTxn(alloc, &txn, ordinal)) orelse return error.InvalidDocIdentity;
            defer alloc.free(doc_id);
            const mapped_ordinal = (try lookupOrdinalTxn(alloc, &txn, doc_id)) orelse return error.InvalidDocIdentity;
            if (mapped_ordinal != ordinal) return error.InvalidDocIdentity;
            try validateStateForDocTxn(&txn, namespace, ordinal, doc_id);
        }
    }

    if (max_ordinal > 0 and next_ordinal <= max_ordinal) return error.InvalidDocIdentity;
}

pub fn liveDocSetFromStoreAlloc(alloc: Allocator, store: *docstore_mod.DocStore) !doc_set.ResolvedDocSet {
    return try visibleDocSetFromStoreAlloc(alloc, store, null);
}

pub fn visibleDocSetFromStoreAlloc(alloc: Allocator, store: *docstore_mod.DocStore, generation: ?u64) !doc_set.ResolvedDocSet {
    const State = struct {
        alloc: Allocator,
        generation: ?u64,
        ordinals: std.ArrayListUnmanaged(DocOrdinal) = .empty,

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            const ordinal = internal_keys.parseIdentityOrdinalKey(key, internal_keys.identity_ordinal_state_kind) orelse return .@"continue";
            const ordinal_state = decodeOrdinalState(value) catch return .@"continue";
            const visible = if (state.generation) |at| ordinal_state.isVisibleAt(at) else ordinal_state.isLive();
            if (!visible) return .@"continue";
            try state.ordinals.append(state.alloc, ordinal);
            return .@"continue";
        }
    };

    const lower = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_ordinal_state_kind };
    const upper = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_ordinal_state_kind + 1 };
    var state = State{ .alloc = alloc, .generation = generation };
    defer state.ordinals.deinit(alloc);
    try store.scanWithContext(lower[0..], upper[0..], .{}, &state, State.scanEntry);
    return try doc_set.fromOrdinalsAlloc(alloc, state.ordinals.items);
}

pub fn livePrimaryDocSetIfCompleteFromStoreAlloc(alloc: Allocator, store: *docstore_mod.DocStore) !?doc_set.ResolvedDocSet {
    return try visiblePrimaryDocSetIfCompleteFromStoreAlloc(alloc, store, null);
}

pub fn visiblePrimaryDocSetIfCompleteFromStoreAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    generation: ?u64,
) !?doc_set.ResolvedDocSet {
    const State = struct {
        alloc: Allocator,
        doc_ids: std.ArrayListUnmanaged([]u8) = .empty,

        fn deinit(self: *@This()) void {
            for (self.doc_ids.items) |doc_id| self.alloc.free(doc_id);
            self.doc_ids.deinit(self.alloc);
            self.* = undefined;
        }

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            _ = value;
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            const doc_id = (try internal_keys.decodePrimaryDocumentKeyAlloc(state.alloc, key)) orelse return .@"continue";
            errdefer state.alloc.free(doc_id);
            try state.doc_ids.append(state.alloc, doc_id);
            return .@"continue";
        }
    };

    const lower = [_]u8{internal_keys.user_namespace};
    const upper = [_]u8{internal_keys.user_namespace + 1};
    var state = State{ .alloc = alloc };
    defer state.deinit();
    try store.scanWithContext(lower[0..], upper[0..], .{}, &state, State.scanEntry);

    var txn = try store.beginProbeTxn();
    defer txn.abort();

    var ordinals = std.ArrayListUnmanaged(DocOrdinal).empty;
    defer ordinals.deinit(alloc);
    for (state.doc_ids.items) |doc_id| {
        const ordinal = (try lookupOrdinalTxn(alloc, &txn, doc_id)) orelse return null;
        const ordinal_state = (try lookupStateTxn(&txn, ordinal)) orelse return null;
        const visible = if (generation) |at| ordinal_state.isVisibleAt(at) else ordinal_state.isLive();
        if (!visible) return null;
        try ordinals.append(alloc, ordinal);
    }

    return try doc_set.fromOrdinalsAlloc(alloc, ordinals.items);
}

const DocOrdinalRow = struct {
    doc_id: []u8,
    ordinal: DocOrdinal,
};

const DocOrdinalRows = struct {
    items: std.ArrayListUnmanaged(DocOrdinalRow) = .empty,

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.items.items) |row| alloc.free(row.doc_id);
        self.items.deinit(alloc);
        self.* = .{};
    }
};

const OrdinalDocRow = struct {
    ordinal: DocOrdinal,
    doc_id: []u8,
};

const OrdinalDocRows = struct {
    items: std.ArrayListUnmanaged(OrdinalDocRow) = .empty,

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.items.items) |row| alloc.free(row.doc_id);
        self.items.deinit(alloc);
        self.* = .{};
    }
};

const CanonicalOrdinalRow = struct {
    canonical_doc_id: u64,
    ordinal: DocOrdinal,
};

const CanonicalOrdinalRows = struct {
    items: std.ArrayListUnmanaged(CanonicalOrdinalRow) = .empty,

    fn deinit(self: *@This(), alloc: Allocator) void {
        self.items.deinit(alloc);
        self.* = .{};
    }
};

fn hasIdentityRows(store: *docstore_mod.DocStore) !bool {
    const State = struct {
        found: bool = false,

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            _ = key;
            _ = value;
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            state.found = true;
            return .stop;
        }
    };

    const lower = [_]u8{internal_keys.identity_namespace};
    const upper = [_]u8{internal_keys.identity_namespace + 1};
    var state = State{};
    try store.scanWithContext(lower[0..], upper[0..], .{}, &state, State.scanEntry);
    return state.found;
}

fn collectDocOrdinalRowsAlloc(alloc: Allocator, store: *docstore_mod.DocStore, rows: *DocOrdinalRows) !void {
    const State = struct {
        alloc: Allocator,
        rows: *DocOrdinalRows,

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            if (value.len != @sizeOf(u32)) return error.InvalidDocIdentity;
            const ordinal = std.mem.readInt(u32, value[0..4], .big);
            const doc_id = try parseIdentityDocToOrdinalKeyAlloc(state.alloc, key);
            errdefer state.alloc.free(doc_id);
            try state.rows.items.append(state.alloc, .{ .doc_id = doc_id, .ordinal = ordinal });
            return .@"continue";
        }
    };

    const lower = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_doc_to_ordinal_kind };
    const upper = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_doc_to_ordinal_kind + 1 };
    var state = State{ .alloc = alloc, .rows = rows };
    try store.scanWithContext(lower[0..], upper[0..], .{}, &state, State.scanEntry);
}

fn collectOrdinalDocRowsAlloc(alloc: Allocator, store: *docstore_mod.DocStore, rows: *OrdinalDocRows) !void {
    const State = struct {
        alloc: Allocator,
        rows: *OrdinalDocRows,

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            const ordinal = internal_keys.parseIdentityOrdinalKey(key, internal_keys.identity_ordinal_to_doc_kind) orelse return error.InvalidDocIdentity;
            try state.rows.items.append(state.alloc, .{
                .ordinal = ordinal,
                .doc_id = try state.alloc.dupe(u8, value),
            });
            return .@"continue";
        }
    };

    const lower = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_ordinal_to_doc_kind };
    const upper = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_ordinal_to_doc_kind + 1 };
    var state = State{ .alloc = alloc, .rows = rows };
    try store.scanWithContext(lower[0..], upper[0..], .{}, &state, State.scanEntry);
}

fn collectCanonicalOrdinalRowsAlloc(alloc: Allocator, store: *docstore_mod.DocStore, rows: *CanonicalOrdinalRows) !void {
    const State = struct {
        alloc: Allocator,
        rows: *CanonicalOrdinalRows,

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            const canonical_doc_id = internal_keys.parseIdentityCanonicalKey(key) orelse return error.InvalidDocIdentity;
            if (value.len != @sizeOf(u32)) return error.InvalidDocIdentity;
            const ordinal = std.mem.readInt(u32, value[0..4], .big);
            try state.rows.items.append(state.alloc, .{
                .canonical_doc_id = canonical_doc_id,
                .ordinal = ordinal,
            });
            return .@"continue";
        }
    };

    const lower = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_canonical_to_ordinal_kind };
    const upper = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_canonical_to_ordinal_kind + 1 };
    var state = State{ .alloc = alloc, .rows = rows };
    try store.scanWithContext(lower[0..], upper[0..], .{}, &state, State.scanEntry);
}

fn collectStateOrdinalsAlloc(alloc: Allocator, store: *docstore_mod.DocStore, ordinals: *std.ArrayListUnmanaged(DocOrdinal)) !void {
    const State = struct {
        alloc: Allocator,
        ordinals: *std.ArrayListUnmanaged(DocOrdinal),

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            _ = try decodeOrdinalState(value);
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            const ordinal = internal_keys.parseIdentityOrdinalKey(key, internal_keys.identity_ordinal_state_kind) orelse return error.InvalidDocIdentity;
            try state.ordinals.append(state.alloc, ordinal);
            return .@"continue";
        }
    };

    const lower = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_ordinal_state_kind };
    const upper = [_]u8{ internal_keys.identity_namespace, internal_keys.identity_ordinal_state_kind + 1 };
    var state = State{ .alloc = alloc, .ordinals = ordinals };
    try store.scanWithContext(lower[0..], upper[0..], .{}, &state, State.scanEntry);
}

fn parseIdentityDocToOrdinalKeyAlloc(alloc: Allocator, key: []const u8) ![]u8 {
    if (key.len < 4) return error.InvalidDocIdentity;
    if (key[0] != internal_keys.identity_namespace or key[1] != internal_keys.identity_doc_to_ordinal_kind) return error.InvalidDocIdentity;
    const terminator = internal_keys.findComponentTerminator(key, 2) orelse return error.InvalidDocIdentity;
    if (terminator + 2 != key.len) return error.InvalidDocIdentity;
    return try internal_keys.decodeBodyAlloc(alloc, key[2..terminator]);
}

fn validateStateForDocTxn(txn: anytype, namespace: Namespace, ordinal: DocOrdinal, doc_id: []const u8) !void {
    const state = (try lookupStateTxn(txn, ordinal)) orelse return error.InvalidDocIdentity;
    try state.validate();
    const expected = canonicalDocIdForNamespace(namespace, doc_id);
    if (state.canonical_doc_id != expected) return error.InvalidDocIdentity;
}

pub fn liveFilteredDocSetFromStoreAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    set: *const doc_set.ResolvedDocSet,
) !doc_set.ResolvedDocSet {
    return try visibleFilteredDocSetFromStoreAlloc(alloc, store, set, null);
}

pub fn visibleFilteredDocSetFromStoreAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    set: *const doc_set.ResolvedDocSet,
    generation: ?u64,
) !doc_set.ResolvedDocSet {
    return switch (set.*) {
        .all => (try visiblePrimaryDocSetIfCompleteFromStoreAlloc(alloc, store, generation)) orelse .all,
        .none => .none,
        .doc_keys => |keys| blk: {
            var txn = try store.beginProbeTxn();
            defer txn.abort();
            break :blk try resolvedDocSetForIdsAtGenerationTxn(alloc, &txn, keys, generation);
        },
        .ordinals => |ordinals| try visibleFilteredOrdinalsFromStoreAlloc(alloc, store, ordinals, generation),
        .ordinal_bitmap => |*bitmap| blk: {
            var ordinals = std.ArrayListUnmanaged(DocOrdinal).empty;
            defer ordinals.deinit(alloc);
            var iter = bitmap.iterator();
            while (iter.next()) |ordinal| try ordinals.append(alloc, ordinal);
            break :blk try visibleFilteredOrdinalsFromStoreAlloc(alloc, store, ordinals.items, generation);
        },
    };
}

fn visibleFilteredOrdinalsFromStoreAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    ordinals: []const DocOrdinal,
    generation: ?u64,
) !doc_set.ResolvedDocSet {
    var txn = try store.beginProbeTxn();
    defer txn.abort();
    var live = std.ArrayListUnmanaged(DocOrdinal).empty;
    defer live.deinit(alloc);
    for (ordinals) |ordinal| {
        const state = (try lookupStateTxn(&txn, ordinal)) orelse continue;
        const visible = if (generation) |at| state.isVisibleAt(at) else state.isLive();
        if (!visible) continue;
        try live.append(alloc, ordinal);
    }
    return try doc_set.fromOrdinalsAlloc(alloc, live.items);
}

pub fn appendBatchIdentityMetadataAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_id: u64,
    shard_id: u64,
    generation: u64,
    out: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    doc_upserts: []const []const u8,
    doc_deletes: []const []const u8,
) !void {
    return try appendBatchIdentityMetadataForNamespaceAlloc(
        alloc,
        store,
        .{ .table_id = table_id, .shard_id = shard_id },
        generation,
        out,
        doc_upserts,
        doc_deletes,
    );
}

pub fn appendBatchIdentityMetadataForNamespaceAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    namespace: Namespace,
    generation: u64,
    out: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    doc_upserts: []const []const u8,
    doc_deletes: []const []const u8,
) !void {
    if (doc_upserts.len == 0 and doc_deletes.len == 0) return;

    if (try appendBatchIdentityMetadataAllNewFastPath(
        alloc,
        store,
        namespace,
        generation,
        out,
        doc_upserts,
        doc_deletes,
    )) return;

    var txn = try store.beginProbeTxn();
    defer txn.abort();

    const missing_namespace = blk: {
        if (try loadNamespaceTxn(&txn)) |stored_namespace| {
            if (!stored_namespace.eql(namespace)) return error.IdentityNamespaceMismatch;
            break :blk false;
        }
        try appendNamespaceWrite(alloc, out, namespace);
        break :blk true;
    };

    var visibility_summary = (try readVisibilitySummaryTxn(&txn)) orelse if (missing_namespace) VisibilitySummary{} else null;
    var visibility_summary_dirty = visibility_summary != null and missing_namespace;

    var seen_upserts = std.StringHashMapUnmanaged(void).empty;
    defer seen_upserts.deinit(alloc);
    var allocated_ordinals = std.StringHashMapUnmanaged(DocOrdinal).empty;
    defer allocated_ordinals.deinit(alloc);
    var upsert_states = std.AutoHashMapUnmanaged(DocOrdinal, OrdinalState).empty;
    defer upsert_states.deinit(alloc);

    var next_ordinal = try readNextOrdinalTxn(&txn);
    var reserved_new_ordinal = false;
    for (doc_upserts) |doc_id| {
        if (seen_upserts.contains(doc_id)) continue;
        try seen_upserts.put(alloc, doc_id, {});

        if (try lookupOrdinalTxn(alloc, &txn, doc_id)) |ordinal| {
            const existing_state = try lookupStateTxn(&txn, ordinal);
            var state = existing_state orelse OrdinalState{
                .canonical_doc_id = canonicalDocIdForNamespace(namespace, doc_id),
                .created_generation = generation,
            };
            if (existing_state == null or state.deleted_generation != null) {
                if (visibility_summary) |*summary| {
                    if (state.deleted_generation != null) {
                        noteSummaryResurrect(summary, generation);
                    } else {
                        noteSummaryLiveCreate(summary, generation);
                    }
                    visibility_summary_dirty = true;
                }
                state.created_generation = generation;
                state.deleted_generation = null;
                try appendOrdinalStateWrite(alloc, out, ordinal, state);
            }
            if (try lookupCanonicalOrdinalTxn(&txn, state.canonical_doc_id)) |mapped| {
                if (mapped != ordinal) return error.InvalidDocIdentity;
            } else {
                try appendCanonicalOrdinalWrite(alloc, out, state.canonical_doc_id, ordinal);
            }
            try upsert_states.put(alloc, ordinal, state);
            continue;
        }

        const canonical_doc_id = canonicalDocIdForNamespace(namespace, doc_id);
        if (try lookupCanonicalOrdinalTxn(&txn, canonical_doc_id)) |_| return error.InvalidDocIdentity;

        const ordinal = try reserveOrdinalLocal(&next_ordinal);
        reserved_new_ordinal = true;
        try allocated_ordinals.put(alloc, doc_id, ordinal);
        const state = OrdinalState{
            .canonical_doc_id = canonical_doc_id,
            .created_generation = generation,
        };
        if (visibility_summary) |*summary| {
            noteSummaryLiveCreate(summary, generation);
            visibility_summary_dirty = true;
        }
        try appendIdentityWritesForLiveDoc(alloc, out, doc_id, ordinal, state);
        try upsert_states.put(alloc, ordinal, state);
    }

    var seen_deletes = std.StringHashMapUnmanaged(void).empty;
    defer seen_deletes.deinit(alloc);
    for (doc_deletes) |doc_id| {
        if (seen_deletes.contains(doc_id)) continue;
        try seen_deletes.put(alloc, doc_id, {});

        const ordinal = allocated_ordinals.get(doc_id) orelse (try lookupOrdinalTxn(alloc, &txn, doc_id) orelse continue);
        var state = upsert_states.get(ordinal) orelse if (try lookupStateTxn(&txn, ordinal)) |existing| existing else OrdinalState{
            .canonical_doc_id = canonicalDocIdForNamespace(namespace, doc_id),
            .created_generation = generation,
        };
        if (state.deleted_generation == null) {
            if (visibility_summary) |*summary| {
                noteSummaryDelete(summary, generation);
                visibility_summary_dirty = true;
            }
            state.deleted_generation = generation;
            try appendOrdinalStateWrite(alloc, out, ordinal, state);
        }
    }

    if (reserved_new_ordinal) {
        try appendNextOrdinalWrite(alloc, out, next_ordinal);
    }
    if (visibility_summary_dirty) {
        try appendVisibilitySummaryWrite(alloc, out, visibility_summary.?);
    }
}

pub fn loadAllNewTrustedStateForNamespace(store: *docstore_mod.DocStore, namespace: Namespace) !?AllNewTrustedState {
    var txn = try store.beginProbeTxn();
    defer txn.abort();

    const namespace_written = if (try loadNamespaceTxn(&txn)) |stored_namespace| blk: {
        if (!stored_namespace.eql(namespace)) return error.IdentityNamespaceMismatch;
        break :blk true;
    } else false;

    const next_ordinal = try readNextOrdinalTxn(&txn);
    if (next_ordinal != 1) return null;
    const summary = (try readVisibilitySummaryTxn(&txn)) orelse VisibilitySummary{};
    if (summary.live_ordinals != 0 or summary.tombstone_ordinals != 0) return null;
    return .{
        .next_ordinal = next_ordinal,
        .visibility_summary = summary,
        .namespace_written = namespace_written,
    };
}

pub fn appendBatchIdentityMetadataAllNewTrustedForNamespaceAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    namespace: Namespace,
    generation: u64,
    out: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    doc_upserts: []const []const u8,
) !bool {
    if (doc_upserts.len == 0) return true;

    const identity_write_capacity = try std.math.add(usize, try std.math.mul(usize, doc_upserts.len, 4), 2);
    try out.ensureUnusedCapacity(alloc, identity_write_capacity);

    var txn = try store.beginProbeTxn();
    defer txn.abort();

    const missing_namespace = if (try loadNamespaceTxn(&txn)) |stored_namespace| blk: {
        if (!stored_namespace.eql(namespace)) return error.IdentityNamespaceMismatch;
        break :blk false;
    } else true;

    var seen_doc_ids = std.StringHashMapUnmanaged(void).empty;
    defer seen_doc_ids.deinit(alloc);
    var seen_canonical_ids = std.AutoHashMapUnmanaged(u64, void).empty;
    defer seen_canonical_ids.deinit(alloc);
    for (doc_upserts) |doc_id| {
        if (seen_doc_ids.contains(doc_id)) return false;
        try seen_doc_ids.put(alloc, doc_id, {});

        const canonical_doc_id = canonicalDocIdForNamespace(namespace, doc_id);
        if (seen_canonical_ids.contains(canonical_doc_id)) return false;
        try seen_canonical_ids.put(alloc, canonical_doc_id, {});
    }

    var visibility_summary = (try readVisibilitySummaryTxn(&txn)) orelse if (missing_namespace) VisibilitySummary{} else null;
    if (missing_namespace) try appendNamespaceWrite(alloc, out, namespace);

    var next_ordinal = try readNextOrdinalTxn(&txn);
    const available_ordinals: usize = std.math.maxInt(DocOrdinal) - next_ordinal;
    if (doc_upserts.len > available_ordinals) return error.DocOrdinalExhausted;

    for (doc_upserts) |doc_id| {
        const ordinal = try reserveOrdinalLocal(&next_ordinal);
        const state = OrdinalState{
            .canonical_doc_id = canonicalDocIdForNamespace(namespace, doc_id),
            .created_generation = generation,
        };
        if (visibility_summary) |*summary| noteSummaryLiveCreate(summary, generation);
        try appendIdentityWritesForLiveDoc(alloc, out, doc_id, ordinal, state);
    }
    try appendNextOrdinalWrite(alloc, out, next_ordinal);
    if (visibility_summary) |summary| try appendVisibilitySummaryWrite(alloc, out, summary);
    return true;
}

pub fn appendBatchIdentityMetadataAllNewTrustedStateForNamespaceAlloc(
    alloc: Allocator,
    namespace: Namespace,
    generation: u64,
    out: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    doc_upserts: []const []const u8,
    state: *AllNewTrustedState,
) !bool {
    if (doc_upserts.len == 0) return true;

    const identity_write_capacity = try std.math.add(usize, try std.math.mul(usize, doc_upserts.len, 4), 2);
    try out.ensureUnusedCapacity(alloc, identity_write_capacity);

    var seen_doc_ids = std.StringHashMapUnmanaged(void).empty;
    defer seen_doc_ids.deinit(alloc);
    var seen_canonical_ids = std.AutoHashMapUnmanaged(u64, void).empty;
    defer seen_canonical_ids.deinit(alloc);
    for (doc_upserts) |doc_id| {
        if (seen_doc_ids.contains(doc_id)) return false;
        try seen_doc_ids.put(alloc, doc_id, {});

        const canonical_doc_id = canonicalDocIdForNamespace(namespace, doc_id);
        if (seen_canonical_ids.contains(canonical_doc_id)) return false;
        try seen_canonical_ids.put(alloc, canonical_doc_id, {});
    }

    var next_ordinal = state.next_ordinal;
    const available_ordinals: usize = std.math.maxInt(DocOrdinal) - next_ordinal;
    if (doc_upserts.len > available_ordinals) return error.DocOrdinalExhausted;

    var visibility_summary = state.visibility_summary;
    if (!state.namespace_written) try appendNamespaceWrite(alloc, out, namespace);

    for (doc_upserts) |doc_id| {
        const ordinal = try reserveOrdinalLocal(&next_ordinal);
        const doc_state = OrdinalState{
            .canonical_doc_id = canonicalDocIdForNamespace(namespace, doc_id),
            .created_generation = generation,
        };
        noteSummaryLiveCreate(&visibility_summary, generation);
        try appendIdentityWritesForLiveDoc(alloc, out, doc_id, ordinal, doc_state);
    }
    try appendNextOrdinalWrite(alloc, out, next_ordinal);
    try appendVisibilitySummaryWrite(alloc, out, visibility_summary);

    state.next_ordinal = next_ordinal;
    state.visibility_summary = visibility_summary;
    state.namespace_written = true;
    return true;
}

const IdentityLookup = struct {
    key: []u8,
};

fn identityLookupLessThan(_: void, lhs: IdentityLookup, rhs: IdentityLookup) bool {
    return std.mem.lessThan(u8, lhs.key, rhs.key);
}

fn appendBatchIdentityMetadataAllNewFastPath(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    namespace: Namespace,
    generation: u64,
    out: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    doc_upserts: []const []const u8,
    doc_deletes: []const []const u8,
) !bool {
    if (doc_upserts.len == 0 or doc_deletes.len != 0) return false;
    const identity_write_capacity = try std.math.add(usize, try std.math.mul(usize, doc_upserts.len, 4), 2);
    try out.ensureUnusedCapacity(alloc, identity_write_capacity);

    var txn = try store.beginProbeTxn();
    defer txn.abort();

    const missing_namespace = if (try loadNamespaceTxn(&txn)) |stored_namespace| blk: {
        if (!stored_namespace.eql(namespace)) return error.IdentityNamespaceMismatch;
        break :blk false;
    } else true;

    var doc_lookups = try alloc.alloc(IdentityLookup, doc_upserts.len);
    defer {
        for (doc_lookups) |lookup| alloc.free(lookup.key);
        alloc.free(doc_lookups);
    }
    for (doc_upserts, 0..) |doc_id, i| {
        doc_lookups[i] = .{
            .key = try internal_keys.identityDocToOrdinalKeyAlloc(alloc, doc_id),
        };
    }
    std.sort.pdq(IdentityLookup, doc_lookups, {}, identityLookupLessThan);
    if (identityLookupsContainDuplicateKeys(doc_lookups)) return false;
    if (!missing_namespace and try anyIdentityLookupExists(alloc, &txn, doc_lookups)) return false;

    var canonical_lookups = try alloc.alloc(IdentityLookup, doc_upserts.len);
    defer {
        for (canonical_lookups) |lookup| alloc.free(lookup.key);
        alloc.free(canonical_lookups);
    }
    for (doc_upserts, 0..) |doc_id, i| {
        const canonical_key = internal_keys.identityCanonicalToOrdinalKey(canonicalDocIdForNamespace(namespace, doc_id));
        canonical_lookups[i] = .{
            .key = try alloc.dupe(u8, canonical_key[0..]),
        };
    }
    std.sort.pdq(IdentityLookup, canonical_lookups, {}, identityLookupLessThan);
    if (identityLookupsContainDuplicateKeys(canonical_lookups)) return false;
    if (!missing_namespace and try anyIdentityLookupExists(alloc, &txn, canonical_lookups)) return false;

    var visibility_summary = (try readVisibilitySummaryTxn(&txn)) orelse if (missing_namespace) VisibilitySummary{} else null;
    if (missing_namespace) try appendNamespaceWrite(alloc, out, namespace);
    var next_ordinal = try readNextOrdinalTxn(&txn);
    const available_ordinals: usize = std.math.maxInt(DocOrdinal) - next_ordinal;
    if (doc_upserts.len > available_ordinals) return error.DocOrdinalExhausted;
    for (doc_upserts) |doc_id| {
        const ordinal = try reserveOrdinalLocal(&next_ordinal);
        const state = OrdinalState{
            .canonical_doc_id = canonicalDocIdForNamespace(namespace, doc_id),
            .created_generation = generation,
        };
        if (visibility_summary) |*summary| noteSummaryLiveCreate(summary, generation);
        try appendIdentityWritesForLiveDoc(alloc, out, doc_id, ordinal, state);
    }
    try appendNextOrdinalWrite(alloc, out, next_ordinal);
    if (visibility_summary) |summary| try appendVisibilitySummaryWrite(alloc, out, summary);
    return true;
}

fn identityLookupsContainDuplicateKeys(lookups: []const IdentityLookup) bool {
    if (lookups.len <= 1) return false;
    var previous = lookups[0].key;
    for (lookups[1..]) |lookup| {
        if (std.mem.eql(u8, previous, lookup.key)) return true;
        previous = lookup.key;
    }
    return false;
}

fn anyIdentityLookupExists(alloc: Allocator, txn: anytype, lookups: []const IdentityLookup) !bool {
    if (lookups.len == 0) return false;
    var keys = try alloc.alloc([]const u8, lookups.len);
    defer alloc.free(keys);
    const values = try alloc.alloc(?[]const u8, lookups.len);
    defer alloc.free(values);
    for (lookups, 0..) |lookup, i| keys[i] = lookup.key;
    try txn.getManySorted(keys, values);
    for (values) |maybe_value| {
        if (maybe_value != null) return true;
    }
    return false;
}

fn readNextOrdinalTxn(txn: anytype) !DocOrdinal {
    const mutable_txn = txn;
    const raw = mutable_txn.get(internal_keys.identity_next_ordinal_key[0..]) catch |err| switch (err) {
        error.NotFound => return 1,
        else => return err,
    };
    if (raw.len != @sizeOf(u32)) return error.InvalidDocIdentity;
    const next = std.mem.readInt(u32, raw[0..4], .big);
    if (next == 0) return error.DocOrdinalExhausted;
    return next;
}

fn reserveOrdinalTxn(txn: anytype) !DocOrdinal {
    const mutable_txn = txn;
    var next = try readNextOrdinalTxn(mutable_txn);
    const ordinal = try reserveOrdinalLocal(&next);
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, next, .big);
    try mutable_txn.put(internal_keys.identity_next_ordinal_key[0..], &buf);
    return ordinal;
}

fn reserveOrdinalLocal(next: *DocOrdinal) !DocOrdinal {
    const ordinal = next.*;
    if (ordinal == 0 or ordinal == std.math.maxInt(DocOrdinal)) return error.DocOrdinalExhausted;
    next.* = ordinal + 1;
    return ordinal;
}

fn writeDocOrdinalMappingTxn(alloc: Allocator, txn: anytype, doc_id: []const u8, ordinal: DocOrdinal) !void {
    const mutable_txn = txn;
    const key = try internal_keys.identityDocToOrdinalKeyAlloc(alloc, doc_id);
    defer alloc.free(key);
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, ordinal, .big);
    try mutable_txn.put(key, &buf);
}

fn writeOrdinalDocMappingTxn(txn: anytype, ordinal: DocOrdinal, doc_id: []const u8) !void {
    const mutable_txn = txn;
    const key = internal_keys.identityOrdinalToDocKey(ordinal);
    try mutable_txn.put(key[0..], doc_id);
}

fn writeOrdinalStateTxn(txn: anytype, ordinal: DocOrdinal, state: OrdinalState) !void {
    const mutable_txn = txn;
    const key = internal_keys.identityOrdinalStateKey(ordinal);
    const encoded = encodeOrdinalState(state);
    try mutable_txn.put(key[0..], encoded[0..]);
}

fn writeCanonicalOrdinalMappingTxn(txn: anytype, canonical_doc_id: u64, ordinal: DocOrdinal) !void {
    const mutable_txn = txn;
    const key = internal_keys.identityCanonicalToOrdinalKey(canonical_doc_id);
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, ordinal, .big);
    try mutable_txn.put(key[0..], &buf);
}

fn deleteCanonicalOrdinalMappingTxn(txn: anytype, canonical_doc_id: u64) !void {
    const mutable_txn = txn;
    const key = internal_keys.identityCanonicalToOrdinalKey(canonical_doc_id);
    mutable_txn.delete(key[0..]) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

fn ensureCanonicalOrdinalMappingTxn(txn: anytype, canonical_doc_id: u64, ordinal: DocOrdinal) !void {
    if (try lookupCanonicalOrdinalTxn(txn, canonical_doc_id)) |mapped| {
        if (mapped != ordinal) return error.InvalidDocIdentity;
        return;
    }
    try writeCanonicalOrdinalMappingTxn(txn, canonical_doc_id, ordinal);
}

fn appendIdentityWritesForLiveDoc(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    doc_id: []const u8,
    ordinal: DocOrdinal,
    state: OrdinalState,
) !void {
    const doc_to_ordinal = try internal_keys.identityDocToOrdinalKeyAlloc(alloc, doc_id);
    var ordinal_buf = try alloc.alloc(u8, @sizeOf(u32));
    std.mem.writeInt(u32, ordinal_buf[0..4], ordinal, .big);
    try writes.append(alloc, .{ .key = doc_to_ordinal, .value = ordinal_buf });

    const ordinal_to_doc_key = internal_keys.identityOrdinalToDocKey(ordinal);
    try writes.append(alloc, .{
        .key = try alloc.dupe(u8, ordinal_to_doc_key[0..]),
        .value = try alloc.dupe(u8, doc_id),
    });

    try appendOrdinalStateWrite(alloc, writes, ordinal, state);
    try appendCanonicalOrdinalWrite(alloc, writes, state.canonical_doc_id, ordinal);
}

fn noteSummaryLiveCreate(summary: *VisibilitySummary, generation: u64) void {
    summary.live_ordinals += 1;
    summary.max_created_generation = @max(summary.max_created_generation, generation);
}

fn noteSummaryResurrect(summary: *VisibilitySummary, generation: u64) void {
    noteSummaryLiveCreate(summary, generation);
    if (summary.tombstone_ordinals > 0) summary.tombstone_ordinals -= 1;
}

fn noteSummaryDelete(summary: *VisibilitySummary, generation: u64) void {
    if (summary.live_ordinals > 0) summary.live_ordinals -= 1;
    summary.tombstone_ordinals += 1;
    if (summary.min_deleted_generation == 0 or generation < summary.min_deleted_generation) {
        summary.min_deleted_generation = generation;
    }
    summary.max_deleted_generation = @max(summary.max_deleted_generation, generation);
}

fn appendOrdinalStateWrite(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    ordinal: DocOrdinal,
    state: OrdinalState,
) !void {
    const state_key = internal_keys.identityOrdinalStateKey(ordinal);
    const encoded_state = encodeOrdinalState(state);
    try writes.append(alloc, .{
        .key = try alloc.dupe(u8, state_key[0..]),
        .value = try alloc.dupe(u8, encoded_state[0..]),
    });
}

fn appendCanonicalOrdinalWrite(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    canonical_doc_id: u64,
    ordinal: DocOrdinal,
) !void {
    const canonical_key = internal_keys.identityCanonicalToOrdinalKey(canonical_doc_id);
    var value = try alloc.alloc(u8, @sizeOf(u32));
    std.mem.writeInt(u32, value[0..4], ordinal, .big);
    try writes.append(alloc, .{
        .key = try alloc.dupe(u8, canonical_key[0..]),
        .value = value,
    });
}

fn appendVisibilitySummaryWrite(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    summary: VisibilitySummary,
) !void {
    const encoded_summary = encodeVisibilitySummary(summary);
    try writes.append(alloc, .{
        .key = try alloc.dupe(u8, internal_keys.identity_visibility_summary_key[0..]),
        .value = try alloc.dupe(u8, encoded_summary[0..]),
    });
}

fn writeVisibilitySummaryTxn(txn: anytype, summary: VisibilitySummary) !void {
    const mutable_txn = txn;
    const encoded_summary = encodeVisibilitySummary(summary);
    try mutable_txn.put(internal_keys.identity_visibility_summary_key[0..], encoded_summary[0..]);
}

fn appendNextOrdinalWrite(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    next_ordinal: DocOrdinal,
) !void {
    var value = try alloc.alloc(u8, @sizeOf(u32));
    std.mem.writeInt(u32, value[0..4], next_ordinal, .big);
    try writes.append(alloc, .{
        .key = try alloc.dupe(u8, internal_keys.identity_next_ordinal_key[0..]),
        .value = value,
    });
}

fn appendNamespaceWrite(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    namespace: Namespace,
) !void {
    var value = try alloc.alloc(u8, 24);
    encodeNamespace(value[0..24], namespace);
    try writes.append(alloc, .{
        .key = try alloc.dupe(u8, internal_keys.identity_namespace_key[0..]),
        .value = value,
    });
}

fn encodeOrdinalState(state: OrdinalState) [25]u8 {
    var out: [25]u8 = undefined;
    std.mem.writeInt(u64, out[0..8], state.canonical_doc_id, .big);
    std.mem.writeInt(u64, out[8..16], state.created_generation, .big);
    if (state.deleted_generation) |deleted| {
        out[16] = 1;
        std.mem.writeInt(u64, out[17..25], deleted, .big);
    } else {
        out[16] = 0;
        @memset(out[17..25], 0);
    }
    return out;
}

fn decodeOrdinalState(raw: []const u8) !OrdinalState {
    if (raw.len != 25) return error.InvalidDocIdentity;
    const state = OrdinalState{
        .canonical_doc_id = std.mem.readInt(u64, raw[0..8], .big),
        .created_generation = std.mem.readInt(u64, raw[8..16], .big),
        .deleted_generation = switch (raw[16]) {
            0 => null,
            1 => std.mem.readInt(u64, raw[17..25], .big),
            else => return error.InvalidDocIdentity,
        },
    };
    try state.validate();
    return state;
}

test "canonical doc id is deterministic and nonzero" {
    const a = canonicalDocId(1, 2, "doc:a");
    try std.testing.expect(a != 0);
    try std.testing.expectEqual(a, canonicalDocId(1, 2, "doc:a"));
    try std.testing.expect(a != canonicalDocId(1, 2, "doc:b"));
    try std.testing.expectEqual(a, canonicalDocIdForNamespace(.{ .table_id = 1, .shard_id = 2 }, "doc:a"));
    try std.testing.expect(a != canonicalDocIdForNamespace(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }, "doc:a"));
}

test "identity namespace persists and rejects explicit mismatches" {
    const mem_backend = @import("../mem_backend.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const configured = Namespace{ .table_id = 7, .shard_id = 11, .range_id = 13 };
    const initialized = try loadOrInitNamespace(&store, configured, true);
    try std.testing.expect(initialized.eql(configured));

    const loaded = (try loadNamespaceFromStore(&store)).?;
    try std.testing.expect(loaded.eql(configured));

    const implicit = try loadOrInitNamespace(&store, null, false);
    try std.testing.expect(implicit.eql(configured));

    const mismatch = loadOrInitNamespace(&store, .{ .table_id = 7, .shard_id = 11, .range_id = 14 }, false);
    try std.testing.expectError(error.IdentityNamespaceMismatch, mismatch);
}

test "identity namespace reassignment rewrites canonical states" {
    const mem_backend = @import("../mem_backend.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const old_namespace = Namespace{ .table_id = 7, .shard_id = 71, .range_id = 701 };
    var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (identity_writes.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        identity_writes.deinit(alloc);
    }
    try appendBatchIdentityMetadataForNamespaceAlloc(
        alloc,
        &store,
        old_namespace,
        10,
        &identity_writes,
        &.{ "doc:a", "doc:b" },
        &.{"doc:b"},
    );
    try store.putBatchWithReplay(null, identity_writes.items, &.{}, null);
    try validateStoreAlloc(alloc, &store);

    const new_namespace = Namespace{ .table_id = 7, .shard_id = 72, .range_id = 702 };
    try reassignNamespaceAlloc(alloc, &store, new_namespace);
    const loaded = (try loadNamespaceFromStore(&store)).?;
    try std.testing.expect(loaded.eql(new_namespace));

    var txn = try store.beginProbeTxn();
    defer txn.abort();
    const ordinal_a = (try lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
    const state_a = (try lookupStateTxn(&txn, ordinal_a)).?;
    try std.testing.expectEqual(canonicalDocIdForNamespace(new_namespace, "doc:a"), state_a.canonical_doc_id);
    try std.testing.expectEqual(ordinal_a, (try lookupCanonicalOrdinalTxn(&txn, state_a.canonical_doc_id)).?);
    try std.testing.expectEqual(@as(?DocOrdinal, null), try lookupCanonicalOrdinalTxn(&txn, canonicalDocIdForNamespace(old_namespace, "doc:a")));
    try std.testing.expectEqual(@as(u64, 10), state_a.created_generation);
    try std.testing.expectEqual(@as(?u64, null), state_a.deleted_generation);

    const ordinal_b = (try lookupOrdinalTxn(alloc, &txn, "doc:b")).?;
    const state_b = (try lookupStateTxn(&txn, ordinal_b)).?;
    try std.testing.expectEqual(canonicalDocIdForNamespace(new_namespace, "doc:b"), state_b.canonical_doc_id);
    try std.testing.expectEqual(ordinal_b, (try lookupCanonicalOrdinalTxn(&txn, state_b.canonical_doc_id)).?);
    try std.testing.expectEqual(@as(?DocOrdinal, null), try lookupCanonicalOrdinalTxn(&txn, canonicalDocIdForNamespace(old_namespace, "doc:b")));
    try std.testing.expectEqual(@as(u64, 10), state_b.created_generation);
    try std.testing.expectEqual(@as(u64, 10), state_b.deleted_generation.?);
    try std.testing.expect(state_b.canonical_doc_id != canonicalDocIdForNamespace(old_namespace, "doc:b"));
}

test "identity validation accepts missing canonical rows but rejects conflicts" {
    const mem_backend = @import("../mem_backend.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const namespace = Namespace{ .table_id = 8, .shard_id = 81, .range_id = 801 };
    var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (identity_writes.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        identity_writes.deinit(alloc);
    }
    try appendBatchIdentityMetadataForNamespaceAlloc(
        alloc,
        &store,
        namespace,
        20,
        &identity_writes,
        &.{ "doc:a", "doc:b" },
        &.{},
    );
    try store.putBatchWithReplay(null, identity_writes.items, &.{}, null);
    try validateStoreAlloc(alloc, &store);

    var probe = try store.beginProbeTxn();
    const ordinal_a = (try lookupOrdinalTxn(alloc, &probe, "doc:a")).?;
    const ordinal_b = (try lookupOrdinalTxn(alloc, &probe, "doc:b")).?;
    const canonical_a = (try lookupStateTxn(&probe, ordinal_a)).?.canonical_doc_id;
    probe.abort();

    var delete_txn = try store.beginWriteTxn();
    errdefer delete_txn.abort();
    try deleteCanonicalOrdinalMappingTxn(&delete_txn, canonical_a);
    try delete_txn.commit();
    try validateStoreAlloc(alloc, &store);

    var corrupt_txn = try store.beginWriteTxn();
    errdefer corrupt_txn.abort();
    try writeCanonicalOrdinalMappingTxn(&corrupt_txn, canonical_a, ordinal_b);
    try corrupt_txn.commit();
    try std.testing.expectError(error.InvalidDocIdentity, validateStoreAlloc(alloc, &store));
}

test "identity allocation rejects canonical row conflicts before reserving ordinal" {
    const mem_backend = @import("../mem_backend.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const namespace = Namespace{ .table_id = 9, .shard_id = 91, .range_id = 901 };
    try writeNamespaceToStore(&store, namespace);
    const canonical_doc_id = canonicalDocIdForNamespace(namespace, "doc:a");
    var corrupt_txn = try store.beginWriteTxn();
    errdefer corrupt_txn.abort();
    try writeCanonicalOrdinalMappingTxn(&corrupt_txn, canonical_doc_id, 77);
    try corrupt_txn.commit();

    var txn = try store.beginWriteTxn();
    defer txn.abort();
    try std.testing.expectError(error.InvalidDocIdentity, ensureOrdinalForNamespaceTxn(alloc, &txn, namespace, 30, "doc:a"));
    try std.testing.expectEqual(@as(DocOrdinal, 1), try readNextOrdinalTxn(&txn));

    var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (identity_writes.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        identity_writes.deinit(alloc);
    }
    try std.testing.expectError(error.InvalidDocIdentity, appendBatchIdentityMetadataForNamespaceAlloc(
        alloc,
        &store,
        namespace,
        30,
        &identity_writes,
        &.{"doc:a"},
        &.{},
    ));
    try std.testing.expectEqual(@as(usize, 0), identity_writes.items.len);
}

test "ordinal state round trip" {
    const state = OrdinalState{
        .canonical_doc_id = 11,
        .created_generation = 22,
        .deleted_generation = 33,
    };
    const encoded = encodeOrdinalState(state);
    const decoded = try decodeOrdinalState(&encoded);
    try std.testing.expectEqual(state.canonical_doc_id, decoded.canonical_doc_id);
    try std.testing.expectEqual(state.created_generation, decoded.created_generation);
    try std.testing.expectEqual(state.deleted_generation.?, decoded.deleted_generation.?);
    try std.testing.expect(state.isVisibleAt(22));
    try std.testing.expect(!state.isVisibleAt(33));
    try std.testing.expect(!state.isVisibleAt(34));

    const future_state = OrdinalState{
        .canonical_doc_id = 44,
        .created_generation = 50,
    };
    try std.testing.expect(!future_state.isVisibleAt(49));
    try std.testing.expect(future_state.isVisibleAt(50));

    const invalid = OrdinalState{
        .canonical_doc_id = 44,
        .created_generation = 50,
        .deleted_generation = 49,
    };
    try std.testing.expectError(error.InvalidDocIdentity, invalid.validate());
}

test "batch identity metadata persists ordinal mappings and delete generations" {
    const mem_backend = @import("../mem_backend.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    var first_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &first_writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        10,
        &first_writes,
        &.{ "doc:a", "doc\x00b" },
        &.{},
    );
    const first_summary = (try visibilitySummaryFromWrites(first_writes.items)).?;
    try std.testing.expect(allVisibleFromSummary(first_summary, 10));
    try std.testing.expect(!allVisibleFromSummary(first_summary, 9));
    try store.putBatchWithReplay(null, first_writes.items, &.{}, null);

    try std.testing.expectEqual(@as(?bool, true), try allVisibleFromSummaryFast(&store, null));
    try std.testing.expectEqual(@as(?bool, true), try allVisibleFromSummaryFast(&store, 10));
    try std.testing.expectEqual(@as(?bool, false), try allVisibleFromSummaryFast(&store, 9));

    {
        var txn = try store.beginProbeTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(?DocOrdinal, 1), try lookupOrdinalTxn(alloc, &txn, "doc:a"));
        try std.testing.expectEqual(@as(?DocOrdinal, 2), try lookupOrdinalTxn(alloc, &txn, "doc\x00b"));
        try std.testing.expectEqual(@as(DocOrdinal, 3), try readNextOrdinalTxn(&txn));
    }

    var second_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &second_writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        11,
        &second_writes,
        &.{"doc:a"},
        &.{"doc\x00b"},
    );
    const second_summary = (try visibilitySummaryFromWrites(second_writes.items)).?;
    try std.testing.expect(!allVisibleFromSummary(second_summary, null));
    try store.putBatchWithReplay(null, second_writes.items, &.{}, null);

    try std.testing.expectEqual(@as(?bool, false), try allVisibleFromSummaryFast(&store, null));

    var verify_txn = try store.beginProbeTxn();
    defer verify_txn.abort();
    try std.testing.expectEqual(@as(?DocOrdinal, 1), try lookupOrdinalTxn(alloc, &verify_txn, "doc:a"));
    try std.testing.expectEqual(@as(DocOrdinal, 3), try readNextOrdinalTxn(&verify_txn));
    const state = (try lookupStateTxn(&verify_txn, 2)).?;
    try std.testing.expectEqual(@as(u64, 10), state.created_generation);
    try std.testing.expectEqual(@as(u64, 11), state.deleted_generation.?);

    const stats = try fullStatsFromStore(&store);
    try std.testing.expect(stats.complete);
    try std.testing.expectEqual(@as(u64, 2), stats.state_rows);
    try std.testing.expectEqual(@as(u64, 10), stats.min_created_generation);
    try std.testing.expectEqual(@as(u64, 10), stats.max_created_generation);
    try std.testing.expectEqual(@as(u64, 11), stats.min_deleted_generation);
    try std.testing.expectEqual(@as(u64, 11), stats.max_deleted_generation);

    var live = try liveDocSetFromStoreAlloc(alloc, &store);
    defer live.deinit(alloc);
    try std.testing.expect(live.containsOrdinal(1));
    try std.testing.expect(!live.containsOrdinal(2));

    var live_resolved = try resolvedDocSetForIdsTxn(alloc, &verify_txn, &.{ "doc:a", "doc\x00b" });
    defer live_resolved.deinit(alloc);
    try std.testing.expect(live_resolved.containsOrdinal(1));
    try std.testing.expect(!live_resolved.containsOrdinal(2));

    var stale_set = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2, 999 });
    defer stale_set.deinit(alloc);
    var live_filtered = try liveFilteredDocSetFromStoreAlloc(alloc, &store, &stale_set);
    defer live_filtered.deinit(alloc);
    try std.testing.expect(live_filtered.containsOrdinal(1));
    try std.testing.expect(!live_filtered.containsOrdinal(2));
    try std.testing.expect(!live_filtered.containsOrdinal(999));

    var visible_at_create = try visibleFilteredDocSetFromStoreAlloc(alloc, &store, &stale_set, 10);
    defer visible_at_create.deinit(alloc);
    try std.testing.expect(visible_at_create.containsOrdinal(1));
    try std.testing.expect(visible_at_create.containsOrdinal(2));
    try std.testing.expect(!visible_at_create.containsOrdinal(999));

    var visible_at_delete = try visibleFilteredDocSetFromStoreAlloc(alloc, &store, &stale_set, 11);
    defer visible_at_delete.deinit(alloc);
    try std.testing.expect(visible_at_delete.containsOrdinal(1));
    try std.testing.expect(!visible_at_delete.containsOrdinal(2));

    var deleted_resolved = try resolvedDocSetForIdsTxn(alloc, &verify_txn, &.{"doc\x00b"});
    defer deleted_resolved.deinit(alloc);
    try std.testing.expectEqual(@as(?usize, 0), deleted_resolved.estimatedCardinality());

    var snapshot_resolved = try resolvedDocSetForIdsAtGenerationTxn(alloc, &verify_txn, &.{ "doc:a", "doc\x00b" }, 10);
    defer snapshot_resolved.deinit(alloc);
    try std.testing.expect(snapshot_resolved.containsOrdinal(1));
    try std.testing.expect(snapshot_resolved.containsOrdinal(2));

    var mixed_fallback = try resolvedDocSetForIdsTxn(alloc, &verify_txn, &.{ "doc:a", "doc:missing", "doc\x00b" });
    defer mixed_fallback.deinit(alloc);
    switch (mixed_fallback) {
        .doc_keys => |keys| {
            try std.testing.expectEqual(@as(usize, 2), keys.len);
            try std.testing.expectEqualStrings("doc:a", keys[0]);
            try std.testing.expectEqualStrings("doc:missing", keys[1]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "batch identity metadata delete observes buffered resurrection state" {
    const mem_backend = @import("../mem_backend.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    var first_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &first_writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        10,
        &first_writes,
        &.{"doc:a"},
        &.{},
    );
    try store.putBatchWithReplay(null, first_writes.items, &.{}, null);

    var delete_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &delete_writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        20,
        &delete_writes,
        &.{},
        &.{"doc:a"},
    );
    try store.putBatchWithReplay(null, delete_writes.items, &.{}, null);

    {
        var resurrect_txn = try store.beginWriteTxn();
        errdefer resurrect_txn.abort();
        const resolved = try ensureOrdinalTxn(alloc, &resurrect_txn, 0, 0, 25, "doc:a");
        try std.testing.expectEqual(@as(DocOrdinal, 1), resolved.ordinal);
        try resurrect_txn.commit();
    }
    {
        var txn = try store.beginProbeTxn();
        defer txn.abort();
        const state = (try lookupStateTxn(&txn, 1)).?;
        try std.testing.expectEqual(@as(u64, 25), state.created_generation);
        try std.testing.expectEqual(@as(?u64, null), state.deleted_generation);
    }

    var second_delete_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &second_delete_writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        26,
        &second_delete_writes,
        &.{},
        &.{"doc:a"},
    );
    try store.putBatchWithReplay(null, second_delete_writes.items, &.{}, null);

    var resurrect_delete_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &resurrect_delete_writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        30,
        &resurrect_delete_writes,
        &.{"doc:a"},
        &.{"doc:a"},
    );
    try store.putBatchWithReplay(null, resurrect_delete_writes.items, &.{}, null);

    var txn = try store.beginProbeTxn();
    defer txn.abort();
    try std.testing.expectEqual(@as(?DocOrdinal, 1), try lookupOrdinalTxn(alloc, &txn, "doc:a"));
    const state = (try lookupStateTxn(&txn, 1)).?;
    try std.testing.expectEqual(@as(u64, 30), state.created_generation);
    try std.testing.expectEqual(@as(u64, 30), state.deleted_generation.?);

    var live = try resolvedDocSetForIdsTxn(alloc, &txn, &.{"doc:a"});
    defer live.deinit(alloc);
    try std.testing.expectEqual(@as(?usize, 0), live.estimatedCardinality());
}

test "batch identity metadata fails closed at ordinal capacity" {
    const mem_backend = @import("../mem_backend.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const last_allocatable: DocOrdinal = std.math.maxInt(DocOrdinal) - 1;
    var seed_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &seed_writes);
    try appendNextOrdinalWrite(alloc, &seed_writes, last_allocatable);
    try store.putBatchWithReplay(null, seed_writes.items, &.{}, null);

    var too_many_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &too_many_writes);
    try std.testing.expectError(error.DocOrdinalExhausted, appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        20,
        &too_many_writes,
        &.{ "doc:last", "doc:overflow" },
        &.{},
    ));
    {
        var txn = try store.beginProbeTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(?DocOrdinal, null), try lookupOrdinalTxn(alloc, &txn, "doc:last"));
        try std.testing.expectEqual(@as(?DocOrdinal, null), try lookupOrdinalTxn(alloc, &txn, "doc:overflow"));
        try std.testing.expectEqual(last_allocatable, try readNextOrdinalTxn(&txn));
    }

    var final_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &final_writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        21,
        &final_writes,
        &.{"doc:last"},
        &.{},
    );
    try store.putBatchWithReplay(null, final_writes.items, &.{}, null);

    var exhausted_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &exhausted_writes);
    try std.testing.expectError(error.DocOrdinalExhausted, appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        22,
        &exhausted_writes,
        &.{"doc:overflow"},
        &.{},
    ));

    var verify_txn = try store.beginProbeTxn();
    defer verify_txn.abort();
    try std.testing.expectEqual(@as(?DocOrdinal, last_allocatable), try lookupOrdinalTxn(alloc, &verify_txn, "doc:last"));
    try std.testing.expectEqual(@as(?DocOrdinal, null), try lookupOrdinalTxn(alloc, &verify_txn, "doc:overflow"));
    try std.testing.expectEqual(std.math.maxInt(DocOrdinal), try readNextOrdinalTxn(&verify_txn));

    const stats = try fastStatsFromStore(&store);
    try std.testing.expectEqual(std.math.maxInt(DocOrdinal), stats.next_ordinal);
}

test "validate store rejects invalid ordinal generation history" {
    const mem_backend = @import("../mem_backend.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        10,
        &writes,
        &.{"doc:a"},
        &.{},
    );
    try store.putBatchWithReplay(null, writes.items, &.{}, null);

    const invalid_state = encodeOrdinalState(.{
        .canonical_doc_id = canonicalDocId(0, 0, "doc:a"),
        .created_generation = 10,
        .deleted_generation = 9,
    });
    const state_key = internal_keys.identityOrdinalStateKey(1);
    try store.putBatchWithReplay(null, &.{.{
        .key = state_key[0..],
        .value = invalid_state[0..],
    }}, &.{}, null);

    try std.testing.expectError(error.InvalidDocIdentity, validateStoreAlloc(alloc, &store));
}

test "live primary doc set requires complete live primary coverage" {
    const mem_backend = @import("../mem_backend.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    var initial_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &initial_writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        10,
        &initial_writes,
        &.{"doc:a"},
        &.{},
    );
    try appendPrimaryDocWrite(alloc, &initial_writes, "doc:a");
    try appendPrimaryDocWrite(alloc, &initial_writes, "doc:b");
    try store.putBatchWithReplay(null, initial_writes.items, &.{}, null);

    try std.testing.expectEqual(@as(?doc_set.ResolvedDocSet, null), try livePrimaryDocSetIfCompleteFromStoreAlloc(alloc, &store));

    var repair_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &repair_writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        11,
        &repair_writes,
        &.{"doc:b"},
        &.{},
    );
    try store.putBatchWithReplay(null, repair_writes.items, &.{}, null);

    var live = (try livePrimaryDocSetIfCompleteFromStoreAlloc(alloc, &store)) orelse return error.TestUnexpectedResult;
    defer live.deinit(alloc);
    try std.testing.expect(live.containsOrdinal(1));
    try std.testing.expect(live.containsOrdinal(2));

    var visible_at_11 = (try visiblePrimaryDocSetIfCompleteFromStoreAlloc(alloc, &store, 11)) orelse return error.TestUnexpectedResult;
    defer visible_at_11.deinit(alloc);
    try std.testing.expect(visible_at_11.containsOrdinal(1));
    try std.testing.expect(visible_at_11.containsOrdinal(2));

    var tombstone_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer freeIdentityWrites(alloc, &tombstone_writes);
    try appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        12,
        &tombstone_writes,
        &.{},
        &.{"doc:b"},
    );
    try store.putBatchWithReplay(null, tombstone_writes.items, &.{}, null);

    try std.testing.expectEqual(@as(?doc_set.ResolvedDocSet, null), try livePrimaryDocSetIfCompleteFromStoreAlloc(alloc, &store));
    try std.testing.expectEqual(@as(?doc_set.ResolvedDocSet, null), try visiblePrimaryDocSetIfCompleteFromStoreAlloc(alloc, &store, 12));

    var visible_before_tombstone = (try visiblePrimaryDocSetIfCompleteFromStoreAlloc(alloc, &store, 11)) orelse return error.TestUnexpectedResult;
    defer visible_before_tombstone.deinit(alloc);
    try std.testing.expect(visible_before_tombstone.containsOrdinal(1));
    try std.testing.expect(visible_before_tombstone.containsOrdinal(2));
}

fn appendPrimaryDocWrite(alloc: Allocator, writes: *std.ArrayListUnmanaged(docstore_mod.KVPair), doc_id: []const u8) !void {
    const key = try internal_keys.documentKeyAlloc(alloc, doc_id);
    var key_owned = true;
    errdefer if (key_owned) alloc.free(key);
    const value = try alloc.dupe(u8, "{}");
    var value_owned = true;
    errdefer if (value_owned) alloc.free(value);
    try writes.append(alloc, .{ .key = key, .value = value });
    key_owned = false;
    value_owned = false;
}

fn freeIdentityWrites(alloc: Allocator, writes: *std.ArrayListUnmanaged(docstore_mod.KVPair)) void {
    for (writes.items) |item| {
        alloc.free(@constCast(item.key));
        alloc.free(@constCast(item.value));
    }
    writes.deinit(alloc);
}
