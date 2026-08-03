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
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const fs_paths = @import("../../common/fs_paths.zig");
const background_runtime_mod = @import("../background_runtime.zig");
const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const shard_mod = @import("../shard.zig");
const artifact_replay = @import("artifact_replay.zig");
const db_config = @import("config.zig");
const db_core = @import("core.zig");
const db_internal = @import("internal.zig");
const derived_async = @import("derived_async.zig");
const doc_identity = @import("doc_identity.zig");
const generation_lifecycle = @import("generation_lifecycle.zig");
const mapper = @import("document_mapper.zig");
const apply_state = @import("derived/apply_state.zig");
const range_state_mod = @import("range_state.zig");
const range_cardinality = @import("range_cardinality.zig");
const root_identity = @import("root_identity.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_store_mod = @import("relational_store.zig");
const types = @import("types.zig");
const index_manager_mod = @import("catalog/index_manager.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const DerivedCoverageOutcome = derived_async.DerivedCoverageOutcome;

pub const ShadowState = struct {
    manager: *index_manager_mod.IndexManager,
    base_path: []u8,
    indexes_path: []u8,
    range_start: []u8,
    range_end: []u8,
};

const putLeakyJsonStringField = db_internal.putLeakyJsonStringField;
const putLeakyJsonU64Field = db_internal.putLeakyJsonU64Field;

const threadedIo = db_internal.threadedIo;

fn loadOrCreateDurableRootIdentity(
    alloc: Allocator,
    backend_runtime: ?*background_runtime_mod.BackendRuntime,
    path: []const u8,
) !root_identity.State {
    if (backend_runtime) |runtime| {
        if (runtime.io()) |io| return try root_identity.loadOrCreate(alloc, io, path);
    }
    if (comptime builtin.os.tag == .freestanding) return error.Unsupported;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try root_identity.loadOrCreate(alloc, io_impl.io(), path);
}

fn ensureDirPath(path: []const u8) !void {
    if (path.len == 0) return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), path);
}

fn byteRangesEqual(a: types.ByteRange, b: types.ByteRange) bool {
    return std.mem.eql(u8, a.start, b.start) and std.mem.eql(u8, a.end, b.end);
}

fn splitBootstrapMarkersEqual(a: range_state_mod.SplitBootstrapMarker, b: range_state_mod.SplitBootstrapMarker) bool {
    return a.transition_id == b.transition_id and
        a.attempt_epoch == b.attempt_epoch and
        a.source_group_id == b.source_group_id and
        a.destination_group_id == b.destination_group_id and
        a.bootstrap_complete == b.bootstrap_complete;
}

fn documentRangeLowerAlloc(alloc: Allocator, raw_key: []const u8) ![]u8 {
    return try internal_keys.documentRangeLowerAlloc(alloc, raw_key);
}

fn documentRangeUpperAlloc(alloc: Allocator, raw_key: []const u8) !?[]u8 {
    return try internal_keys.documentRangeUpperAlloc(alloc, raw_key);
}

fn isBaseDocumentStoreKeyForMode(relational_base_rows: bool, key: []const u8) bool {
    if (relational_base_rows) return internal_keys.isRelationalRowKey(key);
    return internal_keys.isPrimaryDocumentKey(key);
}

fn skipNonPrimaryMedianKey(key: []const u8) bool {
    return !internal_keys.isPrimaryDocumentKey(key);
}

fn skipNonRelationalMedianKey(key: []const u8) bool {
    return !internal_keys.isRelationalRowKey(key);
}

fn isSplitMetadataKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "splitstate:") or
        std.mem.startsWith(u8, key, "splitdelta:");
}

fn splitLogicalWriteFromPhysicalAlloc(
    alloc: Allocator,
    relational_base_rows: bool,
    write: types.BatchWrite,
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
) !?types.BatchWrite {
    if (internal_keys.isInternalPhysicalTableDataKey(write.key) and
        !internal_keys.isStoredDocumentRowKey(write.key)) return null;

    if (!internal_keys.isStoredDocumentRowKey(write.key)) return write;
    if (!isBaseDocumentStoreKeyForMode(relational_base_rows, write.key)) return null;

    const raw = (try internal_keys.decodeStoredDocumentRowKeyAlloc(alloc, write.key)) orelse return error.InvalidInternalUserKey;
    try owned_keys.append(alloc, raw);
    const value = if (relational_base_rows) blk: {
        const materialized = try mapper.materializeRelationalRowValueAlloc(alloc, write.value);
        try owned_values.append(alloc, materialized);
        break :blk materialized;
    } else write.value;
    return .{
        .key = raw,
        .value = value,
    };
}

test "split logical writes skip relational secondary physical table data" {
    const alloc = std.testing.allocator;

    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    const doc_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_key);
    const doc_logical = (try splitLogicalWriteFromPhysicalAlloc(
        alloc,
        false,
        .{ .key = doc_key, .value = "{\"title\":\"alpha\"}" },
        &owned_keys,
        &owned_values,
    )) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("doc:a", doc_logical.key);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", doc_logical.value);

    const row_value = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 1.0 },
        },
    });
    defer alloc.free(row_value);
    const row_key = try internal_keys.relationalRowKeyAlloc(alloc, "row:a");
    defer alloc.free(row_key);
    const row_logical = (try splitLogicalWriteFromPhysicalAlloc(
        alloc,
        true,
        .{ .key = row_key, .value = row_value },
        &owned_keys,
        &owned_values,
    )) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("row:a", row_logical.key);
    try std.testing.expect(std.mem.indexOf(u8, row_logical.value, "\"amount\"") != null);

    const relational_secondary_keys = [_][]u8{
        try internal_keys.relationalColumnKeyAlloc(alloc, "row:a", "status"),
        try internal_keys.relationalColumnIndexKeyAlloc(alloc, "status", "row:a"),
        try internal_keys.relationalArrayElementIndexKeyAlloc(alloc, "tags", "hot", "row:a"),
        try internal_keys.relationalArrayValueIndexKeyAlloc(alloc, "tags", "[hot]", "row:a"),
        try internal_keys.relationalJsonValueIndexKeyAlloc(alloc, "attrs", "billing.plan", "\"pro\"", "row:a"),
        try internal_keys.relationalJsonPathIndexKeyAlloc(alloc, "attrs", "billing.plan", "row:a"),
        try internal_keys.relationalColumnIndexByDocKeyAlloc(alloc, "row:a", "status"),
        try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:a", "orders", "row:a"),
        try internal_keys.relationalUniqueKeyAlloc(alloc, "orders_external_id_key", "external:a"),
        try internal_keys.relationalTemporalUniqueKeyAlloc(alloc, "prices_sku_valid_time_key", "sku:a", "10", "20", "row:a"),
        try internal_keys.relationalForeignKeyConflictKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:a"),
    };
    defer {
        for (relational_secondary_keys) |key| alloc.free(key);
    }

    for (relational_secondary_keys) |key| {
        const logical = try splitLogicalWriteFromPhysicalAlloc(
            alloc,
            true,
            .{ .key = key, .value = "not-json-and-not-a-packed-row" },
            &owned_keys,
            &owned_values,
        );
        try std.testing.expect(logical == null);
    }
}

fn jsonObjectOptionalU64(object: std.json.ObjectMap, field_name: []const u8) !?u64 {
    const value = object.get(field_name) orelse return null;
    return switch (value) {
        .integer => |int_value| if (int_value >= 0) @as(u64, @intCast(int_value)) else error.InvalidArgument,
        .number_string => |text| std.fmt.parseUnsigned(u64, text, 10) catch error.InvalidArgument,
        else => error.InvalidArgument,
    };
}

fn identityMetadataRange() struct { lower: [1]u8, upper: [1]u8 } {
    return .{
        .lower = .{internal_keys.identity_namespace},
        .upper = .{internal_keys.identity_namespace + 1},
    };
}

fn putIdentityMetadataRows(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    rows: []const docstore_mod.OwnedKVPair,
) !void {
    if (rows.len == 0) return;

    var writes = try alloc.alloc(docstore_mod.KVPair, rows.len);
    defer alloc.free(writes);
    for (rows, 0..) |row, i| {
        writes[i] = .{
            .key = row.key,
            .value = row.value,
        };
    }
    try store.putBatch(writes, &.{});
}

fn copyIdentityMetadataToStore(
    alloc: Allocator,
    src_store: *docstore_mod.DocStore,
    dest_store: *docstore_mod.DocStore,
) !void {
    const range = identityMetadataRange();
    const rows = try src_store.scanRange(alloc, range.lower[0..], range.upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, rows);
    try putIdentityMetadataRows(alloc, dest_store, rows);
}

fn copyDerivedCoverageMetadataToStore(
    alloc: Allocator,
    src_store: *docstore_mod.DocStore,
    dest_store: *docstore_mod.DocStore,
) !void {
    const lower = [_]u8{ internal_keys.replay_namespace, 0xff, internal_keys.derived_coverage_kind };
    const upper = try internal_keys.nextPrefixAlloc(alloc, &lower);
    defer if (upper) |key| alloc.free(key);

    const CopyState = struct {
        alloc: Allocator,
        dest: *docstore_mod.DocStore,
        writes: std.ArrayListUnmanaged(docstore_mod.KVPair) = .empty,
        owned: std.ArrayListUnmanaged(docstore_mod.OwnedKVPair) = .empty,

        fn deinit(state: *@This()) void {
            state.freeOwned();
            state.writes.deinit(state.alloc);
            state.owned.deinit(state.alloc);
        }

        fn freeOwned(state: *@This()) void {
            for (state.owned.items) |item| {
                state.alloc.free(item.key);
                state.alloc.free(item.value);
            }
            state.owned.clearRetainingCapacity();
        }

        fn flush(state: *@This()) !void {
            if (state.writes.items.len == 0) return;
            try state.dest.putBatch(state.writes.items, &.{});
            state.writes.clearRetainingCapacity();
            state.freeOwned();
        }

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            const owned_key = try state.alloc.dupe(u8, key);
            const owned_value = state.alloc.dupe(u8, value) catch |err| {
                state.alloc.free(owned_key);
                return err;
            };
            state.owned.append(state.alloc, .{ .key = owned_key, .value = owned_value }) catch |err| {
                state.alloc.free(owned_key);
                state.alloc.free(owned_value);
                return err;
            };
            try state.writes.append(state.alloc, .{ .key = owned_key, .value = owned_value });
            if (state.writes.items.len >= 8192) try state.flush();
            return .@"continue";
        }
    };

    var state = CopyState{ .alloc = alloc, .dest = dest_store };
    defer state.deinit();
    try src_store.scanWithContext(&lower, if (upper) |key| key else "", .{}, &state, CopyState.scanEntry);
    try state.flush();
}

fn rebaseRangeCoverageMetadata(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    byte_range: types.ByteRange,
    extra_writes: []const docstore_mod.KVPair,
) !void {
    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }
    var seen_indexes = std.StringHashMapUnmanaged(void).empty;
    defer seen_indexes.deinit(alloc);
    try writes.appendSlice(alloc, extra_writes);

    const append_index = struct {
        fn run(
            inner_alloc: Allocator,
            inner_store: *docstore_mod.DocStore,
            inner_manager: *index_manager_mod.IndexManager,
            range: types.ByteRange,
            index_name: []const u8,
            seen: *std.StringHashMapUnmanaged(void),
            out: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            keys: *std.ArrayListUnmanaged([]u8),
            values: *std.ArrayListUnmanaged([]u8),
        ) !void {
            const gop = try seen.getOrPut(inner_alloc, index_name);
            if (gop.found_existing) return;
            const generation = inner_manager.coverageGenerationForIndex(index_name) orelse return;
            const prefix = try internal_keys.derivedCoverageOutcomeMarkerPrefixAlloc(inner_alloc, index_name, generation);
            defer inner_alloc.free(prefix);
            const upper = try internal_keys.nextPrefixAlloc(inner_alloc, prefix);
            defer if (upper) |key| inner_alloc.free(key);

            const ScanState = struct {
                alloc: Allocator,
                index_name: []const u8,
                generation: u64,
                byte_range: types.ByteRange,
                counts: [std.meta.tags(DerivedCoverageOutcome).len]u64 =
                    [_]u64{0} ** std.meta.tags(DerivedCoverageOutcome).len,

                fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    const doc_key = try internal_keys.derivedCoverageOutcomeDocKeyAlloc(
                        state.alloc,
                        state.index_name,
                        state.generation,
                        key,
                    );
                    defer state.alloc.free(doc_key);
                    if (!state.byte_range.contains(doc_key)) return .@"continue";
                    const outcome = std.meta.stringToEnum(DerivedCoverageOutcome, value) orelse
                        return error.InvalidDerivedCoverageOutcome;
                    const outcome_index = @intFromEnum(outcome);
                    state.counts[outcome_index] = std.math.add(u64, state.counts[outcome_index], 1) catch
                        return error.InvalidDerivedCoverageCounter;
                    return .@"continue";
                }
            };

            var state = ScanState{
                .alloc = inner_alloc,
                .index_name = index_name,
                .generation = generation,
                .byte_range = range,
            };
            try inner_store.scanWithContext(prefix, if (upper) |key| key else "", .{}, &state, ScanState.scanEntry);

            inline for (std.meta.tags(DerivedCoverageOutcome), 0..) |outcome, i| {
                const key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(inner_alloc, index_name, generation, @tagName(outcome));
                keys.append(inner_alloc, key) catch |err| {
                    inner_alloc.free(key);
                    return err;
                };
                const value = try inner_alloc.alloc(u8, 8);
                std.mem.writeInt(u64, value[0..8], state.counts[i], .little);
                values.append(inner_alloc, value) catch |err| {
                    inner_alloc.free(value);
                    return err;
                };
                try out.append(inner_alloc, .{ .key = key, .value = value });
            }
        }
    }.run;

    for (index_manager.dense_indexes.items) |entry| {
        try append_index(alloc, store, index_manager, byte_range, entry.config.name, &seen_indexes, &writes, &owned_keys, &owned_values);
    }
    for (index_manager.sparse_indexes.items) |entry| {
        try append_index(alloc, store, index_manager, byte_range, entry.config.name, &seen_indexes, &writes, &owned_keys, &owned_values);
    }

    const document_count = try range_cardinality.countPrimaryDocuments(alloc, store, byte_range);
    const count_key = try alloc.dupe(u8, &internal_keys.range_document_count_key);
    owned_keys.append(alloc, count_key) catch |err| {
        alloc.free(count_key);
        return err;
    };
    const count_value = try alloc.alloc(u8, 8);
    std.mem.writeInt(u64, count_value[0..8], document_count, .little);
    owned_values.append(alloc, count_value) catch |err| {
        alloc.free(count_value);
        return err;
    };
    try writes.append(alloc, .{ .key = count_key, .value = count_value });

    try store.putBatch(writes.items, &.{});
}

fn ensureReplayFloor(store: *docstore_mod.DocStore, next_sequence: u64) !void {
    try store.ensureReplayNextSequenceAtLeast(next_sequence);
}

fn deleteKeysWithPrefixFromStore(alloc: Allocator, dest_store: *docstore_mod.DocStore, prefix: []const u8) !void {
    const keys = try dest_store.scanPrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, keys);
    if (keys.len == 0) return;

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    for (keys) |item| {
        try deletes.append(alloc, item.key);
    }
    try dest_store.putBatch(&.{}, deletes.items);
}

fn clearSplitMetadataFromStore(alloc: Allocator, dest_store: *docstore_mod.DocStore) !void {
    try deleteKeysWithPrefixFromStore(alloc, dest_store, "splitstate:");
    try deleteKeysWithPrefixFromStore(alloc, dest_store, "splitdelta:");
}

fn clearSystemMetadataFromSplitDestination(alloc: Allocator, dest_store: *docstore_mod.DocStore) !void {
    try deleteKeysWithPrefixFromStore(alloc, dest_store, "\x00\x00__metadata__:");
}

const SplitDestinationStorePlan = union(enum) {
    lmdb,
    lsm: lsm_backend_mod.Options,
    unsupported,
};

const OpenedSplitDestinationStore = struct {
    store: docstore_mod.DocStore,
    owner: db_core.PrimaryStoreOwner = .none,

    fn close(self: *OpenedSplitDestinationStore, alloc: Allocator) void {
        self.store.close();
        self.owner.close(alloc);
        self.* = undefined;
    }
};

fn putIndexedSplitBatchDirect(
    dest_store: *docstore_mod.DocStore,
    dest_indexes: *index_manager_mod.IndexManager,
    writes: []const types.BatchWrite,
    dense_handoffs: []const index_manager_mod.DenseSplitHandoff,
    text_handoffs: []const index_manager_mod.TextSplitHandoff,
    sparse_handoffs: []const index_manager_mod.SparseSplitHandoff,
    column_index_policy: relational_store_mod.ColumnIndexPolicy,
) !void {
    if (writes.len == 0) return;

    var raw_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer raw_writes.deinit(dest_indexes.alloc);
    try raw_writes.ensureUnusedCapacity(dest_indexes.alloc, writes.len);
    for (writes) |write| raw_writes.appendAssumeCapacity(.{ .key = write.key, .value = write.value });

    var owned_column_index_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_column_index_keys.items) |key| dest_indexes.alloc.free(key);
        owned_column_index_keys.deinit(dest_indexes.alloc);
    }
    var owned_column_index_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_column_index_values.items) |value| dest_indexes.alloc.free(value);
        owned_column_index_values.deinit(dest_indexes.alloc);
    }
    if (dest_indexes.relational_base_rows) {
        for (writes) |write| {
            const doc_key = (try internal_keys.decodeRelationalRowKeyAlloc(dest_indexes.alloc, write.key)) orelse continue;
            defer dest_indexes.alloc.free(doc_key);
            try relational_store_mod.appendColumnBackedIndexWritesForRowWithColumnIndexPolicy(
                dest_indexes.alloc,
                &raw_writes,
                &owned_column_index_keys,
                &owned_column_index_values,
                doc_key,
                write.value,
                column_index_policy,
            );
        }
    }
    try dest_store.putBatch(raw_writes.items, &.{});

    try applySplitEmbeddingArtifactsFromBatch(dest_store, dest_indexes, writes, dense_handoffs, sparse_handoffs);

    var logical_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer logical_writes.deinit(dest_indexes.alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| dest_indexes.alloc.free(key);
        owned_keys.deinit(dest_indexes.alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| dest_indexes.alloc.free(value);
        owned_values.deinit(dest_indexes.alloc);
    }

    for (writes) |write| {
        const logical = try splitLogicalWriteFromPhysicalAlloc(
            dest_indexes.alloc,
            dest_indexes.relational_base_rows,
            write,
            &owned_keys,
            &owned_values,
        ) orelse continue;
        try logical_writes.append(dest_indexes.alloc, logical);
    }

    try dest_indexes.indexSplitBatch(dest_store, logical_writes.items, dense_handoffs, text_handoffs, sparse_handoffs);
}

fn findDenseSplitHandoffLocal(handoffs: []const index_manager_mod.DenseSplitHandoff, index_name: []const u8) ?*const index_manager_mod.DenseSplitHandoff {
    for (handoffs) |*handoff| {
        if (std.mem.eql(u8, handoff.index_name, index_name)) return handoff;
    }
    return null;
}

fn findSparseSplitHandoffLocal(handoffs: []const index_manager_mod.SparseSplitHandoff, index_name: []const u8) ?*const index_manager_mod.SparseSplitHandoff {
    for (handoffs) |*handoff| {
        if (std.mem.eql(u8, handoff.index_name, index_name)) return handoff;
    }
    return null;
}

fn collectEmbeddingArtifactKeysFromBatch(
    alloc: Allocator,
    writes: []const types.BatchWrite,
) ![][]const u8 {
    var keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer keys.deinit(alloc);

    for (writes) |write| {
        if (!internal_keys.isEmbeddingArtifactKey(write.key) and !internal_keys.isDerivedEmbeddingArtifactKey(write.key)) continue;
        try keys.append(alloc, write.key);
    }

    return try keys.toOwnedSlice(alloc);
}

fn collectEmbeddingArtifactKeysInRangeAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower: []const u8,
    upper: []const u8,
) ![][]const u8 {
    const store_lower = try documentRangeLowerAlloc(alloc, lower);
    defer alloc.free(store_lower);
    const store_upper = if (upper.len > 0) try documentRangeUpperAlloc(alloc, upper) else null;
    defer if (store_upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, store_lower, if (store_upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (keys.items) |key| alloc.free(@constCast(key));
        keys.deinit(alloc);
    }

    for (scanned) |entry| {
        if (!internal_keys.isEmbeddingArtifactKey(entry.key) and !internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) continue;
        const owned = try alloc.dupe(u8, entry.key);
        try keys.append(alloc, owned);
    }

    return try keys.toOwnedSlice(alloc);
}

fn collectGraphArtifactKeysInRangeAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower: []const u8,
    upper: []const u8,
) ![][]const u8 {
    const store_lower = try documentRangeLowerAlloc(alloc, lower);
    defer alloc.free(store_lower);
    const store_upper = if (upper.len > 0) try documentRangeUpperAlloc(alloc, upper) else null;
    defer if (store_upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, store_lower, if (store_upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (keys.items) |key| alloc.free(@constCast(key));
        keys.deinit(alloc);
    }

    for (scanned) |entry| {
        if (!internal_keys.isGraphEdgeArtifactKey(entry.key)) continue;
        const owned = try alloc.dupe(u8, entry.key);
        try keys.append(alloc, owned);
    }

    return try keys.toOwnedSlice(alloc);
}

fn freeOwnedConstStringSlice(alloc: Allocator, keys: []const []const u8) void {
    for (keys) |key| alloc.free(@constCast(key));
    if (keys.len > 0) alloc.free(keys);
}

fn applySplitDenseEmbeddingArtifacts(
    dest_store: *docstore_mod.DocStore,
    dest_indexes: *index_manager_mod.IndexManager,
    artifact_keys: []const []const u8,
    index_name: []const u8,
    handoff: ?*const index_manager_mod.DenseSplitHandoff,
) !void {
    var dense_embeddings = try artifact_replay.collectDenseEmbeddingWritesForArtifacts(
        dest_indexes.alloc,
        dest_indexes,
        artifact_keys,
        index_name,
    );
    defer dense_embeddings.deinit();
    if (dense_embeddings.writes.len == 0) return;

    if (handoff) |skip| {
        var filtered = std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite).empty;
        defer filtered.deinit(dest_indexes.alloc);
        for (dense_embeddings.writes) |write| {
            if (skip.shouldSkip(write.doc_key)) continue;
            try filtered.append(dest_indexes.alloc, write);
        }
        if (filtered.items.len == 0) return;
        try dest_indexes.applyDenseEmbeddingWritesByNameWithOptions(dest_store, index_name, filtered.items, .{ .mode = .bulk_ingest });
        return;
    }

    try dest_indexes.applyDenseEmbeddingWritesByNameWithOptions(dest_store, index_name, dense_embeddings.writes, .{ .mode = .bulk_ingest });
}

fn applySplitSparseEmbeddingArtifacts(
    dest_store: *docstore_mod.DocStore,
    dest_indexes: *index_manager_mod.IndexManager,
    artifact_keys: []const []const u8,
    index_name: []const u8,
    handoff: ?*const index_manager_mod.SparseSplitHandoff,
) !void {
    var sparse_embeddings = try artifact_replay.collectSparseEmbeddingWritesForArtifacts(
        dest_indexes.alloc,
        dest_indexes,
        artifact_keys,
        index_name,
    );
    defer sparse_embeddings.deinit();
    if (sparse_embeddings.writes.len == 0) return;

    if (handoff) |skip| {
        var filtered = std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite).empty;
        defer filtered.deinit(dest_indexes.alloc);
        for (sparse_embeddings.writes) |write| {
            if (skip.shouldSkip(write.doc_key)) continue;
            try filtered.append(dest_indexes.alloc, write);
        }
        if (filtered.items.len == 0) return;
        try dest_indexes.applySparseEmbeddingWritesByNameWithOptions(dest_store, index_name, filtered.items, .{ .mode = .bulk_ingest });
        return;
    }

    try dest_indexes.applySparseEmbeddingWritesByNameWithOptions(dest_store, index_name, sparse_embeddings.writes, .{ .mode = .bulk_ingest });
}

fn applySplitEmbeddingArtifacts(
    dest_store: *docstore_mod.DocStore,
    dest_indexes: *index_manager_mod.IndexManager,
    artifact_keys: []const []const u8,
    dense_handoffs: []const index_manager_mod.DenseSplitHandoff,
    sparse_handoffs: []const index_manager_mod.SparseSplitHandoff,
) !void {
    if (artifact_keys.len == 0) return;

    for (dest_indexes.dense_indexes.items) |entry| {
        try applySplitDenseEmbeddingArtifacts(
            dest_store,
            dest_indexes,
            artifact_keys,
            entry.config.name,
            findDenseSplitHandoffLocal(dense_handoffs, entry.config.name),
        );
    }

    for (dest_indexes.sparse_indexes.items) |entry| {
        try applySplitSparseEmbeddingArtifacts(
            dest_store,
            dest_indexes,
            artifact_keys,
            entry.config.name,
            findSparseSplitHandoffLocal(sparse_handoffs, entry.config.name),
        );
    }
}

fn applySplitEmbeddingArtifactsFromBatch(
    dest_store: *docstore_mod.DocStore,
    dest_indexes: *index_manager_mod.IndexManager,
    writes: []const types.BatchWrite,
    dense_handoffs: []const index_manager_mod.DenseSplitHandoff,
    sparse_handoffs: []const index_manager_mod.SparseSplitHandoff,
) !void {
    const artifact_keys = try collectEmbeddingArtifactKeysFromBatch(dest_indexes.alloc, writes);
    defer if (artifact_keys.len > 0) dest_indexes.alloc.free(artifact_keys);
    try applySplitEmbeddingArtifacts(dest_store, dest_indexes, artifact_keys, dense_handoffs, sparse_handoffs);
}

fn applySplitEmbeddingArtifactsInRange(
    alloc: Allocator,
    lower: []const u8,
    upper: []const u8,
    dest_store: *docstore_mod.DocStore,
    dest_indexes: *index_manager_mod.IndexManager,
    dense_handoffs: []const index_manager_mod.DenseSplitHandoff,
    sparse_handoffs: []const index_manager_mod.SparseSplitHandoff,
) !void {
    const artifact_keys = try collectEmbeddingArtifactKeysInRangeAlloc(alloc, dest_store, lower, upper);
    defer freeOwnedConstStringSlice(alloc, artifact_keys);
    try applySplitEmbeddingArtifacts(dest_store, dest_indexes, artifact_keys, dense_handoffs, sparse_handoffs);
}

fn applySplitGraphArtifacts(
    dest_store: *docstore_mod.DocStore,
    dest_indexes: *index_manager_mod.IndexManager,
    artifact_keys: []const []const u8,
) !void {
    if (artifact_keys.len == 0) return;

    for (dest_indexes.graph_indexes.items) |entry| {
        var graph_mutations = try artifact_replay.collectGraphMutationsForArtifacts(
            dest_indexes.alloc,
            dest_store,
            artifact_keys,
            entry.config.name,
            .{},
        );
        defer graph_mutations.deinit();
        try dest_indexes.applyGraphMutationsByName(entry.config.name, graph_mutations.writes, graph_mutations.deletes);
    }
}

pub fn applySplitGraphArtifactsInRange(
    alloc: Allocator,
    lower: []const u8,
    upper: []const u8,
    dest_store: *docstore_mod.DocStore,
    dest_indexes: *index_manager_mod.IndexManager,
) !void {
    const artifact_keys = try collectGraphArtifactKeysInRangeAlloc(alloc, dest_store, lower, upper);
    defer freeOwnedConstStringSlice(alloc, artifact_keys);
    try applySplitGraphArtifacts(dest_store, dest_indexes, artifact_keys);
}

fn streamRangeIntoSplitDestinationDirect(
    comptime DB: type,
    alloc: Allocator,
    src: *DB,
    lower: []const u8,
    upper: []const u8,
    dest_store: *docstore_mod.DocStore,
    dest_indexes: *index_manager_mod.IndexManager,
    dense_handoffs: []const index_manager_mod.DenseSplitHandoff,
    text_handoffs: []const index_manager_mod.TextSplitHandoff,
    sparse_handoffs: []const index_manager_mod.SparseSplitHandoff,
    column_index_policy: relational_store_mod.ColumnIndexPolicy,
) !void {
    const batch_size = 8192;

    const store_lower = try documentRangeLowerAlloc(alloc, lower);
    defer alloc.free(store_lower);
    const store_upper = if (upper.len > 0) try documentRangeUpperAlloc(alloc, upper) else null;
    defer if (store_upper) |buf| alloc.free(buf);

    const scanned = try src.core.scanStoreRange(alloc, store_lower, if (store_upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer writes.deinit(alloc);
    for (scanned) |entry| {
        if (isSplitMetadataKey(entry.key) or std.mem.startsWith(u8, entry.key, "\x00\x00__metadata__:")) continue;
        try writes.append(alloc, .{
            .key = entry.key,
            .value = entry.value,
        });
        if (writes.items.len == batch_size) {
            try putIndexedSplitBatchDirect(dest_store, dest_indexes, writes.items, dense_handoffs, text_handoffs, sparse_handoffs, column_index_policy);
            writes.clearRetainingCapacity();
        }
    }

    if (writes.items.len > 0) try putIndexedSplitBatchDirect(dest_store, dest_indexes, writes.items, dense_handoffs, text_handoffs, sparse_handoffs, column_index_policy);
}

fn indexExistingSplitDestinationDirect(
    alloc: Allocator,
    lower: []const u8,
    upper: []const u8,
    dest_store: *docstore_mod.DocStore,
    dest_indexes: *index_manager_mod.IndexManager,
    dense_handoffs: []const index_manager_mod.DenseSplitHandoff,
    text_handoffs: []const index_manager_mod.TextSplitHandoff,
    sparse_handoffs: []const index_manager_mod.SparseSplitHandoff,
    column_index_policy: relational_store_mod.ColumnIndexPolicy,
) !void {
    const batch_size = 8192;

    const store_lower = try documentRangeLowerAlloc(alloc, lower);
    defer alloc.free(store_lower);
    const store_upper = if (upper.len > 0) try documentRangeUpperAlloc(alloc, upper) else null;
    defer if (store_upper) |buf| alloc.free(buf);

    const scanned = try dest_store.scanRange(alloc, store_lower, if (store_upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer writes.deinit(alloc);
    for (scanned) |entry| {
        if (isSplitMetadataKey(entry.key)) continue;
        try writes.append(alloc, .{
            .key = entry.key,
            .value = entry.value,
        });
        if (writes.items.len == batch_size) {
            try putIndexedSplitBatchDirect(dest_store, dest_indexes, writes.items, dense_handoffs, text_handoffs, sparse_handoffs, column_index_policy);
            writes.clearRetainingCapacity();
        }
    }

    if (writes.items.len > 0) try putIndexedSplitBatchDirect(dest_store, dest_indexes, writes.items, dense_handoffs, text_handoffs, sparse_handoffs, column_index_policy);
}

pub fn restoreIntentMarkerPathAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/.restore-intent", .{path});
}

pub fn restoreStateMarkerPathAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/.restore-state", .{path});
}

pub fn restoreImportMarkerPathAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/.restore-importing", .{path});
}

pub fn restoreRepairMarkerPathAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/.restore-runtime-repair-complete", .{path});
}

pub fn deleteFileIfExists(io: Io, path: []const u8) !void {
    std.Io.Dir.cwd().deleteFile(io, path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}

pub const RestoreState = struct {
    backup_id: []u8,
    location: []u8,
    artifact_sha256: []u8,
    snapshot_path: []u8,
    group_id: u64,
    phase: []u8,
    primary_restored: bool,
    runtime_repair_complete: bool,
    last_error: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.backup_id);
        alloc.free(self.location);
        alloc.free(self.artifact_sha256);
        alloc.free(self.snapshot_path);
        alloc.free(self.phase);
        alloc.free(self.last_error);
        self.* = undefined;
    }
};

pub const RestoreIdentity = struct {
    backup_id: []const u8,
    location: []const u8,
    artifact_sha256: []const u8,
    snapshot_path: []const u8,
    group_id: u64,
};

const restore_marker_format_version: u32 = 1;
const max_restore_marker_bytes: usize = 64 * 1024;

const RestoreStateDisk = struct {
    format_version: u32 = restore_marker_format_version,
    backup_id: []const u8,
    location: []const u8,
    artifact_sha256: []const u8,
    snapshot_path: []const u8,
    group_id: u64,
    phase: []const u8,
    primary_restored: bool,
    runtime_repair_complete: bool,
    last_error: []const u8,
};

const RestoreImportDisk = struct {
    format_version: u32 = restore_marker_format_version,
    snapshot_root: []const u8,
    backup_id: []const u8,
    location: []const u8,
    artifact_sha256: []const u8,
    snapshot_path: []const u8,
    group_id: u64,
};

pub fn restoreStateAlloc(
    alloc: Allocator,
    backup_id: []const u8,
    location: []const u8,
    artifact_sha256: []const u8,
    snapshot_path: []const u8,
    group_id: u64,
    phase: []const u8,
    primary_restored: bool,
    runtime_repair_complete: bool,
    last_error: []const u8,
) !RestoreState {
    const backup_id_owned = try alloc.dupe(u8, backup_id);
    errdefer alloc.free(backup_id_owned);
    const location_owned = try alloc.dupe(u8, location);
    errdefer alloc.free(location_owned);
    const artifact_sha256_owned = try alloc.dupe(u8, artifact_sha256);
    errdefer alloc.free(artifact_sha256_owned);
    const snapshot_path_owned = try alloc.dupe(u8, snapshot_path);
    errdefer alloc.free(snapshot_path_owned);
    const phase_owned = try alloc.dupe(u8, phase);
    errdefer alloc.free(phase_owned);
    const last_error_owned = try alloc.dupe(u8, last_error);
    errdefer alloc.free(last_error_owned);

    return .{
        .backup_id = backup_id_owned,
        .location = location_owned,
        .artifact_sha256 = artifact_sha256_owned,
        .snapshot_path = snapshot_path_owned,
        .group_id = group_id,
        .phase = phase_owned,
        .primary_restored = primary_restored,
        .runtime_repair_complete = runtime_repair_complete,
        .last_error = last_error_owned,
    };
}

pub const RestoreImportState = struct {
    snapshot_root: []u8,
    identity: RestoreState,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.snapshot_root);
        self.identity.deinit(alloc);
        self.* = undefined;
    }
};

fn validRestoreArtifactSha256(value: []const u8) bool {
    if (value.len != std.crypto.hash.sha2.Sha256.digest_length * 2) return false;
    for (value) |c| {
        if (!std.ascii.isDigit(c) and !(c >= 'a' and c <= 'f')) return false;
    }
    return true;
}

fn validRestoreIdentity(identity: RestoreIdentity) bool {
    return identity.backup_id.len > 0 and
        identity.location.len > 0 and
        validRestoreArtifactSha256(identity.artifact_sha256) and
        identity.snapshot_path.len > 0 and
        identity.group_id != 0;
}

fn writeRestoreMarkerAtomicWithIo(
    alloc: Allocator,
    io: Io,
    root: []const u8,
    path: []const u8,
    raw: []const u8,
) !void {
    if (raw.len > max_restore_marker_bytes) return error.RestoreMarkerTooLarge;
    var entropy: [8]u8 = undefined;
    try io.randomSecure(&entropy);
    const nonce = std.fmt.bytesToHex(entropy, .lower);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{s}", .{ path, &nonce });
    defer alloc.free(tmp_path);
    errdefer if (std.fs.path.isAbsolute(tmp_path))
        std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {}
    else
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var writer_buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &writer_buffer);
        try writer.interface.writeAll(raw);
        try writer.end();
        try file.sync(io);
    }
    if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.renameAbsolute(tmp_path, path, io)
    else
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
    try fs_paths.syncDirPortable(io, root);
}

pub fn readRestoreStateForPathAlloc(alloc: Allocator, path: []const u8) !?RestoreState {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try readRestoreStateForPathAllocWithIo(alloc, io_impl.io(), path);
}

pub fn readRestoreStateForPathAllocWithIo(alloc: Allocator, io: Io, path: []const u8) !?RestoreState {
    const state_path = try restoreStateMarkerPathAlloc(alloc, path);
    defer alloc.free(state_path);

    const raw = std.Io.Dir.cwd().readFileAlloc(io, state_path, alloc, .limited(max_restore_marker_bytes)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    var parsed = std.json.parseFromSlice(RestoreStateDisk, alloc, raw, .{ .allocate = .alloc_always }) catch
        return error.InvalidRestoreState;
    defer parsed.deinit();
    const disk = parsed.value;
    if (disk.format_version != restore_marker_format_version or
        disk.backup_id.len == 0 or
        disk.location.len == 0 or
        !validRestoreArtifactSha256(disk.artifact_sha256) or
        disk.snapshot_path.len == 0 or
        disk.group_id == 0 or
        disk.phase.len == 0)
    {
        return error.InvalidRestoreState;
    }
    return try restoreStateAlloc(
        alloc,
        disk.backup_id,
        disk.location,
        disk.artifact_sha256,
        disk.snapshot_path,
        disk.group_id,
        disk.phase,
        disk.primary_restored,
        disk.runtime_repair_complete,
        disk.last_error,
    );
}

pub fn writeRestoreStateForPath(alloc: Allocator, path: []const u8, state: RestoreState) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try writeRestoreStateForPathWithIo(alloc, io_impl.io(), path, state);
}

pub fn writeRestoreStateForPathWithIo(alloc: Allocator, io: Io, path: []const u8, state: RestoreState) !void {
    if (!validRestoreIdentity(.{
        .backup_id = state.backup_id,
        .location = state.location,
        .artifact_sha256 = state.artifact_sha256,
        .snapshot_path = state.snapshot_path,
        .group_id = state.group_id,
    }) or state.phase.len == 0) return error.InvalidRestoreState;
    try fs_paths.createDirPathPortable(io, path);
    const state_path = try restoreStateMarkerPathAlloc(alloc, path);
    defer alloc.free(state_path);
    const raw = try std.json.Stringify.valueAlloc(alloc, RestoreStateDisk{
        .backup_id = state.backup_id,
        .location = state.location,
        .artifact_sha256 = state.artifact_sha256,
        .snapshot_path = state.snapshot_path,
        .group_id = state.group_id,
        .phase = state.phase,
        .primary_restored = state.primary_restored,
        .runtime_repair_complete = state.runtime_repair_complete,
        .last_error = state.last_error,
    }, .{});
    defer alloc.free(raw);
    try writeRestoreMarkerAtomicWithIo(alloc, io, path, state_path, raw);
}

pub fn readRestoreImportStateAlloc(alloc: Allocator, path: []const u8) !?RestoreImportState {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try readRestoreImportStateAllocWithIo(alloc, io_impl.io(), path);
}

pub fn readRestoreImportStateAllocWithIo(alloc: Allocator, io: Io, path: []const u8) !?RestoreImportState {
    const import_marker_path = try restoreImportMarkerPathAlloc(alloc, path);
    defer alloc.free(import_marker_path);

    const raw = std.Io.Dir.cwd().readFileAlloc(io, import_marker_path, alloc, .limited(max_restore_marker_bytes)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    var parsed = std.json.parseFromSlice(RestoreImportDisk, alloc, raw, .{ .allocate = .alloc_always }) catch
        return error.InvalidRestoreImportMarker;
    defer parsed.deinit();
    const disk = parsed.value;
    if (disk.format_version != restore_marker_format_version or
        disk.snapshot_root.len == 0 or
        disk.backup_id.len == 0 or
        disk.location.len == 0 or
        !validRestoreArtifactSha256(disk.artifact_sha256) or
        disk.snapshot_path.len == 0 or
        disk.group_id == 0)
    {
        return error.InvalidRestoreImportMarker;
    }
    const snapshot_root = try alloc.dupe(u8, disk.snapshot_root);
    errdefer alloc.free(snapshot_root);
    const identity = try restoreStateAlloc(
        alloc,
        disk.backup_id,
        disk.location,
        disk.artifact_sha256,
        disk.snapshot_path,
        disk.group_id,
        "runtime_repair",
        true,
        false,
        "",
    );
    return .{
        .snapshot_root = snapshot_root,
        .identity = identity,
    };
}

pub fn Impl(comptime DB: type) type {
    return struct {
        pub fn getSplitBootstrapMarker(self: *DB, alloc: Allocator) !?range_state_mod.SplitBootstrapMarker {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try self.core.loadSplitBootstrapMarker(alloc);
        }

        pub fn setSplitBootstrapMarker(self: *DB, marker: range_state_mod.SplitBootstrapMarker) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.saveSplitBootstrapMarker(marker);
        }

        pub fn replaceRaftDocumentSnapshot(
            self: *DB,
            alloc: Allocator,
            byte_range: types.ByteRange,
            writes: []const types.BatchWrite,
        ) !void {
            db_internal.lockAtomicWithBackoff(&self.generation_replace_mutex);
            defer self.generation_replace_mutex.unlock();
            if (byte_range.start.len > 0 and byte_range.end.len > 0 and
                std.mem.order(u8, byte_range.start, byte_range.end) != .lt)
            {
                return error.InvalidAppliedDataRange;
            }
            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(alloc);
            try seen.ensureTotalCapacity(alloc, @intCast(writes.len));
            for (writes) |write| {
                if (!byte_range.contains(write.key)) return error.KeyOutOfRange;
                const result = try seen.getOrPut(alloc, write.key);
                if (result.found_existing) return error.DuplicateDocumentKey;
            }

            const lower = try internal_keys.documentRangeLowerAlloc(alloc, "");
            defer alloc.free(lower);
            const upper = (try internal_keys.documentRangeUpperAlloc(alloc, "")) orelse return error.InvalidAppliedDataRange;
            defer alloc.free(upper);
            self.core.lockApplyShared();
            const existing_documents = self.core.scanStoreRange(alloc, lower, upper) catch |err| {
                self.core.unlockApplyShared();
                return err;
            };
            self.core.unlockApplyShared();
            defer docstore_mod.DocStore.freeResults(alloc, existing_documents);

            var deletes = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (deletes.items) |key| alloc.free(key);
                deletes.deinit(alloc);
            }
            for (existing_documents) |entry| {
                const logical_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, entry.key)) orelse continue;
                if (seen.contains(logical_key)) {
                    alloc.free(logical_key);
                    continue;
                }
                errdefer alloc.free(logical_key);
                try deletes.append(alloc, logical_key);
            }

            const range_value = try range_state_mod.encodeRangeAlloc(alloc, byte_range);
            defer alloc.free(range_value);
            const range_write: docstore_mod.KVPair = .{ .key = range_state_mod.range_key, .value = range_value };
            try DB.WritePathCallbacks.batch_internal(self, .{
                .writes = writes,
                .deletes = deletes.items,
                .sync_level = .write,
            }, null, .{
                .validate_range_ownership = false,
                .wait_for_sync_level = false,
                .bypass_ha_write_gate = true,
                .extra_store_writes = &.{range_write},
            });
            try Self.refreshSplitBootstrapRangeInMemory(self, byte_range);
        }

        pub fn appendRaftDocumentSnapshotChunk(
            self: *DB,
            staged_generation: *const generation_lifecycle.StagedGeneration,
            byte_range: types.ByteRange,
            writes: []const types.BatchWrite,
        ) !void {
            try staged_generation.validatePath(self.core.path);
            if (byte_range.start.len > 0 and byte_range.end.len > 0 and
                std.mem.order(u8, byte_range.start, byte_range.end) != .lt)
            {
                return error.InvalidAppliedDataRange;
            }
            for (writes) |write| if (!byte_range.contains(write.key)) return error.KeyOutOfRange;
            try DB.WritePathCallbacks.batch_internal(self, .{
                .writes = writes,
                .sync_level = .write,
            }, null, .{
                .validate_range_ownership = false,
                .wait_for_sync_level = false,
                .bypass_ha_write_gate = true,
            });
        }

        pub fn finishRaftDocumentSnapshot(
            self: *DB,
            staged_generation: *const generation_lifecycle.StagedGeneration,
            byte_range: types.ByteRange,
        ) !void {
            try staged_generation.validatePath(self.core.path);
            const range_value = try range_state_mod.encodeRangeAlloc(self.alloc, byte_range);
            defer self.alloc.free(range_value);
            const range_write: docstore_mod.KVPair = .{ .key = range_state_mod.range_key, .value = range_value };
            try DB.WritePathCallbacks.batch_internal(self, .{ .sync_level = .write }, null, .{
                .validate_range_ownership = false,
                .wait_for_sync_level = false,
                .bypass_ha_write_gate = true,
                .extra_store_writes = &.{range_write},
            });
            try Self.refreshSplitBootstrapRangeInMemory(self, byte_range);
        }

        pub fn replaceSplitBootstrap(
            self: *DB,
            alloc: Allocator,
            byte_range: types.ByteRange,
            writes: []const types.BatchWrite,
            base_delta_sequence: u64,
            marker: range_state_mod.SplitBootstrapMarker,
        ) !bool {
            db_internal.lockAtomicWithBackoff(&self.generation_replace_mutex);
            defer self.generation_replace_mutex.unlock();

            if (marker.transition_id == 0 or marker.attempt_epoch == 0 or
                marker.source_group_id == 0 or marker.destination_group_id == 0 or
                marker.bootstrap_complete)
            {
                return error.InvalidSplitBootstrapMarker;
            }
            if (byte_range.start.len > 0 and byte_range.end.len > 0 and
                std.mem.order(u8, byte_range.start, byte_range.end) != .lt)
            {
                return error.InvalidAppliedDataRange;
            }
            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(alloc);
            try seen.ensureTotalCapacity(alloc, @intCast(writes.len));
            for (writes) |write| {
                if (!byte_range.contains(write.key)) return error.KeyOutOfRange;
                const result = try seen.getOrPut(alloc, write.key);
                if (result.found_existing) return error.DuplicateDocumentKey;
            }

            if (try Self.getSplitBootstrapMarker(self, alloc)) |existing| {
                const same_attempt = existing.transition_id == marker.transition_id and
                    existing.attempt_epoch == marker.attempt_epoch and
                    existing.source_group_id == marker.source_group_id and
                    existing.destination_group_id == marker.destination_group_id;
                if (same_attempt) {
                    const reserved_sequence = try Self.getSplitDeltaFinalSeq(self, alloc);
                    if (base_delta_sequence < reserved_sequence) return error.StaleSplitBootstrap;
                    if (!byteRangesEqual(self.core.byteRange(), byte_range)) return error.ConflictingSplitTransition;
                    if (existing.bootstrap_complete) {
                        if (base_delta_sequence > reserved_sequence) return error.SplitBootstrapComplete;
                        return false;
                    }
                    // Source leadership can change after an earlier worker has
                    // reserved and populated this exact transfer. Replaying begin
                    // must not erase those chunks beneath its pending completion.
                    if (base_delta_sequence == reserved_sequence) return false;
                }
                if (!same_attempt and
                    (existing.source_group_id != marker.source_group_id or
                        existing.destination_group_id != marker.destination_group_id or
                        existing.attempt_epoch >= marker.attempt_epoch))
                {
                    return error.ConflictingSplitTransition;
                }
            }

            const lower = try internal_keys.documentRangeLowerAlloc(alloc, "");
            defer alloc.free(lower);
            const upper = (try internal_keys.documentRangeUpperAlloc(alloc, "")) orelse return error.InvalidAppliedDataRange;
            defer alloc.free(upper);
            self.core.lockApplyShared();
            const existing_documents = self.core.scanStoreRange(alloc, lower, upper) catch |err| {
                self.core.unlockApplyShared();
                return err;
            };
            self.core.unlockApplyShared();
            defer docstore_mod.DocStore.freeResults(alloc, existing_documents);

            var deletes = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (deletes.items) |key| alloc.free(key);
                deletes.deinit(alloc);
            }
            for (existing_documents) |entry| {
                const logical_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, entry.key)) orelse continue;
                if (seen.contains(logical_key)) {
                    alloc.free(logical_key);
                    continue;
                }
                errdefer alloc.free(logical_key);
                try deletes.append(alloc, logical_key);
            }

            const range_value = try range_state_mod.encodeRangeAlloc(alloc, byte_range);
            defer alloc.free(range_value);
            var sequence_buf: [8]u8 = undefined;
            var marker_buf: [4 * @sizeOf(u64) + 1]u8 = undefined;
            const metadata_writes = try range_state_mod.splitBootstrapMetadataWrites(
                range_value,
                base_delta_sequence,
                marker,
                &sequence_buf,
                &marker_buf,
            );
            DB.WritePathCallbacks.batch_internal(self, .{
                .writes = writes,
                .deletes = deletes.items,
                .sync_level = .write,
            }, null, .{
                .validate_range_ownership = false,
                .wait_for_sync_level = false,
                .bypass_ha_write_gate = true,
                .extra_store_writes = &metadata_writes,
            }) catch |err| {
                if (try Self.getSplitBootstrapMarker(self, alloc)) |committed| {
                    if (splitBootstrapMarkersEqual(committed, marker)) {
                        try Self.refreshSplitBootstrapRangeInMemory(self, byte_range);
                        return true;
                    }
                }
                return err;
            };
            try Self.refreshSplitBootstrapRangeInMemory(self, byte_range);
            return true;
        }

        pub fn completeSplitBootstrap(
            self: *DB,
            alloc: Allocator,
            byte_range: types.ByteRange,
            base_delta_sequence: u64,
            marker: range_state_mod.SplitBootstrapMarker,
        ) !bool {
            db_internal.lockAtomicWithBackoff(&self.generation_replace_mutex);
            defer self.generation_replace_mutex.unlock();

            if (marker.transition_id == 0 or marker.attempt_epoch == 0 or
                marker.source_group_id == 0 or marker.destination_group_id == 0 or
                !marker.bootstrap_complete)
            {
                return error.InvalidSplitBootstrapMarker;
            }
            if (byte_range.start.len > 0 and byte_range.end.len > 0 and
                std.mem.order(u8, byte_range.start, byte_range.end) != .lt)
            {
                return error.InvalidAppliedDataRange;
            }

            const existing = (try Self.getSplitBootstrapMarker(self, alloc)) orelse
                return error.SplitBootstrapRequired;
            if (existing.transition_id != marker.transition_id or
                existing.attempt_epoch != marker.attempt_epoch or
                existing.source_group_id != marker.source_group_id or
                existing.destination_group_id != marker.destination_group_id)
            {
                return error.ConflictingSplitTransition;
            }
            const reserved_sequence = try Self.getSplitDeltaFinalSeq(self, alloc);
            if (base_delta_sequence != reserved_sequence) return error.StaleSplitBootstrap;
            if (!byteRangesEqual(self.core.byteRange(), byte_range)) return error.ConflictingSplitTransition;
            if (existing.bootstrap_complete) {
                try Self.refreshSplitBootstrapRangeInMemory(self, byte_range);
                return false;
            }

            const range_value = try range_state_mod.encodeRangeAlloc(alloc, byte_range);
            defer alloc.free(range_value);
            var sequence_buf: [8]u8 = undefined;
            var marker_buf: [4 * @sizeOf(u64) + 1]u8 = undefined;
            const metadata_writes = try range_state_mod.splitBootstrapMetadataWrites(
                range_value,
                base_delta_sequence,
                marker,
                &sequence_buf,
                &marker_buf,
            );
            try DB.WritePathCallbacks.batch_internal(self, .{ .sync_level = .write }, null, .{
                .validate_range_ownership = false,
                .wait_for_sync_level = false,
                .bypass_ha_write_gate = true,
                .extra_store_writes = &metadata_writes,
            });
            try Self.refreshSplitBootstrapRangeInMemory(self, byte_range);
            return true;
        }

        fn refreshSplitBootstrapRangeInMemory(self: *DB, byte_range: types.ByteRange) !void {
            const start = try self.alloc.dupe(u8, byte_range.start);
            errdefer self.alloc.free(start);
            const end = try self.alloc.dupe(u8, byte_range.end);
            self.core.lockApply();
            defer self.core.unlockApply();
            self.core.replaceRangeInMemoryOwned(start, end);
        }

        pub fn clearSplitBootstrapMarker(self: *DB) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.clearSplitBootstrapMarker();
        }

        const Self = @This();

        pub fn updateRangeAfterGate(self: *DB, byte_range: types.ByteRange) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            const start = try self.alloc.dupe(u8, byte_range.start);
            errdefer self.alloc.free(start);
            const end = try self.alloc.dupe(u8, byte_range.end);
            errdefer self.alloc.free(end);
            const owned_range: types.ByteRange = .{ .start = start, .end = end };
            const range_value = try range_state_mod.encodeRangeAlloc(self.alloc, owned_range);
            defer self.alloc.free(range_value);
            const range_write: docstore_mod.KVPair = .{ .key = range_state_mod.range_key, .value = range_value };
            try rebaseRangeCoverageMetadata(
                self.alloc,
                self.core.store,
                self.core.index_manager,
                owned_range,
                &.{range_write},
            );
            self.core.adoptRangeInMemoryOwned(start, end);
        }

        pub fn getRange(self: *DB) types.ByteRange {
            return self.core.byteRange();
        }

        pub fn findMedianKey(self: *DB, alloc: Allocator) ![]u8 {
            const byte_range = Self.getRange(self);
            const lower = try self.core.documentRangeLowerAlloc(byte_range.start);
            defer self.core.alloc.free(lower);
            const upper = if (byte_range.end.len > 0) try self.core.documentRangeUpperAlloc(byte_range.end) else null;
            defer if (upper) |buf| self.core.alloc.free(buf);

            const skip_fn = if (self.relationalColumnsForStore() != null)
                &skipNonRelationalMedianKey
            else
                &skipNonPrimaryMedianKey;
            const internal_key = self.core.findMedianStoreKey(alloc, lower, if (upper) |buf| buf else "", .{
                .skip_fn = skip_fn,
            }) catch |err| switch (err) {
                error.NotFound => return try doc_identity.findMedianDocIdAlloc(alloc, self.core.store, byte_range.start, byte_range.end),
                else => return err,
            };
            defer alloc.free(internal_key);

            return (try internal_keys.decodeStoredDocumentRowKeyAlloc(alloc, internal_key)) orelse error.NotFound;
        }

        fn tryFinalizePrimarySplitFast(self: *DB, split_lower: []const u8) !bool {
            const owner_fast = try self.core.primary_store_owner.rewriteLeftInPlace(split_lower);
            if (owner_fast) return true;

            return try self.core.rewriteLeftStoreInPlace(split_lower);
        }

        fn finalizePrimarySplitPreservingIdentity(
            self: *DB,
            split_lower: []const u8,
            retained_range: types.ByteRange,
        ) !void {
            const range = identityMetadataRange();
            const identity_rows = try self.core.store.scanRange(self.alloc, range.lower[0..], range.upper[0..]);
            defer docstore_mod.DocStore.freeResults(self.alloc, identity_rows);

            _ = try Self.tryFinalizePrimarySplitFast(self, split_lower);
            try putIdentityMetadataRows(self.alloc, self.core.store, identity_rows);
            try rebaseRangeCoverageMetadata(
                self.alloc,
                self.core.store,
                self.core.index_manager,
                retained_range,
                &.{},
            );
        }

        fn splitDestinationStorePlan(self: *DB) SplitDestinationStorePlan {
            return switch (self.primary_backend) {
                .lmdb => .lmdb,
                .lsm => |opts| .{ .lsm = db_config.splitLsmOptions(.{ .lsm = opts }, self.primary_lsm_storage, opts.cache).? },
                .mem, .lsm_memory => .unsupported,
            };
        }

        fn openSplitDestinationStore(self: *DB, dest_dir: []const u8) !OpenedSplitDestinationStore {
            const dest_path_z = try self.alloc.dupeZ(u8, dest_dir);
            defer self.alloc.free(dest_path_z);

            return switch (Self.splitDestinationStorePlan(self)) {
                .lmdb => .{
                    .store = try docstore_mod.DocStore.open(self.alloc, dest_path_z, .{
                        .no_sync = true,
                        .no_meta_sync = true,
                    }),
                },
                .lsm => |split_opts| blk: {
                    var dest_opts = split_opts;
                    dest_opts.background_executor = null;
                    var handle = try lsm_backend_mod.BackendHandle.open(self.alloc, dest_dir, dest_opts);
                    errdefer handle.close();

                    var runtime_store = try handle.backend.runtimeStore(self.alloc, .{ .name = "docs" });
                    errdefer runtime_store.deinit();

                    break :blk .{
                        .store = try docstore_mod.DocStore.openRuntime(self.alloc, runtime_store),
                        .owner = .{ .lsm = .{
                            .handle = handle,
                            .split_options = null,
                        } },
                    };
                },
                .unsupported => error.Unsupported,
            };
        }

        fn tryPreparePrimarySplitFast(self: *DB, split_lower: []const u8, dest_dir: []const u8) !bool {
            const owner_fast = try self.core.primary_store_owner.prepareSplitRightToDir(split_lower, dest_dir);
            if (owner_fast) return true;

            return try self.core.splitRightStoreToDir(split_lower, dest_dir);
        }

        fn collectSplitFrontierDocKeys(alloc: Allocator, store: *docstore_mod.DocStore, lower: []const u8, upper: []const u8, relational_base_rows: bool) ![][]u8 {
            const store_lower = try documentRangeLowerAlloc(alloc, lower);
            defer alloc.free(store_lower);
            const store_upper = if (upper.len > 0) try documentRangeUpperAlloc(alloc, upper) else null;
            defer if (store_upper) |buf| alloc.free(buf);

            const scanned = try store.scanRange(alloc, store_lower, if (store_upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(alloc, scanned);

            var keys = std.ArrayListUnmanaged([]u8).empty;
            errdefer {
                for (keys.items) |key| alloc.free(key);
                keys.deinit(alloc);
            }

            for (scanned) |entry| {
                if (!isBaseDocumentStoreKeyForMode(relational_base_rows, entry.key)) continue;
                const raw = (try internal_keys.decodeStoredDocumentRowKeyAlloc(alloc, entry.key)) orelse continue;
                try keys.append(alloc, raw);
            }

            return try keys.toOwnedSlice(alloc);
        }

        fn relationalBaseRows(self: *DB) bool {
            const schema = self.core.schema orelse return false;
            return schema.storage_mode == .relational;
        }

        fn relationalColumnIndexPolicy(self: *DB) relational_store_mod.ColumnIndexPolicy {
            const schema = self.core.schema orelse return relational_store_mod.ColumnIndexPolicy.empty();
            if (schema.storage_mode != .relational) return relational_store_mod.ColumnIndexPolicy.empty();
            return relational_store_mod.ColumnIndexPolicy.fromSchema(schema);
        }

        fn rebuildRelationalColumnIndexesInStoreRange(
            self: *DB,
            store: *docstore_mod.DocStore,
            start: []const u8,
            end: []const u8,
        ) !void {
            if (!Self.relationalBaseRows(self)) return;
            try relational_store_mod.rebuildAllColumnIndexesFromRowsInRangeWithColumnIndexPolicy(
                self.alloc,
                store,
                start,
                end,
                Self.relationalColumnIndexPolicy(self),
            );
        }

        fn resetManagedIndexAppliedSequencesForRestoreRepair(self: *DB, alloc: Allocator) !void {
            const managed_indexes = try self.core.managedIndexes(alloc);
            defer {
                for (managed_indexes) |index_ref| alloc.free(@constCast(index_ref.name));
                alloc.free(managed_indexes);
            }

            for (managed_indexes) |index_ref| {
                const sequence: u64 = switch (index_ref.kind) {
                    .full_text => try DB.LifecycleCallbacks.probe_derived_replay_target_sequence(
                        self,
                        alloc,
                        self.core.replaySource(),
                        index_ref,
                        0,
                    ),
                    else => 0,
                };
                try self.core.saveAppliedSequence(index_ref.name, sequence);
            }
        }

        fn rebaseManagedIndexAppliedSequencesIfNeeded(self: *DB) !void {
            const managed_indexes = try self.core.managedIndexes(self.alloc);
            defer {
                for (managed_indexes) |index_ref| self.alloc.free(@constCast(index_ref.name));
                self.alloc.free(managed_indexes);
            }

            var max_applied: u64 = 0;
            for (managed_indexes) |index_ref| {
                const applied = try self.core.loadAppliedSequence(self.alloc, index_ref.name);
                max_applied = @max(max_applied, applied);
            }
            if (max_applied == 0) return;

            // Applied watermarks are independent of the retained replay log.
            // Preserve the watermark and advance the floor so future replay
            // sequence allocation cannot reuse already-applied sequence numbers.
            try ensureReplayFloor(self.core.store, max_applied + 1);
        }

        fn refreshManagedIndexWorkersLocked(self: *DB) !void {
            if (!self.start_index_workers) return;

            const managed_indexes = try self.core.managedIndexes(self.alloc);
            defer {
                for (managed_indexes) |index_ref| self.alloc.free(@constCast(index_ref.name));
                self.alloc.free(managed_indexes);
            }

            for (managed_indexes) |index_ref| {
                self.executor.removeWorker(index_ref.name);
            }
            for (managed_indexes) |index_ref| {
                const applied = try self.core.loadAppliedSequence(self.alloc, index_ref.name);
                try self.executor.addWorker(index_ref.name, index_ref, applied);
            }
        }

        fn registerSplitDestinationIndexesDirect(
            alloc: Allocator,
            io: std.Io,
            dest_store: *docstore_mod.DocStore,
            applied_sequence_checkpoint_path: ?[]const u8,
            dest_indexes: *index_manager_mod.IndexManager,
            configs: []const types.IndexConfig,
            applied_sequence: u64,
        ) !void {
            if (configs.len == 0) return;

            try dest_indexes.addAllNoBackfill(dest_store, configs);
            var updates = try alloc.alloc(apply_state.AppliedSequenceUpdate, configs.len);
            defer alloc.free(updates);
            for (configs, 0..) |cfg, i| {
                updates[i] = .{
                    .index_name = cfg.name,
                    .sequence = applied_sequence,
                    .config_hash = types.indexConfigHash(cfg),
                };
            }
            try DB.DerivedAsyncCallbacks.save_dense_projection_metadata_for_applied_sequence_updates(dest_indexes, updates);
            try apply_state.saveAppliedSequencesWithCheckpoint(alloc, io, dest_store, applied_sequence_checkpoint_path, updates);
        }

        fn prepareSplitDestination(self: *DB, byte_range: types.ByteRange, dest_dir: []const u8) !void {
            try ensureDirPath(dest_dir);
            const split_lower = try documentRangeLowerAlloc(self.alloc, byte_range.start);
            defer self.alloc.free(split_lower);

            const page_split_built = try Self.tryPreparePrimarySplitFast(self, split_lower, dest_dir);

            var opened_dest_store = try Self.openSplitDestinationStore(self, dest_dir);
            defer opened_dest_store.close(self.alloc);
            const dest_store = &opened_dest_store.store;
            var dest_indexes = try index_manager_mod.IndexManager.initWithOptions(
                self.alloc,
                dest_dir,
                self.index_backends,
            );
            defer dest_indexes.deinit();
            dest_indexes.setRelationalBaseRows(Self.relationalBaseRows(self));
            dest_indexes.setRelaxedSplitDurability(true);
            const dest_applied_sequence_checkpoint_path = try apply_state.checkpointPathAlloc(self.alloc, dest_dir);
            defer self.alloc.free(dest_applied_sequence_checkpoint_path);
            dest_indexes.setAppliedSequenceCheckpointPath(dest_applied_sequence_checkpoint_path);

            try clearSplitMetadataFromStore(self.alloc, dest_store);
            try clearSystemMetadataFromSplitDestination(self.alloc, dest_store);
            try copyIdentityMetadataToStore(self.alloc, self.core.store, dest_store);
            if (!page_split_built) try copyDerivedCoverageMetadataToStore(self.alloc, self.core.store, dest_store);
            const replay_floor = self.core.nextDerivedAppendSequence();
            try ensureReplayFloor(dest_store, replay_floor);

            const split_doc_frontier = if (page_split_built)
                try Self.collectSplitFrontierDocKeys(self.alloc, dest_store, byte_range.start, byte_range.end, Self.relationalBaseRows(self))
            else
                &.{};
            defer {
                for (split_doc_frontier) |key| self.alloc.free(key);
                if (split_doc_frontier.len > 0) self.alloc.free(split_doc_frontier);
            }

            dest_indexes.updateRange(byte_range);
            try range_state_mod.saveRange(dest_store, byte_range);
            try self.core.saveSchemaCloneTo(dest_store);

            const configs = try self.core.listIndexes(self.alloc);
            defer types.freeIndexConfigs(self.alloc, configs);
            const io = self.backend_runtime.io() orelse return error.MissingBackendRuntimeIo;
            try Self.registerSplitDestinationIndexesDirect(self.alloc, io, dest_store, dest_applied_sequence_checkpoint_path, &dest_indexes, configs, replay_floor -| 1);
            const collect_skip_doc_keys = true;
            var split_handoffs = try self.core.collectSplitIndexHandoffs(
                &dest_indexes,
                dest_store,
                split_doc_frontier,
                byte_range,
                collect_skip_doc_keys,
            );
            defer split_handoffs.deinit(self.alloc);
            _ = try dest_indexes.copyGraphSplitDestinationFrom(self.core.index_manager, byte_range.start, byte_range.end);
            _ = try dest_indexes.rebuildGraphSplitDestination(byte_range.start, byte_range.end);
            if (page_split_built) {
                try applySplitEmbeddingArtifactsInRange(
                    self.alloc,
                    byte_range.start,
                    byte_range.end,
                    dest_store,
                    &dest_indexes,
                    split_handoffs.dense,
                    split_handoffs.sparse,
                );
            }
            if (page_split_built) {
                if (dest_indexes.splitDestinationNeedsDocumentIndexing(split_handoffs.dense, split_handoffs.text, split_handoffs.sparse)) {
                    if (!collect_skip_doc_keys) return error.UnexpectedSplitResidualIndexing;
                    try indexExistingSplitDestinationDirect(
                        self.alloc,
                        byte_range.start,
                        byte_range.end,
                        dest_store,
                        &dest_indexes,
                        split_handoffs.dense,
                        split_handoffs.text,
                        split_handoffs.sparse,
                        Self.relationalColumnIndexPolicy(self),
                    );
                }
            } else {
                try streamRangeIntoSplitDestinationDirect(
                    DB,
                    self.alloc,
                    self,
                    byte_range.start,
                    byte_range.end,
                    dest_store,
                    &dest_indexes,
                    split_handoffs.dense,
                    split_handoffs.text,
                    split_handoffs.sparse,
                    Self.relationalColumnIndexPolicy(self),
                );
            }
            try applySplitGraphArtifactsInRange(
                self.alloc,
                byte_range.start,
                byte_range.end,
                dest_store,
                &dest_indexes,
            );
            try Self.rebuildRelationalColumnIndexesInStoreRange(self, dest_store, byte_range.start, byte_range.end);

            try rebaseRangeCoverageMetadata(self.alloc, dest_store, &dest_indexes, byte_range, &.{});

            try dest_indexes.syncAll(true);
            try dest_store.sync(true);
        }

        pub fn getSplitState(self: *DB, alloc: Allocator) !?types.SplitState {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            const state = self.core.splitState() orelse return null;
            return .{
                .phase = state.phase,
                .split_key = try alloc.dupe(u8, state.split_key),
                .new_shard_id = state.new_shard_id,
                .started_at = state.started_at,
                .original_range_end = try alloc.dupe(u8, state.original_range_end),
            };
        }

        pub fn setSplitState(self: *DB, state: ?types.SplitState) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            if (state == null) {
                try self.core.setSplitState(null);
                return;
            }
            try self.core.setSplitState(.{
                .phase = state.?.phase,
                .split_key = state.?.split_key,
                .new_shard_id = state.?.new_shard_id,
                .started_at = state.?.started_at,
                .original_range_end = state.?.original_range_end,
            });
        }

        pub fn clearSplitState(self: *DB) !void {
            try Self.setSplitState(self, null);
        }

        pub fn getSplitDeltaSeq(self: *DB) u64 {
            return self.core.splitDeltaSequence();
        }

        pub fn getSplitDeltaFinalSeq(self: *DB, alloc: Allocator) !u64 {
            return try self.core.loadSplitDeltaFinalSeq(alloc);
        }

        pub fn setSplitDeltaFinalSeq(self: *DB, seq: u64) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.saveSplitDeltaFinalSeq(seq);
        }

        pub fn clearSplitDeltaFinalSeq(self: *DB) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.clearSplitDeltaFinalSeq();
        }

        pub fn listSplitDeltaEntriesAfter(self: *DB, alloc: Allocator, after_seq: u64) ![]types.SplitDeltaEntry {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            const deltas = try self.core.listSplitDeltasAfter(alloc, after_seq);
            defer shard_mod.freeDeltas(alloc, deltas);

            var entries = std.ArrayListUnmanaged(types.SplitDeltaEntry).empty;
            errdefer {
                for (entries.items) |*entry| entry.deinit(alloc);
                entries.deinit(alloc);
            }

            for (deltas) |delta| {
                var writes = try alloc.alloc(types.BatchWrite, delta.writes.len);
                errdefer alloc.free(writes);
                for (delta.writes, 0..) |write, i| {
                    writes[i] = .{
                        .key = try alloc.dupe(u8, write.key),
                        .value = try alloc.dupe(u8, write.value),
                    };
                }

                var deletes = try alloc.alloc([]u8, delta.deletes.len);
                errdefer {
                    for (deletes) |key| alloc.free(key);
                    alloc.free(deletes);
                }
                for (delta.deletes, 0..) |key, i| {
                    deletes[i] = try alloc.dupe(u8, key);
                }

                try entries.append(alloc, .{
                    .sequence = delta.sequence,
                    .timestamp = delta.timestamp,
                    .writes = writes,
                    .deletes = deletes,
                });
            }

            return try entries.toOwnedSlice(alloc);
        }

        pub fn clearSplitDeltaEntries(self: *DB) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.clearSplitDeltas();
        }

        pub fn createShadowIndexManager(self: *DB, split_key: []const u8, original_range_end: []const u8) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            if (self.shadow != null) return error.ShadowIndexManagerExists;

            const base_path = try std.fmt.allocPrint(self.alloc, "{s}/.shadow-{d}", .{ self.core.path, platform.time.monotonicNs() });
            errdefer self.alloc.free(base_path);
            const indexes_path = try std.fmt.allocPrint(self.alloc, "{s}/indexes", .{base_path});
            errdefer self.alloc.free(indexes_path);

            try ensureDirPath(indexes_path);

            const shadow_manager = try self.alloc.create(index_manager_mod.IndexManager);
            errdefer self.alloc.destroy(shadow_manager);
            shadow_manager.* = try index_manager_mod.IndexManager.initWithOptions(
                self.alloc,
                base_path,
                self.index_backends,
            );
            errdefer shadow_manager.deinit();
            shadow_manager.setRelationalBaseRows(Self.relationalBaseRows(self));

            const shadow_start = try self.alloc.dupe(u8, split_key);
            errdefer self.alloc.free(shadow_start);
            const shadow_end = try self.alloc.dupe(u8, original_range_end);
            errdefer self.alloc.free(shadow_end);
            shadow_manager.updateRange(.{ .start = shadow_start, .end = shadow_end });

            try self.core.registerShadowIndexes(self.alloc, shadow_manager);

            self.shadow = .{
                .manager = shadow_manager,
                .base_path = base_path,
                .indexes_path = indexes_path,
                .range_start = shadow_start,
                .range_end = shadow_end,
            };
        }

        pub fn closeShadowIndexManager(self: *DB) !void {
            const shadow = self.shadow orelse return;
            shadow.manager.deinit();
            self.alloc.destroy(shadow.manager);
            self.alloc.free(shadow.base_path);
            self.alloc.free(shadow.indexes_path);
            self.alloc.free(shadow.range_start);
            self.alloc.free(shadow.range_end);
            self.shadow = null;
        }

        pub fn getShadowIndexDir(self: *DB) []const u8 {
            const shadow = self.shadow orelse return "";
            return shadow.indexes_path;
        }

        pub fn shouldAppendSplitDelta(self: *DB) bool {
            const state = self.core.splitState() orelse return false;
            return state.phase == .splitting;
        }

        pub fn splitShadowRequiresMaterializedDerivedBatch(self: *DB) bool {
            if (self.shadow == null) return false;
            const state = self.core.splitState() orelse return false;
            return state.phase == .splitting;
        }

        pub fn rebuildGraphIndexesForTargetCoverage(self: *DB, alloc: Allocator) !void {
            try applySplitGraphArtifactsInRange(
                alloc,
                "",
                "",
                self.core.store,
                self.core.index_manager,
            );
        }

        pub fn markSplitOffDocumentArtifactChildRangesLocked(
            self: *DB,
            split_state: shard_mod.SplitState,
            split_lower: []const u8,
        ) !void {
            if (split_state.new_shard_id == 0) return;

            const parent_lower = try documentRangeLowerAlloc(self.alloc, self.core.byteRange().start);
            defer self.alloc.free(parent_lower);
            const parent_upper = split_lower;
            const split_upper = if (split_state.original_range_end.len > 0)
                try documentRangeUpperAlloc(self.alloc, split_state.original_range_end)
            else
                null;
            defer if (split_upper) |upper| self.alloc.free(upper);

            const scanned = try self.core.scanStoreRange(self.alloc, parent_lower, parent_upper);
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer writes.deinit(self.alloc);
            var owned_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_values.items) |value| self.alloc.free(value);
                owned_values.deinit(self.alloc);
            }
            var changed_artifact_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (changed_artifact_keys.items) |key| self.alloc.free(key);
                changed_artifact_keys.deinit(self.alloc);
            }

            for (scanned) |entry| {
                if (std.mem.indexOf(u8, entry.value, "\"child_ranges\"") == null) continue;
                var artifact_ref = (try db_internal.decodeArtifactRefIfKnownAlloc(self.alloc, entry.key)) orelse continue;
                defer artifact_ref.deinit(self.alloc);
                if (artifact_ref.kind != .asset or artifact_ref.unit_id != null) continue;

                var arena = std.heap.ArenaAllocator.init(self.alloc);
                defer arena.deinit();
                const arena_alloc = arena.allocator();
                var manifest_value = try std.json.parseFromSliceLeaky(std.json.Value, arena_alloc, entry.value, .{ .allocate = .alloc_always });
                if (manifest_value != .object) continue;
                const child_ranges = manifest_value.object.getPtr("child_ranges") orelse continue;
                if (child_ranges.* != .array) continue;

                var changed = false;
                for (child_ranges.array.items) |*item| {
                    if (item.* != .object) continue;
                    if (!documentArtifactChildRangeMovedBySplit(item.object, split_lower, split_upper)) continue;
                    try putLeakyJsonStringField(arena_alloc, &item.object, "placement", "remote");
                    try putLeakyJsonU64Field(arena_alloc, &item.object, "owner_group_id", split_state.new_shard_id);
                    try putLeakyJsonU64Field(arena_alloc, &item.object, "placement_generation", (try jsonObjectOptionalU64(item.object, "placement_generation") orelse 0) + 1);
                    try putLeakyJsonStringField(arena_alloc, &item.object, "route_status", "remote_committed");
                    try item.object.put(arena_alloc, "split_eligible", .{ .bool = false });
                    changed = true;
                }
                if (!changed) continue;

                const updated_manifest = try std.json.Stringify.valueAlloc(self.alloc, manifest_value, .{});
                errdefer self.alloc.free(updated_manifest);
                try owned_values.append(self.alloc, updated_manifest);
                try writes.append(self.alloc, .{
                    .key = entry.key,
                    .value = updated_manifest,
                });
                try db_internal.appendUniqueOwnedKey(self.alloc, &changed_artifact_keys, entry.key);
            }

            if (writes.items.len == 0) return;
            const sequence = self.core.reserveDerivedAppendSequence();
            var batch_ctx = self.batchContext();
            const replay_payload = try DB.WritePathCallbacks.encode_change_record_payload_context(&batch_ctx, .{
                .sequence = sequence,
                .changed_artifact_keys = changed_artifact_keys.items,
            }, sequence);
            defer self.alloc.free(replay_payload);
            try self.core.store.putBatchWithReplay(self.backend_runtime.io(), writes.items, &.{}, .{
                .sequence = sequence,
                .payload = replay_payload,
            });
            DB.WritePathCallbacks.mirror_ha_replay_payload_best_effort(self, replay_payload);
            if (DB.WritePathCallbacks.should_append_split_delta(self)) {
                try self.core.appendSplitDelta(DB.WritePathCallbacks.current_time_ns(), writes.items, &.{});
            }
            self.executor.trackBacklogBytes(sequence, @intCast(replay_payload.len)) catch {};
        }

        fn documentArtifactChildRangeMovedBySplit(
            object: std.json.ObjectMap,
            split_lower: []const u8,
            split_upper: ?[]const u8,
        ) bool {
            const split_eligible = blk: {
                const value = object.get("split_eligible") orelse break :blk false;
                if (value != .bool) return false;
                break :blk value.bool;
            };
            if (!split_eligible) return false;

            const placement = object.get("placement") orelse return false;
            if (placement != .string) return false;
            if (std.mem.eql(u8, placement.string, "remote")) return false;

            if (object.get("owner_group_id")) |owner| {
                if (owner == .integer and owner.integer > 0) return false;
            }

            const route_status = object.get("route_status") orelse null;
            if (route_status) |status| {
                if (status != .string) return false;
                if (std.mem.eql(u8, status.string, "remote_committed")) return false;
            }

            const start_key = object.get("start_key") orelse return false;
            if (start_key != .string) return false;
            if (std.mem.order(u8, start_key.string, split_lower) == .lt) return false;
            const end_key = object.get("end_key_exclusive") orelse return false;
            if (end_key != .string) return false;
            if (split_upper) |upper| {
                if (end_key.string.len == 0) return false;
                if (std.mem.order(u8, end_key.string, upper) == .gt) return false;
            }
            return true;
        }

        pub fn split(
            self: *DB,
            curr_range: types.ByteRange,
            split_key: []const u8,
            dest_dir1: []const u8,
            dest_dir2: []const u8,
            prepare_only: bool,
        ) !void {
            _ = dest_dir1;
            while (true) {
                const target_sequence = self.core.nextDerivedSequence();
                try self.runMaintenanceUntil(target_sequence, .{});

                self.core.lockApply();
                if (self.core.nextDerivedSequence() == target_sequence) break;
                self.core.unlockApply();
            }
            defer self.core.unlockApply();

            if (!byteRangesEqual(self.core.byteRange(), curr_range)) return error.KeyOutOfRange;
            if (!curr_range.contains(split_key)) return error.KeyOutOfRange;

            const range2 = types.ByteRange{
                .start = split_key,
                .end = curr_range.end,
            };

            if (self.core.splitState() == null) {
                try self.core.prepareSplit(split_key);
            }

            try Self.prepareSplitDestination(self, range2, dest_dir2);
            if (prepare_only) return;

            try Self.finalizeSplitLocked(self, .{
                .start = curr_range.start,
                .end = split_key,
            });
        }

        pub fn finalizeSplit(self: *DB, new_range: types.ByteRange) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            try Self.finalizeSplitLocked(self, new_range);
        }

        pub fn finalizeSplitLocked(self: *DB, new_range: types.ByteRange) !void {
            const split_state = self.core.splitState() orelse return error.SplitInProgress;
            if (!std.mem.eql(u8, split_state.split_key, new_range.end)) return error.KeyOutOfRange;
            const replay_floor = self.core.nextDerivedAppendSequence();

            const split_lower = try documentRangeLowerAlloc(self.alloc, split_state.split_key);
            defer self.alloc.free(split_lower);
            try Self.finalizePrimarySplitPreservingIdentity(self, split_lower, new_range);
            try Self.rebuildRelationalColumnIndexesInStoreRange(self, self.core.store, new_range.start, new_range.end);
            try self.core.store.ensureReplayNextSequenceAtLeast(replay_floor);
            try self.core.pruneSplitRangeFromPrimaryIndexes(split_state.split_key, split_state.original_range_end);
            try Self.rebaseManagedIndexAppliedSequencesIfNeeded(self);
            try Self.markSplitOffDocumentArtifactChildRangesLocked(self, split_state, split_lower);

            if (split_state.phase == .prepare) {
                try self.core.completeSplitTransition(split_state.new_shard_id, split_state.split_key);
            } else if (split_state.phase != .splitting and split_state.phase != .finalizing) {
                return error.SplitInProgress;
            }

            try self.core.finalizeSplitState();
            try Self.refreshManagedIndexWorkersLocked(self);
            try Self.closeShadowIndexManager(self);
            try Self.refreshManagedIndexWorkersLocked(self);
        }

        pub fn snapshot(self: *DB, id: []const u8) !u64 {
            try DB.LifecycleCallbacks.prepare_snapshot(self);

            self.core.lockApply();
            defer self.core.unlockApply();

            try self.core.syncStore(true);
            try self.core.index_manager.syncAll(true);

            const snapshot_root = try std.fmt.allocPrint(self.alloc, "{s}.snapshots/{s}", .{ self.core.path, id });
            defer self.alloc.free(snapshot_root);
            try ensureDirPath(snapshot_root);

            return try self.core.writeSnapshot(snapshot_root);
        }

        pub fn restoreSnapshotStoreTo(
            alloc: Allocator,
            snapshot_root: []const u8,
            path: []const u8,
            opts: anytype,
            restore_identity: ?RestoreIdentity,
            restore_io: ?Io,
        ) !void {
            const shared_io = restore_io orelse if (opts.backend_runtime) |runtime| runtime.io() else null;
            if (restore_identity) |identity| {
                if (shared_io) |io|
                    try beginRestoreImportWithIo(alloc, io, path, snapshot_root, identity)
                else
                    try beginRestoreImport(alloc, path, snapshot_root, identity);
            }
            var opened_primary = try db_internal.openPrimaryStore(alloc, path, .{
                .map_size = opts.map_size,
                .no_sync = opts.no_sync,
                .primary_backend = opts.primary_backend,
                .primary_runtime_store = opts.primary_runtime_store,
                .storage = opts.storage,
                .index_backends = opts.index_backends,
            });
            defer {
                opened_primary.store.close();
                opened_primary.owner.close(alloc);
            }

            try db_core.clearAllKeysFromStore(alloc, &opened_primary.store);
            try db_core.importStoreSnapshot(alloc, &opened_primary.store, snapshot_root);
            try doc_identity.validateStoreAlloc(alloc, &opened_primary.store);
            try validateRestoredIdentityNamespace(&opened_primary.store, opts);
            try db_core.importChangeJournalSnapshot(alloc, &opened_primary.store, snapshot_root);
            if (opts.physical_root_mode == .filesystem_managed) {
                _ = if (shared_io) |io|
                    try root_identity.loadOrCreate(alloc, io, path)
                else
                    try loadOrCreateDurableRootIdentity(alloc, opts.backend_runtime, path);
            }
            if (restore_identity) |identity| {
                if (shared_io) |io|
                    try markRestorePrimaryRestoredForPathWithArtifactWithIo(
                        alloc,
                        io,
                        path,
                        identity.backup_id,
                        identity.location,
                        identity.artifact_sha256,
                        identity.snapshot_path,
                        identity.group_id,
                    )
                else
                    try markRestorePrimaryRestoredForPathWithArtifact(
                        alloc,
                        path,
                        identity.backup_id,
                        identity.location,
                        identity.artifact_sha256,
                        identity.snapshot_path,
                        identity.group_id,
                    );
            }
        }

        fn validateRestoredIdentityNamespace(store: *docstore_mod.DocStore, opts: anytype) !void {
            if (opts.identity_namespace == null or opts.prefer_existing_identity_namespace) return;
            const stored = (try doc_identity.loadNamespaceFromStore(store)) orelse return;
            if (!stored.eql(opts.identity_namespace.?)) return error.IdentityNamespaceMismatch;
        }

        pub fn restoreSnapshotTo(alloc: Allocator, snapshot_root: []const u8, path: []const u8, opts: anytype) !void {
            try restoreSnapshotStoreTo(alloc, snapshot_root, path, opts, null, null);
            // Graph reverse indexes are derived from stored outgoing edge keys,
            // so restore them after the logical store and derived log are rehydrated.
            var restored = try DB.open(alloc, path, opts);
            defer restored.close();
            _ = try restored.rebuildDenseIndexesForTargetCoverage(alloc);
            _ = try restored.rebuildSparseIndexesForTargetCoverage(alloc);
            try restored.rebuildGraphIndexesForTargetCoverage(alloc);
            try restored.core.index_manager.syncAll(true);
        }

        pub fn restoreSnapshotToStagedGeneration(
            staged_generation: *const generation_lifecycle.StagedGeneration,
            alloc: Allocator,
            snapshot_root: []const u8,
            path: []const u8,
            opts: anytype,
        ) !void {
            try staged_generation.validatePath(path);
            var staged_opts = opts;
            staged_opts.staged_generation = staged_generation;
            try restoreSnapshotTo(alloc, snapshot_root, path, staged_opts);
        }

        pub fn restoreSnapshotToDeferredRuntimeRepair(
            staged_generation: *const generation_lifecycle.StagedGeneration,
            alloc: Allocator,
            snapshot_root: []const u8,
            path: []const u8,
            opts: anytype,
            identity: RestoreIdentity,
        ) !void {
            try staged_generation.validatePath(path);
            try restoreSnapshotStoreTo(alloc, snapshot_root, path, opts, identity, null);
        }

        pub fn restoreSnapshotToDeferredRuntimeRepairWithIo(
            staged_generation: *const generation_lifecycle.StagedGeneration,
            alloc: Allocator,
            io: Io,
            snapshot_root: []const u8,
            path: []const u8,
            opts: anytype,
            identity: RestoreIdentity,
        ) !void {
            try staged_generation.validatePath(path);
            try restoreSnapshotStoreTo(alloc, snapshot_root, path, opts, identity, io);
        }

        pub fn beginRestoreImport(alloc: Allocator, path: []const u8, snapshot_root: []const u8, identity: RestoreIdentity) !void {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            return try beginRestoreImportWithIo(alloc, io_impl.io(), path, snapshot_root, identity);
        }

        pub fn beginRestoreImportWithIo(alloc: Allocator, io: Io, path: []const u8, snapshot_root: []const u8, identity: RestoreIdentity) !void {
            if (snapshot_root.len == 0 or !validRestoreIdentity(identity))
                return error.InvalidRestoreImportMarker;
            try fs_paths.createDirPathPortable(io, path);
            const restore_intent_path = try restoreIntentMarkerPathAlloc(alloc, path);
            defer alloc.free(restore_intent_path);
            const restore_state_path = try restoreStateMarkerPathAlloc(alloc, path);
            defer alloc.free(restore_state_path);
            const import_marker_path = try restoreImportMarkerPathAlloc(alloc, path);
            defer alloc.free(import_marker_path);

            try deleteFileIfExists(io, restore_intent_path);
            try deleteFileIfExists(io, restore_state_path);
            const marker = try std.json.Stringify.valueAlloc(alloc, RestoreImportDisk{
                .snapshot_root = snapshot_root,
                .backup_id = identity.backup_id,
                .location = identity.location,
                .artifact_sha256 = identity.artifact_sha256,
                .snapshot_path = identity.snapshot_path,
                .group_id = identity.group_id,
            }, .{});
            defer alloc.free(marker);
            try writeRestoreMarkerAtomicWithIo(alloc, io, path, import_marker_path, marker);
        }

        pub fn recoverIncompleteRestoreImportIfNeeded(alloc: Allocator, path: []const u8, opts: anytype) !bool {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            return try recoverIncompleteRestoreImportIfNeededWithIo(alloc, io_impl.io(), path, opts);
        }

        pub fn recoverIncompleteRestoreImportIfNeededWithIo(alloc: Allocator, io: Io, path: []const u8, opts: anytype) !bool {
            if (try readRestoreStateForPathAllocWithIo(alloc, io, path)) |state_value| {
                var state = state_value;
                state.deinit(alloc);
                return false;
            }

            var import_state = (try readRestoreImportStateAllocWithIo(alloc, io, path)) orelse return false;
            defer import_state.deinit(alloc);
            const identity_state = import_state.identity;
            const identity: RestoreIdentity = .{
                .backup_id = identity_state.backup_id,
                .location = identity_state.location,
                .artifact_sha256 = identity_state.artifact_sha256,
                .snapshot_path = identity_state.snapshot_path,
                .group_id = identity_state.group_id,
            };
            std.log.warn("recovering incomplete restore import phase=startup", .{});
            try restoreSnapshotStoreTo(alloc, import_state.snapshot_root, path, opts, identity, io);
            return true;
        }

        pub fn readRestoreStateForPath(alloc: Allocator, path: []const u8) !?RestoreState {
            return try readRestoreStateForPathAlloc(alloc, path);
        }

        pub fn readRestoreStateForPathWithIo(alloc: Allocator, io: Io, path: []const u8) !?RestoreState {
            return try readRestoreStateForPathAllocWithIo(alloc, io, path);
        }

        pub fn markRestorePrimaryRestoredForPathWithArtifact(
            alloc: Allocator,
            path: []const u8,
            backup_id: []const u8,
            location: []const u8,
            artifact_sha256: []const u8,
            snapshot_path: []const u8,
            group_id: u64,
        ) !void {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            return try markRestorePrimaryRestoredForPathWithArtifactWithIo(
                alloc,
                io_impl.io(),
                path,
                backup_id,
                location,
                artifact_sha256,
                snapshot_path,
                group_id,
            );
        }

        pub fn markRestorePrimaryRestoredForPathWithArtifactWithIo(
            alloc: Allocator,
            io: Io,
            path: []const u8,
            backup_id: []const u8,
            location: []const u8,
            artifact_sha256: []const u8,
            snapshot_path: []const u8,
            group_id: u64,
        ) !void {
            var state = try restoreStateAlloc(alloc, backup_id, location, artifact_sha256, snapshot_path, group_id, "runtime_repair", true, false, "");
            defer state.deinit(alloc);
            try writeRestoreStateForPathWithIo(alloc, io, path, state);
            const import_marker_path = try restoreImportMarkerPathAlloc(alloc, path);
            defer alloc.free(import_marker_path);
            try deleteFileIfExists(io, import_marker_path);
        }

        pub fn markRestoreCompleteForPath(
            alloc: Allocator,
            path: []const u8,
            backup_id: []const u8,
            location: []const u8,
            artifact_sha256: []const u8,
            snapshot_path: []const u8,
            group_id: u64,
        ) !void {
            var state = try restoreStateAlloc(alloc, backup_id, location, artifact_sha256, snapshot_path, group_id, "complete", true, true, "");
            defer state.deinit(alloc);
            try writeRestoreStateForPath(alloc, path, state);
            const repair_marker_path = try restoreRepairMarkerPathAlloc(alloc, path);
            defer alloc.free(repair_marker_path);
            var io_impl = threadedIo();
            defer io_impl.deinit();
            try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
                .sub_path = repair_marker_path,
                .data = "done\n",
            });
        }

        pub fn restoreRuntimeRepairNeededForPath(alloc: Allocator, path: []const u8) !bool {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            return try restoreRuntimeRepairNeededForPathWithIo(alloc, io_impl.io(), path);
        }

        pub fn restoreRuntimeRepairNeededForPathWithIo(alloc: Allocator, io: Io, path: []const u8) !bool {
            var state = (try readRestoreStateForPathAllocWithIo(alloc, io, path)) orelse return false;
            defer state.deinit(alloc);
            return state.primary_restored and !state.runtime_repair_complete;
        }

        pub fn markRestoreRuntimeRepairNeeded(alloc: Allocator, path: []const u8) !void {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            return try markRestoreRuntimeRepairNeededWithIo(alloc, io_impl.io(), path);
        }

        pub fn markRestoreRuntimeRepairNeededWithIo(alloc: Allocator, io: Io, path: []const u8) !void {
            const repair_marker_path = try restoreRepairMarkerPathAlloc(alloc, path);
            defer alloc.free(repair_marker_path);
            const import_marker_path = try restoreImportMarkerPathAlloc(alloc, path);
            defer alloc.free(import_marker_path);
            try deleteFileIfExists(io, repair_marker_path);

            var state = (try readRestoreStateForPathAllocWithIo(alloc, io, path)) orelse return error.InvalidRestoreState;
            defer state.deinit(alloc);
            const new_phase = try alloc.dupe(u8, "runtime_repair");
            alloc.free(state.phase);
            state.phase = new_phase;
            state.primary_restored = true;
            state.runtime_repair_complete = false;
            try writeRestoreStateForPathWithIo(alloc, io, path, state);
            try deleteFileIfExists(io, import_marker_path);
        }

        pub fn markRestoreRuntimeRepairCompleteWithIo(alloc: Allocator, io: Io, path: []const u8) !void {
            var state = (try readRestoreStateForPathAllocWithIo(alloc, io, path)) orelse return error.InvalidRestoreState;
            defer state.deinit(alloc);
            const new_phase = try alloc.dupe(u8, "complete");
            alloc.free(state.phase);
            state.phase = new_phase;
            state.primary_restored = true;
            state.runtime_repair_complete = true;
            try writeRestoreStateForPathWithIo(alloc, io, path, state);
            const repair_marker_path = try restoreRepairMarkerPathAlloc(alloc, path);
            defer alloc.free(repair_marker_path);
            try writeRestoreMarkerAtomicWithIo(alloc, io, path, repair_marker_path, "done\n");
        }

        pub fn restoreRuntimeRepairNeeded(self: *DB) !bool {
            if (self.backend_runtime.io()) |io| {
                return try restoreRuntimeRepairNeededForPathWithIo(self.alloc, io, self.core.path);
            }
            return try restoreRuntimeRepairNeededForPath(self.alloc, self.core.path);
        }

        fn updateRestoreRuntimeRepairPhaseWithIo(self: *DB, alloc: Allocator, io: Io, phase: []const u8, complete: bool) !void {
            var state = (try readRestoreStateForPathAllocWithIo(alloc, io, self.core.path)) orelse return error.InvalidRestoreState;
            defer state.deinit(alloc);
            const new_phase = try alloc.dupe(u8, phase);
            alloc.free(state.phase);
            state.phase = new_phase;
            state.primary_restored = true;
            state.runtime_repair_complete = complete;
            const new_last_error = try alloc.dupe(u8, "");
            alloc.free(state.last_error);
            state.last_error = new_last_error;
            try writeRestoreStateForPathWithIo(alloc, io, self.core.path, state);
        }

        pub fn repairRestoreRuntimeStateStepIfNeeded(self: *DB, alloc: Allocator) !bool {
            if (self.backend_runtime.io()) |io| {
                return try Self.repairRestoreRuntimeStateStepIfNeededWithIo(self, alloc, io);
            }
            var io_impl = threadedIo();
            defer io_impl.deinit();
            return try Self.repairRestoreRuntimeStateStepIfNeededWithIo(self, alloc, io_impl.io());
        }

        pub fn repairRestoreRuntimeStateStepIfNeededWithIo(self: *DB, alloc: Allocator, io: Io) !bool {
            var state = (try readRestoreStateForPathAllocWithIo(alloc, io, self.core.path)) orelse return false;
            defer state.deinit(alloc);
            if (!state.primary_restored or state.runtime_repair_complete) return false;
            const phase = state.phase;

            if (std.mem.eql(u8, phase, "runtime_repair") or std.mem.eql(u8, phase, "reset_watermarks")) {
                std.log.info("restore runtime repair reset managed index watermarks path={s}", .{self.core.path});
                try Self.resetManagedIndexAppliedSequencesForRestoreRepair(self, alloc);
                try Self.refreshManagedIndexWorkersLocked(self);
                try Self.updateRestoreRuntimeRepairPhaseWithIo(self, alloc, io, "rebuild_graph", false);
                return true;
            }
            if (std.mem.eql(u8, phase, "rebuild_graph")) {
                std.log.info("restore runtime repair rebuild graph state path={s}", .{self.core.path});
                _ = try Self.rebuildGraphDerivedState(self);
                try Self.updateRestoreRuntimeRepairPhaseWithIo(self, alloc, io, "rebuild_artifacts", false);
                return true;
            }
            if (std.mem.eql(u8, phase, "rebuild_artifacts")) {
                std.log.info("restore runtime repair rebuild stored embedding artifacts path={s}", .{self.core.path});
                _ = try self.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
                _ = try self.rebuildSparseIndexesForTargetCoverage(alloc);
                _ = try Self.rebuildSparseIndexesFromPrimaryDocsForRestore(self, alloc);
                try Self.updateRestoreRuntimeRepairPhaseWithIo(self, alloc, io, "replay_enrichments", false);
                return true;
            }
            if (std.mem.eql(u8, phase, "replay_enrichments")) {
                if (self.core.hasGeneratedEnrichmentTargets()) {
                    if (self.enrichment_runtime == null) return error.RestoreRepairRequiresEnrichmentRuntime;
                    std.log.info("restore runtime repair replay generated enrichments path={s}", .{self.core.path});
                    _ = try self.replayGeneratedEnrichmentsFromStoredDocs(alloc);
                }
                try Self.updateRestoreRuntimeRepairPhaseWithIo(self, alloc, io, "drain_async", false);
                return true;
            }
            if (std.mem.eql(u8, phase, "drain_async")) {
                std.log.info("restore runtime repair drain async work path={s}", .{self.core.path});
                // Earlier repair phases synchronously rebuild restored runtime state.
                // Publish replay-driven query state before the final index
                // sync/complete marker, but preserve the replay journal until
                // restore repair has durably crossed that boundary.
                try DB.LifecycleCallbacks.drain_replay_stages_until_stable_without_truncation(self);
                try DB.LifecycleCallbacks.flush_applied_sequences_for_idle(self);
                try Self.updateRestoreRuntimeRepairPhaseWithIo(self, alloc, io, "rebuild_replayed_artifacts", false);
                return true;
            }
            if (std.mem.eql(u8, phase, "rebuild_replayed_artifacts")) {
                std.log.info("restore runtime repair rebuild replayed embedding artifacts path={s}", .{self.core.path});
                _ = try self.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
                _ = try self.rebuildSparseIndexesForTargetCoverage(alloc);
                try Self.updateRestoreRuntimeRepairPhaseWithIo(self, alloc, io, "sync_indexes", false);
                return true;
            }
            if (std.mem.eql(u8, phase, "sync_indexes")) {
                std.log.info("restore runtime repair sync indexes path={s}", .{self.core.path});
                _ = try self.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
                if (try self.hasPendingDenseArtifactRebuild(alloc) or
                    try DB.LifecycleCallbacks.dense_artifact_watermark_repair_needed(self, alloc))
                {
                    return error.RestoreRuntimeRepairIncomplete;
                }
                try DB.LifecycleCallbacks.save_all_live_index_status_snapshots(self, alloc);
                try self.core.index_manager.syncAll(true);
                try self.core.syncStore(true);
                try markRestoreRuntimeRepairCompleteWithIo(alloc, io, self.core.path);
                std.log.info("restore runtime repair marked complete path={s}", .{self.core.path});
                return true;
            }
            return error.InvalidRestoreState;
        }

        fn rebuildSparseIndexesFromPrimaryDocsForRestore(self: *DB, alloc: Allocator) !usize {
            if (self.core.index_manager.sparse_indexes.items.len == 0) return 0;

            const lower = try self.core.documentRangeLowerAlloc(self.getRange().start);
            defer self.core.alloc.free(lower);
            const upper = if (self.getRange().end.len > 0) try self.core.documentRangeUpperAlloc(self.getRange().end) else null;
            defer if (upper) |buf| self.core.alloc.free(buf);

            const pairs = try self.core.store.scanRange(alloc, lower, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(alloc, pairs);

            var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            defer writes.deinit(alloc);
            const relational_base_rows = self.relationalColumnsForStore() != null;
            for (pairs) |kv| {
                if (!db_internal.isBaseDocumentStoreKeyForMode(relational_base_rows, kv.key)) continue;
                try writes.append(alloc, .{
                    .key = kv.key,
                    .value = kv.value,
                });
            }
            if (writes.items.len == 0) return 0;

            var rebuilt: usize = 0;
            self.core.lockApply();
            defer self.core.unlockApply();
            for (self.core.index_manager.sparse_indexes.items) |*entry| {
                try self.core.index_manager.indexSparseBatchByName(self.core.store, entry.config.name, writes.items);
                rebuilt += writes.items.len;
            }
            return rebuilt;
        }

        pub fn repairRestoreRuntimeStateIfNeeded(self: *DB, alloc: Allocator) !bool {
            if (self.backend_runtime.io()) |io| {
                return try Self.repairRestoreRuntimeStateIfNeededWithIo(self, alloc, io);
            }
            var io_impl = threadedIo();
            defer io_impl.deinit();
            return try Self.repairRestoreRuntimeStateIfNeededWithIo(self, alloc, io_impl.io());
        }

        pub fn repairRestoreRuntimeStateIfNeededWithIo(self: *DB, alloc: Allocator, io: Io) !bool {
            var repaired = false;
            while (try Self.repairRestoreRuntimeStateStepIfNeededWithIo(self, alloc, io)) {
                repaired = true;
            }
            return repaired;
        }

        pub fn rebuildGraphDerivedState(self: *DB) !usize {
            self.core.lockApply();
            defer self.core.unlockApply();
            try applySplitGraphArtifactsInRange(
                self.alloc,
                self.getRange().start,
                self.getRange().end,
                self.core.store,
                self.core.index_manager,
            );
            return try self.core.index_manager.rebuildGraphSplitDestination(
                self.getRange().start,
                self.getRange().end,
            );
        }
    };
}
