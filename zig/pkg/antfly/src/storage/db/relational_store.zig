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

//! Relational base-store facade.
//!
//! Relational rows live in their own document-scoped keyspace and are the base
//! document record for relational tables. The implementation still uses the same
//! DocStore batch transaction underneath, so writes commit atomically with the
//! rest of the DB batch while callers use a participant-shaped interface.

const std = @import("std");
const Allocator = std.mem.Allocator;

const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const typed_dv = @import("../../section/typed_doc_values.zig");
const transactions_mod = @import("../transactions.zig");

pub const OwnedRow = struct {
    doc_key: []u8,
    row_value: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.row_value);
        self.* = undefined;
    }
};

pub const OwnedColumnValue = struct {
    doc_key: []u8,
    value_type: typed_dv.ValueType,
    is_json: bool,
    value: typed_dv.TypedValue,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        if (self.value_type == .bytes_val) alloc.free(self.value.bytes_val);
        self.* = undefined;
    }
};

pub fn rowKeyAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    return try internal_keys.relationalRowKeyAlloc(alloc, doc_key);
}

pub const WriteParticipant = struct {
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    writes_start: usize,
    deletes_start: usize,
    owned_keys_start: usize,
    owned_values_start: usize,
    prepared: bool = false,
    closed: bool = false,

    pub fn init(
        alloc: Allocator,
        store: *docstore_mod.DocStore,
        writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
        deletes: *std.ArrayListUnmanaged([]const u8),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
    ) WriteParticipant {
        return .{
            .alloc = alloc,
            .store = store,
            .writes = writes,
            .deletes = deletes,
            .owned_keys = owned_keys,
            .owned_values = owned_values,
            .writes_start = writes.items.len,
            .deletes_start = deletes.items.len,
            .owned_keys_start = owned_keys.items.len,
            .owned_values_start = owned_values.items.len,
        };
    }

    pub fn prepareUpsert(
        self: *WriteParticipant,
        table: []const u8,
        doc_key: []const u8,
        typed_row: []const u8,
        txn_id: ?transactions_mod.TxnId,
    ) !void {
        _ = table;
        _ = txn_id;
        if (self.closed) return error.ParticipantClosed;
        try appendUpsert(self.alloc, self.store, self.writes, self.deletes, self.owned_keys, self.owned_values, doc_key, typed_row);
        self.prepared = true;
    }

    pub fn prepareDelete(
        self: *WriteParticipant,
        table: []const u8,
        doc_key: []const u8,
        txn_id: ?transactions_mod.TxnId,
    ) !void {
        _ = table;
        _ = txn_id;
        if (self.closed) return error.ParticipantClosed;
        try appendDelete(self.alloc, self.store, self.deletes, self.owned_keys, doc_key);
        self.prepared = true;
    }

    pub fn commit(self: *WriteParticipant, txn_id: ?transactions_mod.TxnId, commit_version: u64) !void {
        _ = txn_id;
        _ = commit_version;
        if (self.closed) return error.ParticipantClosed;
        self.closed = true;
    }

    pub fn abort(self: *WriteParticipant, txn_id: ?transactions_mod.TxnId) void {
        _ = txn_id;
        if (!self.closed) {
            for (self.owned_keys.items[self.owned_keys_start..]) |key| self.alloc.free(key);
            for (self.owned_values.items[self.owned_values_start..]) |value| self.alloc.free(value);
            self.owned_keys.shrinkRetainingCapacity(self.owned_keys_start);
            self.owned_values.shrinkRetainingCapacity(self.owned_values_start);
            self.writes.shrinkRetainingCapacity(self.writes_start);
            self.deletes.shrinkRetainingCapacity(self.deletes_start);
        }
        self.closed = true;
    }

    pub fn get(self: *WriteParticipant, doc_key: []const u8, read_version: ?u64) !?[]u8 {
        _ = read_version;
        return try getMaterializedAlloc(self.alloc, self.store, doc_key);
    }

    pub fn scanRows(self: *WriteParticipant, lower_doc_key: []const u8, upper_doc_key: []const u8, read_version: ?u64) ![]OwnedRow {
        _ = read_version;
        return try scanRowsAlloc(self.alloc, self.store, lower_doc_key, upper_doc_key);
    }

    pub fn scanColumn(self: *WriteParticipant, column_path: []const u8, lower_doc_key: []const u8, upper_doc_key: []const u8, read_version: ?u64) ![]OwnedColumnValue {
        _ = read_version;
        return try scanColumnAlloc(self.alloc, self.store, column_path, lower_doc_key, upper_doc_key);
    }
};

pub fn appendUpsert(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
) !void {
    try appendUpsertInternal(alloc, store, writes, deletes, owned_keys, owned_values, doc_key, row_value);
}

pub fn appendDelete(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
) !void {
    try appendDeleteInternal(alloc, store, deletes, owned_keys, doc_key);
}

pub fn appendUpsertOwnedBatch(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    doc_key: []const u8,
    row_value: []const u8,
) !void {
    try appendUpsertInternal(alloc, store, writes, deletes, null, null, doc_key, row_value);
}

pub fn appendDeleteOwnedBatch(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    doc_key: []const u8,
) !void {
    try appendDeleteInternal(alloc, store, deletes, null, doc_key);
}

pub fn getRawAlloc(alloc: Allocator, store: *docstore_mod.DocStore, doc_key: []const u8) !?[]u8 {
    const key = try rowKeyAlloc(alloc, doc_key);
    defer alloc.free(key);
    return store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

pub fn getMaterializedAlloc(alloc: Allocator, store: *docstore_mod.DocStore, doc_key: []const u8) !?[]u8 {
    const raw = try getRawAlloc(alloc, store, doc_key) orelse return null;
    defer alloc.free(raw);
    return try relational_row_codec.reconstructValueAlloc(alloc, raw);
}

pub fn freeRows(alloc: Allocator, rows: []OwnedRow) void {
    for (rows) |*row| row.deinit(alloc);
    alloc.free(rows);
}

pub fn freeColumnValues(alloc: Allocator, values: []OwnedColumnValue) void {
    for (values) |*value| value.deinit(alloc);
    alloc.free(values);
}

pub fn scanRowsAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]OwnedRow {
    const lower = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower);
    const upper = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var out = std.ArrayListUnmanaged(OwnedRow).empty;
    errdefer {
        for (out.items) |*row| row.deinit(alloc);
        out.deinit(alloc);
    }

    for (scanned) |entry| {
        if (!internal_keys.isRelationalRowKey(entry.key)) continue;
        const doc_key = (try internal_keys.decodeRelationalRowKeyAlloc(alloc, entry.key)) orelse continue;
        errdefer alloc.free(doc_key);
        const row_value = try alloc.dupe(u8, entry.value);
        errdefer alloc.free(row_value);
        try out.append(alloc, .{
            .doc_key = doc_key,
            .row_value = row_value,
        });
    }

    return try out.toOwnedSlice(alloc);
}

pub fn scanColumnAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    column_path: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]OwnedColumnValue {
    const lower = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower);
    const upper = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var out = std.ArrayListUnmanaged(OwnedColumnValue).empty;
    errdefer {
        for (out.items) |*value| value.deinit(alloc);
        out.deinit(alloc);
    }

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalColumnKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.column_path, column_path)) continue;
        const cell = (try relational_row_codec.findCellByPath(entry.value, column_path)) orelse continue;
        const doc_key = try alloc.dupe(u8, decoded.doc_key);
        var doc_key_owned = true;
        errdefer if (doc_key_owned) alloc.free(doc_key);
        const value = try cloneTypedValue(alloc, cell.value_type, cell.value);
        var value_owned = cell.value_type == .bytes_val;
        errdefer if (value_owned) alloc.free(value.bytes_val);
        try out.append(alloc, .{
            .doc_key = doc_key,
            .value_type = cell.value_type,
            .is_json = cell.is_json,
            .value = value,
        });
        doc_key_owned = false;
        value_owned = false;
    }

    return try out.toOwnedSlice(alloc);
}

fn appendUpsertInternal(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
) !void {
    try appendExistingColumnDeletes(alloc, store, deletes, owned_keys, doc_key);

    const row_key = try rowKeyAlloc(alloc, doc_key);
    var row_key_owned = true;
    errdefer if (row_key_owned) alloc.free(row_key);
    if (owned_keys) |keys| {
        try keys.append(alloc, row_key);
        row_key_owned = false;
    }
    try writes.append(alloc, .{
        .key = row_key,
        .value = row_value,
    });
    row_key_owned = false;

    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    for (row.cells) |cell| {
        const key = try internal_keys.relationalColumnKeyAlloc(alloc, doc_key, cell.path);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (owned_keys) |keys| {
            try keys.append(alloc, key);
            key_owned = false;
        }

        const value = try relational_row_codec.serialize(alloc, &.{cell});
        var value_owned = true;
        errdefer if (value_owned) alloc.free(value);
        if (owned_values) |values| {
            try values.append(alloc, value);
            value_owned = false;
        }

        try writes.append(alloc, .{
            .key = key,
            .value = value,
        });
        key_owned = false;
        value_owned = false;
    }
}

fn appendDeleteInternal(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
) !void {
    try appendExistingColumnDeletes(alloc, store, deletes, owned_keys, doc_key);

    const key = try rowKeyAlloc(alloc, doc_key);
    var key_owned = true;
    errdefer if (key_owned) alloc.free(key);
    if (owned_keys) |keys| {
        try keys.append(alloc, key);
        key_owned = false;
    }
    try deletes.append(alloc, key);
    key_owned = false;
}

fn appendExistingColumnDeletes(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
) !void {
    const old_row = try getRawAlloc(alloc, store, doc_key) orelse return;
    defer alloc.free(old_row);
    var row = try relational_row_codec.deserialize(alloc, old_row);
    defer row.deinit(alloc);
    for (row.cells) |cell| {
        const key = try internal_keys.relationalColumnKeyAlloc(alloc, doc_key, cell.path);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (owned_keys) |keys| {
            try keys.append(alloc, key);
            key_owned = false;
        }
        try deletes.append(alloc, key);
        key_owned = false;
    }
}

fn cloneTypedValue(alloc: Allocator, value_type: typed_dv.ValueType, value: typed_dv.TypedValue) !typed_dv.TypedValue {
    return switch (value_type) {
        .u64_val => .{ .u64_val = value.u64_val },
        .f64_val => .{ .f64_val = value.f64_val },
        .bool_val => .{ .bool_val = value.bool_val },
        .geo_point => .{ .geo_point = value.geo_point },
        .bytes_val => .{ .bytes_val = try alloc.dupe(u8, value.bytes_val) },
    };
}

test "relational base store writes materialize and delete by document key" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cells = [_]relational_row_codec.Cell{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "alpha" },
        },
    };
    const row = try relational_row_codec.serialize(alloc, &cells);
    defer alloc.free(row);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
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

    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row);
    try store.putBatch(writes.items, deletes.items);

    const materialized = (try getMaterializedAlloc(alloc, &store, "doc:a")).?;
    defer alloc.free(materialized);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", materialized);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDelete(alloc, &store, &deletes, &owned_keys, "doc:a");
    try store.putBatch(writes.items, deletes.items);
    try std.testing.expect((try getRawAlloc(alloc, &store, "doc:a")) == null);
}

test "relational write participant prepares commit and abort boundaries" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "prepared" },
        },
    });
    defer alloc.free(row);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
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

    var participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    try participant.prepareUpsert("events", "doc:a", row, null);
    participant.abort(null);
    try std.testing.expect((try getRawAlloc(alloc, &store, "doc:a")) == null);

    var committed = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    try committed.prepareUpsert("events", "doc:a", row, null);
    try store.putBatch(writes.items, deletes.items);
    try committed.commit(null, 1);

    const materialized = (try committed.get("doc:a", null)).?;
    defer alloc.free(materialized);
    try std.testing.expectEqualStrings("{\"title\":\"prepared\"}", materialized);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var delete_participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    try delete_participant.prepareDelete("events", "doc:a", null);
    try store.putBatch(writes.items, deletes.items);
    try delete_participant.commit(null, 2);
    try std.testing.expect((try getRawAlloc(alloc, &store, "doc:a")) == null);
}

test "relational base store scans rows and columns by document range" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row_a = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "alpha" },
        },
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 10.5 },
        },
    });
    defer alloc.free(row_a);
    const row_b = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "beta" },
        },
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 20.25 },
        },
    });
    defer alloc.free(row_b);
    const row_c = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "gamma" },
        },
    });
    defer alloc.free(row_c);

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "doc:b");
    defer alloc.free(primary_key);
    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
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

    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a);
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b);
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:c", row_c);
    try writes.append(alloc, .{ .key = primary_key, .value = "{\"ignored\":true}" });
    try store.putBatch(writes.items, deletes.items);

    const rows = try scanRowsAlloc(alloc, &store, "doc:a", "doc:b");
    defer freeRows(alloc, rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("doc:a", rows[0].doc_key);
    try std.testing.expectEqualStrings("doc:b", rows[1].doc_key);

    const amounts = try scanColumnAlloc(alloc, &store, "amount", "doc:a", "doc:b");
    defer freeColumnValues(alloc, amounts);
    try std.testing.expectEqual(@as(usize, 2), amounts.len);
    try std.testing.expectEqualStrings("doc:a", amounts[0].doc_key);
    try std.testing.expectEqual(@as(f64, 10.5), amounts[0].value.f64_val);
    try std.testing.expectEqualStrings("doc:b", amounts[1].doc_key);
    try std.testing.expectEqual(@as(f64, 20.25), amounts[1].value.f64_val);

    const doc_b_amount_key = try internal_keys.relationalColumnKeyAlloc(alloc, "doc:b", "amount");
    defer alloc.free(doc_b_amount_key);
    const doc_b_amount = try store.get(alloc, doc_b_amount_key);
    defer alloc.free(doc_b_amount);

    const row_b_without_amount = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "beta-updated" },
        },
    });
    defer alloc.free(row_b_without_amount);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b_without_amount);
    try store.putBatch(writes.items, deletes.items);
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_amount_key));

    const amounts_after_update = try scanColumnAlloc(alloc, &store, "amount", "doc:a", "doc:c");
    defer freeColumnValues(alloc, amounts_after_update);
    try std.testing.expectEqual(@as(usize, 1), amounts_after_update.len);
    try std.testing.expectEqualStrings("doc:a", amounts_after_update[0].doc_key);

    const doc_a_amount_key = try internal_keys.relationalColumnKeyAlloc(alloc, "doc:a", "amount");
    defer alloc.free(doc_a_amount_key);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDelete(alloc, &store, &deletes, &owned_keys, "doc:a");
    try store.putBatch(writes.items, deletes.items);
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_a_amount_key));
}
