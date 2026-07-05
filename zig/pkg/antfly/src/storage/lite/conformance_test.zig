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
const backend_erased = @import("../backend_erased.zig");
const docstore = @import("docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const relational_row_codec = @import("../db/algebraic/relational_row_codec.zig");
const relational_store = @import("../db/relational_store.zig");
const runtime_docstore = @import("../docstore.zig");
const schema_mod = @import("../schema.zig");

const Allocator = std.mem.Allocator;

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

fn expectBoundStoreConformance(runtime: *backend_erased.Store) !void {
    const caps = runtime.capabilities();
    try std.testing.expect(caps.cursors);
    try std.testing.expect(caps.ordered_ranges);
    try std.testing.expect(caps.reverse_ranges);
    try std.testing.expect(caps.single_writer);
    try std.testing.expect(!caps.native_namespaces);
    try std.testing.expect(caps.write_batches == .atomic);
    try std.testing.expect(caps.read_snapshots == .snapshot);

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:b", "B");
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    {
        var batch = try runtime.beginBatch();
        try batch.put("doc:c", "C");
        try batch.delete("doc:b");
        try batch.commit();
    }

    {
        var snapshot = try runtime.beginRead();
        defer snapshot.abort();

        var writer = try runtime.beginWrite();
        try writer.put("doc:a", "A2");
        try writer.commit();

        try std.testing.expectEqualStrings("A", try snapshot.get("doc:a"));
        try std.testing.expectError(error.NotFound, snapshot.get("doc:b"));
        try std.testing.expectEqualStrings("C", try snapshot.get("doc:c"));
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A2", try txn.get("doc:a"));
        try std.testing.expectError(error.NotFound, txn.get("doc:b"));
        try std.testing.expectEqualStrings("C", try txn.get("doc:c"));

        var cur = try txn.openCursor();
        defer cur.close();
        try std.testing.expectEqualStrings("doc:a", (try cur.first()).?.key);
        try std.testing.expectEqualStrings("doc:c", (try cur.next()).?.key);
        try std.testing.expect((try cur.next()) == null);
        try std.testing.expectEqualStrings("doc:c", (try cur.last()).?.key);
        try std.testing.expectEqualStrings("doc:a", (try cur.seekAtOrBefore("doc:b")).?.key);
        try std.testing.expectEqualStrings("doc:c", (try cur.seekAtOrAfter("doc:b")).?.key);
    }
}

fn seedDurableState(runtime: *backend_erased.Store) !void {
    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.put("doc:b", "B");
        try txn.commit();
    }

    {
        var batch = try runtime.beginBatch();
        try batch.delete("doc:b");
        try batch.put("doc:c", "C");
        try batch.commit();
    }
}

fn expectReopenedBoundState(runtime: *backend_erased.Store) !void {
    var txn = try runtime.beginRead();
    defer txn.abort();
    try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
    try std.testing.expectError(error.NotFound, txn.get("doc:b"));
    try std.testing.expectEqualStrings("C", try txn.get("doc:c"));

    var cur = try txn.openCursor();
    defer cur.close();
    try std.testing.expectEqualStrings("doc:a", (try cur.first()).?.key);
    try std.testing.expectEqualStrings("doc:c", (try cur.next()).?.key);
    try std.testing.expect((try cur.next()) == null);
    try std.testing.expectEqualStrings("doc:c", (try cur.last()).?.key);
}

test "storage.lite native docstore conforms to bound backend contract" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-conformance.aflite");
    defer allocator.free(path);

    var store = try docstore.Store.create(allocator, path, true);
    defer store.close();

    var runtime = try store.runtimeStore(allocator);
    defer runtime.deinit();
    try expectBoundStoreConformance(&runtime);
}

test "storage.lite native docstore conformance survives reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-conformance-reopen.aflite");
    defer allocator.free(path);

    {
        var store = try docstore.Store.create(allocator, path, true);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();
        try seedDurableState(&runtime);
    }

    {
        var store = try docstore.Store.open(allocator, path, true);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();
        try expectReopenedBoundState(&runtime);
    }
}

test "storage.lite relational ordered tuple doc range cleanup survives reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-relational-ordered-tuple-cleanup.aflite");
    defer allocator.free(path);

    const row_a = try relational_row_codec.serialize(allocator, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 1.0 } },
    });
    defer allocator.free(row_a);
    const row_b = try relational_row_codec.serialize(allocator, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 2.0 } },
    });
    defer allocator.free(row_b);

    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const columns = [_]schema_mod.RelationalColumn{
        .{
            .name = "status",
            .path = "status",
            .field_type = .keyword,
            .indexed = true,
            .index_name = "orders_status_amount_idx",
            .index_access_method = .ordered_tuple,
            .index_generation = 7,
            .index_schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
            .index_lifecycle = .ready,
            .index_keys = index_keys[0..],
        },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
    };
    const policy = relational_store.ColumnIndexPolicy.fromColumns(columns[0..]);

    const tuple_a = try relational_store.orderedTupleValueForIndexKeysAlloc(allocator, row_a, index_keys[0..], columns[0..]);
    defer allocator.free(tuple_a);
    const tuple_b = try relational_store.orderedTupleValueForIndexKeysAlloc(allocator, row_b, index_keys[0..], columns[0..]);
    defer allocator.free(tuple_b);
    const doc_a_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(allocator, "orders_status_amount_idx", tuple_a, "doc:a");
    defer allocator.free(doc_a_forward);
    const doc_b_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(allocator, "orders_status_amount_idx", tuple_b, "doc:b");
    defer allocator.free(doc_b_forward);
    const doc_a_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(allocator, "doc:a", "orders_status_amount_idx", tuple_a);
    defer allocator.free(doc_a_reverse);
    const doc_b_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(allocator, "doc:b", "orders_status_amount_idx", tuple_b);
    defer allocator.free(doc_b_reverse);

    {
        var lite = try docstore.Store.create(allocator, path, true);
        defer lite.close();

        const runtime = try lite.runtimeStore(allocator);
        var store = try runtime_docstore.DocStore.openRuntime(allocator, runtime);
        defer store.close();

        var writes = std.ArrayListUnmanaged(runtime_docstore.KVPair).empty;
        defer writes.deinit(allocator);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(allocator);
        var owned_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_keys.items) |key| allocator.free(key);
            owned_keys.deinit(allocator);
        }
        var owned_values = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_values.items) |value| allocator.free(value);
            owned_values.deinit(allocator);
        }

        try relational_store.appendUpsertWithColumnIndexPolicy(allocator, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a, policy);
        try relational_store.appendUpsertWithColumnIndexPolicy(allocator, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b, policy);
        try store.putBatch(writes.items, deletes.items);

        const before_b_reverse = try store.get(allocator, doc_b_reverse);
        allocator.free(before_b_reverse);

        try relational_store.deleteColumnIndexesByDocRange(allocator, &store, "doc:b", "doc:c");

        const remaining_a_forward = try store.get(allocator, doc_a_forward);
        allocator.free(remaining_a_forward);
        const remaining_a_reverse = try store.get(allocator, doc_a_reverse);
        allocator.free(remaining_a_reverse);
        try std.testing.expectError(error.NotFound, store.get(allocator, doc_b_forward));
        try std.testing.expectError(error.NotFound, store.get(allocator, doc_b_reverse));
    }

    {
        var reopened_lite = try docstore.Store.open(allocator, path, false);
        defer reopened_lite.close();

        const reopened_runtime = try reopened_lite.runtimeStore(allocator);
        var reopened = try runtime_docstore.DocStore.openRuntime(allocator, reopened_runtime);
        defer reopened.close();

        const remaining_a_forward = try reopened.get(allocator, doc_a_forward);
        allocator.free(remaining_a_forward);
        const remaining_a_reverse = try reopened.get(allocator, doc_a_reverse);
        allocator.free(remaining_a_reverse);
        try std.testing.expectError(error.NotFound, reopened.get(allocator, doc_b_forward));
        try std.testing.expectError(error.NotFound, reopened.get(allocator, doc_b_reverse));
    }
}

test "storage.lite relational ordered tuple repair survives reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-relational-ordered-tuple-repair.aflite");
    defer allocator.free(path);

    const row_a = try relational_row_codec.serialize(allocator, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 1.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "a" } },
    });
    defer allocator.free(row_a);
    const row_b = try relational_row_codec.serialize(allocator, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 2.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "b" } },
    });
    defer allocator.free(row_b);
    const stale_row = try relational_row_codec.serialize(allocator, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "closed" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 99.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "stale" } },
    });
    defer allocator.free(stale_row);

    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const include_columns = [_][]const u8{"note"};
    const columns = [_]schema_mod.RelationalColumn{
        .{
            .name = "status",
            .path = "status",
            .field_type = .keyword,
            .indexed = true,
            .index_name = "orders_status_amount_idx",
            .index_access_method = .ordered_tuple,
            .index_generation = 7,
            .index_schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
            .index_lifecycle = .ready,
            .index_keys = index_keys[0..],
            .index_include_columns = include_columns[0..],
        },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = true },
        .{ .name = "note", .path = "note", .field_type = .keyword },
    };
    const policy = relational_store.ColumnIndexPolicy.fromColumns(columns[0..]);

    const tuple_a = try relational_store.orderedTupleValueForIndexKeysAlloc(allocator, row_a, index_keys[0..], columns[0..]);
    defer allocator.free(tuple_a);
    const tuple_b = try relational_store.orderedTupleValueForIndexKeysAlloc(allocator, row_b, index_keys[0..], columns[0..]);
    defer allocator.free(tuple_b);
    const stale_tuple = try relational_store.orderedTupleValueForIndexKeysAlloc(allocator, stale_row, index_keys[0..], columns[0..]);
    defer allocator.free(stale_tuple);
    const doc_a_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(allocator, "orders_status_amount_idx", tuple_a, "doc:a");
    defer allocator.free(doc_a_forward);
    const doc_a_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(allocator, "doc:a", "orders_status_amount_idx", tuple_a);
    defer allocator.free(doc_a_reverse);
    const doc_b_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(allocator, "orders_status_amount_idx", tuple_b, "doc:b");
    defer allocator.free(doc_b_forward);
    const doc_b_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(allocator, "doc:b", "orders_status_amount_idx", tuple_b);
    defer allocator.free(doc_b_reverse);
    const stale_doc_a_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(allocator, "orders_status_amount_idx", stale_tuple, "doc:a");
    defer allocator.free(stale_doc_a_forward);
    const stale_doc_z_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(allocator, "orders_status_amount_idx", stale_tuple, "doc:z");
    defer allocator.free(stale_doc_z_forward);
    const stale_doc_z_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(allocator, "doc:z", "orders_status_amount_idx", stale_tuple);
    defer allocator.free(stale_doc_z_reverse);
    const amount_b_index = try internal_keys.relationalColumnIndexKeyAlloc(allocator, "amount", "doc:b");
    defer allocator.free(amount_b_index);
    const amount_b_reverse = try internal_keys.relationalColumnIndexByDocKeyAlloc(allocator, "doc:b", "amount");
    defer allocator.free(amount_b_reverse);

    {
        var lite = try docstore.Store.create(allocator, path, true);
        defer lite.close();

        const runtime = try lite.runtimeStore(allocator);
        var store = try runtime_docstore.DocStore.openRuntime(allocator, runtime);
        defer store.close();

        var writes = std.ArrayListUnmanaged(runtime_docstore.KVPair).empty;
        defer writes.deinit(allocator);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(allocator);
        var owned_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_keys.items) |key| allocator.free(key);
            owned_keys.deinit(allocator);
        }
        var owned_values = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_values.items) |value| allocator.free(value);
            owned_values.deinit(allocator);
        }

        try relational_store.appendUpsertWithColumnIndexPolicy(allocator, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a, policy);
        try relational_store.appendUpsertWithColumnIndexPolicy(allocator, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b, policy);
        try store.putBatch(writes.items, deletes.items);

        try store.putBatch(&.{
            .{ .key = stale_doc_a_forward, .value = "" },
            .{ .key = stale_doc_z_forward, .value = "" },
            .{ .key = stale_doc_z_reverse, .value = "" },
        }, &.{ doc_b_forward, doc_b_reverse, amount_b_index, amount_b_reverse });

        try std.testing.expectError(error.NotFound, store.get(allocator, doc_b_forward));

        const report = try relational_store.repairColumnBackedIndexesFromRowsInRangeWithColumnIndexPolicy(allocator, &store, "doc:a", "doc:zz", policy);
        try std.testing.expectEqual(@as(u64, 2), report.scanned_rows);
        try std.testing.expectEqual(@as(u64, 2), report.indexed_rows);
        try std.testing.expect(report.deleted_orphan_entries >= 1);
        try std.testing.expect(report.written_entries >= 6);

        const repaired_b_forward = try store.get(allocator, doc_b_forward);
        allocator.free(repaired_b_forward);
        try std.testing.expectError(error.NotFound, store.get(allocator, stale_doc_a_forward));
        try std.testing.expectError(error.NotFound, store.get(allocator, stale_doc_z_forward));
        try std.testing.expectError(error.NotFound, store.get(allocator, stale_doc_z_reverse));
    }

    {
        var reopened_lite = try docstore.Store.open(allocator, path, false);
        defer reopened_lite.close();

        const reopened_runtime = try reopened_lite.runtimeStore(allocator);
        var reopened = try runtime_docstore.DocStore.openRuntime(allocator, reopened_runtime);
        defer reopened.close();

        const repaired_a_forward = try reopened.get(allocator, doc_a_forward);
        allocator.free(repaired_a_forward);
        const repaired_a_reverse = try reopened.get(allocator, doc_a_reverse);
        allocator.free(repaired_a_reverse);
        const repaired_b_forward = try reopened.get(allocator, doc_b_forward);
        allocator.free(repaired_b_forward);
        const repaired_b_reverse = try reopened.get(allocator, doc_b_reverse);
        allocator.free(repaired_b_reverse);
        const repaired_amount_b = try reopened.get(allocator, amount_b_index);
        allocator.free(repaired_amount_b);
        const repaired_amount_b_reverse = try reopened.get(allocator, amount_b_reverse);
        allocator.free(repaired_amount_b_reverse);
        try std.testing.expectError(error.NotFound, reopened.get(allocator, stale_doc_a_forward));
        try std.testing.expectError(error.NotFound, reopened.get(allocator, stale_doc_z_forward));
        try std.testing.expectError(error.NotFound, reopened.get(allocator, stale_doc_z_reverse));
    }
}
