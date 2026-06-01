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

pub fn rowKeyAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    return try internal_keys.relationalRowKeyAlloc(alloc, doc_key);
}

pub fn appendUpsert(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
) !void {
    const key = try rowKeyAlloc(alloc, doc_key);
    errdefer alloc.free(key);
    try owned_keys.append(alloc, key);
    try writes.append(alloc, .{
        .key = key,
        .value = row_value,
    });
}

pub fn appendDelete(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
) !void {
    const key = try rowKeyAlloc(alloc, doc_key);
    errdefer alloc.free(key);
    try owned_keys.append(alloc, key);
    try deletes.append(alloc, key);
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
    return try relational_row_codec.materializeOwnedDocumentValueAlloc(alloc, raw);
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

    try appendUpsert(alloc, &writes, &owned_keys, "doc:a", row);
    try store.putBatch(writes.items, deletes.items);

    const materialized = (try getMaterializedAlloc(alloc, &store, "doc:a")).?;
    defer alloc.free(materialized);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", materialized);

    writes.clearRetainingCapacity();
    try appendDelete(alloc, &deletes, &owned_keys, "doc:a");
    try store.putBatch(writes.items, deletes.items);
    try std.testing.expect((try getRawAlloc(alloc, &store, "doc:a")) == null);
}
