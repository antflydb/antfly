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

//! Pinned ordered-index preparation over the existing PreparedRelationalWrite.
//! This is an execution primitive, not index activation or constraint approval:
//! the catalog must select writable generations, and the commit path must check
//! that its published snapshot still matches before consuming prepared effects.
//! One worker-owned batch retains a plan/epoch and uses flat reusable buffers;
//! no row-local reference count, name lookup, or JSON parse is needed.

const std = @import("std");
const schema_registry = @import("schema_registry.zig");
const schema = @import("../schema.zig");
const native = @import("../relational_index.zig");
const tuples = @import("relational_index_keys.zig");
const mapper = @import("document_mapper.zig");
const rows = @import("algebraic/relational_row_codec.zig");
const Allocator = std.mem.Allocator;

/// Already selected by the catalog for key preparation. Constraint/predicate
/// evaluation and covering payloads are separate consumers of the same typed
/// row; callers must not discard those requirements when selecting definitions.
pub const Definition = struct {
    name: []const u8,
    generation: u64,
    slot: u32 = 0,
    keys: []const native.RelationalIndexKey,
};

pub const BoundIndex = struct {
    name: []const u8,
    generation: u64,
    slot: u32,
    tuple: tuples.TuplePlan,

    pub fn id(self: BoundIndex) native.RelationalIndexId {
        return .{ .generation = self.generation, .slot = self.slot };
    }
};

const Snapshot = struct {
    alloc: Allocator,
    references: std.atomic.Value(usize) = .init(1),
    schema_view: schema_registry.SchemaView,
    indexes: []BoundIndex,
    fingerprint: [std.crypto.hash.Blake3.digest_length]u8,

    fn release(self: *Snapshot) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;
        for (self.indexes) |*index| {
            self.alloc.free(index.name);
            index.tuple.deinit();
        }
        self.alloc.free(self.indexes);
        self.schema_view.release();
        self.alloc.destroy(self);
    }
};

pub const View = struct {
    snapshot: *Snapshot,

    /// Retains the schema independently and resolves/copies all definition data.
    /// Callers may release both the original view and request arena on return.
    pub fn init(alloc: Allocator, schema_view: schema_registry.SchemaView, definitions: []const Definition) !View {
        if (schema_view.storageMode() != .relational) return error.InvalidRelationalIndexDefinition;
        var retained = schema_view.clone();
        errdefer retained.release();
        const indexes = try alloc.alloc(BoundIndex, definitions.len);
        var initialized: usize = 0;
        errdefer {
            for (indexes[0..initialized]) |*index| {
                alloc.free(index.name);
                index.tuple.deinit();
            }
            alloc.free(indexes);
        }
        for (definitions, indexes) |definition, *index| {
            if (definition.name.len == 0 or !std.unicode.utf8ValidateSlice(definition.name) or definition.generation == 0)
                return error.InvalidRelationalIndexDefinition;
            const name = try alloc.dupe(u8, definition.name);
            errdefer alloc.free(name);
            const tuple = try tuples.TuplePlan.init(alloc, retained.tableSchema().*, retained.physicalLayout(), definition.keys);
            index.* = .{ .name = name, .generation = definition.generation, .slot = definition.slot, .tuple = tuple };
            initialized += 1;
        }
        // Stable slot order is independent of a request/map's iteration order.
        std.mem.sort(BoundIndex, indexes, {}, struct {
            fn less(_: void, a: BoundIndex, b: BoundIndex) bool {
                return std.mem.lessThan(u8, a.name, b.name);
            }
        }.less);
        var identities = std.AutoHashMapUnmanaged(u128, void).empty;
        defer identities.deinit(alloc);
        for (indexes, 0..) |index, i| {
            if (i != 0 and std.mem.eql(u8, indexes[i - 1].name, index.name))
                return error.DuplicateRelationalIndexName;
            if ((try identities.getOrPut(alloc, index.id().mapKey())).found_existing)
                return error.DuplicateRelationalIndexId;
        }
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly:relational-index-preparation-snapshot\x00");
        var number: [8]u8 = undefined;
        std.mem.writeInt(u64, &number, retained.version(), .little);
        hash.update(&number);
        std.mem.writeInt(u64, &number, indexes.len, .little);
        hash.update(&number);
        for (indexes) |index| {
            std.mem.writeInt(u64, &number, index.name.len, .little);
            hash.update(&number);
            hash.update(index.name);
            std.mem.writeInt(u64, &number, index.generation, .little);
            hash.update(&number);
            std.mem.writeInt(u64, &number, index.slot, .little);
            hash.update(&number);
            hash.update(&index.tuple.fingerprint);
        }
        const snapshot = try alloc.create(Snapshot);
        snapshot.* = .{ .alloc = alloc, .schema_view = retained, .indexes = indexes, .fingerprint = undefined };
        hash.final(&snapshot.fingerprint);
        return .{ .snapshot = snapshot };
    }

    pub fn clone(self: View) View {
        const previous = self.snapshot.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0);
        return self;
    }

    pub fn release(self: *View) void {
        self.snapshot.release();
        self.* = undefined;
    }

    pub fn boundIndexes(self: View) []const BoundIndex {
        return self.snapshot.indexes;
    }

    pub fn schemaView(self: View) *const schema_registry.SchemaView {
        // Borrowed: the caller must retain this View or explicitly clone.
        return &self.snapshot.schema_view;
    }

    pub fn fingerprint(self: View) [std.crypto.hash.Blake3.digest_length]u8 {
        return self.snapshot.fingerprint;
    }
};

const Entry = struct {
    start: usize,
    end: usize,
    has_null: bool,
};

pub const Key = struct {
    bytes: []const u8,
    has_null: bool,
};

/// Single-worker builder; share immutable Views, not this mutable buffer.
/// Offsets survive growth. Returned Key slices borrow bytes until the next
/// append/reset/deinit. Admission is provided by the caller's backing allocator.
pub const Batch = struct {
    alloc: Allocator,
    view: View,
    bytes: std.ArrayList(u8) = .empty,
    entries: std.ArrayList(Entry) = .empty,
    row_count: usize = 0,

    pub fn init(alloc: Allocator, view: View) Batch {
        return .{ .alloc = alloc, .view = view.clone() };
    }

    pub fn deinit(self: *Batch) void {
        self.bytes.deinit(self.alloc);
        self.entries.deinit(self.alloc);
        self.view.release();
        self.* = undefined;
    }

    pub fn reset(self: *Batch) void {
        self.bytes.clearRetainingCapacity();
        self.entries.clearRetainingCapacity();
        self.row_count = 0;
    }

    pub fn reserve(self: *Batch, row_capacity: usize, key_bytes: usize) !void {
        const count = try std.math.mul(usize, row_capacity, self.view.boundIndexes().len);
        try self.entries.ensureTotalCapacity(self.alloc, count);
        try self.bytes.ensureTotalCapacity(self.alloc, key_bytes);
    }

    /// Pointer identity is the publication fence, not a digest supplied by a
    /// request. The commit path must also check that the schema view is current.
    pub fn isForPlan(self: *const Batch, current: View) bool {
        return self.view.snapshot == current.snapshot;
    }

    pub fn appendPrepared(self: *Batch, prepared: *const mapper.PreparedRelationalWrite) !usize {
        const view = self.view.schemaView();
        if (prepared.schema_version != view.version()) return error.RelationalRowSchemaMismatch;
        return self.append(try prepared.typedView(view.tableSchema().*, view.physicalLayout()));
    }

    /// Used for old rows/rebuilds after the caller obtains a validated row view.
    /// A failed row leaves all previously prepared effects intact, even if a
    /// later component or index fails after earlier keys have been appended.
    pub fn append(self: *Batch, row: rows.OrdinalRowView) !usize {
        const schema_view = self.view.schemaView();
        if (row.layout != schema_view.physicalLayout() or
            row.table_schema.relational_columns.ptr != schema_view.tableSchema().relational_columns.ptr or
            row.table_schema.relational_columns.len != schema_view.tableSchema().relational_columns.len)
            return error.RelationalRowSchemaMismatch;
        const next_count = try std.math.add(usize, self.row_count, 1);
        const bytes_start = self.bytes.items.len;
        const entries_start = self.entries.items.len;
        errdefer {
            self.bytes.shrinkRetainingCapacity(bytes_start);
            self.entries.shrinkRetainingCapacity(entries_start);
        }
        try self.entries.ensureUnusedCapacity(self.alloc, self.view.boundIndexes().len);
        for (self.view.boundIndexes()) |index| {
            const start = self.bytes.items.len;
            const has_null = try index.tuple.append(self.alloc, &self.bytes, row);
            self.entries.appendAssumeCapacity(.{ .start = start, .end = self.bytes.items.len, .has_null = has_null });
        }
        const result = self.row_count;
        self.row_count = next_count;
        return result;
    }

    pub fn key(self: *const Batch, row: usize, index: usize) !Key {
        if (row >= self.row_count or index >= self.view.boundIndexes().len) return error.InvalidRelationalIndexPosition;
        const entry = self.entries.items[row * self.view.boundIndexes().len + index];
        return .{ .bytes = self.bytes.items[entry.start..entry.end], .has_null = entry.has_null };
    }
};

const test_columns = [_]schema.RelationalColumn{
    .{ .name = "id", .path = "id", .column_type = .integer },
    .{ .name = "label", .path = "label", .column_type = .string, .allows_null = true },
};
const test_schema = schema.TableSchema{ .version = 7, .storage_mode = .relational, .relational_columns = &test_columns };
const test_definitions = [_]Definition{
    .{ .name = "z_label", .generation = 4, .keys = &.{.{ .column = "label", .collation = "ci" }} },
    .{ .name = "a_composite", .generation = 2, .keys = &.{ .{ .column = "id" }, .{ .column = "label", .direction = .desc } } },
};

test "relational index plan retains epoch and definitions through prepared batch ownership" {
    const alloc = std.testing.allocator;
    var registry = try schema_registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    var schema_view = registry.acquire().?;
    const definition_name = try alloc.dupe(u8, "by_id");
    const definitions = [_]Definition{.{ .name = definition_name, .generation = 1, .keys = &.{.{ .column = "id" }} }};
    var view = try View.init(alloc, schema_view, &definitions);
    var batch = Batch.init(alloc, view);
    defer batch.deinit();
    @memset(definition_name, 'x');
    alloc.free(definition_name);
    schema_view.release();
    registry.deinit();
    view.release();
    // The batch is now the sole plan/epoch owner.
    const pinned = batch.view.schemaView();
    var prepared = try mapper.PreparedRelationalWrite.init(alloc, "row", "{\"id\":9007199254740993,\"label\":\"Alpha\"}", null, pinned.tableSchema().*, pinned.physicalLayout());
    defer prepared.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), try batch.appendPrepared(&prepared));
    try std.testing.expectEqualStrings("by_id", batch.view.boundIndexes()[0].name);
    try std.testing.expectEqual(@as(usize, 9), (try batch.key(0, 0)).bytes.len);
    try std.testing.expect(!(try batch.key(0, 0)).has_null);
}

test "relational index plan identity binds generation and canonical index order" {
    const alloc = std.testing.allocator;
    var registry = try schema_registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer registry.deinit();
    var schema_view = registry.acquire().?;
    defer schema_view.release();
    var a = try View.init(alloc, schema_view, &test_definitions);
    defer a.release();
    var b = try View.init(alloc, schema_view, &.{ test_definitions[1], test_definitions[0] });
    defer b.release();
    try std.testing.expectEqualSlices(u8, &a.fingerprint(), &b.fingerprint());
    try std.testing.expectEqualStrings("a_composite", a.boundIndexes()[0].name);
    var changed = test_definitions;
    changed[0].generation += 1;
    var c = try View.init(alloc, schema_view, &changed);
    defer c.release();
    try std.testing.expect(!std.mem.eql(u8, &a.fingerprint(), &c.fingerprint()));
    var batch = Batch.init(alloc, a);
    defer batch.deinit();
    try std.testing.expect(batch.isForPlan(a));
    try std.testing.expect(!batch.isForPlan(b)); // A digest does not establish publication identity.
    try std.testing.expectError(error.DuplicateRelationalIndexName, View.init(alloc, schema_view, &.{ test_definitions[0], test_definitions[0] }));
    changed = test_definitions;
    changed[0].generation = changed[1].generation;
    try std.testing.expectError(error.DuplicateRelationalIndexId, View.init(alloc, schema_view, &changed));
    changed = test_definitions;
    changed[0].slot = 9;
    var different_slot = try View.init(alloc, schema_view, &changed);
    defer different_slot.release();
    try std.testing.expect(!std.mem.eql(u8, &a.fingerprint(), &different_slot.fingerprint()));
    changed[0].generation = 0;
    try std.testing.expectError(error.InvalidRelationalIndexDefinition, View.init(alloc, schema_view, &changed));
}

test "relational index plan batches prepared rows with reusable allocation free buffers" {
    const alloc = std.testing.allocator;
    var registry = try schema_registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer registry.deinit();
    var schema_view = registry.acquire().?;
    defer schema_view.release();
    var view = try View.init(alloc, schema_view, &test_definitions);
    defer view.release();
    var prepared = try mapper.PreparedRelationalWrite.init(alloc, "row", "{\"id\":7,\"label\":\"Alpha\"}", null, schema_view.tableSchema().*, schema_view.physicalLayout());
    defer prepared.deinit(alloc);
    prepared.releaseParsed(); // Index keys need only the owned typed row.
    var batch = Batch.init(alloc, view);
    defer batch.deinit();
    try batch.reserve(200, 200 * 64);
    var no_alloc = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0, .resize_fail_index = 0 });
    batch.alloc = no_alloc.allocator();
    defer batch.alloc = alloc;
    for (0..2) |_| {
        for (0..200) |i| {
            try std.testing.expectEqual(i, try batch.appendPrepared(&prepared));
            for (0..view.boundIndexes().len) |index| {
                const actual = try batch.key(i, index);
                try std.testing.expect(!actual.has_null);
                try std.testing.expectEqualSlices(u8, (try batch.key(0, index)).bytes, actual.bytes);
            }
        }
        try std.testing.expectError(error.InvalidRelationalIndexPosition, batch.key(200, 0));
        batch.reset();
    }
    try std.testing.expectEqual(@as(usize, 0), no_alloc.allocations);
}

fn testPlanAllocations(alloc: Allocator) !void {
    var registry = try schema_registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer registry.deinit();
    var schema_view = registry.acquire().?;
    defer schema_view.release();
    var view = try View.init(alloc, schema_view, &test_definitions);
    defer view.release();
    var prepared = try mapper.PreparedRelationalWrite.init(alloc, "row", "{\"id\":7,\"label\":\"Alpha\"}", null, schema_view.tableSchema().*, schema_view.physicalLayout());
    defer prepared.deinit(alloc);
    var batch = Batch.init(alloc, view);
    defer batch.deinit();
    _ = try batch.appendPrepared(&prepared);
}

test "relational index plan rejects foreign prepared epochs and rolls back late failures" {
    const alloc = std.testing.allocator;
    var registry = try schema_registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer registry.deinit();
    var schema_view = registry.acquire().?;
    defer schema_view.release();
    var view = try View.init(alloc, schema_view, &.{
        .{ .name = "first_id", .generation = 1, .keys = &.{.{ .column = "id" }} },
        .{ .name = "second_label", .generation = 2, .keys = &.{.{ .column = "label" }} },
    });
    defer view.release();
    var prepared = try mapper.PreparedRelationalWrite.init(alloc, "row", "{\"id\":7,\"label\":\"a\"}", null, schema_view.tableSchema().*, schema_view.physicalLayout());
    defer prepared.deinit(alloc);
    var batch = Batch.init(alloc, view);
    defer batch.deinit();
    _ = try batch.appendPrepared(&prepared);
    const original = try alloc.dupe(u8, batch.bytes.items);
    defer alloc.free(original);

    // Same numeric version, even the same schema bytes, is not a shared epoch.
    var foreign_registry = try schema_registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer foreign_registry.deinit();
    var foreign_view = foreign_registry.acquire().?;
    defer foreign_view.release();
    var foreign = try mapper.PreparedRelationalWrite.init(alloc, "row", "{\"id\":7,\"label\":\"a\"}", null, foreign_view.tableSchema().*, foreign_view.physicalLayout());
    defer foreign.deinit(alloc);
    try std.testing.expectError(error.RelationalRowSchemaMismatch, batch.appendPrepared(&foreign));
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqualSlices(u8, original, batch.bytes.items);

    const large_json = "{\"id\":9,\"label\":\"" ++ "x" ** 4096 ++ "\"}";
    var large = try mapper.PreparedRelationalWrite.init(alloc, "row", large_json, null, schema_view.tableSchema().*, schema_view.physicalLayout());
    defer large.deinit(alloc);
    // First index fits; the second runs out of space. No partial row survives.
    try batch.reserve(2, 64);
    try std.testing.expect(batch.bytes.capacity < 4096);
    var no_alloc = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0, .resize_fail_index = 0 });
    batch.alloc = no_alloc.allocator();
    defer batch.alloc = alloc;
    try std.testing.expectError(error.OutOfMemory, batch.appendPrepared(&large));
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqual(@as(usize, 2), batch.entries.items.len);
    try std.testing.expectEqualSlices(u8, original, batch.bytes.items);
    // The same batch remains usable after the failed append.
    try std.testing.expectEqual(@as(usize, 1), try batch.appendPrepared(&prepared));
}

test "relational index plan releases partially initialized snapshots and batches on allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testPlanAllocations, .{});
}
