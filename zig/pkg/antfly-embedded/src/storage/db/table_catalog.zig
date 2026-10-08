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

const std = @import("std");
const schema_mod = @import("../schema.zig");
const row_codec = @import("algebraic/relational_row_codec.zig");

pub const key = "\x00\x00__metadata__:table_catalog";
pub const encoded_len: usize = 72;
const magic = "ATBL";
const format_version: u32 = 3;
pub const default_transaction_admission_bytes: u64 = 128 * 1024 * 1024;

pub const IndexState = enum(u8) {
    none = 0,
    pending = 1,
    building = 2,
    ready = 3,
    failed = 4,
};

pub const RowPolicyPhase = enum(u8) {
    disabled = 0,
    /// Metadata has announced a new generation, but owners are not yet all
    /// installed. External reads and writes fail closed in this phase.
    preparing = 1,
    /// The generation is active and every external operation needs a bound
    /// principal/setting/policy proof; raw DB entrypoints remain denied.
    active = 2,
};

/// Small transactional table facts used on hot control paths. This deliberately
/// contains no variable-length data, making catalog updates allocation-free.
pub const Catalog = struct {
    mode_initialized: bool = false,
    storage_mode: schema_mod.StorageMode = .document,
    active_schema_version: u32 = 0,
    schema_format_version: u32 = schema_mod.storage_format_version,
    row_format_version: u32 = row_codec.ordinal_version,
    /// Presence summary: zero means empty and one means user data is present.
    /// Exact cardinality lives in the identity visibility summary, avoiding a
    /// hot catalog rewrite on every mutation.
    row_count: u64 = 0,
    generation: u64 = 0,
    index_state: IndexState = .none,
    reconciled: bool = false,
    /// Logical command policy, replicated with the table rather than inferred
    /// from a replica's local memory envelope. Local pressure only delays apply.
    transaction_admission_bytes: u64 = default_transaction_admission_bytes,
    row_policy_phase: RowPolicyPhase = .disabled,
    row_policy_generation: u64 = 0,
    row_policy_catalog_epoch: u64 = 0,

    pub fn encode(self: Catalog) [encoded_len]u8 {
        var out: [encoded_len]u8 = @splat(0);
        @memcpy(out[0..4], magic);
        std.mem.writeInt(u32, out[4..8], format_version, .little);
        out[8] = @intFromBool(self.mode_initialized);
        out[9] = @backingInt(self.storage_mode);
        out[10] = @backingInt(self.index_state);
        out[11] = @intFromBool(self.reconciled);
        std.mem.writeInt(u32, out[12..16], self.active_schema_version, .little);
        std.mem.writeInt(u32, out[16..20], self.schema_format_version, .little);
        std.mem.writeInt(u32, out[20..24], self.row_format_version, .little);
        std.mem.writeInt(u64, out[24..32], self.row_count, .little);
        std.mem.writeInt(u64, out[32..40], self.generation, .little);
        std.mem.writeInt(u64, out[40..48], self.transaction_admission_bytes, .little);
        out[48] = @backingInt(self.row_policy_phase);
        std.mem.writeInt(u64, out[56..64], self.row_policy_generation, .little);
        std.mem.writeInt(u64, out[64..72], self.row_policy_catalog_epoch, .little);
        return out;
    }

    pub fn decode(data: []const u8) !Catalog {
        if ((data.len != encoded_len and data.len != 48) or !std.mem.eql(u8, data[0..4], magic)) return error.InvalidTableCatalog;
        const version = std.mem.readInt(u32, data[4..8], .little);
        if (version != format_version and version != 2) return error.UnsupportedTableCatalogVersion;
        if ((version == 2 and data.len != 48) or (version == format_version and data.len != encoded_len)) return error.InvalidTableCatalog;
        const row_count = std.mem.readInt(u64, data[24..32], .little);
        if (data[8] > 1 or data[9] > @backingInt(schema_mod.StorageMode.relational) or
            data[10] > @backingInt(IndexState.failed) or data[11] > 1 or row_count > 1)
            return error.InvalidTableCatalog;
        const catalog: Catalog = .{
            .mode_initialized = data[8] == 1,
            .storage_mode = @fromBackingInt(data[9]),
            .index_state = @fromBackingInt(data[10]),
            .reconciled = data[11] == 1,
            .active_schema_version = std.mem.readInt(u32, data[12..16], .little),
            .schema_format_version = std.mem.readInt(u32, data[16..20], .little),
            .row_format_version = std.mem.readInt(u32, data[20..24], .little),
            .row_count = row_count,
            .generation = std.mem.readInt(u64, data[32..40], .little),
            .transaction_admission_bytes = std.mem.readInt(u64, data[40..48], .little),
            .row_policy_phase = if (version == 2) .disabled else std.enums.fromInt(RowPolicyPhase, data[48]) orelse return error.InvalidTableCatalog,
            .row_policy_generation = if (version == 2) 0 else std.mem.readInt(u64, data[56..64], .little),
            .row_policy_catalog_epoch = if (version == 2) 0 else std.mem.readInt(u64, data[64..72], .little),
        };
        // A disabled catalog retains the last generation as a revocation
        // high-water mark. Resetting it would allow an old signed view to be
        // replayed after a later re-enable at the same generation.
        if ((catalog.row_policy_generation == 0) != (catalog.row_policy_catalog_epoch == 0) or
            (catalog.row_policy_phase != .disabled and catalog.row_policy_generation == 0)) return error.InvalidTableCatalog;
        if (catalog.row_policy_phase != .disabled and
            (catalog.storage_mode != .relational or catalog.active_schema_version == 0)) return error.InvalidTableCatalog;
        if (version == format_version) for (data[49..56]) |reserved| {
            if (reserved != 0) return error.InvalidTableCatalog;
        };
        if (catalog.transaction_admission_bytes == 0) return error.InvalidTableCatalog;
        // The preceding deployed schema format remains readable. Merely
        // opening an owner must not rewrite it or make all document tables
        // unavailable when precise SQL descriptors are introduced.
        if ((catalog.schema_format_version < 15 or catalog.schema_format_version > schema_mod.storage_format_version) or
            catalog.row_format_version != row_codec.ordinal_version)
            return error.UnsupportedTableCapabilityVersion;
        return catalog;
    }

    /// Bind the transactional catalog to the runtime schema loaded from the
    /// same store snapshot. A mismatch is corruption (or an unsupported writer),
    /// never a state that request paths should attempt to repair implicitly.
    pub fn validateForSchema(self: Catalog, table_schema: ?schema_mod.TableSchema) !void {
        if (table_schema) |schema| {
            if (!self.mode_initialized or self.storage_mode != schema.storage_mode or
                self.active_schema_version != schema.version)
                return error.TableCatalogSchemaMismatch;
            if (self.schema_format_version < 16) for (schema.relational_columns) |column| {
                if (column.sql_element_type != null) return error.UnsupportedTableCapabilityVersion;
            };
            if (self.schema_format_version < 17) for (schema.relational_columns) |column| {
                if (column.column_type == .sql_array) return error.UnsupportedTableCapabilityVersion;
            };
            if (self.schema_format_version < 18 and schema.requires_typed_expressions) return error.UnsupportedTableCapabilityVersion;
            if (self.schema_format_version < 19 and schema.requires_predicate_expressions) return error.UnsupportedTableCapabilityVersion;
            return;
        }
        if (self.storage_mode != .document or self.active_schema_version != 0)
            return error.TableCatalogSchemaMismatch;
    }
};

pub fn load(alloc: std.mem.Allocator, store: anytype) !?Catalog {
    const raw = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    return try Catalog.decode(raw);
}

test "relational index system SQL catalog fences typed expression capability without precise columns" {
    const table: schema_mod.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .requires_typed_expressions = true, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer }} };
    var catalog: Catalog = .{ .schema_format_version = 17, .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 1 };
    try std.testing.expectError(error.UnsupportedTableCapabilityVersion, catalog.validateForSchema(table));
    catalog.schema_format_version = schema_mod.storage_format_version;
    try catalog.validateForSchema(table);
}

test "relational index system SQL catalog fences membership even without numeric types" {
    const table: schema_mod.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .requires_predicate_expressions = true, .relational_columns = &.{.{ .name = "label", .path = "label", .column_type = .string }} };
    var catalog: Catalog = .{ .schema_format_version = 18, .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 1 };
    try std.testing.expectError(error.UnsupportedTableCapabilityVersion, catalog.validateForSchema(table));
    catalog.schema_format_version = schema_mod.storage_format_version;
    try catalog.validateForSchema(table);
}

test "relational index system SQL catalog admits deployed schemas but fences precise type capabilities" {
    const old: Catalog = .{ .schema_format_version = 15, .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 1 };
    const raw = old.encode();
    const decoded = try Catalog.decode(&raw);
    const plain: schema_mod.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer }} };
    try decoded.validateForSchema(plain);
    var typed = plain;
    typed.relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer, .sql_element_type = .int32 }};
    try std.testing.expectError(error.UnsupportedTableCapabilityVersion, decoded.validateForSchema(typed));
    var current = old;
    current.schema_format_version = schema_mod.storage_format_version;
    const current_raw = current.encode();
    try (try Catalog.decode(&current_raw)).validateForSchema(typed);
    var unsupported = raw;
    std.mem.writeInt(u32, unsupported[16..20], 14, .little);
    try std.testing.expectError(error.UnsupportedTableCapabilityVersion, Catalog.decode(&unsupported));
}

test "relational index system SQL array catalog capabilities cannot be downgraded" {
    const array: schema_mod.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = .int64 }} };
    var catalog: Catalog = .{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 1 };
    try catalog.validateForSchema(array);
    catalog.schema_format_version = 16;
    const bytes = catalog.encode();
    const deployed = try Catalog.decode(&bytes);
    try std.testing.expectError(error.UnsupportedTableCapabilityVersion, deployed.validateForSchema(array));
    var scalar = array;
    scalar.relational_columns = &.{.{ .name = "a", .path = "a", .column_type = .integer, .sql_element_type = .int64 }};
    try deployed.validateForSchema(scalar);
}

test "table catalog has a stable canonical representation" {
    const expected = Catalog{
        .mode_initialized = true,
        .storage_mode = .relational,
        .active_schema_version = 19,
        .row_count = 1,
        .generation = 8,
        .index_state = .building,
        .reconciled = true,
    };
    const encoded = expected.encode();
    try std.testing.expectEqual(expected, try Catalog.decode(&encoded));

    var corrupt = encoded;
    corrupt[8] = 2;
    try std.testing.expectError(error.InvalidTableCatalog, Catalog.decode(&corrupt));
    corrupt = encoded;
    corrupt[4] = 99;
    try std.testing.expectError(error.UnsupportedTableCatalogVersion, Catalog.decode(&corrupt));
    corrupt = encoded;
    std.mem.writeInt(u64, corrupt[24..32], 2, .little);
    try std.testing.expectError(error.InvalidTableCatalog, Catalog.decode(&corrupt));
    corrupt = encoded;
    std.mem.writeInt(u32, corrupt[16..20], schema_mod.storage_format_version + 1, .little);
    try std.testing.expectError(error.UnsupportedTableCapabilityVersion, Catalog.decode(&corrupt));
    corrupt = encoded;
    std.mem.writeInt(u32, corrupt[20..24], row_codec.ordinal_version + 1, .little);
    try std.testing.expectError(error.UnsupportedTableCapabilityVersion, Catalog.decode(&corrupt));
    corrupt = encoded;
    corrupt[48] = 3;
    try std.testing.expectError(error.InvalidTableCatalog, Catalog.decode(&corrupt));
    corrupt = encoded;
    corrupt[48] = @backingInt(RowPolicyPhase.active);
    try std.testing.expectError(error.InvalidTableCatalog, Catalog.decode(&corrupt));

    const preparing: Catalog = .{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 3, .row_policy_phase = .preparing, .row_policy_generation = 11, .row_policy_catalog_epoch = 19 };
    const preparing_bytes = preparing.encode();
    try std.testing.expectEqual(preparing, try Catalog.decode(&preparing_bytes));
    var revoked = preparing;
    revoked.row_policy_phase = .disabled;
    revoked.row_policy_generation += 1;
    const revoked_bytes = revoked.encode();
    try std.testing.expectEqual(revoked, try Catalog.decode(&revoked_bytes));
    var old: [48]u8 = undefined;
    @memcpy(&old, encoded[0..48]);
    std.mem.writeInt(u32, old[4..8], 2, .little);
    try std.testing.expectEqual(RowPolicyPhase.disabled, (try Catalog.decode(&old)).row_policy_phase);
}

test "table catalog is bound to its active runtime schema" {
    const runtime_schema: schema_mod.TableSchema = .{
        .version = 19,
        .storage_mode = .relational,
    };
    const catalog: Catalog = .{
        .mode_initialized = true,
        .storage_mode = .relational,
        .active_schema_version = 19,
    };
    try catalog.validateForSchema(runtime_schema);

    var mismatched = catalog;
    mismatched.active_schema_version += 1;
    try std.testing.expectError(error.TableCatalogSchemaMismatch, mismatched.validateForSchema(runtime_schema));
    mismatched = catalog;
    mismatched.storage_mode = .document;
    try std.testing.expectError(error.TableCatalogSchemaMismatch, mismatched.validateForSchema(runtime_schema));

    try (Catalog{}).validateForSchema(null);
    mismatched = .{ .mode_initialized = true, .storage_mode = .relational };
    try std.testing.expectError(error.TableCatalogSchemaMismatch, mismatched.validateForSchema(null));
}
