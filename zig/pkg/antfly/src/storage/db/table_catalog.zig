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
const schema_mod = @import("../schema.zig");
const row_codec = @import("algebraic/relational_row_codec.zig");

pub const key = "\x00\x00__metadata__:table_catalog";
pub const encoded_len: usize = 48;
const magic = "ATBL";
const format_version: u32 = 2;
pub const default_transaction_admission_bytes: u64 = 128 * 1024 * 1024;

pub const IndexState = enum(u8) {
    none = 0,
    pending = 1,
    building = 2,
    ready = 3,
    failed = 4,
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

    pub fn encode(self: Catalog) [encoded_len]u8 {
        var out: [encoded_len]u8 = @splat(0);
        @memcpy(out[0..4], magic);
        std.mem.writeInt(u32, out[4..8], format_version, .little);
        out[8] = @intFromBool(self.mode_initialized);
        out[9] = @intFromEnum(self.storage_mode);
        out[10] = @intFromEnum(self.index_state);
        out[11] = @intFromBool(self.reconciled);
        std.mem.writeInt(u32, out[12..16], self.active_schema_version, .little);
        std.mem.writeInt(u32, out[16..20], self.schema_format_version, .little);
        std.mem.writeInt(u32, out[20..24], self.row_format_version, .little);
        std.mem.writeInt(u64, out[24..32], self.row_count, .little);
        std.mem.writeInt(u64, out[32..40], self.generation, .little);
        std.mem.writeInt(u64, out[40..48], self.transaction_admission_bytes, .little);
        return out;
    }

    pub fn decode(data: []const u8) !Catalog {
        if (data.len != encoded_len or !std.mem.eql(u8, data[0..4], magic)) return error.InvalidTableCatalog;
        if (std.mem.readInt(u32, data[4..8], .little) != format_version) return error.UnsupportedTableCatalogVersion;
        const row_count = std.mem.readInt(u64, data[24..32], .little);
        if (data[8] > 1 or data[9] > @intFromEnum(schema_mod.StorageMode.relational) or
            data[10] > @intFromEnum(IndexState.failed) or data[11] > 1 or row_count > 1)
            return error.InvalidTableCatalog;
        const catalog: Catalog = .{
            .mode_initialized = data[8] == 1,
            .storage_mode = @enumFromInt(data[9]),
            .index_state = @enumFromInt(data[10]),
            .reconciled = data[11] == 1,
            .active_schema_version = std.mem.readInt(u32, data[12..16], .little),
            .schema_format_version = std.mem.readInt(u32, data[16..20], .little),
            .row_format_version = std.mem.readInt(u32, data[20..24], .little),
            .row_count = row_count,
            .generation = std.mem.readInt(u64, data[32..40], .little),
            .transaction_admission_bytes = std.mem.readInt(u64, data[40..48], .little),
        };
        if (catalog.transaction_admission_bytes == 0) return error.InvalidTableCatalog;
        if (catalog.schema_format_version != schema_mod.storage_format_version or
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
