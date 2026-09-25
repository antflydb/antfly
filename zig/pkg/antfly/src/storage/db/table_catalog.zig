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
pub const encoded_len: usize = 64;
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
    /// Replicated bounds on outstanding completion obligations, including
    /// unresolved coordinator notifications. Activation requires compatible
    /// peers; a local setting cannot fence an older live Raft state machine.
    /// Zero preserves the legacy policy until that activation boundary exists.
    transaction_recovery_max_count: u64 = 0,
    transaction_recovery_max_bytes: u64 = 0,

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
        std.mem.writeInt(u64, out[48..56], self.transaction_recovery_max_count, .little);
        std.mem.writeInt(u64, out[56..64], self.transaction_recovery_max_bytes, .little);
        return out;
    }

    /// Disabled tables keep the exact old wire representation. Merely running
    /// an upgraded binary must not make normal tables unreadable on old peers.
    pub fn encodeForPersistence(self: Catalog, buffer: *[encoded_len]u8) []const u8 {
        buffer.* = self.encode();
        if (self.transaction_recovery_max_count == 0 and self.transaction_recovery_max_bytes == 0) {
            std.mem.writeInt(u32, buffer[4..8], 2, .little);
            return buffer[0..48];
        }
        return buffer;
    }

    pub fn decode(data: []const u8) !Catalog {
        if ((data.len != encoded_len and data.len != 48) or !std.mem.eql(u8, data[0..4], magic)) return error.InvalidTableCatalog;
        const version = std.mem.readInt(u32, data[4..8], .little);
        if (version != 2 and version != format_version) return error.UnsupportedTableCatalogVersion;
        if ((version == 2 and data.len != 48) or (version == format_version and data.len != encoded_len)) return error.InvalidTableCatalog;
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
            .transaction_recovery_max_count = if (version == 2) 0 else std.mem.readInt(u64, data[48..56], .little),
            .transaction_recovery_max_bytes = if (version == 2) 0 else std.mem.readInt(u64, data[56..64], .little),
        };
        if (catalog.transaction_admission_bytes == 0) return error.InvalidTableCatalog;
        if ((catalog.transaction_recovery_max_count == 0) != (catalog.transaction_recovery_max_bytes == 0)) return error.InvalidTableCatalog;
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

test "workload admission catalog recovery policy preserves explicit legacy disablement" {
    const catalog: Catalog = .{ .transaction_recovery_max_count = 7, .transaction_recovery_max_bytes = 32768 };
    const encoded = catalog.encode();
    const decoded = try Catalog.decode(&encoded);
    try std.testing.expectEqual(@as(u64, 7), decoded.transaction_recovery_max_count);
    try std.testing.expectEqual(@as(u64, 32768), decoded.transaction_recovery_max_bytes);
    var legacy = encoded[0..48].*;
    std.mem.writeInt(u32, legacy[4..8], 2, .little);
    const old = try Catalog.decode(&legacy);
    try std.testing.expectEqual(@as(u64, 0), old.transaction_recovery_max_count);
    try std.testing.expectEqual(@as(u64, 0), old.transaction_recovery_max_bytes);
    const upgraded = try Catalog.decode(&old.encode());
    try std.testing.expectEqual(@as(u64, 0), upgraded.transaction_recovery_max_count);
}

test "workload admission disabled catalog policy preserves old on-disk format" {
    var buffer: [encoded_len]u8 = undefined;
    const old = (Catalog{}).encodeForPersistence(&buffer);
    try std.testing.expectEqual(@as(usize, 48), old.len);
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, old[4..8], .little));
    try std.testing.expectEqual(Catalog{}, try Catalog.decode(old));
    const active: Catalog = .{ .transaction_recovery_max_count = 2, .transaction_recovery_max_bytes = 8192 };
    const current = active.encodeForPersistence(&buffer);
    try std.testing.expectEqual(@as(usize, 64), current.len);
    try std.testing.expectEqual(active, try Catalog.decode(current));
}
