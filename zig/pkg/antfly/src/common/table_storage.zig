// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Persisted source-artifact ownership, independent of ANN serving options.
const std = @import("std");

pub const DenseEmbeddings = enum {
    primary_lsm,
    vector_store,
};

pub const Settings = struct {
    // Compatibility default for persisted records, not fresh-table admission.
    dense_embeddings: DenseEmbeddings = .primary_lsm,

    pub fn resolveStandaloneCreate(requested: ?Settings, num_shards: u32, replicated: bool, external_storage: bool) !Settings {
        const settings = requested orelse if (num_shards == 1 and !replicated and !external_storage)
            Settings{ .dense_embeddings = .vector_store }
        else
            Settings{};
        try settings.validateStandalone(num_shards, replicated, external_storage);
        return settings;
    }

    pub fn parse(value: std.json.Value) !Settings {
        if (value != .object) return error.InvalidTableStorageSettings;
        var result: Settings = .{};
        var fields = value.object.iterator();
        while (fields.next()) |field| {
            if (!std.mem.eql(u8, field.key_ptr.*, "dense_embeddings"))
                return error.InvalidTableStorageSettings;
            if (field.value_ptr.* != .string) return error.InvalidTableStorageSettings;
            result.dense_embeddings = std.meta.stringToEnum(DenseEmbeddings, field.value_ptr.string) orelse
                return error.InvalidTableStorageSettings;
        }
        return result;
    }

    pub fn validateStandalone(self: Settings, num_shards: u32, replicated: bool, external_storage: bool) !void {
        if (self.dense_embeddings == .primary_lsm) return;
        if (num_shards != 1 or replicated or external_storage)
            return error.VectorStoreRequiresLocalSingleShardTable;
    }
};

test "table storage settings reject malformed and unknown ownership" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "null", "[]", "true", "{\"dense_embeddings\":null}", "{\"dense_embeddings\":\"typo\"}", "{\"dense_embedding\":\"vector_store\"}" }) |raw| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidTableStorageSettings, Settings.parse(parsed.value));
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"dense_embeddings\":\"vector_store\"}", .{});
    defer parsed.deinit();
    const settings = try Settings.parse(parsed.value);
    try std.testing.expectEqual(DenseEmbeddings.vector_store, settings.dense_embeddings);
    try settings.validateStandalone(1, false, false);
    try std.testing.expectError(error.VectorStoreRequiresLocalSingleShardTable, settings.validateStandalone(2, false, false));
    try std.testing.expectError(error.VectorStoreRequiresLocalSingleShardTable, settings.validateStandalone(1, true, false));
    try std.testing.expectError(error.VectorStoreRequiresLocalSingleShardTable, settings.validateStandalone(1, false, true));
}

test "table storage creation policy preserves explicit choices and legacy records" {
    try std.testing.expectEqual(.vector_store, (try Settings.resolveStandaloneCreate(null, 1, false, false)).dense_embeddings);
    try std.testing.expectEqual(.primary_lsm, (try Settings.resolveStandaloneCreate(.{}, 1, false, false)).dense_embeddings);
    try std.testing.expectEqual(.primary_lsm, (try Settings.resolveStandaloneCreate(null, 2, false, false)).dense_embeddings);
    try std.testing.expectEqual(.primary_lsm, (try Settings.resolveStandaloneCreate(null, 1, true, false)).dense_embeddings);
    try std.testing.expectEqual(.primary_lsm, (try Settings.resolveStandaloneCreate(null, 1, false, true)).dense_embeddings);
    const source = Settings{ .dense_embeddings = .vector_store };
    try std.testing.expectError(error.VectorStoreRequiresLocalSingleShardTable, Settings.resolveStandaloneCreate(source, 2, false, false));
    try std.testing.expectError(error.VectorStoreRequiresLocalSingleShardTable, Settings.resolveStandaloneCreate(source, 1, true, false));
    try std.testing.expectError(error.VectorStoreRequiresLocalSingleShardTable, Settings.resolveStandaloneCreate(source, 1, false, true));
    var legacy = try std.json.parseFromSlice(Settings, std.testing.allocator, "{}", .{});
    defer legacy.deinit();
    try std.testing.expectEqual(.primary_lsm, legacy.value.dense_embeddings);
}
