// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Persisted source-artifact ownership, independent of ANN serving options.
const std = @import("std");

pub const DenseEmbeddings = enum {
    primary_lsm,
    vector_store,
};

pub const Settings = struct {
    dense_embeddings: DenseEmbeddings = .primary_lsm,

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
