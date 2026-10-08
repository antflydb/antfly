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

//! Persisted source-artifact ownership, independent of ANN serving options.
const std = @import("std");

pub const Engine = enum { native, object };

pub const DenseEmbeddings = enum {
    primary_lsm,
    vector_store,
};

pub const Settings = struct {
    engine: Engine = .native,

    // Compatibility default for persisted records, not fresh-table admission.
    dense_embeddings: DenseEmbeddings = .primary_lsm,

    pub fn jsonStringify(self: Settings, jw: anytype) !void {
        try jw.beginObject();
        // Keep legacy table JSON stable when the engine is implicit.
        if (self.engine != .native) {
            try jw.objectField("engine");
            try jw.write(self.engine);
        }
        try jw.objectField("dense_embeddings");
        try jw.write(self.dense_embeddings);
        try jw.endObject();
    }

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
            if (std.mem.eql(u8, field.key_ptr.*, "engine")) {
                if (field.value_ptr.* != .string) return error.InvalidTableStorageSettings;
                result.engine = std.meta.stringToEnum(Engine, field.value_ptr.string) orelse return error.InvalidTableStorageSettings;
                continue;
            }
            if (!std.mem.eql(u8, field.key_ptr.*, "dense_embeddings"))
                return error.InvalidTableStorageSettings;
            if (field.value_ptr.* != .string) return error.InvalidTableStorageSettings;
            result.dense_embeddings = std.meta.stringToEnum(DenseEmbeddings, field.value_ptr.string) orelse
                return error.InvalidTableStorageSettings;
        }
        if (result.engine == .object and result.dense_embeddings == .vector_store) return error.InvalidTableStorageSettings;
        return result;
    }

    pub fn validateCreate(self: Settings, num_shards: ?u32, replicated: bool) !void {
        if (self.engine == .object) {
            if (self.dense_embeddings != .primary_lsm) return error.InvalidTableStorageSettings;
            if (num_shards != null or replicated) return error.ObjectTablePlacementUnsupported;
        }
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

test "table storage settings object engine rejects data shard placement and vector ownership" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"engine\":\"object\"}", .{});
    defer parsed.deinit();
    const object = try Settings.parse(parsed.value);
    try std.testing.expectEqual(.object, object.engine);
    try object.validateCreate(null, false);
    try std.testing.expectError(error.ObjectTablePlacementUnsupported, object.validateCreate(1, false));
    try std.testing.expectError(error.ObjectTablePlacementUnsupported, object.validateCreate(null, true));
    try std.testing.expectError(error.InvalidTableStorageSettings, (Settings{ .engine = .object, .dense_embeddings = .vector_store }).validateCreate(null, false));
    const native = try Settings.resolveStandaloneCreate(null, 1, false, false);
    try std.testing.expectEqual(.native, native.engine);
    const explicit = try Settings.resolveStandaloneCreate(object, 1, false, false);
    try std.testing.expectEqual(.primary_lsm, explicit.dense_embeddings);
}
