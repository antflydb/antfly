// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Persisted source-artifact ownership, independent of ANN serving options.
const std = @import("std");

pub const DenseEmbeddings = enum {
    primary_lsm,
    vector_store,
};

pub const TransactionRecovery = struct {
    protocol_version: u32,
    max_count: u64,
    max_bytes: u64,
    max_transaction_bytes: u64,
    completion_protocol_version: u32 = 0,
    profile_version: u32 = 0,

    pub fn validate(self: @This()) !void {
        if (self.protocol_version != 1 or self.max_count == 0 or self.max_count > 65536 or
            self.max_transaction_bytes < 4096 or self.max_transaction_bytes > self.max_bytes or
            self.max_bytes > (1 << 40)) return error.InvalidTableStorageSettings;
        if (!((self.completion_protocol_version == 0 and self.profile_version == 0) or
            (self.completion_protocol_version == 1 and self.profile_version == 1))) return error.InvalidTableStorageSettings;
    }

    pub fn requiresDurableCompletion(self: @This()) bool {
        return self.completion_protocol_version != 0;
    }

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.beginObject();
        inline for (.{ "protocol_version", "max_count", "max_bytes", "max_transaction_bytes" }) |field| {
            try jw.objectField(field);
            try jw.write(@field(self, field));
        }
        if (self.completion_protocol_version != 0 or self.profile_version != 0) {
            try jw.objectField("completion_protocol_version");
            try jw.write(self.completion_protocol_version);
            try jw.objectField("profile_version");
            try jw.write(self.profile_version);
        }
        try jw.endObject();
    }
};

pub const Settings = struct {
    // Compatibility default for persisted records, not fresh-table admission.
    dense_embeddings: DenseEmbeddings = .primary_lsm,
    transaction_recovery: ?TransactionRecovery = null,

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("dense_embeddings");
        try jw.write(self.dense_embeddings);
        // Preserve old persisted/provisioning JSON for inactive tables.
        if (self.transaction_recovery) |policy| {
            try jw.objectField("transaction_recovery");
            try jw.write(policy);
        }
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
            if (std.mem.eql(u8, field.key_ptr.*, "transaction_recovery")) {
                if (field.value_ptr.* == .null) continue;
                if (field.value_ptr.* != .object) return error.InvalidTableStorageSettings;
                const object = field.value_ptr.object;
                var policy_fields = object.iterator();
                while (policy_fields.next()) |entry| {
                    const known = std.mem.eql(u8, entry.key_ptr.*, "protocol_version") or std.mem.eql(u8, entry.key_ptr.*, "max_count") or
                        std.mem.eql(u8, entry.key_ptr.*, "max_bytes") or std.mem.eql(u8, entry.key_ptr.*, "max_transaction_bytes") or
                        std.mem.eql(u8, entry.key_ptr.*, "completion_protocol_version") or std.mem.eql(u8, entry.key_ptr.*, "profile_version");
                    if (!known) return error.InvalidTableStorageSettings;
                }
                const version = try positiveInteger(object.get("protocol_version") orelse return error.InvalidTableStorageSettings);
                if (version > std.math.maxInt(u32)) return error.InvalidTableStorageSettings;
                result.transaction_recovery = .{
                    .protocol_version = @intCast(version),
                    .max_count = try positiveInteger(object.get("max_count") orelse return error.InvalidTableStorageSettings),
                    .max_bytes = try positiveInteger(object.get("max_bytes") orelse return error.InvalidTableStorageSettings),
                    .max_transaction_bytes = try positiveInteger(object.get("max_transaction_bytes") orelse return error.InvalidTableStorageSettings),
                    .completion_protocol_version = try optionalVersion(object.get("completion_protocol_version")),
                    .profile_version = try optionalVersion(object.get("profile_version")),
                };
                try result.transaction_recovery.?.validate();
                continue;
            }
            if (!std.mem.eql(u8, field.key_ptr.*, "dense_embeddings"))
                return error.InvalidTableStorageSettings;
            if (field.value_ptr.* != .string) return error.InvalidTableStorageSettings;
            result.dense_embeddings = std.meta.stringToEnum(DenseEmbeddings, field.value_ptr.string) orelse
                return error.InvalidTableStorageSettings;
        }
        return result;
    }

    fn positiveInteger(value: std.json.Value) !u64 {
        if (value != .integer or value.integer <= 0) return error.InvalidTableStorageSettings;
        return @intCast(value.integer);
    }

    fn optionalVersion(value: ?std.json.Value) !u32 {
        const v = value orelse return 0;
        if (v != .integer or v.integer < 0 or v.integer > std.math.maxInt(u32)) return error.InvalidTableStorageSettings;
        return @intCast(v.integer);
    }

    pub fn validateStandalone(self: Settings, num_shards: u32, replicated: bool, external_storage: bool) !void {
        if (self.transaction_recovery) |policy| try policy.validate();
        if (self.dense_embeddings == .primary_lsm) return;
        if (num_shards != 1 or replicated or external_storage)
            return error.VectorStoreRequiresLocalSingleShardTable;
    }
};

test "workload admission table completion versions preserve legacy spelling and require paired support" {
    const alloc = std.testing.allocator;
    const legacy = "{\"protocol_version\":1,\"max_count\":4,\"max_bytes\":65536,\"max_transaction_bytes\":8192}";
    var old = try std.json.parseFromSlice(TransactionRecovery, alloc, legacy, .{});
    defer old.deinit();
    try old.value.validate();
    try std.testing.expect(!old.value.requiresDurableCompletion());
    const encoded = try std.json.Stringify.valueAlloc(alloc, old.value, .{});
    defer alloc.free(encoded);
    try std.testing.expectEqualStrings(legacy, encoded);
    var policy = old.value;
    policy.completion_protocol_version = 1;
    try std.testing.expectError(error.InvalidTableStorageSettings, policy.validate());
    policy.profile_version = 1;
    try policy.validate();
    try std.testing.expect(policy.requiresDurableCompletion());
    const persisted = try std.json.Stringify.valueAlloc(alloc, Settings{ .transaction_recovery = policy }, .{});
    defer alloc.free(persisted);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, persisted, .{});
    defer parsed.deinit();
    try std.testing.expectEqualDeep(policy, (try Settings.parse(parsed.value)).transaction_recovery.?);
    policy.profile_version = 2;
    try std.testing.expectError(error.InvalidTableStorageSettings, policy.validate());
}

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

test "workload admission table recovery activation is explicit and bounded" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"transaction_recovery":{"protocol_version":1,"max_count":4,"max_bytes":65536,"max_transaction_bytes":8192}}
    , .{});
    defer parsed.deinit();
    const settings = try Settings.parse(parsed.value);
    try std.testing.expectEqual(@as(u64, 8192), settings.transaction_recovery.?.max_transaction_bytes);
    try settings.validateStandalone(3, true, false);
    var invalid = settings.transaction_recovery.?;
    invalid.protocol_version = 2;
    try std.testing.expectError(error.InvalidTableStorageSettings, invalid.validate());
    invalid = settings.transaction_recovery.?;
    invalid.max_transaction_bytes = 65537;
    try std.testing.expectError(error.InvalidTableStorageSettings, invalid.validate());
    invalid = settings.transaction_recovery.?;
    invalid.max_count = 65537;
    try std.testing.expectError(error.InvalidTableStorageSettings, invalid.validate());
}

test "workload admission inactive recovery preserves storage settings wire compatibility" {
    const raw = try std.json.Stringify.valueAlloc(std.testing.allocator, Settings{}, .{});
    defer std.testing.allocator.free(raw);
    try std.testing.expectEqualStrings("{\"dense_embeddings\":\"primary_lsm\"}", raw);
}
