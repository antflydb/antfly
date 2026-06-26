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
const scraping = @import("antfly_scraping");

const common_secrets = @import("../../common/secrets.zig");
const metadata_table_provisioner = @import("../../metadata/table_provisioner.zig");
const asset_producer_mod = @import("../../storage/db/enrichment/asset_producer.zig");
const asset_producer_runtime = @import("../../asset_producer_runtime.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const db_mod = @import("../../storage/db/mod.zig");
const db_embedder = @import("../../storage/db/enrichment/embedder.zig");
const doc_identity = @import("../../storage/db/doc_identity.zig");
const hbc_mod = @import("../../storage/hbc_adapter.zig");
const lmdb = @import("../../storage/lmdb.zig");
const lsm_backend = @import("../../storage/lsm_backend/mod.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");
const resource_manager_mod = @import("../../storage/resource_manager.zig");
const table_catalog = @import("../table_catalog.zig");
const tables_api = @import("../tables.zig");

const local_schema_json_key = db_mod.local_schema_json_key;

pub const ManagedDbOpenMode = enum {
    default,
    default_async,
    writer_no_replay,
    startup_catch_up,
    restore_repair,
    query_readonly,
    status_only,
};

pub const ManagedDbOpenOptions = struct {
    drain_resolver_backfill: bool = true,
    ha_write_gate: ?db_mod.HAWriteGate = null,
    ha_async_effect_mirror: ?db_mod.HAAsyncEffectMirror = null,
    ha_async_batch_mirror: ?db_mod.HAAsyncBatchMirror = null,
    ha_async_metadata_mirror: ?db_mod.HAAsyncMetadataMirror = null,
};

pub const TableManagedMetadata = struct {
    indexes_json: ?[]u8,
    schema_json: ?[]u8,

    pub fn deinit(self: TableManagedMetadata, alloc: std.mem.Allocator) void {
        if (self.indexes_json) |value| alloc.free(value);
        if (self.schema_json) |value| alloc.free(value);
    }
};

pub fn haMirrorForManagedDbOpenMode(mode: ManagedDbOpenMode, mirror: ?db_mod.HAAsyncEffectMirror) ?db_mod.HAAsyncEffectMirror {
    return switch (mode) {
        .default, .default_async, .writer_no_replay => mirror,
        .startup_catch_up, .restore_repair, .query_readonly, .status_only => null,
    };
}

pub fn loadLocalTableSchemaJson(alloc: std.mem.Allocator, db: *db_mod.DB) !?[]u8 {
    return db.core.store.get(alloc, local_schema_json_key) catch |err| switch (err) {
        lmdb.Error.NotFound => null,
        else => return err,
    };
}

pub fn applyLocalTableSchemaJson(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema_json: []const u8,
) !void {
    try db.applyTableSchemaJson(alloc, schema_json, .{});
}

pub fn drainManagedDbBeforeClose(db: *db_mod.DB) !void {
    // Provisioned writes open a managed DB per request, so queued enrichment
    // must drain before the DB is closed or semantic indexes can stay empty.
    try db.runUntilIdle();
    try db.core.index_manager.syncAll(false);
}

pub fn isTransientReplayVisibilityError(err: anyerror) bool {
    return err == error.WriterLocked or err == error.ReplayDocumentNotVisible;
}

pub fn loadTableIndexesJson(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?[]u8 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    return try alloc.dupe(u8, table.indexes_json);
}

pub fn loadTableManagedMetadata(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?TableManagedMetadata {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    const indexes_json = try alloc.dupe(u8, table.indexes_json);
    errdefer alloc.free(indexes_json);
    const schema_json = try alloc.dupe(u8, table.schema_json);
    return .{
        .indexes_json = indexes_json,
        .schema_json = schema_json,
    };
}

pub fn loadTableIdentityNamespaceForGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
) !?doc_identity.Namespace {
    _ = alloc;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    for (snapshot.ranges) |range| {
        if (range.table_id != table.table_id or range.group_id != group_id) continue;
        return .{
            .table_id = table.table_id,
            .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
            .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
        };
    }
    return null;
}

pub fn findTableRecord(tables: []const metadata_table_manager.TableRecord, table_id: u64) ?metadata_table_manager.TableRecord {
    for (tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

pub fn findRangeRecord(ranges: []const metadata_table_manager.RangeRecord, group_id: u64) ?metadata_table_manager.RangeRecord {
    for (ranges) |range| {
        if (range.group_id == group_id) return range;
    }
    return null;
}

pub fn validateProvisionedDbIdentityNamespaceExpected(expected: ?doc_identity.Namespace, db: *const db_mod.DB) !void {
    const namespace = expected orelse return;
    if (!db.core.identity_namespace.eql(namespace)) return error.DocIdentityNamespaceMismatch;
}

pub fn validateProvisionedDbIdentityNamespace(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    db: *const db_mod.DB,
) !void {
    const expected = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    try validateProvisionedDbIdentityNamespaceExpected(expected, db);
}

pub fn loadTableSchemaJson(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?[]u8 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    return try alloc.dupe(u8, table.schema_json);
}

pub fn validateTableWritesAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    writes: anytype,
) !void {
    if (writes.len == 0) return;
    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse return;
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, writes);
}

fn freeOwnedBatchWrites(alloc: std.mem.Allocator, writes: []const db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(@constCast(writes));
}

const SchemaValidationWriteState = struct {
    const Kind = enum { write, delete };

    const Entry = struct {
        key: []u8,
        kind: Kind,
        value: ?[]u8 = null,

        fn deinit(self: *Entry, alloc: std.mem.Allocator) void {
            alloc.free(self.key);
            if (self.value) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    entries: std.ArrayListUnmanaged(Entry) = .empty,

    fn deinit(self: *SchemaValidationWriteState, alloc: std.mem.Allocator) void {
        for (self.entries.items) |*entry| entry.deinit(alloc);
        self.entries.deinit(alloc);
        self.* = undefined;
    }

    fn findIndex(self: *const SchemaValidationWriteState, key: []const u8) ?usize {
        for (self.entries.items, 0..) |entry, i| {
            if (std.mem.eql(u8, entry.key, key)) return i;
        }
        return null;
    }

    fn applyBorrowedWrite(self: *SchemaValidationWriteState, alloc: std.mem.Allocator, key: []const u8, value: []const u8) !void {
        const owned_value = try alloc.dupe(u8, value);
        errdefer alloc.free(owned_value);
        if (self.findIndex(key)) |idx| {
            const entry = &self.entries.items[idx];
            if (entry.value) |old_value| alloc.free(old_value);
            entry.kind = .write;
            entry.value = owned_value;
            return;
        }

        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        try self.entries.append(alloc, .{
            .key = owned_key,
            .kind = .write,
            .value = owned_value,
        });
    }

    fn applyDelete(self: *SchemaValidationWriteState, alloc: std.mem.Allocator, key: []const u8) !void {
        if (self.findIndex(key)) |idx| {
            const entry = &self.entries.items[idx];
            if (entry.value) |old_value| alloc.free(old_value);
            entry.kind = .delete;
            entry.value = null;
            return;
        }

        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        try self.entries.append(alloc, .{
            .key = owned_key,
            .kind = .delete,
        });
    }

    fn applyOwnedWrite(self: *SchemaValidationWriteState, alloc: std.mem.Allocator, key: []const u8, value: []u8) !void {
        if (self.findIndex(key)) |idx| {
            const entry = &self.entries.items[idx];
            if (entry.value) |old_value| alloc.free(old_value);
            entry.kind = .write;
            entry.value = value;
            return;
        }

        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        try self.entries.append(alloc, .{
            .key = owned_key,
            .kind = .write,
            .value = value,
        });
    }

    fn baseValue(self: *const SchemaValidationWriteState, key: []const u8) ?[]const u8 {
        const idx = self.findIndex(key) orelse return null;
        const entry = self.entries.items[idx];
        return switch (entry.kind) {
            .write => entry.value.?,
            .delete => null,
        };
    }

    fn hasRequestState(self: *const SchemaValidationWriteState, key: []const u8) bool {
        return self.findIndex(key) != null;
    }

    fn toOwnedWrites(self: *const SchemaValidationWriteState, alloc: std.mem.Allocator) ![]db_mod.types.BatchWrite {
        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.kind == .write) count += 1;
        }

        var out = try alloc.alloc(db_mod.types.BatchWrite, count);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            alloc.free(out);
        }

        var i: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.kind != .write) continue;
            {
                const owned_key = try alloc.dupe(u8, entry.key);
                errdefer alloc.free(owned_key);
                const owned_value = try alloc.dupe(u8, entry.value.?);
                errdefer alloc.free(owned_value);
                out[i] = .{
                    .key = owned_key,
                    .value = owned_value,
                };
            }
            filled += 1;
            i += 1;
        }
        return out;
    }
};

fn resolveWritesForSchemaValidation(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    base_writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) ![]db_mod.types.BatchWrite {
    var state = SchemaValidationWriteState{};
    defer state.deinit(alloc);

    for (base_writes) |write| {
        try state.applyBorrowedWrite(alloc, write.key, write.value);
    }

    for (deletes) |key| {
        try state.applyDelete(alloc, key);
    }

    for (transforms) |transform| {
        const has_request_state = state.hasRequestState(transform.key);
        const existing_from_request = state.baseValue(transform.key);
        const existing_from_db = if (!has_request_state) try db.get(alloc, transform.key) else null;
        defer if (existing_from_db) |body| alloc.free(body);
        const existing = existing_from_request orelse existing_from_db;
        const resolved = db_mod.transform.resolveDocumentTransform(alloc, existing, transform) catch |err| switch (err) {
            error.InvalidArgument => return error.InvalidBatchRequest,
            else => return err,
        } orelse continue;

        state.applyOwnedWrite(alloc, transform.key, resolved) catch |err| {
            alloc.free(resolved);
            return err;
        };
    }

    return try state.toOwnedWrites(alloc);
}

fn transactionWritesToBatchWrites(
    alloc: std.mem.Allocator,
    writes: []const db_mod.types.TransactionWrite,
) ![]db_mod.types.BatchWrite {
    var out = try alloc.alloc(db_mod.types.BatchWrite, writes.len);
    for (writes, 0..) |write, i| {
        out[i] = .{
            .key = write.key,
            .value = write.value,
        };
    }
    return out;
}

pub fn validateTableBatchAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (writes.len == 0 and deletes.len == 0 and transforms.len == 0) return;
    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse return;
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return;

    const effective_writes = try resolveWritesForSchemaValidation(alloc, db, writes, deletes, transforms);
    defer freeOwnedBatchWrites(alloc, effective_writes);
    if (effective_writes.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, effective_writes);
}

pub fn validateTransactionAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    writes: []const db_mod.types.TransactionWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    const batch_writes = try transactionWritesToBatchWrites(alloc, writes);
    defer alloc.free(batch_writes);
    try validateTableBatchAgainstLocalSchema(alloc, db, batch_writes, deletes, transforms);
}

pub fn validateTableWritesAgainstCatalogSchema(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    writes: anytype,
) !void {
    if (writes.len == 0) return;
    const schema_json = (try loadTableSchemaJson(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, writes);
}

pub fn validateTableBatchAgainstCatalogSchema(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    db: *db_mod.DB,
    table_name: []const u8,
    writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (writes.len == 0 and deletes.len == 0 and transforms.len == 0) return;
    const schema_json = (try loadTableSchemaJson(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    try validateTableBatchAgainstSchemaJson(alloc, db, schema_json, writes, deletes, transforms);
}

pub fn validateTableBatchAgainstSchemaJson(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema_json: ?[]const u8,
    writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (writes.len == 0 and deletes.len == 0 and transforms.len == 0) return;
    const raw_schema_json = schema_json orelse return;
    if (raw_schema_json.len == 0) return;

    const effective_writes = try resolveWritesForSchemaValidation(alloc, db, writes, deletes, transforms);
    defer freeOwnedBatchWrites(alloc, effective_writes);
    if (effective_writes.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, raw_schema_json);
    defer parsed_schema.deinit(alloc);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, effective_writes);
}

pub fn validateTransactionAgainstCatalogSchema(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    db: *db_mod.DB,
    table_name: []const u8,
    writes: []const db_mod.types.TransactionWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    const batch_writes = try transactionWritesToBatchWrites(alloc, writes);
    defer alloc.free(batch_writes);
    try validateTableBatchAgainstCatalogSchema(alloc, catalog, db, table_name, batch_writes, deletes, transforms);
}

pub fn openManagedDbForTableWithCacheAndRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse return try db_mod.DB.open(alloc, path, .{
        .lsm_cache = lsm_cache,
        .hbc_cache = hbc_cache,
        .lsm_root_generation = lsm_root_generation,
        .resource_manager = resource_manager,
        .backend_runtime = backend_runtime,
    });
    defer alloc.free(indexes_json);

    return try openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(alloc, path, indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, backend_runtime);
}

pub fn openManagedDbForTableGroupWithCacheAndRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbForTableGroupWithCacheAndRuntimeAndHAWriteGate(alloc, path, catalog, table_name, group_id, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, backend_runtime, null, null);
}

pub fn openManagedDbForTableGroupWithCacheAndRuntimeAndHAWriteGate(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    ha_write_gate: ?db_mod.HAWriteGate,
    ha_async_mirror: ?db_mod.HAAsyncEffectMirror,
) !db_mod.DB {
    const effective_ha_mirror = haMirrorForManagedDbOpenMode(.default, ha_async_mirror);
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    const metadata = try loadTableManagedMetadata(alloc, catalog, table_name);
    defer if (metadata) |managed| {
        if (managed.indexes_json) |value| alloc.free(value);
        if (managed.schema_json) |value| alloc.free(value);
    };
    const indexes_json = if (metadata) |managed| managed.indexes_json else null;
    const schema_json = if (metadata) |managed| managed.schema_json else null;
    if (indexes_json == null) {
        var db = try db_mod.DB.open(alloc, path, .{
            .lsm_cache = lsm_cache,
            .hbc_cache = hbc_cache,
            .lsm_root_generation = lsm_root_generation,
            .resource_manager = resource_manager,
            .backend_runtime = backend_runtime,
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = identity_namespace != null,
            .ha_write_gate = ha_write_gate,
            .ha_async_effect_mirror = effective_ha_mirror,
            .ha_async_batch_mirror = effective_ha_mirror,
            .ha_async_metadata_mirror = effective_ha_mirror,
        });
        errdefer db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
        if (schema_json) |value| {
            if (value.len > 0) try applyLocalTableSchemaJson(alloc, &db, value);
        }
        return db;
    }

    var db = try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
        alloc,
        path,
        indexes_json.?,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .default,
        backend_runtime,
        null,
        null,
        null,
        identity_namespace,
        .{
            .ha_write_gate = ha_write_gate,
            .ha_async_effect_mirror = effective_ha_mirror,
            .ha_async_batch_mirror = effective_ha_mirror,
            .ha_async_metadata_mirror = effective_ha_mirror,
        },
    );
    errdefer db.close();
    if (schema_json) |value| {
        if (value.len > 0) try applyLocalTableSchemaJson(alloc, &db, value);
    }
    return db;
}

pub fn openManagedDbForTableWithIndexesJsonAndCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: ?[]const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
) !db_mod.DB {
    return try openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(alloc, path, indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, null);
}

pub fn openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: ?[]const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    const raw_indexes_json = indexes_json orelse return try db_mod.DB.open(alloc, path, .{
        .lsm_cache = lsm_cache,
        .hbc_cache = hbc_cache,
        .lsm_root_generation = lsm_root_generation,
        .resource_manager = resource_manager,
        .backend_runtime = backend_runtime,
    });
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntime(alloc, path, raw_indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, .default, backend_runtime);
}

pub fn openManagedDbForStatusWithCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse {
        var db = try db_mod.DB.open(alloc, path, .{
            .lsm_cache = lsm_cache,
            .hbc_cache = hbc_cache,
            .lsm_root_generation = lsm_root_generation,
            .resource_manager = resource_manager,
            .backend_runtime = backend_runtime,
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .transaction_recovery = .{ .enabled = false },
            .text_merge = .{ .enabled = false },
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = identity_namespace != null,
        });
        errdefer db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
        return db;
    };
    defer alloc.free(indexes_json);

    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .status_only,
        backend_runtime,
        identity_namespace,
    );
}

pub fn openManagedDbForStatusWithIndexesJsonAndCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntime(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .status_only,
        backend_runtime,
    );
}

pub fn openManagedDbWithIndexesJsonAndCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheMode(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .default,
    );
}

pub fn openManagedDbWithIndexesJsonAndCacheMode(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntime(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        null,
    );
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        null,
    );
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    identity_namespace: ?doc_identity.Namespace,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        null,
        null,
        null,
        identity_namespace,
    );
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntfly(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        antfly_provider,
        secret_store,
        remote_content,
        null,
    );
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    identity_namespace: ?doc_identity.Namespace,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        antfly_provider,
        secret_store,
        remote_content,
        identity_namespace,
        .{},
    );
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    identity_namespace: ?doc_identity.Namespace,
    options: ManagedDbOpenOptions,
) !db_mod.DB {
    const EnrichmentSet = struct {
        dense: ?db_embedder.DenseEmbedder = null,
        sparse: ?db_embedder.SparseEmbedder = null,
        asset_runtime: ?*asset_producer_runtime.Runtime = null,
        generated: bool = false,

        fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            if (self.dense) |owned| owned.deinit(allocator);
            if (self.sparse) |owned| owned.deinit(allocator);
            if (self.asset_runtime) |runtime| {
                runtime.deinit();
                allocator.destroy(runtime);
            }
        }

        fn enabled(self: @This()) bool {
            return self.dense != null or self.sparse != null or self.asset_runtime != null or self.generated;
        }

        fn config(self: @This()) db_mod.enrichment_runtime.Config {
            return .{
                .dense_embedder = self.dense,
                .sparse_embedder = self.sparse,
                .asset_producer = if (self.asset_runtime) |runtime| runtime.ownedProducer() else null,
                .enable_without_producers = self.generated,
            };
        }
    };

    const createEnrichments = struct {
        fn run(
            allocator: std.mem.Allocator,
            raw_indexes_json: []const u8,
            runtime: ?*db_mod.background_runtime.BackendRuntime,
            local_provider: ?managed_embedder.AntflyProvider,
            store: ?*common_secrets.FileStore,
            remote: ?*const scraping.RemoteContentConfig,
        ) !EnrichmentSet {
            const asset_runtime = if (try indexesJsonNeedsAssetProducer(allocator, raw_indexes_json)) blk: {
                const io = if (runtime) |backend| backend.io() orelse return error.MissingBackendRuntimeIo else return error.MissingBackendRuntimeIo;
                break :blk try asset_producer_runtime.Runtime.createOwned(allocator, io, .{
                    .antfly_provider = local_provider,
                    .secret_store = store,
                });
            } else null;
            errdefer if (asset_runtime) |owned| {
                owned.deinit();
                allocator.destroy(owned);
            };
            return .{
                .dense = try managed_embedder.ManagedEmbedder.createDenseEmbedderWithOptions(allocator, raw_indexes_json, .{ .antfly_provider = local_provider, .secret_store = store, .remote_content = remote }),
                .sparse = try managed_embedder.ManagedEmbedder.createSparseEmbedderWithOptions(allocator, raw_indexes_json, .{ .antfly_provider = local_provider, .secret_store = store, .remote_content = remote }),
                .asset_runtime = asset_runtime,
                .generated = try indexesJsonHasGeneratedEnrichment(allocator, raw_indexes_json),
            };
        }
    }.run;

    var enrichments = if (mode == .startup_catch_up)
        EnrichmentSet{}
    else
        try createEnrichments(alloc, indexes_json, backend_runtime, antfly_provider, secret_store, remote_content);
    errdefer enrichments.deinit(alloc);

    const openDb = struct {
        fn run(
            allocator: std.mem.Allocator,
            db_path: []const u8,
            enrichment_cfg: ?db_mod.enrichment_runtime.Config,
            cache: ?*lsm_backend.Cache,
            vector_cache: ?*hbc_mod.Cache,
            root_generation: u64,
            manager: ?*resource_manager_mod.ResourceManager,
            open_mode: ManagedDbOpenMode,
            runtime: ?*db_mod.background_runtime.BackendRuntime,
            store: ?*common_secrets.FileStore,
            remote: ?*const scraping.RemoteContentConfig,
            namespace: ?doc_identity.Namespace,
            open_options: ManagedDbOpenOptions,
        ) !db_mod.DB {
            const base: db_mod.OpenOptions = .{
                .lsm_cache = cache,
                .hbc_cache = vector_cache,
                .lsm_root_generation = root_generation,
                .resource_manager = manager,
                .backend_runtime = runtime,
                .secret_store = store,
                .remote_content = remote,
                .identity_namespace = namespace,
                .prefer_existing_identity_namespace = namespace != null,
                .enrichment = enrichment_cfg,
                .ha_write_gate = open_options.ha_write_gate,
                .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
            };
            return switch (open_mode) {
                .default => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, base)
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                        .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                        .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
                    }),
                .default_async, .writer_no_replay => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                        .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                        .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
                        .open_mode = .writer_no_replay,
                        .index_open_parallelism = 1,
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                        .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                        .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
                        .open_mode = .writer_no_replay,
                        .index_open_parallelism = 1,
                    }),
                .startup_catch_up => try db_mod.DB.open(allocator, db_path, .{
                    .lsm_cache = cache,
                    .hbc_cache = vector_cache,
                    .lsm_root_generation = root_generation,
                    .resource_manager = manager,
                    .backend_runtime = runtime,
                    .secret_store = store,
                    .remote_content = remote,
                    .identity_namespace = namespace,
                    .prefer_existing_identity_namespace = namespace != null,
                    .ha_write_gate = open_options.ha_write_gate,
                    .open_mode = .writer_no_replay,
                    .start_index_workers = false,
                    .start_optional_runtimes = false,
                    .ttl_cleanup = .{ .enabled = false },
                    .transaction_recovery = .{ .enabled = false },
                    .text_merge = .{ .enabled = false },
                }),
                .restore_repair => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .writer_no_replay,
                        .start_index_workers = true,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .writer_no_replay,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    }),
                .query_readonly => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .query_readonly,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .query_readonly,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    }),
                .status_only => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .status_only,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .status_only,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    }),
            };
        }
    }.run;

    var db = blk: {
        const enrichment_cfg = if (enrichments.enabled()) enrichments.config() else null;
        const opened = try openDb(alloc, path, enrichment_cfg, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, secret_store, remote_content, identity_namespace, options);
        enrichments.dense = null;
        enrichments.sparse = null;
        enrichments.asset_runtime = null;
        break :blk opened;
    };
    var db_open = true;
    errdefer if (db_open) db.close();

    try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
    if (mode == .status_only) return db;

    const summary = try metadata_table_provisioner.reconcileDbIndexesWithOptions(alloc, &db, indexes_json, .{
        .drain_resolver_backfill = options.drain_resolver_backfill,
    });
    if (summary.indexManagerCatalogChanged()) {
        db.close();
        db_open = false;
        enrichments = if (mode == .startup_catch_up)
            EnrichmentSet{}
        else
            try createEnrichments(alloc, indexes_json, backend_runtime, antfly_provider, secret_store, remote_content);
        db = blk: {
            const enrichment_cfg = if (enrichments.enabled()) enrichments.config() else null;
            const opened = try openDb(alloc, path, enrichment_cfg, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, secret_store, remote_content, identity_namespace, options);
            enrichments.dense = null;
            enrichments.sparse = null;
            enrichments.asset_runtime = null;
            break :blk opened;
        };
        db_open = true;
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
    }

    if ((mode == .default or mode == .default_async) and summary.indexes_added > 0) {
        if (db.enrichment_runtime != null) {
            _ = try db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
        }
    }
    return db;
}

pub fn indexesJsonNeedsAssetProducer(alloc: std.mem.Allocator, indexes_json: []const u8) !bool {
    if (indexes_json.len == 0) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    return try jsonValueNeedsAssetProducer(alloc, parsed.value);
}

pub fn indexesJsonHasGeneratedEnrichment(alloc: std.mem.Allocator, indexes_json: []const u8) !bool {
    if (indexes_json.len == 0) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    return try jsonValueHasGeneratedEnrichment(alloc, parsed.value);
}

fn jsonValueHasGeneratedEnrichment(alloc: std.mem.Allocator, value: std.json.Value) anyerror!bool {
    switch (value) {
        .object => |object| {
            if (object.get("kind")) |kind| {
                if (kind == .string and (std.mem.eql(u8, kind.string, "asset") or std.mem.eql(u8, kind.string, "chunk"))) return true;
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (try jsonValueHasGeneratedEnrichment(alloc, entry.value_ptr.*)) return true;
            }
            return false;
        },
        .array => |array| {
            for (array.items) |item| {
                if (try jsonValueHasGeneratedEnrichment(alloc, item)) return true;
            }
            return false;
        },
        .string => |raw| {
            return try jsonStringHasGeneratedEnrichment(alloc, raw);
        },
        else => return false,
    }
}

fn jsonStringHasGeneratedEnrichment(alloc: std.mem.Allocator, raw: []const u8) anyerror!bool {
    const trimmed = std.mem.trim(u8, raw, &std.ascii.whitespace);
    if (!jsonStringLooksStructured(trimmed)) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, trimmed, .{}) catch return false;
    defer parsed.deinit();
    return try jsonValueHasGeneratedEnrichment(alloc, parsed.value);
}

fn jsonValueNeedsAssetProducer(alloc: std.mem.Allocator, value: std.json.Value) anyerror!bool {
    switch (value) {
        .object => |object| {
            if (try objectIsModelBackedAssetEnrichment(alloc, object)) return true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (try jsonValueNeedsAssetProducer(alloc, entry.value_ptr.*)) return true;
            }
            return false;
        },
        .array => |array| {
            for (array.items) |item| {
                if (try jsonValueNeedsAssetProducer(alloc, item)) return true;
            }
            return false;
        },
        .string => |raw| {
            return try jsonStringNeedsAssetProducer(alloc, raw);
        },
        else => return false,
    }
}

fn jsonStringNeedsAssetProducer(alloc: std.mem.Allocator, raw: []const u8) anyerror!bool {
    const trimmed = std.mem.trim(u8, raw, &std.ascii.whitespace);
    if (!jsonStringLooksStructured(trimmed)) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, trimmed, .{}) catch return false;
    defer parsed.deinit();
    return try jsonValueNeedsAssetProducer(alloc, parsed.value);
}

fn jsonStringLooksStructured(trimmed: []const u8) bool {
    return trimmed.len >= 2 and
        ((trimmed[0] == '{' and trimmed[trimmed.len - 1] == '}') or
            (trimmed[0] == '[' and trimmed[trimmed.len - 1] == ']'));
}

fn objectIsModelBackedAssetEnrichment(alloc: std.mem.Allocator, object: std.json.ObjectMap) !bool {
    const kind = object.get("kind") orelse return false;
    if (kind != .string or !std.mem.eql(u8, kind.string, "asset")) return false;
    const producer_value = object.get("producer_json") orelse return false;
    const producer_json = switch (producer_value) {
        .string => |raw| raw,
        .object, .array => try std.json.Stringify.valueAlloc(alloc, producer_value, .{}),
        else => return false,
    };
    const owns_producer_json = producer_value != .string;
    defer if (owns_producer_json) alloc.free(@constCast(producer_json));
    var producer_cfg = asset_producer_mod.parseProducerConfig(alloc, producer_json) catch return false;
    defer producer_cfg.deinit(alloc);
    return producer_cfg.type != .copy;
}

test "provisioning detects model backed graph shorthand assets inside config_json strings" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try indexesJsonNeedsAssetProducer(alloc,
        \\[{
        \\  "name":"relations_graph",
        \\  "kind":"graph",
        \\  "config_json":"{\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"field\":\"body\",\"producer_json\":{\"type\":\"extractor\",\"config\":{\"provider\":\"antfly\"}}}}"
        \\}]
    ));
}

test "provisioning does not require asset producer for copy graph shorthand assets inside config_json strings" {
    const alloc = std.testing.allocator;
    try std.testing.expect(!(try indexesJsonNeedsAssetProducer(alloc,
        \\[{
        \\  "name":"relations_graph",
        \\  "kind":"graph",
        \\  "config_json":"{\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"field\":\"relations\"}}"
        \\}]
    )));
    try std.testing.expect(try indexesJsonHasGeneratedEnrichment(alloc,
        \\[{
        \\  "name":"relations_graph",
        \\  "kind":"graph",
        \\  "config_json":"{\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"field\":\"relations\"}}"
        \\}]
    ));
}
