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

//! Durable logical table ownership for an embedded database. Table IDs never
//! repeat: DROP followed by CREATE cannot expose the previous table's data.
const h = @import("handles.zig");
const api = @import("db.zig");
const std = h.std;
const catalog = @import("../storage/lite/embedded_catalog.zig");
const catalog_key = catalog.key;
const Record = catalog.Record;
const Catalog = catalog.Catalog;
pub const Table = struct { name: []u8, id: u64, namespace: []u8, db: h.db_mod.DB };

fn openTable(handle: *h.Handle, name: []const u8, id: u64) !*Table {
    const alloc = handle.alloc;
    const table = try alloc.create(Table);
    errdefer alloc.destroy(table);
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    const namespace = try catalog.namespaceAlloc(alloc, id);
    errdefer alloc.free(namespace);
    const path = if (handle.owned_lite_backend != null)
        try alloc.dupe(u8, handle.embedded_path orelse return error.UnsupportedSqlExecution)
    else
        try std.fs.path.join(alloc, &.{ handle.embedded_path orelse return error.UnsupportedSqlExecution, namespace });
    defer alloc.free(path);
    var options = handle.embedded_open_options;
    options.open_mode = handle.open_mode;
    options.backend_runtime = handle.db.backend_runtime;
    options.identity_namespace = .{ .table_id = id, .shard_id = id, .range_id = id };
    options.prefer_existing_identity_namespace = false;
    options.online_source_authority = .native;
    // The root restores this namespace's managed providers after open. Do not
    // start a temporary providerless runtime against a persisted replay tail.
    if (handle.embedded_native_providers) options.enrichment = null;
    if (handle.owned_lite_backend) |*backend| try backend.configureDbOpenOptionsForNamespace(&options, namespace);
    table.* = .{ .name = owned_name, .id = id, .namespace = namespace, .db = try h.db_mod.DB.open(alloc, path, options) };
    errdefer table.db.close();
    if (handle.lite_scheduling_owner) |owner|
        table.db.borrowed_group_source_state = try owner.groupSourceState(&table.db);
    @import("entity_sink.zig").attach(handle, &table.db);
    try api.refreshLiteManagedEmbeddingRuntimeForDatabase(handle, &table.db);
    if (handle.embedded_open_options.start_optional_runtime_workers) table.db.startResidentBackgroundWorkersIfNeeded();
    return table;
}

fn destroy(handle: *h.Handle, table: *Table) void {
    if (handle.lease_snapshot) table.db.closeImmutableSnapshot() else table.db.close();
    handle.alloc.free(table.name);
    handle.alloc.free(table.namespace);
    handle.alloc.destroy(table);
}

pub fn closeAll(handle: *h.Handle) void {
    var iterator = handle.embedded_tables.valueIterator();
    while (iterator.next()) |table| destroy(handle, table.*);
    handle.embedded_tables.deinit(handle.alloc);
    handle.embedded_tables = .empty;
    handle.embedded_table_errors.deinit(handle.alloc);
    handle.embedded_table_errors = .empty;
    var names = handle.embedded_table_records.keyIterator();
    while (names.next()) |name| handle.alloc.free(name.*);
    handle.embedded_table_records.deinit(handle.alloc);
    handle.embedded_table_records = .empty;
    handle.embedded_catalog_loaded = false;
    handle.embedded_tables_discovered = false;
}

/// Catalog discovery never opens an index/provider belonging to another table.
pub fn loadCatalog(handle: *h.Handle) !void {
    if (handle.embedded_catalog_loaded) return;
    var read = try handle.db.core.store.beginReadTxn();
    defer read.abort();
    const bytes = read.get(catalog_key) catch |err| switch (err) {
        error.NotFound => {
            handle.embedded_catalog_loaded = true;
            return;
        },
        else => return err,
    };
    const parsed = try std.json.parseFromSlice(Catalog, handle.alloc, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    errdefer {
        var names = handle.embedded_table_records.keyIterator();
        while (names.next()) |name| handle.alloc.free(name.*);
        handle.embedded_table_records.deinit(handle.alloc);
        handle.embedded_table_records = .empty;
    }
    for (parsed.value.tables) |record| {
        const name = try handle.alloc.dupe(u8, record.name);
        errdefer handle.alloc.free(name);
        try handle.embedded_table_records.putNoClobber(handle.alloc, name, record.id);
    }
    handle.embedded_next_table_id = parsed.value.next_id;
    handle.embedded_catalog_loaded = true;
}

/// Explicit database-wide maintenance opens its namespaces; normal open/status
/// and point access do not require unrelated tables to be healthy.
pub fn load(handle: *h.Handle) !void {
    try loadCatalog(handle);
    var names = handle.embedded_table_records.keyIterator();
    while (names.next()) |name| _ = try get(handle, name.*);
    handle.embedded_tables_discovered = true;
}

/// Integrity and unqualified DDL need every namespace, including inbound FK
/// owners. This is handle-local: a refreshed native view starts undiscovered
/// even when its durable recovery state is known clean. Cache discovery so
/// repeated statements do not enumerate/open every table again.
pub fn ensureDiscovered(handle: *h.Handle) !void {
    if (!handle.embedded_tables_discovered) try loadAvailable(handle);
}

/// Background discovery isolates a broken namespace and exposes its error in
/// pending-work diagnostics instead of stranding healthy tables' maintenance.
pub fn loadAvailable(handle: *h.Handle) !void {
    // An interrupted rescan must not leave a prior successful discovery cached
    // after its failure diagnostics have been cleared.
    handle.embedded_tables_discovered = false;
    try loadCatalog(handle);
    handle.embedded_table_errors.clearRetainingCapacity();
    var names = handle.embedded_table_records.keyIterator();
    while (names.next()) |name| {
        _ = get(handle, name.*) catch |err| {
            try handle.embedded_table_errors.put(handle.alloc, name.*, @errorName(err));
            continue;
        };
    }
    // Keep unavailable namespaces retryable on hosted handles too: they may
    // have no maintenance worker to retry a transient open failure for us.
    handle.embedded_tables_discovered = handle.embedded_table_errors.count() == 0;
}

fn persist(handle: *h.Handle) !void {
    const records = try handle.alloc.alloc(Record, handle.embedded_table_records.count());
    defer handle.alloc.free(records);
    var iterator = handle.embedded_table_records.iterator();
    for (records) |*record| {
        const table = iterator.next().?;
        record.* = .{ .name = table.key_ptr.*, .id = table.value_ptr.* };
    }
    std.mem.sort(Record, records, {}, struct {
        fn less(_: void, a: Record, b: Record) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.less);
    const bytes = try std.json.Stringify.valueAlloc(handle.alloc, Catalog{ .next_id = handle.embedded_next_table_id, .tables = records }, .{});
    defer handle.alloc.free(bytes);
    var write = try handle.db.core.store.beginWriteTxn();
    errdefer write.abort();
    try write.put(catalog_key, bytes);
    write.commit() catch {
        // An uncertain catalog publication cannot be followed by operations
        // using the old in-memory catalog. Reopening reloads durable truth.
        handle.sql_decision_uncertain = true;
        return error.SqlMutationOutcomeUnknown;
    };
}

pub fn noteReaderVersion(handle: *h.Handle) !void {
    const root = handle.parent_handle orelse handle;
    if (root == handle) return;
    const database = handle.database();
    const version = try database.core.index_manager.authoritativeReaderVersion(database.core.store);
    // Temporary fences belong to the child's reservation. Status combines the
    // effective versions of loaded tables; only permanent format requirements
    // are stamped into the root. Preserve a root reservation's baseline too.
    try root.db.core.index_manager.aggregateAuthoritativeReaderVersion(root.db.core.store, version);
}

pub fn requireDatabase(handle: *h.Handle) !void {
    if (handle.parent_id != null) return error.InvalidArgument;
    if (handle.storage_owner_context != null or handle.storage_owner_path != null or handle.storage_owner_group_id != 0 or handle.readable_lease_hook != null) return error.UnsupportedSqlExecution;
}

/// Read schema declarations at the captured Lite root without constructing
/// index/provider runtimes. This is metadata, not a serving-capability proof;
/// integrity reads still open and validate every owner they actually use.
pub fn liteSchemaJson(handle: *h.Handle, alloc: std.mem.Allocator, id: u64) !?[]u8 {
    const backend = if (handle.owned_lite_backend) |*value| value else return error.UnsupportedSqlExecution;
    const namespace = try catalog.namespaceAlloc(alloc, id);
    defer alloc.free(namespace);
    var options: h.db_mod.OpenOptions = .{};
    try backend.configureDbOpenOptionsForNamespace(&options, namespace);
    var probe = try options.primary_runtime_store.?.beginProbe();
    defer probe.abort();
    const json = probe.get(@import("../storage/internal_keys.zig").public_schema_json_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try alloc.dupe(u8, json);
}

pub fn get(handle: *h.Handle, name: []const u8) !*h.db_mod.DB {
    try loadCatalog(handle);
    if (std.mem.eql(u8, name, "default")) return &handle.db;
    handle.embedded_table_open_mutex.lockUncancelable(h.handleLockIo());
    defer handle.embedded_table_open_mutex.unlock(h.handleLockIo());
    if (handle.embedded_tables.get(name)) |table| return &table.db;
    const id = handle.embedded_table_records.get(name) orelse return error.UndefinedTable;
    const table = openTable(handle, name, id) catch |err| {
        const owned_name = handle.embedded_table_records.getKey(name).?;
        try handle.embedded_table_errors.put(handle.alloc, owned_name, @errorName(err));
        return err;
    };
    errdefer destroy(handle, table);
    try handle.embedded_tables.putNoClobber(handle.alloc, table.name, table);
    _ = handle.embedded_table_errors.remove(name);
    return &table.db;
}

pub fn create(handle: *h.Handle, name: []const u8, schema: []const u8, if_not_exists: bool) !void {
    try api.requireEmbeddedCatalogOwnership(handle);
    @import("sql_ddl.zig").invalidate(handle);
    try requireDatabase(handle);
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return error.SqlReadOnlyTransaction;
    if (name.len == 0 or name.len > 1024 or std.mem.indexOfScalar(u8, name, 0) != null or !std.unicode.utf8ValidateSlice(name)) return error.InvalidSqlParameters;
    try loadCatalog(handle);
    if (std.mem.eql(u8, name, "default") or handle.embedded_table_records.contains(name)) {
        if (if_not_exists) return;
        return error.TableAlreadyExists;
    }
    var parsed_schema = try h.tables_api.parseValidatedTableSchema(handle.alloc, schema);
    defer parsed_schema.deinit(handle.alloc);
    const id = handle.embedded_next_table_id;
    handle.embedded_next_table_id = std.math.add(u64, id, 1) catch return error.SqlProgramLimitExceeded;
    // Reserve the ID durably before opening a namespace. A failed create
    // leaves an unreachable namespace, never one that can later be reused.
    try persist(handle);
    const table = try openTable(handle, name, id);
    errdefer destroy(handle, table);
    var arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const fks = try parsed_schema.relationalForeignKeyDefinitions(a);
    var full_schema: ?[]const u8 = null;
    if (fks.len != 0) {
        var bare = try std.json.parseFromSliceLeaky(std.json.Value, a, schema, .{});
        const version = std.math.add(u32, parsed_schema.version, 1) catch return error.SqlProgramLimitExceeded;
        try bare.object.put(a, "version", .{ .integer = version });
        full_schema = try std.json.Stringify.valueAlloc(a, bare, .{});
        try bare.object.put(a, "version", .{ .integer = parsed_schema.version });
        try bare.object.put(a, "foreign_keys", .{ .array = std.array_list.Managed(std.json.Value).init(a) });
        try table.db.setSchemaJson(handle.alloc, try std.json.Stringify.valueAlloc(a, bare, .{}));
    } else try table.db.setSchemaJson(handle.alloc, schema);
    try h.antfly.lite.connection.provisionDefaultFullTextIndex(&table.db);
    try handle.embedded_tables.putNoClobber(handle.alloc, table.name, table);
    errdefer _ = handle.embedded_tables.remove(table.name);
    const owned_name = try handle.alloc.dupe(u8, name);
    errdefer handle.alloc.free(owned_name);
    try handle.embedded_table_records.putNoClobber(handle.alloc, owned_name, id);
    errdefer _ = handle.embedded_table_records.remove(name);
    if (full_schema) |target| {
        var adapter = @import("sql.zig").Adapter(h.antfly){ .handle = handle, .db = &table.db, .table_name = name };
        try @import("sql_fk.zig").publishInitial(&adapter, a, target, parsed_schema.version);
    } else try persist(handle);
}

/// An admitted initial publication owns this reserved namespace even before
/// its logical table becomes visible in the durable catalog.
pub fn recoverCreate(handle: *h.Handle, name: []const u8, id: u64) !void {
    try load(handle);
    if (handle.embedded_tables.get(name)) |existing| {
        if (existing.id != id) return error.IdentityNamespaceMismatch;
        return;
    }
    if (std.mem.eql(u8, name, "default") or id >= handle.embedded_next_table_id) return error.InvalidGenerationPublication;
    const table = try openTable(handle, name, id);
    errdefer destroy(handle, table);
    try handle.embedded_tables.putNoClobber(handle.alloc, table.name, table);
    errdefer _ = handle.embedded_tables.remove(table.name);
    const owned_name = try handle.alloc.dupe(u8, name);
    errdefer handle.alloc.free(owned_name);
    try handle.embedded_table_records.putNoClobber(handle.alloc, owned_name, id);
}

pub fn publishCreated(handle: *h.Handle) !void {
    try persist(handle);
}

pub fn drop(handle: *h.Handle, name: []const u8, if_exists: bool) !void {
    try api.requireEmbeddedCatalogOwnership(handle);
    try requireDatabase(handle);
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return error.SqlReadOnlyTransaction;
    try load(handle);
    const table = handle.embedded_tables.get(name) orelse {
        if (if_exists) return;
        return error.UndefinedTable;
    };
    try ensureTableUnused(handle, name, table.id);
    if (table.db.core.identity_namespace.table_id != handle.db.core.identity_namespace.table_id) try checkDependency(handle, &handle.db, name);
    var others = handle.embedded_tables.valueIterator();
    while (others.next()) |other| {
        if (other.* == table) continue;
        try checkDependency(handle, &other.*.db, name);
    }
    // Retire outgoing FK generations at every parent before the namespace
    // becomes unreachable. A durable publication job also owns the final DROP.
    var arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    if (try table.db.getSchemaJson(a)) |bytes| {
        var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{});
        const parsed = try h.tables_api.parseValidatedTableSchema(a, bytes);
        const fks = try parsed.relationalForeignKeyDefinitions(a);
        if (fks.len != 0) {
            try schema.object.put(a, "foreign_keys", .{ .array = std.array_list.Managed(std.json.Value).init(a) });
            const version = std.math.add(u32, parsed.version, 1) catch return error.SqlProgramLimitExceeded;
            try schema.object.put(a, "version", .{ .integer = version });
            const target = try std.json.Stringify.valueAlloc(a, schema, .{});
            var adapter = @import("sql.zig").Adapter(h.antfly){ .handle = handle, .db = &table.db, .table_name = name };
            if (try @import("sql_fk.zig").publish(&adapter, a, target, parsed.version, true)) return;
        }
    }
    try finishDrop(handle, name);
}

fn ensureTableUnused(handle: *h.Handle, name: []const u8, table_id: u64) !void {
    if (handle.lite_connection_context) |connection| try connection.ensureOtherConnectionsCanDrop();
    for (handle.table_handles.items) |id| {
        const child, const slot = h.handleRegistry().enter(id) orelse continue;
        defer h.HandleRegistry.leave(slot);
        // A child can still cache a pointer into a retired generation. That
        // address may have been reused by an unrelated table after refresh.
        if (child.selected_table_name) |selected_name| {
            if (child.selected_table_id == table_id and std.mem.eql(u8, selected_name, name)) return error.SqlStatementReadUnavailable;
        }
    }
    var sessions = handle.sql_sessions.valueIterator();
    while (sessions.next()) |session| {
        for (session.*.entries.items) |entry| {
            if (std.mem.eql(u8, entry.table.physical_name, name)) return error.SqlStatementReadUnavailable;
        }
    }
    // A stream owns snapshots and borrows this table's runtime until close.
    if (handle.sql_cursors.count() != 0) return error.SqlStatementReadUnavailable;
}

/// Recovery calls this only after the exact outgoing generation retirement.
pub fn finishDrop(handle: *h.Handle, name: []const u8) !void {
    const table = handle.embedded_tables.get(name) orelse return;
    // Durable FK jobs can finish a drop during recovery on another connection.
    // Recheck borrowed resources before retiring the shared table runtime.
    try ensureTableUnused(handle, name, table.id);
    _ = handle.embedded_tables.remove(name);
    errdefer handle.embedded_tables.putAssumeCapacity(table.name, table);
    const record = handle.embedded_table_records.fetchRemove(name) orelse return error.UndefinedTable;
    errdefer handle.embedded_table_records.putAssumeCapacity(record.key, record.value);
    try persist(handle);
    handle.alloc.free(record.key);
    destroy(handle, table);
}

fn checkDependency(handle: *h.Handle, db: *h.db_mod.DB, name: []const u8) !void {
    const schema = (try db.getSchemaJson(handle.alloc)) orelse return;
    defer handle.alloc.free(schema);
    const parsed = try std.json.parseFromSlice(std.json.Value, handle.alloc, schema, .{});
    defer parsed.deinit();
    const fks = parsed.value.object.get("foreign_keys") orelse return;
    if (fks == .array) for (fks.array.items) |fk| {
        const parent = if (fk == .object) fk.object.get("parent_table") else null;
        if (parent) |value| if (value == .string and std.mem.eql(u8, value.string, name)) return error.SqlDependentConstraint;
    };
}

pub fn antfly_db_create_table_json(ptr: ?*anyopaque, name: h.capi.Slice, schema: h.capi.Slice) callconv(.c) h.capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_create_table_json), .{ .name = "antfly_db_create_table_json" }), .{ ptr, name, schema });
    h.capi.clearLastError();
    const guard = api.enterHandle(ptr, .exclusive) orelse return h.capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return h.capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    create(guard.handle, name.bytes(), schema.bytes(), false) catch |err| return h.capi.mapError(err);
    return .ok;
}
pub fn antfly_db_drop_table(ptr: ?*anyopaque, name: h.capi.Slice) callconv(.c) h.capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_drop_table), .{ .name = "antfly_db_drop_table" }), .{ ptr, name });
    h.capi.clearLastError();
    const guard = api.enterHandle(ptr, .exclusive) orelse return h.capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return h.capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    drop(guard.handle, name.bytes(), false) catch |err| return h.capi.mapError(err);
    return .ok;
}
pub fn antfly_db_list_tables_json(ptr: ?*anyopaque, out: *h.capi.Buffer) callconv(.c) h.capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_list_tables_json), .{ .name = "antfly_db_list_tables_json" }), .{ ptr, out });
    h.capi.clearLastError();
    out.* = .{};
    const guard = api.enterHandleWithIntent(ptr, .exclusive, .read_only) orelse return h.capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return h.capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if (handle.parent_id != null) return h.capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": table catalog operations require a root database handle");
    loadCatalog(handle) catch |err| return h.capi.mapError(err);
    const names = handle.alloc.alloc([]const u8, handle.embedded_table_records.count() + 1) catch |err| return h.capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(names);
    names[0] = "default";
    var iterator = handle.embedded_table_records.keyIterator();
    for (names[1..]) |*name| name.* = iterator.next().?.*;
    std.mem.sort([]const u8, names, {}, struct {
        fn less(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.less);
    out.* = api.stringifyJson(names) catch |err| return h.capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

/// Table handles use the existing document, index, and enrichment APIs. They
/// borrow the namespace and are invalidated safely when the database closes.
pub fn antfly_db_open_table(ptr: ?*anyopaque, name: h.capi.Slice, out: *?*anyopaque) callconv(.c) h.capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_open_table), .{ .name = "antfly_db_open_table" }), .{ ptr, name, out });
    h.capi.clearLastError();
    out.* = null;
    const guard = api.enterHandleWithIntent(ptr, .exclusive, .read_only) orelse return h.capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return h.capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const root = guard.handle;
    requireDatabase(root) catch |err| return h.capi.mapError(err);
    var i: usize = 0;
    while (i < root.table_handles.items.len) {
        if (h.handleRegistry().enter(root.table_handles.items[i])) |entry| {
            h.HandleRegistry.leave(entry[1]);
            i += 1;
        } else {
            _ = root.table_handles.swapRemove(i);
        }
    }
    if (root.table_handles.items.len >= 1024) return h.capi.recordError(.busy, @src().fn_name ++ ": open table handle limit (1024) exceeded");
    const db = get(root, name.bytes()) catch |err| return h.capi.mapError(err);
    const child = root.alloc.create(h.Handle) catch |err| return h.capi.mapErrorContext(err, @src().fn_name);
    child.* = .{ .alloc = root.alloc, .db = undefined, .selected_db = db, .parent_id = ptr, .parent_handle = root, .open_mode = root.open_mode, .lite_profile = root.lite_profile, .lite_generated_enrichment_replay = root.lite_generated_enrichment_replay };
    child.selected_table_name = root.alloc.dupe(u8, name.bytes()) catch |err| {
        root.alloc.destroy(child);
        return h.capi.mapErrorContext(err, @src().fn_name ++ ": allocating the selected table name");
    };
    child.selected_table_id = db.core.identity_namespace.table_id;
    const id = h.handleRegistry().register(child) catch |err| {
        root.alloc.free(child.selected_table_name.?);
        root.alloc.destroy(child);
        return h.capi.mapErrorContext(err, @src().fn_name ++ ": registering the table handle");
    };
    root.table_handles.append(root.alloc, id) catch |err| {
        h.closeHandleId(id);
        return h.capi.mapErrorContext(err, @src().fn_name ++ ": tracking the open table handle");
    };
    out.* = id;
    return .ok;
}

comptime {
    @import("exports.zig").publish(@This(), &.{
        "antfly_db_create_table_json",
        "antfly_db_drop_table",
        "antfly_db_list_tables_json",
        "antfly_db_open_table",
    });
}
