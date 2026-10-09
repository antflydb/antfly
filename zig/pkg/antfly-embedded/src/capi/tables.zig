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
const catalog_key = "\x00antfly/embedded/tables/v1";
const Record = struct { name: []const u8, id: u64 };
const Catalog = struct { next_id: u64 = 2, tables: []const Record = &.{} };
pub const Table = struct { name: []u8, id: u64, db: h.db_mod.DB };

fn openTable(handle: *h.Handle, name: []const u8, id: u64) !*Table {
    const alloc = handle.alloc;
    const table = try alloc.create(Table);
    errdefer alloc.destroy(table);
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    const namespace = try std.fmt.allocPrint(alloc, "embedded/tables/{d}", .{id});
    defer alloc.free(namespace);
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
    if (handle.owned_lite_backend) |*backend| try backend.configureDbOpenOptionsForNamespace(&options, namespace);
    table.* = .{ .name = owned_name, .id = id, .db = try h.db_mod.DB.open(alloc, path, options) };
    errdefer table.db.close();
    try api.refreshLiteManagedEmbeddingRuntimeForDatabase(handle, &table.db);
    if (handle.embedded_open_options.start_optional_runtime_workers) table.db.startQuarantineRetryWorkerIfNeeded();
    return table;
}

fn destroy(handle: *h.Handle, table: *Table) void {
    if (handle.lease_snapshot) table.db.closeImmutableSnapshot() else table.db.close();
    handle.alloc.free(table.name);
    handle.alloc.destroy(table);
}

pub fn closeAll(handle: *h.Handle) void {
    var iterator = handle.embedded_tables.valueIterator();
    while (iterator.next()) |table| destroy(handle, table.*);
    handle.embedded_tables.deinit(handle.alloc);
}

pub fn load(handle: *h.Handle) !void {
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
    const parsed = try std.json.parseFromSlice(Catalog, handle.alloc, bytes, .{});
    defer parsed.deinit();
    errdefer {
        closeAll(handle);
        handle.embedded_tables = .empty;
    }
    for (parsed.value.tables) |record| {
        const table = try openTable(handle, record.name, record.id);
        errdefer destroy(handle, table);
        try handle.embedded_tables.putNoClobber(handle.alloc, table.name, table);
    }
    handle.embedded_next_table_id = parsed.value.next_id;
    handle.embedded_catalog_loaded = true;
}

fn persist(handle: *h.Handle) !void {
    const records = try handle.alloc.alloc(Record, handle.embedded_tables.count());
    defer handle.alloc.free(records);
    var iterator = handle.embedded_tables.valueIterator();
    for (records) |*record| {
        const table = iterator.next().?.*;
        record.* = .{ .name = table.name, .id = table.id };
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

pub fn requireDatabase(handle: *h.Handle) !void {
    if (handle.parent_id != null) return error.InvalidArgument;
    if (handle.storage_owner_context != null or handle.storage_owner_path != null or handle.storage_owner_group_id != 0 or handle.readable_lease_hook != null) return error.UnsupportedSqlExecution;
}

pub fn get(handle: *h.Handle, name: []const u8) !*h.db_mod.DB {
    try load(handle);
    if (std.mem.eql(u8, name, "default")) return &handle.db;
    return &(handle.embedded_tables.get(name) orelse return error.UndefinedTable).db;
}

pub fn create(handle: *h.Handle, name: []const u8, schema: []const u8, if_not_exists: bool) !void {
    try requireDatabase(handle);
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return error.SqlReadOnlyTransaction;
    if (name.len == 0 or name.len > 1024 or std.mem.indexOfScalar(u8, name, 0) != null or !std.unicode.utf8ValidateSlice(name)) return error.InvalidSqlParameters;
    try load(handle);
    if (std.mem.eql(u8, name, "default") or handle.embedded_tables.contains(name)) {
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
    try table.db.setSchemaJson(handle.alloc, schema);
    try h.antfly.lite.connection.provisionDefaultFullTextIndex(&table.db);
    try handle.embedded_tables.putNoClobber(handle.alloc, table.name, table);
    errdefer _ = handle.embedded_tables.remove(table.name);
    try persist(handle);
}

pub fn drop(handle: *h.Handle, name: []const u8, if_exists: bool) !void {
    try requireDatabase(handle);
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return error.SqlReadOnlyTransaction;
    try load(handle);
    const table = handle.embedded_tables.get(name) orelse {
        if (if_exists) return;
        return error.UndefinedTable;
    };
    for (handle.table_handles.items) |id| {
        const child, const slot = h.handle_registry.enter(id) orelse continue;
        defer h.HandleRegistry.leave(slot);
        // A child can still cache a pointer into a retired generation. That
        // address may have been reused by an unrelated table after refresh.
        if (child.selected_table_name) |selected_name| {
            if (child.selected_table_id == table.id and std.mem.eql(u8, selected_name, name)) return error.SqlStatementReadUnavailable;
        }
    }
    var sessions = handle.sql_sessions.valueIterator();
    while (sessions.next()) |session| {
        for (session.*.entries.items) |entry| {
            if (std.mem.eql(u8, entry.table.physical_name, name)) return error.SqlStatementReadUnavailable;
        }
    }
    if (table.db.core.identity_namespace.table_id != handle.db.core.identity_namespace.table_id) try checkDependency(handle, &handle.db, name);
    var others = handle.embedded_tables.valueIterator();
    while (others.next()) |other| {
        if (other.* == table) continue;
        try checkDependency(handle, &other.*.db, name);
    }
    // A stream owns snapshots and borrows this table's runtime until close.
    if (handle.sql_cursors.count() != 0) return error.SqlStatementReadUnavailable;
    _ = handle.embedded_tables.remove(name);
    errdefer handle.embedded_tables.putAssumeCapacity(table.name, table);
    try persist(handle);
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

pub export fn antfly_db_create_table_json(ptr: ?*anyopaque, name: h.capi.Slice, schema: h.capi.Slice) h.capi.ErrorCode {
    const guard = api.enterHandle(ptr, .exclusive) orelse return .invalid_argument;
    if (guard.entry_error) |code| {
        guard.leave();
        return code;
    }
    defer guard.leave();
    create(guard.handle, name.bytes(), schema.bytes(), false) catch |err| return h.capi.mapError(err);
    return .ok;
}
pub export fn antfly_db_drop_table(ptr: ?*anyopaque, name: h.capi.Slice) h.capi.ErrorCode {
    const guard = api.enterHandle(ptr, .exclusive) orelse return .invalid_argument;
    if (guard.entry_error) |code| {
        guard.leave();
        return code;
    }
    defer guard.leave();
    drop(guard.handle, name.bytes(), false) catch |err| return h.capi.mapError(err);
    return .ok;
}
pub export fn antfly_db_list_tables_json(ptr: ?*anyopaque, out: *h.capi.Buffer) h.capi.ErrorCode {
    out.* = .{};
    const guard = api.enterHandle(ptr, .exclusive) orelse return .invalid_argument;
    if (guard.entry_error) |code| {
        guard.leave();
        return code;
    }
    defer guard.leave();
    const handle = guard.handle;
    if (handle.parent_id != null) return .invalid_argument;
    load(handle) catch |err| return h.capi.mapError(err);
    const names = handle.alloc.alloc([]const u8, handle.embedded_tables.count() + 1) catch return .internal;
    defer handle.alloc.free(names);
    names[0] = "default";
    var iterator = handle.embedded_tables.keyIterator();
    for (names[1..]) |*name| name.* = iterator.next().?.*;
    std.mem.sort([]const u8, names, {}, struct {
        fn less(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.less);
    out.* = api.stringifyJson(names) catch return .internal;
    return .ok;
}

/// Table handles use the existing document, index, and enrichment APIs. They
/// borrow the namespace and are invalidated safely when the database closes.
pub export fn antfly_db_open_table(ptr: ?*anyopaque, name: h.capi.Slice, out: *?*anyopaque) h.capi.ErrorCode {
    out.* = null;
    const guard = api.enterHandle(ptr, .exclusive) orelse return .invalid_argument;
    if (guard.entry_error) |code| {
        guard.leave();
        return code;
    }
    defer guard.leave();
    const root = guard.handle;
    requireDatabase(root) catch |err| return h.capi.mapError(err);
    @import("sql_commit.zig").recover(root) catch |err| return h.capi.mapError(err);
    var i: usize = 0;
    while (i < root.table_handles.items.len) {
        if (h.handle_registry.enter(root.table_handles.items[i])) |entry| {
            h.HandleRegistry.leave(entry[1]);
            i += 1;
        } else {
            _ = root.table_handles.swapRemove(i);
        }
    }
    if (root.table_handles.items.len >= 1024) return .busy;
    const db = get(root, name.bytes()) catch |err| return h.capi.mapError(err);
    const child = root.alloc.create(h.Handle) catch return .internal;
    child.* = .{ .alloc = root.alloc, .db = undefined, .selected_db = db, .parent_id = ptr, .parent_handle = root, .open_mode = root.open_mode, .lite_profile = root.lite_profile, .lite_generated_enrichment_replay = root.lite_generated_enrichment_replay };
    child.selected_table_name = root.alloc.dupe(u8, name.bytes()) catch {
        root.alloc.destroy(child);
        return .internal;
    };
    child.selected_table_id = db.core.identity_namespace.table_id;
    const id = h.handle_registry.register(child) catch {
        root.alloc.free(child.selected_table_name.?);
        root.alloc.destroy(child);
        return .internal;
    };
    root.table_handles.append(root.alloc, id) catch {
        h.closeHandleId(id);
        return .internal;
    };
    out.* = id;
    return .ok;
}
