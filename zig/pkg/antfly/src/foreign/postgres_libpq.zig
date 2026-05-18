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
const builtin = @import("builtin");
const filter = @import("filter.zig");
const foreign_source = @import("source.zig");
const postgres_source = @import("postgres_source.zig");
const sql = @import("sql.zig");

const Allocator = std.mem.Allocator;
const Mutex = std.atomic.Mutex;

const PGconn = opaque {};
const PGresult = opaque {};

const ConnStatusType = c_uint;
const ExecStatusType = c_uint;
const Oid = c_uint;

const CONNECTION_OK: ConnStatusType = 0;
const PGRES_COMMAND_OK: ExecStatusType = 1;
const PGRES_TUPLES_OK: ExecStatusType = 2;
const PGRES_FATAL_ERROR: ExecStatusType = 7;
const PG_DIAG_SQLSTATE: c_int = 'C';

const FnPQconnectdb = *const fn ([*:0]const u8) callconv(.c) ?*PGconn;
const FnPQexec = *const fn (?*PGconn, [*:0]const u8) callconv(.c) ?*PGresult;
const FnPQstatus = *const fn (?*PGconn) callconv(.c) ConnStatusType;
const FnPQerrorMessage = *const fn (?*PGconn) callconv(.c) [*:0]const u8;
const FnPQfinish = *const fn (?*PGconn) callconv(.c) void;
const FnPQexecParams = *const fn (?*PGconn, [*:0]const u8, c_int, ?[*]const Oid, ?[*]const ?[*:0]const u8, ?[*]const c_int, ?[*]const c_int, c_int) callconv(.c) ?*PGresult;
const FnPQresultStatus = *const fn (?*PGresult) callconv(.c) ExecStatusType;
const FnPQresultErrorMessage = *const fn (?*PGresult) callconv(.c) [*:0]const u8;
const FnPQresultErrorField = *const fn (?*PGresult, c_int) callconv(.c) ?[*:0]const u8;
const FnPQntuples = *const fn (?*PGresult) callconv(.c) c_int;
const FnPQnfields = *const fn (?*PGresult) callconv(.c) c_int;
const FnPQfname = *const fn (?*PGresult, c_int) callconv(.c) ?[*:0]const u8;
const FnPQftype = *const fn (?*PGresult, c_int) callconv(.c) Oid;
const FnPQgetisnull = *const fn (?*PGresult, c_int, c_int) callconv(.c) c_int;
const FnPQgetlength = *const fn (?*PGresult, c_int, c_int) callconv(.c) c_int;
const FnPQgetvalue = *const fn (?*PGresult, c_int, c_int) callconv(.c) [*]const u8;
const FnPQclear = *const fn (?*PGresult) callconv(.c) void;

const TypeOid = struct {
    const boolean = 16;
    const int2 = 21;
    const int4 = 23;
    const int8 = 20;
    const float4 = 700;
    const float8 = 701;
    const numeric = 1700;
    const json = 114;
    const jsonb = 3802;
};

pub const Executor = struct {
    alloc: Allocator,
    lib: std.DynLib,
    exec_mutex: Mutex = .unlocked,
    cache_mutex: Mutex = .unlocked,
    columns_cache: std.StringHashMapUnmanaged([]foreign_source.Column) = .empty,
    connections: std.StringHashMapUnmanaged(*PGconn) = .empty,

    pqconnectdb: FnPQconnectdb,
    pqexec: FnPQexec,
    pqstatus: FnPQstatus,
    pqerrorMessage: FnPQerrorMessage,
    pqfinish: FnPQfinish,
    pqexecParams: FnPQexecParams,
    pqresultStatus: FnPQresultStatus,
    pqresultErrorMessage: FnPQresultErrorMessage,
    pqresultErrorField: FnPQresultErrorField,
    pqntuples: FnPQntuples,
    pqnfields: FnPQnfields,
    pqfname: FnPQfname,
    pqftype: FnPQftype,
    pqgetisnull: FnPQgetisnull,
    pqgetlength: FnPQgetlength,
    pqgetvalue: FnPQgetvalue,
    pqclear: FnPQclear,

    pub fn init(alloc: Allocator) !@This() {
        var lib = try openDefaultLibpq();
        errdefer lib.close();
        return .{
            .alloc = alloc,
            .lib = lib,
            .pqconnectdb = try lookupRequired(&lib, FnPQconnectdb, "PQconnectdb"),
            .pqexec = try lookupRequired(&lib, FnPQexec, "PQexec"),
            .pqstatus = try lookupRequired(&lib, FnPQstatus, "PQstatus"),
            .pqerrorMessage = try lookupRequired(&lib, FnPQerrorMessage, "PQerrorMessage"),
            .pqfinish = try lookupRequired(&lib, FnPQfinish, "PQfinish"),
            .pqexecParams = try lookupRequired(&lib, FnPQexecParams, "PQexecParams"),
            .pqresultStatus = try lookupRequired(&lib, FnPQresultStatus, "PQresultStatus"),
            .pqresultErrorMessage = try lookupRequired(&lib, FnPQresultErrorMessage, "PQresultErrorMessage"),
            .pqresultErrorField = try lookupRequired(&lib, FnPQresultErrorField, "PQresultErrorField"),
            .pqntuples = try lookupRequired(&lib, FnPQntuples, "PQntuples"),
            .pqnfields = try lookupRequired(&lib, FnPQnfields, "PQnfields"),
            .pqfname = try lookupRequired(&lib, FnPQfname, "PQfname"),
            .pqftype = try lookupRequired(&lib, FnPQftype, "PQftype"),
            .pqgetisnull = try lookupRequired(&lib, FnPQgetisnull, "PQgetisnull"),
            .pqgetlength = try lookupRequired(&lib, FnPQgetlength, "PQgetlength"),
            .pqgetvalue = try lookupRequired(&lib, FnPQgetvalue, "PQgetvalue"),
            .pqclear = try lookupRequired(&lib, FnPQclear, "PQclear"),
        };
    }

    pub fn deinit(self: *@This()) void {
        var conn_it = self.connections.iterator();
        while (conn_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.pqfinish(entry.value_ptr.*);
        }
        self.connections.deinit(self.alloc);

        lock(&self.cache_mutex);
        var it = self.columns_cache.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            freeColumns(self.alloc, entry.value_ptr.*);
        }
        self.columns_cache.deinit(self.alloc);
        self.cache_mutex.unlock();
        self.lib.close();
        self.* = undefined;
    }

    pub fn asQueryExecutor(self: *@This()) postgres_source.QueryExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .deinit = deinitQueryExecutor,
                .query = Executor.query,
                .statistics = Executor.statistics,
                .discover_columns = Executor.discoverColumns,
                .begin_snapshot_query = Executor.beginSnapshotQuery,
                .begin_prepared_replication_snapshot = Executor.beginPreparedReplicationSnapshot,
                .prepare_replication = Executor.prepareReplication,
                .poll_changes = Executor.pollChanges,
                .cleanup_replication = Executor.cleanupReplication,
            },
        };
    }

    fn deinitQueryExecutor(ptr: *anyopaque, alloc: Allocator) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.deinit();
        alloc.destroy(self);
    }

    fn query(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery) !foreign_source.QueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        lock(&self.exec_mutex);
        defer self.exec_mutex.unlock();
        return try self.queryPreparedAlloc(alloc, dsn, prepared);
    }

    fn statistics(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8) !foreign_source.TableStatistics {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        lock(&self.exec_mutex);
        defer self.exec_mutex.unlock();
        _ = alloc;
        _ = dsn;
        _ = table;
        // The live libpq stats probe is still less stable than the direct query path.
        // Until that path is hardened, fall back to "no stats" so foreign joins use
        // conservative planning instead of crashing the request path.
        return .{};
    }

    fn discoverColumns(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8) ![]foreign_source.Column {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        lock(&self.exec_mutex);
        defer self.exec_mutex.unlock();
        return try self.discoverColumnsAlloc(alloc, dsn, table);
    }

    const SnapshotQuery = struct {
        executor: *Executor,
        conn: ?*PGconn,

        fn asSnapshotQuery(self: *@This()) postgres_source.QueryExecutor.SnapshotQuery {
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = SnapshotQuery.destroy,
                    .query = SnapshotQuery.query,
                },
            };
        }

        fn destroy(ptr: *anyopaque, alloc: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const exec_mutex = &self.executor.exec_mutex;
            lock(exec_mutex);
            _ = self.executor.execSimpleAllowCommand(self.conn, alloc, "ROLLBACK") catch {};
            self.executor.pqfinish(self.conn);
            exec_mutex.unlock();
            alloc.destroy(self);
        }

        fn query(ptr: *anyopaque, alloc: Allocator, prepared: sql.PreparedQuery) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            lock(&self.executor.exec_mutex);
            defer self.executor.exec_mutex.unlock();

            var owned = prepared;
            defer owned.deinit(alloc);
            const result = try self.executor.execPrepared(self.conn, alloc, owned);
            defer self.executor.pqclear(result);
            return try self.executor.readQueryResultAlloc(alloc, result);
        }
    };

    fn beginSnapshotQuery(ptr: *anyopaque, alloc: Allocator, dsn: []const u8) !postgres_source.QueryExecutor.SnapshotQuery {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        lock(&self.exec_mutex);
        defer self.exec_mutex.unlock();

        const conn = try self.connectFresh(alloc, dsn);
        errdefer self.pqfinish(conn);
        const begin_result = try self.execSimpleAllowCommand(conn, alloc, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
        self.pqclear(begin_result);

        const snapshot_query = try alloc.create(SnapshotQuery);
        snapshot_query.* = .{
            .executor = self,
            .conn = conn,
        };
        return snapshot_query.asSnapshotQuery();
    }

    fn beginPreparedReplicationSnapshot(
        ptr: *anyopaque,
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationPollParams,
    ) !postgres_source.QueryExecutor.PreparedReplicationSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const slot_name = params.slot_name orelse return error.InvalidQueryRequest;
        const publication_name = params.publication_name orelse return error.InvalidQueryRequest;

        lock(&self.exec_mutex);
        defer self.exec_mutex.unlock();

        const sql_conn = try self.connectFresh(alloc, dsn);
        errdefer self.pqfinish(sql_conn);
        try self.ensurePublicationAlloc(alloc, dsn, sql_conn, publication_name, params.table, params.filter_query_json);
        if (try self.logicalReplicationSlotExistsAlloc(alloc, sql_conn, slot_name)) {
            return error.UnsupportedExactCutover;
        }

        const repl_conn = try self.connectReplicationFresh(alloc, dsn);
        defer self.pqfinish(repl_conn);
        var exported = try self.createLogicalReplicationSlotExportSnapshotAlloc(alloc, repl_conn, slot_name);
        defer exported.deinit(alloc);

        const begin_result = try self.execSimpleAllowCommand(sql_conn, alloc, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
        self.pqclear(begin_result);
        const quoted_snapshot = try quoteSqlStringLiteralAlloc(alloc, exported.snapshot_name);
        defer alloc.free(quoted_snapshot);
        const import_sql = try std.fmt.allocPrint(
            alloc,
            "SET TRANSACTION SNAPSHOT {s}",
            .{quoted_snapshot},
        );
        defer alloc.free(import_sql);
        const import_result = try self.execSimpleAllowCommand(sql_conn, alloc, import_sql);
        self.pqclear(import_result);

        const snapshot_query = try alloc.create(SnapshotQuery);
        errdefer alloc.destroy(snapshot_query);
        snapshot_query.* = .{
            .executor = self,
            .conn = sql_conn,
        };
        return .{
            .checkpoint = try alloc.dupe(u8, exported.checkpoint),
            .snapshot_query = snapshot_query.asSnapshotQuery(),
        };
    }

    fn pollChanges(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPollResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        lock(&self.exec_mutex);
        defer self.exec_mutex.unlock();
        return try self.pollChangesAlloc(alloc, dsn, params);
    }

    fn prepareReplication(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPrepareResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        lock(&self.exec_mutex);
        defer self.exec_mutex.unlock();
        return try self.prepareReplicationAlloc(alloc, dsn, params);
    }

    fn cleanupReplication(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationCleanupParams) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        lock(&self.exec_mutex);
        defer self.exec_mutex.unlock();
        return try self.cleanupReplicationAlloc(alloc, dsn, params);
    }

    fn queryPreparedAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery) !foreign_source.QueryResult {
        var owned = prepared;
        defer owned.deinit(alloc);

        std.log.info("postgres libpq query begin sql_len={d}", .{owned.sql_text.len});
        const conn = try self.connect(dsn);
        std.log.info("postgres libpq query connected sql_len={d}", .{owned.sql_text.len});
        const result = try self.execPrepared(conn, alloc, owned);
        defer self.pqclear(result);

        return try self.readQueryResultAlloc(alloc, result);
    }

    fn statisticsAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, table: []const u8) !foreign_source.TableStatistics {
        const conn = try self.connect(dsn);
        const args = try alloc.alloc(sql.ParameterValue, 1);
        args[0] = .{ .string = try alloc.dupe(u8, table) };

        var prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT COALESCE(n_live_tup, 0), COALESCE(pg_total_relation_size(relid), 0) FROM pg_stat_user_tables WHERE relname = $1"),
            .args = args,
        };
        defer prepared.deinit(alloc);

        const result = try self.execSimple(conn, alloc, prepared.sql_text);
        defer self.pqclear(result);

        if (self.pqntuples(result) == 0 or self.pqnfields(result) < 2) return .{};
        if (self.pqgetisnull(result, 0, 0) != 0 or self.pqgetisnull(result, 0, 1) != 0) return .{};

        return .{
            .row_count = try parseIntCell(self.pqgetvalue(result, 0, 0), @intCast(self.pqgetlength(result, 0, 0))),
            .size_bytes = try parseIntCell(self.pqgetvalue(result, 0, 1), @intCast(self.pqgetlength(result, 0, 1))),
        };
    }

    fn discoverColumnsAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, table: []const u8) ![]foreign_source.Column {
        lock(&self.cache_mutex);
        defer self.cache_mutex.unlock();

        if (self.columns_cache.get(table)) |cached| return try cloneColumnsAlloc(alloc, cached);

        const conn = try self.connect(dsn);
        const args = try alloc.alloc(sql.ParameterValue, 1);
        args[0] = .{ .string = try alloc.dupe(u8, table) };

        var prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position"),
            .args = args,
        };
        defer prepared.deinit(alloc);

        const result = try self.execPrepared(conn, alloc, prepared);
        defer self.pqclear(result);

        const column_count: usize = @intCast(self.pqntuples(result));
        if (column_count == 0) return error.ForeignTableNotFound;

        const discovered = try alloc.alloc(foreign_source.Column, column_count);
        errdefer freeColumns(alloc, discovered);
        for (0..column_count) |row_idx| {
            const row: c_int = @intCast(row_idx);
            const name = try copyCellAlloc(alloc, self.pqgetvalue(result, row, 0), @intCast(self.pqgetlength(result, row, 0)));
            errdefer alloc.free(name);
            const data_type = try copyCellAlloc(alloc, self.pqgetvalue(result, row, 1), @intCast(self.pqgetlength(result, row, 1)));
            errdefer alloc.free(data_type);
            const nullable_text = try copyCellAlloc(alloc, self.pqgetvalue(result, row, 2), @intCast(self.pqgetlength(result, row, 2)));
            defer alloc.free(nullable_text);
            discovered[row_idx] = .{
                .name = name,
                .data_type = data_type,
                .nullable = std.mem.eql(u8, nullable_text, "YES"),
            };
        }

        const cache_key = try self.alloc.dupe(u8, table);
        errdefer self.alloc.free(cache_key);
        const cached_columns = try cloneColumnsAlloc(self.alloc, discovered);
        errdefer freeColumns(self.alloc, cached_columns);
        try self.columns_cache.put(self.alloc, cache_key, cached_columns);

        return discovered;
    }

    fn connect(self: *@This(), dsn: []const u8) !?*PGconn {
        if (self.connections.get(dsn)) |cached| {
            if (self.pqstatus(cached) == CONNECTION_OK) return cached;
            if (self.connections.fetchRemove(dsn)) |removed| {
                self.alloc.free(removed.key);
                self.pqfinish(removed.value);
            }
        }

        const dsn_z = try self.alloc.dupeZ(u8, dsn);
        defer self.alloc.free(dsn_z);
        const conn = self.pqconnectdb(dsn_z.ptr) orelse return error.ForeignConnectionFailed;
        if (self.pqstatus(conn) != CONNECTION_OK) {
            _ = self.pqerrorMessage(conn);
            self.pqfinish(conn);
            return error.ForeignConnectionFailed;
        }
        const owned_dsn = try self.alloc.dupe(u8, dsn);
        errdefer self.alloc.free(owned_dsn);
        try self.connections.put(self.alloc, owned_dsn, conn);
        return conn;
    }

    fn connectFresh(self: *@This(), alloc: Allocator, dsn: []const u8) !?*PGconn {
        const dsn_z = try alloc.dupeZ(u8, dsn);
        defer alloc.free(dsn_z);
        const conn = self.pqconnectdb(dsn_z.ptr) orelse return error.ForeignConnectionFailed;
        if (self.pqstatus(conn) != CONNECTION_OK) {
            _ = self.pqerrorMessage(conn);
            self.pqfinish(conn);
            return error.ForeignConnectionFailed;
        }
        return conn;
    }

    fn connectReplicationFresh(self: *@This(), alloc: Allocator, dsn: []const u8) !?*PGconn {
        const repl_dsn = try appendReplicationModeAlloc(alloc, dsn);
        defer alloc.free(repl_dsn);
        return try self.connectFresh(alloc, repl_dsn);
    }

    fn execPrepared(self: *@This(), conn: ?*PGconn, alloc: Allocator, prepared: sql.PreparedQuery) !?*PGresult {
        return try self.execPreparedInternal(conn, alloc, prepared, false);
    }

    fn execPreparedAllowCommand(self: *@This(), conn: ?*PGconn, alloc: Allocator, prepared: sql.PreparedQuery) !?*PGresult {
        return try self.execPreparedInternal(conn, alloc, prepared, true);
    }

    fn execPreparedInternal(self: *@This(), conn: ?*PGconn, alloc: Allocator, prepared: sql.PreparedQuery, allow_command_ok: bool) !?*PGresult {
        var owned_args = try OwnedArgs.init(alloc, prepared.args);
        defer owned_args.deinit(alloc);

        const sql_text_z = try alloc.dupeZ(u8, prepared.sql_text);
        defer alloc.free(sql_text_z);

        const result = self.pqexecParams(
            conn,
            sql_text_z.ptr,
            @intCast(prepared.args.len),
            null,
            if (owned_args.values.len > 0) owned_args.values.ptr else null,
            if (owned_args.lengths.len > 0) owned_args.lengths.ptr else null,
            if (owned_args.formats.len > 0) owned_args.formats.ptr else null,
            0,
        ) orelse return error.ForeignQueryFailed;

        const status = self.pqresultStatus(result);
        if (status != PGRES_TUPLES_OK and !(allow_command_ok and status == PGRES_COMMAND_OK)) {
            defer self.pqclear(result);
            return mapResultError(self.pqresultErrorField(result, PG_DIAG_SQLSTATE), self.pqresultErrorMessage(result));
        }
        return result;
    }

    fn execSimple(self: *@This(), conn: ?*PGconn, alloc: Allocator, sql_text: []const u8) !?*PGresult {
        return try self.execSimpleInternal(conn, alloc, sql_text, false);
    }

    fn execSimpleAllowCommand(self: *@This(), conn: ?*PGconn, alloc: Allocator, sql_text: []const u8) !?*PGresult {
        return try self.execSimpleInternal(conn, alloc, sql_text, true);
    }

    fn execSimpleInternal(self: *@This(), conn: ?*PGconn, alloc: Allocator, sql_text: []const u8, allow_command_ok: bool) !?*PGresult {
        const sql_text_z = try alloc.dupeZ(u8, sql_text);
        defer alloc.free(sql_text_z);

        const result = self.pqexec(conn, sql_text_z.ptr) orelse return error.ForeignQueryFailed;
        const status = self.pqresultStatus(result);
        if (status != PGRES_TUPLES_OK and !(allow_command_ok and status == PGRES_COMMAND_OK)) {
            defer self.pqclear(result);
            return mapResultError(self.pqresultErrorField(result, PG_DIAG_SQLSTATE), self.pqresultErrorMessage(result));
        }
        return result;
    }

    fn readQueryResultAlloc(self: *@This(), alloc: Allocator, result: ?*PGresult) !foreign_source.QueryResult {
        const rows_len: usize = @intCast(self.pqntuples(result));
        const cols_len: usize = @intCast(self.pqnfields(result));
        const rows = try alloc.alloc(std.json.Value, rows_len);
        errdefer {
            for (rows[0..rows_len]) |*row| foreign_source.deinitJsonValue(alloc, row);
            alloc.free(rows);
        }

        var row_idx: usize = 0;
        errdefer {
            for (rows[0..row_idx]) |*row| foreign_source.deinitJsonValue(alloc, row);
        }
        while (row_idx < rows_len) : (row_idx += 1) {
            var object = std.json.ObjectMap.empty;
            errdefer {
                var it = object.iterator();
                while (it.next()) |entry| {
                    alloc.free(@constCast(entry.key_ptr.*));
                    foreign_source.deinitJsonValue(alloc, entry.value_ptr);
                }
                object.deinit(alloc);
            }
            for (0..cols_len) |col_idx| {
                const col: c_int = @intCast(col_idx);
                const key_z = self.pqfname(result, col) orelse return error.ForeignQueryFailed;
                const key = try alloc.dupe(u8, std.mem.span(key_z));
                errdefer alloc.free(key);
                var value = if (self.pqgetisnull(result, @intCast(row_idx), col) != 0)
                    std.json.Value.null
                else
                    try parseCellValueAlloc(
                        alloc,
                        self.pqftype(result, col),
                        self.pqgetvalue(result, @intCast(row_idx), col),
                        @intCast(self.pqgetlength(result, @intCast(row_idx), col)),
                    );
                errdefer foreign_source.deinitJsonValue(alloc, &value);
                try object.put(alloc, key, value);
            }
            rows[row_idx] = .{ .object = object };
        }

        return .{
            .rows = rows,
            .total = rows_len,
        };
    }

    fn pollChangesAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPollResult {
        const slot_name = params.slot_name orelse return error.InvalidQueryRequest;
        const publication_name = params.publication_name orelse return error.InvalidQueryRequest;
        std.log.info("postgres libpq poll begin table={s} slot={s}", .{ params.table, slot_name });
        const conn = try self.connectFresh(alloc, dsn);
        defer self.pqfinish(conn);
        std.log.info("postgres libpq poll connected table={s} slot={s}", .{ params.table, slot_name });
        var observed_checkpoint: ?[]u8 = null;
        errdefer if (observed_checkpoint) |value| alloc.free(value);
        try self.ensurePublicationAlloc(alloc, dsn, conn, publication_name, params.table, params.filter_query_json);
        if (params.checkpoint) |checkpoint| {
            if (checkpoint.len > 0 and !try self.logicalReplicationSlotExistsAlloc(alloc, conn, slot_name)) {
                return error.ForeignReplicationSlotMissing;
            }
            if (checkpoint.len > 0) {
                const advanced_checkpoint = try self.advanceLogicalReplicationSlotCheckpointAlloc(alloc, conn, slot_name, checkpoint);
                observed_checkpoint = advanced_checkpoint;
            }
        } else {
            _ = try self.ensureLogicalReplicationSlotAlloc(alloc, conn, slot_name);
        }

        const quoted_slot_name = try quoteSqlStringLiteralAlloc(alloc, slot_name);
        defer alloc.free(quoted_slot_name);
        const quoted_publication_name = try quoteSqlStringLiteralAlloc(alloc, publication_name);
        defer alloc.free(quoted_publication_name);

        var prepared = sql.PreparedQuery{
            .sql_text = try std.fmt.allocPrint(
                alloc,
                "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes({s}, NULL, {d}, 'proto_version', '2', 'publication_names', {s})",
                .{ quoted_slot_name, params.limit orelse 256, quoted_publication_name },
            ),
        };
        defer prepared.deinit(alloc);

        const result = try self.execPrepared(conn, alloc, prepared);
        defer self.pqclear(result);

        var relation_cache = std.AutoHashMapUnmanaged(u32, PgoutputRelation).empty;
        defer deinitPgoutputRelationCache(alloc, &relation_cache);

        var changes = std.ArrayListUnmanaged(foreign_source.ReplicationChange).empty;
        errdefer {
            for (changes.items) |*change| change.deinit(alloc);
            changes.deinit(alloc);
        }
        var current_txn_first_change_idx: ?usize = null;
        var lag_millis: u64 = 0;
        const poll_now_ms: i64 = @intCast(currentRealtimeMillis());

        const rows_len: usize = @intCast(self.pqntuples(result));
        for (0..rows_len) |row_idx| {
            const row: c_int = @intCast(row_idx);
            if (self.pqgetisnull(result, row, 0) != 0 or self.pqgetisnull(result, row, 1) != 0) continue;
            const lsn = try copyCellAlloc(alloc, self.pqgetvalue(result, row, 0), @intCast(self.pqgetlength(result, row, 0)));
            if (observed_checkpoint) |value| alloc.free(value);
            observed_checkpoint = lsn;
            const data_hex = self.pqgetvalue(result, row, 1)[0..@intCast(self.pqgetlength(result, row, 1))];
            const data = try decodeByteaHexAlloc(alloc, data_hex);
            defer alloc.free(data);

            switch (try parsePgoutputMessageAlloc(alloc, data, &relation_cache)) {
                .none => {},
                .begin => {
                    current_txn_first_change_idx = changes.items.len;
                },
                .commit => |commit_timestamp_ms| {
                    if (current_txn_first_change_idx) |start_idx| {
                        const computed_lag_ms: u64 = if (commit_timestamp_ms > 0 and poll_now_ms > 0)
                            @intCast(@max(@as(i64, 0), poll_now_ms - @as(i64, @intCast(commit_timestamp_ms))))
                        else
                            0;
                        for (changes.items[start_idx..]) |*change| {
                            change.commit_timestamp_ms = commit_timestamp_ms;
                            alloc.free(change.checkpoint);
                            change.checkpoint = try alloc.dupe(u8, lsn);
                        }
                        lag_millis = @max(lag_millis, computed_lag_ms);
                    }
                    current_txn_first_change_idx = null;
                },
                .change => |parsed_change| {
                    var change_without_checkpoint = parsed_change;
                    errdefer change_without_checkpoint.deinit(alloc);
                    alloc.free(change_without_checkpoint.checkpoint);
                    change_without_checkpoint.checkpoint = try alloc.dupe(u8, lsn);
                    change_without_checkpoint.lag_records = 0;
                    try changes.append(alloc, change_without_checkpoint);
                },
            }
        }

        if (rows_len > 0 or changes.items.len > 0) {
            std.log.info(
                "postgres libpq logical poll table={s} slot={s} rows={d} changes={d}",
                .{ params.table, slot_name, rows_len, changes.items.len },
            );
        }

        return .{
            .changes = try changes.toOwnedSlice(alloc),
            .checkpoint = if (observed_checkpoint) |value| value else &.{},
            .lag_records = 0,
            .lag_millis = lag_millis,
        };
    }

    fn prepareReplicationAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPrepareResult {
        const slot_name = params.slot_name orelse return error.InvalidQueryRequest;
        const publication_name = params.publication_name orelse return error.InvalidQueryRequest;
        std.log.info("postgres libpq prepare replication table={s} slot={s}", .{ params.table, slot_name });
        const conn = try self.connectFresh(alloc, dsn);
        defer self.pqfinish(conn);
        try self.ensurePublicationAlloc(alloc, dsn, conn, publication_name, params.table, params.filter_query_json);
        const slot_existed = try self.ensureLogicalReplicationSlotAlloc(alloc, conn, slot_name);
        return .{
            .checkpoint = try self.loadLogicalReplicationSlotCheckpointAlloc(alloc, conn, slot_name),
            .slot_existed = slot_existed,
        };
    }

    fn cleanupReplicationAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationCleanupParams) !void {
        std.log.info("postgres libpq cleanup replication slot={s} publication={s}", .{ params.slot_name, params.publication_name });
        const conn = try self.connectFresh(alloc, dsn);
        defer self.pqfinish(conn);

        const quoted_publication = try sql.postgresDialect().quote_identifier(alloc, params.publication_name);
        defer alloc.free(quoted_publication);
        var drop_publication = sql.PreparedQuery{
            .sql_text = try std.fmt.allocPrint(alloc, "DROP PUBLICATION IF EXISTS {s}", .{quoted_publication}),
            .args = &.{},
        };
        defer drop_publication.deinit(alloc);
        const drop_publication_result = try self.execPreparedAllowCommand(conn, alloc, drop_publication);
        defer self.pqclear(drop_publication_result);

        const drop_args = try alloc.alloc(sql.ParameterValue, 1);
        drop_args[0] = .{ .string = try alloc.dupe(u8, params.slot_name) };
        var drop_slot = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active"),
            .args = drop_args,
        };
        defer drop_slot.deinit(alloc);
        const drop_slot_result = try self.execPrepared(conn, alloc, drop_slot);
        defer self.pqclear(drop_slot_result);
    }

    fn ensurePublicationAlloc(
        self: *@This(),
        alloc: Allocator,
        dsn: []const u8,
        conn: ?*PGconn,
        publication_name: []const u8,
        table: []const u8,
        filter_query_json: ?[]const u8,
    ) !void {
        const check_args = try alloc.alloc(sql.ParameterValue, 1);
        check_args[0] = .{ .string = try alloc.dupe(u8, publication_name) };
        var check_prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT 1 FROM pg_publication WHERE pubname = $1"),
            .args = check_args,
        };
        defer check_prepared.deinit(alloc);
        const check_result = try self.execPrepared(conn, alloc, check_prepared);
        defer self.pqclear(check_result);
        if (@as(usize, @intCast(self.pqntuples(check_result))) > 0) return;

        const quoted_publication = try sql.postgresDialect().quote_identifier(alloc, publication_name);
        defer alloc.free(quoted_publication);
        const quoted_table = try sql.postgresDialect().quote_identifier(alloc, table);
        defer alloc.free(quoted_table);
        var create_sql = std.ArrayListUnmanaged(u8).empty;
        defer create_sql.deinit(alloc);
        const create_prefix = try std.fmt.allocPrint(alloc, "CREATE PUBLICATION {s} FOR TABLE {s}", .{ quoted_publication, quoted_table });
        defer alloc.free(create_prefix);
        try create_sql.appendSlice(alloc, create_prefix);

        var translated_filter: ?filter.Translation = null;
        defer if (translated_filter) |*value| value.deinit(alloc);
        if (filter_query_json) |query_json| {
            const columns = try self.discoverColumnsAlloc(alloc, dsn, table);
            defer freeColumns(alloc, columns);
            translated_filter = try filter.translateAlloc(alloc, sql.postgresDialect(), query_json, columns);
            if (translated_filter.?.where_sql.len > 0) {
                const where_suffix = try std.fmt.allocPrint(alloc, " WHERE ({s})", .{translated_filter.?.where_sql});
                defer alloc.free(where_suffix);
                try create_sql.appendSlice(alloc, where_suffix);
            }
        }

        var create_prepared = sql.PreparedQuery{
            .sql_text = try create_sql.toOwnedSlice(alloc),
            .args = if (translated_filter) |value| try cloneParameterValuesAlloc(alloc, value.args) else &.{},
        };
        defer create_prepared.deinit(alloc);
        const create_result = try self.execPreparedAllowCommand(conn, alloc, create_prepared);
        defer self.pqclear(create_result);
    }

    fn ensureLogicalReplicationSlotAlloc(self: *@This(), alloc: Allocator, conn: ?*PGconn, slot_name: []const u8) !bool {
        const check_args = try alloc.alloc(sql.ParameterValue, 1);
        check_args[0] = .{ .string = try alloc.dupe(u8, slot_name) };
        var check_prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1"),
            .args = check_args,
        };
        defer check_prepared.deinit(alloc);
        const check_result = try self.execPrepared(conn, alloc, check_prepared);
        defer self.pqclear(check_result);
        if (@as(usize, @intCast(self.pqntuples(check_result))) > 0) return true;

        const create_args = try alloc.alloc(sql.ParameterValue, 1);
        create_args[0] = .{ .string = try alloc.dupe(u8, slot_name) };
        var create_prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')"),
            .args = create_args,
        };
        defer create_prepared.deinit(alloc);
        const create_result = try self.execPrepared(conn, alloc, create_prepared);
        defer self.pqclear(create_result);
        return false;
    }

    fn loadLogicalReplicationSlotCheckpointAlloc(self: *@This(), alloc: Allocator, conn: ?*PGconn, slot_name: []const u8) ![]u8 {
        const args = try alloc.alloc(sql.ParameterValue, 1);
        args[0] = .{ .string = try alloc.dupe(u8, slot_name) };
        var prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT COALESCE(confirmed_flush_lsn::text, restart_lsn::text, '') FROM pg_replication_slots WHERE slot_name = $1"),
            .args = args,
        };
        defer prepared.deinit(alloc);
        const result = try self.execPrepared(conn, alloc, prepared);
        defer self.pqclear(result);
        if (self.pqntuples(result) == 0 or self.pqgetisnull(result, 0, 0) != 0) return try alloc.dupe(u8, "");
        return try copyCellAlloc(alloc, self.pqgetvalue(result, 0, 0), @intCast(self.pqgetlength(result, 0, 0)));
    }

    fn advanceLogicalReplicationSlotCheckpointAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        slot_name: []const u8,
        checkpoint: []const u8,
    ) ![]u8 {
        const args = try alloc.alloc(sql.ParameterValue, 2);
        args[0] = .{ .string = try alloc.dupe(u8, slot_name) };
        args[1] = .{ .string = try alloc.dupe(u8, checkpoint) };
        var prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT COALESCE(end_lsn::text, '') FROM pg_replication_slot_advance($1, $2::pg_lsn)"),
            .args = args,
        };
        defer prepared.deinit(alloc);
        const result = try self.execPrepared(conn, alloc, prepared);
        defer self.pqclear(result);
        if (self.pqntuples(result) == 0 or self.pqgetisnull(result, 0, 0) != 0) return try alloc.dupe(u8, checkpoint);
        return try copyCellAlloc(alloc, self.pqgetvalue(result, 0, 0), @intCast(self.pqgetlength(result, 0, 0)));
    }

    const ExportedReplicationSnapshot = struct {
        checkpoint: []u8,
        snapshot_name: []u8,

        fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.free(self.checkpoint);
            alloc.free(self.snapshot_name);
            self.* = undefined;
        }
    };

    fn logicalReplicationSlotExistsAlloc(self: *@This(), alloc: Allocator, conn: ?*PGconn, slot_name: []const u8) !bool {
        const args = try alloc.alloc(sql.ParameterValue, 1);
        args[0] = .{ .string = try alloc.dupe(u8, slot_name) };
        var prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1"),
            .args = args,
        };
        defer prepared.deinit(alloc);
        const result = try self.execPrepared(conn, alloc, prepared);
        defer self.pqclear(result);
        return @as(usize, @intCast(self.pqntuples(result))) > 0;
    }

    fn createLogicalReplicationSlotExportSnapshotAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        slot_name: []const u8,
    ) !ExportedReplicationSnapshot {
        const quoted_slot = try sql.postgresDialect().quote_identifier(alloc, slot_name);
        defer alloc.free(quoted_slot);
        const create_sql = try std.fmt.allocPrint(
            alloc,
            "CREATE_REPLICATION_SLOT {s} LOGICAL pgoutput EXPORT_SNAPSHOT",
            .{quoted_slot},
        );
        defer alloc.free(create_sql);
        const result = try self.execSimple(conn, alloc, create_sql);
        defer self.pqclear(result);
        if (self.pqntuples(result) == 0 or self.pqnfields(result) < 3) return error.ForeignQueryFailed;
        if (self.pqgetisnull(result, 0, 1) != 0 or self.pqgetisnull(result, 0, 2) != 0) return error.ForeignQueryFailed;
        return .{
            .checkpoint = try copyCellAlloc(alloc, self.pqgetvalue(result, 0, 1), @intCast(self.pqgetlength(result, 0, 1))),
            .snapshot_name = try copyCellAlloc(alloc, self.pqgetvalue(result, 0, 2), @intCast(self.pqgetlength(result, 0, 2))),
        };
    }
};

const LazyExecutor = struct {
    alloc: Allocator,
    mutex: Mutex = .unlocked,
    executor: ?*Executor = null,
    warned_init_failure: bool = false,

    fn deinit(self: *@This()) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.executor) |executor| {
            executor.deinit();
            self.alloc.destroy(executor);
            self.executor = null;
        }
    }

    fn asQueryExecutor(self: *@This()) postgres_source.QueryExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .deinit = deinitQueryExecutor,
                .query = query,
                .statistics = statistics,
                .discover_columns = discoverColumns,
                .begin_snapshot_query = beginSnapshotQuery,
                .begin_prepared_replication_snapshot = beginPreparedReplicationSnapshot,
                .prepare_replication = prepareReplication,
                .poll_changes = pollChanges,
                .cleanup_replication = cleanupReplication,
            },
        };
    }

    fn ensureExecutor(self: *@This()) !*Executor {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.executor) |executor| return executor;

        const executor = self.alloc.create(Executor) catch |err| return err;
        errdefer self.alloc.destroy(executor);
        executor.* = Executor.init(self.alloc) catch |err| {
            if (!self.warned_init_failure and err != error.OutOfMemory) {
                std.log.warn("postgres libpq unavailable until first successful Postgres-backed use: {}", .{err});
                self.warned_init_failure = true;
            }
            return err;
        };
        self.executor = executor;
        return executor;
    }

    fn deinitQueryExecutor(ptr: *anyopaque, alloc: Allocator) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.deinit();
        alloc.destroy(self);
    }

    fn query(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery) !foreign_source.QueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor();
        return try executor.asQueryExecutor().query(alloc, dsn, prepared);
    }

    fn statistics(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8) !foreign_source.TableStatistics {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor();
        return try executor.asQueryExecutor().statistics(alloc, dsn, table);
    }

    fn discoverColumns(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8) ![]foreign_source.Column {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor();
        return try executor.asQueryExecutor().discoverColumns(alloc, dsn, table);
    }

    fn beginSnapshotQuery(ptr: *anyopaque, alloc: Allocator, dsn: []const u8) !postgres_source.QueryExecutor.SnapshotQuery {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor();
        return try executor.asQueryExecutor().beginSnapshotQuery(alloc, dsn);
    }

    fn beginPreparedReplicationSnapshot(
        ptr: *anyopaque,
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationPollParams,
    ) !postgres_source.QueryExecutor.PreparedReplicationSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor();
        return try executor.asQueryExecutor().beginPreparedReplicationSnapshot(alloc, dsn, params);
    }

    fn prepareReplication(
        ptr: *anyopaque,
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationPollParams,
    ) !foreign_source.ReplicationPrepareResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor();
        return try executor.asQueryExecutor().prepareReplication(alloc, dsn, params);
    }

    fn pollChanges(
        ptr: *anyopaque,
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationPollParams,
    ) !foreign_source.ReplicationPollResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor();
        return try executor.asQueryExecutor().pollChanges(alloc, dsn, params);
    }

    fn cleanupReplication(
        ptr: *anyopaque,
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationCleanupParams,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor();
        return try executor.asQueryExecutor().cleanupReplication(alloc, dsn, params);
    }
};

fn appendReplicationModeAlloc(alloc: Allocator, dsn: []const u8) ![]u8 {
    if (std.mem.indexOf(u8, dsn, "replication=") != null) return try alloc.dupe(u8, dsn);
    if (std.mem.indexOfScalar(u8, dsn, '?') != null) {
        return try std.fmt.allocPrint(alloc, "{s}&replication=database", .{dsn});
    }
    return try std.fmt.allocPrint(alloc, "{s}?replication=database", .{dsn});
}

fn cloneParameterValuesAlloc(alloc: Allocator, args: []const sql.ParameterValue) ![]sql.ParameterValue {
    if (args.len == 0) return &.{};
    const out = try alloc.alloc(sql.ParameterValue, args.len);
    errdefer {
        for (out[0..]) |*arg| arg.deinit(alloc);
        alloc.free(out);
    }
    for (args, 0..) |arg, i| {
        out[i] = switch (arg) {
            .string => |value| .{ .string = try alloc.dupe(u8, value) },
            .integer => |value| .{ .integer = value },
            .float => |value| .{ .float = value },
            .bool => |value| .{ .bool = value },
            .null => .null,
        };
    }
    return out;
}

const PgoutputColumn = struct {
    name: []u8,
    data_type: Oid,
};

const PgoutputRelation = struct {
    namespace: []u8,
    relation_name: []u8,
    columns: []PgoutputColumn,
};

fn deinitPgoutputRelationCache(alloc: Allocator, cache: *std.AutoHashMapUnmanaged(u32, PgoutputRelation)) void {
    var it = cache.iterator();
    while (it.next()) |entry| {
        alloc.free(entry.value_ptr.namespace);
        alloc.free(entry.value_ptr.relation_name);
        for (entry.value_ptr.columns) |column| alloc.free(column.name);
        if (entry.value_ptr.columns.len > 0) alloc.free(entry.value_ptr.columns);
    }
    cache.deinit(alloc);
}

const ParsedPgoutputMessage = union(enum) {
    none,
    begin,
    commit: u64,
    change: foreign_source.ReplicationChange,
};

fn parsePgoutputMessageAlloc(
    alloc: Allocator,
    data: []const u8,
    relation_cache: *std.AutoHashMapUnmanaged(u32, PgoutputRelation),
) !ParsedPgoutputMessage {
    if (data.len == 0) return .none;
    var pos: usize = 0;
    const tag = data[pos];
    pos += 1;
    switch (tag) {
        'B' => {
            _ = try readU64(data, &pos); // final lsn
            _ = try readI64(data, &pos); // commit timestamp
            _ = try readU32(data, &pos); // xid
            return .begin;
        },
        'C' => {
            _ = try readByte(data, &pos); // flags
            _ = try readU64(data, &pos); // commit lsn
            _ = try readU64(data, &pos); // end lsn
            const commit_ts = try readI64(data, &pos);
            return .{ .commit = postgresEpochMicrosToUnixMillis(commit_ts) };
        },
        'O' => return .none,
        'Y' => return .none,
        'M' => return .none,
        'T' => return .none,
        'R' => {
            const relation_id = try readU32(data, &pos);
            const namespace = try readCStringAlloc(alloc, data, &pos);
            errdefer alloc.free(namespace);
            const relation_name = try readCStringAlloc(alloc, data, &pos);
            errdefer alloc.free(relation_name);
            _ = try readByte(data, &pos); // replica identity
            const column_count = try readU16(data, &pos);
            const columns = try alloc.alloc(PgoutputColumn, column_count);
            var initialized: usize = 0;
            errdefer {
                for (columns[0..initialized]) |column| alloc.free(column.name);
                alloc.free(columns);
            }
            for (0..column_count) |i| {
                _ = try readByte(data, &pos); // flags
                columns[i] = .{
                    .name = try readCStringAlloc(alloc, data, &pos),
                    .data_type = try readU32(data, &pos),
                };
                initialized += 1;
                _ = try readU32(data, &pos); // type modifier
            }
            if (relation_cache.getPtr(relation_id)) |existing| {
                alloc.free(existing.namespace);
                alloc.free(existing.relation_name);
                for (existing.columns) |column| alloc.free(column.name);
                if (existing.columns.len > 0) alloc.free(existing.columns);
                existing.* = .{
                    .namespace = namespace,
                    .relation_name = relation_name,
                    .columns = columns,
                };
            } else {
                try relation_cache.put(alloc, relation_id, .{
                    .namespace = namespace,
                    .relation_name = relation_name,
                    .columns = columns,
                });
            }
            return .none;
        },
        'I' => {
            const relation = relation_cache.get(try readU32(data, &pos)) orelse return error.InvalidReplicationSourceRow;
            const tuple_tag = try readByte(data, &pos);
            if (tuple_tag != 'N') return error.InvalidReplicationSourceRow;
            const row = try parsePgoutputTupleAlloc(alloc, data, &pos, relation);
            return .{ .change = .{
                .op = .insert,
                .checkpoint = try alloc.alloc(u8, 0),
                .row = row,
            } };
        },
        'U' => {
            const relation = relation_cache.get(try readU32(data, &pos)) orelse return error.InvalidReplicationSourceRow;
            var marker = try readByte(data, &pos);
            if (marker == 'K' or marker == 'O') {
                var old_row = try parsePgoutputTupleAlloc(alloc, data, &pos, relation);
                foreign_source.deinitJsonValue(alloc, &old_row);
                marker = try readByte(data, &pos);
            }
            if (marker != 'N') return error.InvalidReplicationSourceRow;
            const row = try parsePgoutputTupleAlloc(alloc, data, &pos, relation);
            return .{ .change = .{
                .op = .update,
                .checkpoint = try alloc.alloc(u8, 0),
                .row = row,
            } };
        },
        'D' => {
            const relation = relation_cache.get(try readU32(data, &pos)) orelse return error.InvalidReplicationSourceRow;
            const marker = try readByte(data, &pos);
            if (marker != 'K' and marker != 'O') return error.InvalidReplicationSourceRow;
            const row = try parsePgoutputTupleAlloc(alloc, data, &pos, relation);
            return .{ .change = .{
                .op = .delete,
                .checkpoint = try alloc.alloc(u8, 0),
                .row = row,
            } };
        },
        else => return .none,
    }
}

fn parsePgoutputTupleAlloc(alloc: Allocator, data: []const u8, pos: *usize, relation: PgoutputRelation) !std.json.Value {
    const column_count = try readU16(data, pos);
    var object = std.json.ObjectMap.empty;
    errdefer {
        var it = object.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            foreign_source.deinitJsonValue(alloc, entry.value_ptr);
        }
        object.deinit(alloc);
    }

    var idx: usize = 0;
    while (idx < column_count and idx < relation.columns.len) : (idx += 1) {
        const kind = try readByte(data, pos);
        switch (kind) {
            'n' => {
                try object.put(alloc, try alloc.dupe(u8, relation.columns[idx].name), .null);
            },
            'u' => {},
            't', 'b' => {
                const len = try readI32(data, pos);
                if (len < 0 or pos.* + @as(usize, @intCast(len)) > data.len) return error.InvalidReplicationSourceRow;
                const raw = data[pos.* .. pos.* + @as(usize, @intCast(len))];
                pos.* += @as(usize, @intCast(len));
                try object.put(
                    alloc,
                    try alloc.dupe(u8, relation.columns[idx].name),
                    try parseLogicalValueAlloc(alloc, relation.columns[idx].data_type, raw, kind == 'b'),
                );
            },
            else => return error.InvalidReplicationSourceRow,
        }
    }

    while (idx < column_count) : (idx += 1) {
        const kind = try readByte(data, pos);
        switch (kind) {
            'n', 'u' => {},
            't', 'b' => {
                const len = try readI32(data, pos);
                if (len < 0 or pos.* + @as(usize, @intCast(len)) > data.len) return error.InvalidReplicationSourceRow;
                pos.* += @as(usize, @intCast(len));
            },
            else => return error.InvalidReplicationSourceRow,
        }
    }

    return .{ .object = object };
}

fn parseLogicalValueAlloc(alloc: Allocator, oid: Oid, raw: []const u8, binary: bool) !std.json.Value {
    if (binary) return .{ .string = try alloc.dupe(u8, raw) };
    return switch (oid) {
        TypeOid.boolean => .{ .bool = parseBoolCell(raw) },
        TypeOid.int2, TypeOid.int4, TypeOid.int8 => .{ .integer = try std.fmt.parseInt(i64, raw, 10) },
        TypeOid.float4, TypeOid.float8 => .{ .float = try std.fmt.parseFloat(f64, raw) },
        TypeOid.numeric => .{ .number_string = try alloc.dupe(u8, raw) },
        TypeOid.json, TypeOid.jsonb => blk: {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
            defer parsed.deinit();
            break :blk try cloneJsonValueAlloc(alloc, parsed.value);
        },
        else => .{ .string = try alloc.dupe(u8, raw) },
    };
}

fn decodeByteaHexAlloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    if (encoded.len >= 2 and encoded[0] == '\\' and encoded[1] == 'x') {
        const hex = encoded[2..];
        if (hex.len % 2 != 0) return error.ForeignQueryFailed;
        const out = try alloc.alloc(u8, hex.len / 2);
        errdefer alloc.free(out);
        for (0..out.len) |i| {
            out[i] = try std.fmt.parseInt(u8, hex[i * 2 .. i * 2 + 2], 16);
        }
        return out;
    }
    return try alloc.dupe(u8, encoded);
}

fn quoteSqlStringLiteralAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '\'');
    for (value) |byte| {
        if (byte == '\'') try out.append(alloc, '\'');
        try out.append(alloc, byte);
    }
    try out.append(alloc, '\'');
    return try out.toOwnedSlice(alloc);
}

fn readByte(data: []const u8, pos: *usize) !u8 {
    if (pos.* >= data.len) return error.InvalidReplicationSourceRow;
    const value = data[pos.*];
    pos.* += 1;
    return value;
}

fn readU16(data: []const u8, pos: *usize) !u16 {
    if (pos.* + 2 > data.len) return error.InvalidReplicationSourceRow;
    const value = std.mem.readInt(u16, data[pos.* .. pos.* + 2][0..2], .big);
    pos.* += 2;
    return value;
}

fn readU32(data: []const u8, pos: *usize) !u32 {
    if (pos.* + 4 > data.len) return error.InvalidReplicationSourceRow;
    const value = std.mem.readInt(u32, data[pos.* .. pos.* + 4][0..4], .big);
    pos.* += 4;
    return value;
}

fn readU64(data: []const u8, pos: *usize) !u64 {
    if (pos.* + 8 > data.len) return error.InvalidReplicationSourceRow;
    const value = std.mem.readInt(u64, data[pos.* .. pos.* + 8][0..8], .big);
    pos.* += 8;
    return value;
}

fn readI32(data: []const u8, pos: *usize) !i32 {
    if (pos.* + 4 > data.len) return error.InvalidReplicationSourceRow;
    const value = std.mem.readInt(i32, data[pos.* .. pos.* + 4][0..4], .big);
    pos.* += 4;
    return value;
}

fn readI64(data: []const u8, pos: *usize) !i64 {
    if (pos.* + 8 > data.len) return error.InvalidReplicationSourceRow;
    const value = std.mem.readInt(i64, data[pos.* .. pos.* + 8][0..8], .big);
    pos.* += 8;
    return value;
}

fn postgresEpochMicrosToUnixMillis(micros_since_2000: i64) u64 {
    const micros_per_milli: i64 = 1_000;
    const unix_offset_millis: i64 = 946_684_800_000;
    if (micros_since_2000 <= 0) return 0;
    return @intCast(@max(@as(i64, 0), @divTrunc(micros_since_2000, micros_per_milli) + unix_offset_millis));
}

fn currentRealtimeMillis() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => {},
        else => return 0,
    }
    const sec: u64 = @intCast(@max(ts.sec, 0));
    const nsec: u64 = @intCast(@max(ts.nsec, 0));
    return sec * std.time.ms_per_s + @divTrunc(nsec, std.time.ns_per_ms);
}

fn readCStringAlloc(alloc: Allocator, data: []const u8, pos: *usize) ![]u8 {
    const end = std.mem.indexOfScalarPos(u8, data, pos.*, 0) orelse return error.InvalidReplicationSourceRow;
    const out = try alloc.dupe(u8, data[pos.*..end]);
    pos.* = end + 1;
    return out;
}

const OwnedArgs = struct {
    values: []?[*:0]const u8 = &.{},
    lengths: []c_int = &.{},
    formats: []c_int = &.{},
    owned_strings: []?[:0]u8 = &.{},

    fn init(alloc: Allocator, args: []const sql.ParameterValue) !@This() {
        if (args.len == 0) return .{};

        const values = try alloc.alloc(?[*:0]const u8, args.len);
        errdefer alloc.free(values);
        const lengths = try alloc.alloc(c_int, args.len);
        errdefer alloc.free(lengths);
        const formats = try alloc.alloc(c_int, args.len);
        errdefer alloc.free(formats);
        const owned_strings = try alloc.alloc(?[:0]u8, args.len);
        errdefer alloc.free(owned_strings);

        var initialized: usize = 0;
        errdefer {
            for (owned_strings[0..initialized]) |maybe_buffer| {
                if (maybe_buffer) |buffer| alloc.free(buffer);
            }
            alloc.free(owned_strings);
            alloc.free(formats);
            alloc.free(lengths);
            alloc.free(values);
        }

        for (args, 0..) |arg, idx| {
            formats[idx] = 0;
            switch (arg) {
                .null => {
                    values[idx] = null;
                    lengths[idx] = 0;
                    owned_strings[idx] = null;
                },
                .bool => |value| {
                    const printed = try std.fmt.allocPrint(alloc, "{}", .{value});
                    defer alloc.free(printed);
                    const text = try alloc.dupeZ(u8, printed);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(printed.len);
                    owned_strings[idx] = text;
                },
                .integer => |value| {
                    const printed = try std.fmt.allocPrint(alloc, "{d}", .{value});
                    defer alloc.free(printed);
                    const text = try alloc.dupeZ(u8, printed);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(printed.len);
                    owned_strings[idx] = text;
                },
                .float => |value| {
                    const printed = try std.fmt.allocPrint(alloc, "{d}", .{value});
                    defer alloc.free(printed);
                    const text = try alloc.dupeZ(u8, printed);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(printed.len);
                    owned_strings[idx] = text;
                },
                .string => |value| {
                    const text = try alloc.dupeZ(u8, value);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(value.len);
                    owned_strings[idx] = text;
                },
            }
            initialized += 1;
        }

        return .{
            .values = values,
            .lengths = lengths,
            .formats = formats,
            .owned_strings = owned_strings,
        };
    }

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.owned_strings) |buffer| {
            if (buffer) |owned| alloc.free(owned);
        }
        if (self.owned_strings.len > 0) alloc.free(self.owned_strings);
        if (self.formats.len > 0) alloc.free(self.formats);
        if (self.lengths.len > 0) alloc.free(self.lengths);
        if (self.values.len > 0) alloc.free(self.values);
        self.* = undefined;
    }
};

fn openDefaultLibpq() !std.DynLib {
    if (comptime builtin.link_libc) if (std.c.getenv("ANTFLY_LIBPQ_PATH")) |value_z| {
        return std.DynLib.open(std.mem.span(value_z)) catch error.LibpqUnavailable;
    };
    const candidates = [_][]const u8{
        "/opt/homebrew/lib/postgresql@18/libpq.dylib",
        "/opt/homebrew/opt/postgresql@18/lib/libpq.dylib",
        "/usr/local/opt/postgresql/lib/libpq.dylib",
        "libpq.dylib",
        "libpq.so.5",
        "libpq.so",
    };
    for (candidates) |candidate| {
        return std.DynLib.open(candidate) catch continue;
    }
    return error.LibpqUnavailable;
}

fn lookupRequired(lib: *std.DynLib, comptime T: type, name: [:0]const u8) !T {
    return lib.lookup(T, name) orelse error.MissingLibpqSymbol;
}

fn mapResultError(sqlstate_z: ?[*:0]const u8, message_z: [*:0]const u8) anyerror {
    const message = std.mem.span(message_z);
    if (message.len > 0) std.log.warn("postgres libpq result error: {s}", .{message});
    const sqlstate = if (sqlstate_z) |value| std.mem.span(value) else "";
    if (std.mem.eql(u8, sqlstate, "42P01")) return error.ForeignTableNotFound;
    if (std.mem.eql(u8, sqlstate, "42704")) return error.ForeignReplicationSlotMissing;
    if (std.mem.eql(u8, sqlstate, "42703")) return error.UnknownColumn;
    if (std.mem.eql(u8, sqlstate, "28P01") or std.mem.eql(u8, sqlstate, "28000")) return error.ForeignAuthFailed;
    if (std.mem.eql(u8, sqlstate, "08001") or std.mem.eql(u8, sqlstate, "08006")) return error.ForeignConnectionFailed;
    if (std.mem.eql(u8, sqlstate, "42601")) return error.InvalidQueryRequest;
    return error.ForeignQueryFailed;
}

fn parseCellValueAlloc(alloc: Allocator, oid: Oid, value_ptr: [*]const u8, len: usize) !std.json.Value {
    const bytes = value_ptr[0..len];
    return switch (oid) {
        TypeOid.boolean => .{ .bool = parseBoolCell(bytes) },
        TypeOid.int2, TypeOid.int4, TypeOid.int8 => .{ .integer = try std.fmt.parseInt(i64, bytes, 10) },
        TypeOid.float4, TypeOid.float8 => .{ .float = try std.fmt.parseFloat(f64, bytes) },
        TypeOid.numeric => .{ .number_string = try alloc.dupe(u8, bytes) },
        TypeOid.json, TypeOid.jsonb => blk: {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
            defer parsed.deinit();
            break :blk try cloneJsonValueAlloc(alloc, parsed.value);
        },
        else => .{ .string = try alloc.dupe(u8, bytes) },
    };
}

fn parseBoolCell(bytes: []const u8) bool {
    return bytes.len > 0 and (bytes[0] == 't' or bytes[0] == '1');
}

fn parseIntCell(value_ptr: [*]const u8, len: usize) !i64 {
    return try std.fmt.parseInt(i64, value_ptr[0..len], 10);
}

fn copyCellAlloc(alloc: Allocator, value_ptr: [*]const u8, len: usize) ![]u8 {
    return try alloc.dupe(u8, value_ptr[0..len]);
}

fn cloneJsonValueAlloc(alloc: Allocator, value: std.json.Value) !std.json.Value {
    return switch (value) {
        .null => .null,
        .bool => |v| .{ .bool = v },
        .integer => |v| .{ .integer = v },
        .float => |v| .{ .float = v },
        .number_string => |v| .{ .number_string = try alloc.dupe(u8, v) },
        .string => |v| .{ .string = try alloc.dupe(u8, v) },
        .array => |arr| blk: {
            var out = std.json.Array.init(alloc);
            errdefer {
                for (out.items) |*item| foreign_source.deinitJsonValue(alloc, item);
                out.deinit();
            }
            for (arr.items) |item| {
                try out.append(try cloneJsonValueAlloc(alloc, item));
            }
            break :blk .{ .array = out };
        },
        .object => |obj| blk: {
            var out = std.json.ObjectMap.empty;
            errdefer {
                var it = out.iterator();
                while (it.next()) |entry| {
                    alloc.free(@constCast(entry.key_ptr.*));
                    foreign_source.deinitJsonValue(alloc, entry.value_ptr);
                }
                out.deinit(alloc);
            }
            var it = obj.iterator();
            while (it.next()) |entry| {
                try out.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try cloneJsonValueAlloc(alloc, entry.value_ptr.*));
            }
            break :blk .{ .object = out };
        },
    };
}

fn cloneColumnsAlloc(alloc: Allocator, columns: []const foreign_source.Column) ![]foreign_source.Column {
    if (columns.len == 0) return &.{};
    const out = try alloc.alloc(foreign_source.Column, columns.len);
    errdefer freeColumns(alloc, out);
    for (columns, 0..) |column, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, column.name),
            .data_type = try alloc.dupe(u8, column.data_type),
            .nullable = column.nullable,
        };
    }
    return out;
}

fn freeColumns(alloc: Allocator, columns: []foreign_source.Column) void {
    for (columns) |*column| column.deinit(alloc);
    if (columns.len > 0) alloc.free(columns);
}

fn lock(mutex: *Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

pub fn registerDefaultExecutor(alloc: Allocator, registry: *foreign_source.Registry) !void {
    const executor = try alloc.create(LazyExecutor);
    errdefer alloc.destroy(executor);
    executor.* = .{
        .alloc = alloc,
    };
    try postgres_source.registerExecutor(alloc, registry, executor.asQueryExecutor());
}

test "postgres libpq registration succeeds without libpq and fails on first use" {
    const alloc = std.testing.allocator;
    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };

    const previous = std.c.getenv("ANTFLY_LIBPQ_PATH");
    const restore = if (previous) |value| try alloc.dupeZ(u8, std.mem.span(value)) else null;
    defer if (restore) |value| {
        _ = c.setenv("ANTFLY_LIBPQ_PATH", value, 1);
        alloc.free(value);
    } else {
        _ = c.unsetenv("ANTFLY_LIBPQ_PATH");
    };
    try std.testing.expectEqual(@as(c_int, 0), c.setenv("ANTFLY_LIBPQ_PATH", "/definitely/missing/libpq", 1));

    var registry = foreign_source.Registry{};
    defer registry.deinit(alloc);
    try registerDefaultExecutor(alloc, &registry);

    var source = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://ignored"),
    });
    defer source.deinit(alloc);

    try std.testing.expectError(error.LibpqUnavailable, source.statistics("users"));
}

test "postgres libpq parser decodes pgoutput relation and row changes" {
    const alloc = std.testing.allocator;

    var relation_cache = std.AutoHashMapUnmanaged(u32, PgoutputRelation).empty;
    defer deinitPgoutputRelationCache(alloc, &relation_cache);

    const Builder = struct {
        fn appendU16(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: u16) !void {
            var buf: [2]u8 = undefined;
            std.mem.writeInt(u16, &buf, value, .big);
            try out.appendSlice(alloc_inner, &buf);
        }

        fn appendU32(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: u32) !void {
            var buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &buf, value, .big);
            try out.appendSlice(alloc_inner, &buf);
        }

        fn appendU64(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: u64) !void {
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, value, .big);
            try out.appendSlice(alloc_inner, &buf);
        }

        fn appendI32(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: i32) !void {
            var buf: [4]u8 = undefined;
            std.mem.writeInt(i32, &buf, value, .big);
            try out.appendSlice(alloc_inner, &buf);
        }

        fn appendI64(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: i64) !void {
            var buf: [8]u8 = undefined;
            std.mem.writeInt(i64, &buf, value, .big);
            try out.appendSlice(alloc_inner, &buf);
        }

        fn appendCString(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: []const u8) !void {
            try out.appendSlice(alloc_inner, value);
            try out.append(alloc_inner, 0);
        }

        fn appendTextTupleValue(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: []const u8) !void {
            try out.append(alloc_inner, 't');
            try appendI32(out, alloc_inner, @intCast(value.len));
            try out.appendSlice(alloc_inner, value);
        }
    };

    var relation_bytes = std.ArrayListUnmanaged(u8).empty;
    defer relation_bytes.deinit(alloc);
    try relation_bytes.append(alloc, 'R');
    try Builder.appendU32(&relation_bytes, alloc, 42);
    try Builder.appendCString(&relation_bytes, alloc, "public");
    try Builder.appendCString(&relation_bytes, alloc, "users");
    try relation_bytes.append(alloc, 'd');
    try Builder.appendU16(&relation_bytes, alloc, 3);
    inline for (.{ .{ "id", TypeOid.int8 }, .{ "name", TypeOid.json }, .{ "tier", TypeOid.boolean } }) |column| {
        try relation_bytes.append(alloc, 0);
        try Builder.appendCString(&relation_bytes, alloc, column.@"0");
        try Builder.appendU32(&relation_bytes, alloc, column.@"1");
        try Builder.appendU32(&relation_bytes, alloc, 0);
    }
    try std.testing.expectEqual(ParsedPgoutputMessage.none, try parsePgoutputMessageAlloc(alloc, relation_bytes.items, &relation_cache));

    var begin_bytes = std.ArrayListUnmanaged(u8).empty;
    defer begin_bytes.deinit(alloc);
    try begin_bytes.append(alloc, 'B');
    try Builder.appendU64(&begin_bytes, alloc, 1);
    try Builder.appendI64(&begin_bytes, alloc, 0);
    try Builder.appendU32(&begin_bytes, alloc, 9);
    try std.testing.expectEqual(ParsedPgoutputMessage.begin, try parsePgoutputMessageAlloc(alloc, begin_bytes.items, &relation_cache));

    var insert_bytes = std.ArrayListUnmanaged(u8).empty;
    defer insert_bytes.deinit(alloc);
    try insert_bytes.append(alloc, 'I');
    try Builder.appendU32(&insert_bytes, alloc, 42);
    try insert_bytes.append(alloc, 'N');
    try Builder.appendU16(&insert_bytes, alloc, 3);
    try Builder.appendTextTupleValue(&insert_bytes, alloc, "7");
    try Builder.appendTextTupleValue(&insert_bytes, alloc, "{\"city\":\"sf\"}");
    try Builder.appendTextTupleValue(&insert_bytes, alloc, "t");

    var insert = switch (try parsePgoutputMessageAlloc(alloc, insert_bytes.items, &relation_cache)) {
        .change => |change| change,
        else => return error.TestUnexpectedResult,
    };
    defer insert.deinit(alloc);
    try std.testing.expectEqual(foreign_source.ReplicationOp.insert, insert.op);
    const insert_row = insert.row.?;
    try std.testing.expectEqual(@as(i64, 7), insert_row.object.get("id").?.integer);
    try std.testing.expectEqualStrings("sf", insert_row.object.get("name").?.object.get("city").?.string);
    try std.testing.expect(insert_row.object.get("tier").?.bool);

    var update_bytes = std.ArrayListUnmanaged(u8).empty;
    defer update_bytes.deinit(alloc);
    try update_bytes.append(alloc, 'U');
    try Builder.appendU32(&update_bytes, alloc, 42);
    try update_bytes.append(alloc, 'N');
    try Builder.appendU16(&update_bytes, alloc, 3);
    try Builder.appendTextTupleValue(&update_bytes, alloc, "7");
    try Builder.appendTextTupleValue(&update_bytes, alloc, "{\"city\":\"la\"}");
    try Builder.appendTextTupleValue(&update_bytes, alloc, "f");

    var update = switch (try parsePgoutputMessageAlloc(alloc, update_bytes.items, &relation_cache)) {
        .change => |change| change,
        else => return error.TestUnexpectedResult,
    };
    defer update.deinit(alloc);
    try std.testing.expectEqual(foreign_source.ReplicationOp.update, update.op);
    try std.testing.expectEqualStrings("la", update.row.?.object.get("name").?.object.get("city").?.string);
    try std.testing.expect(!update.row.?.object.get("tier").?.bool);

    var delete_bytes = std.ArrayListUnmanaged(u8).empty;
    defer delete_bytes.deinit(alloc);
    try delete_bytes.append(alloc, 'D');
    try Builder.appendU32(&delete_bytes, alloc, 42);
    try delete_bytes.append(alloc, 'O');
    try Builder.appendU16(&delete_bytes, alloc, 3);
    try Builder.appendTextTupleValue(&delete_bytes, alloc, "7");
    try Builder.appendTextTupleValue(&delete_bytes, alloc, "{\"city\":\"la\"}");
    try Builder.appendTextTupleValue(&delete_bytes, alloc, "f");

    var delete = switch (try parsePgoutputMessageAlloc(alloc, delete_bytes.items, &relation_cache)) {
        .change => |change| change,
        else => return error.TestUnexpectedResult,
    };
    defer delete.deinit(alloc);
    try std.testing.expectEqual(foreign_source.ReplicationOp.delete, delete.op);
    try std.testing.expectEqual(@as(i64, 7), delete.row.?.object.get("id").?.integer);

    var commit_bytes = std.ArrayListUnmanaged(u8).empty;
    defer commit_bytes.deinit(alloc);
    try commit_bytes.append(alloc, 'C');
    try commit_bytes.append(alloc, 0);
    try Builder.appendU64(&commit_bytes, alloc, 1);
    try Builder.appendU64(&commit_bytes, alloc, 1);
    try Builder.appendI64(&commit_bytes, alloc, 1_000_000);
    const parsed_commit = try parsePgoutputMessageAlloc(alloc, commit_bytes.items, &relation_cache);
    try std.testing.expect(parsed_commit == .commit);
    try std.testing.expect(parsed_commit.commit > 946_684_800_000);
}

test "postgres libpq decodes bytea hex text" {
    const alloc = std.testing.allocator;
    const decoded = try decodeByteaHexAlloc(alloc, "\\x4869");
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("Hi", decoded);
}

test "postgres libpq live logical poll returns inserted row" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) {
        return error.SkipZigTest;
    }
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_probe_live_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_probe_live_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_probe_live_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const create_publication_sql = try std.fmt.allocPrint(alloc, "create publication {s} for table {s}", .{ publication_name, table_name });
    defer alloc.free(create_publication_sql);
    const create_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select * from pg_create_logical_replication_slot('{s}', 'pgoutput')",
        .{slot_name},
    );
    defer alloc.free(create_slot_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u1', 'Alice')", .{table_name});
    defer alloc.free(insert_sql);

    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_publication_sql);
    try execCommandForTest(&executor, conn, alloc, create_slot_sql);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer poll_params.deinit(alloc);

    var empty_poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer empty_poll_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty_poll_result.changes.len);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    const snapshot_query = sql.PreparedQuery{
        .sql_text = try std.fmt.allocPrint(alloc, "SELECT * FROM {s} LIMIT 16 OFFSET 1", .{table_name}),
    };
    var snapshot_query_result = try executor.queryPreparedAlloc(alloc, dsn, snapshot_query);
    defer snapshot_query_result.deinit(alloc);

    var poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer poll_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), poll_result.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.insert, poll_result.changes[0].op);
    try std.testing.expectEqualStrings("u1", poll_result.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("Alice", poll_result.changes[0].row.?.object.get("name").?.string);
}

test "postgres libpq live logical poll can auto-create publication and slot before later insert" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) {
        return error.SkipZigTest;
    }
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_auto_live_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_auto_live_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_auto_live_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u0', 'Seed')", .{table_name});
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u1', 'Alice')", .{table_name});
    defer alloc.free(insert_sql);

    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, seed_sql);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer poll_params.deinit(alloc);

    var empty_poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer empty_poll_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty_poll_result.changes.len);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    var poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer poll_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), poll_result.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.insert, poll_result.changes[0].op);
    try std.testing.expectEqualStrings("u1", poll_result.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("Alice", poll_result.changes[0].row.?.object.get("name").?.string);
}

test "postgres libpq live prepare replication returns slot checkpoint" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) {
        return error.SkipZigTest;
    }
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_prepare_live_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_prepare_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_prepare_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_table_sql);

    var prepare_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
    };
    defer prepare_params.deinit(alloc);

    var prepare_result = try executor.prepareReplicationAlloc(alloc, dsn, prepare_params);
    defer prepare_result.deinit(alloc);

    try std.testing.expect(prepare_result.checkpoint.len > 0);
    try std.testing.expect(!prepare_result.slot_existed);

    var second_prepare = try executor.prepareReplicationAlloc(alloc, dsn, prepare_params);
    defer second_prepare.deinit(alloc);
    try std.testing.expect(second_prepare.checkpoint.len > 0);
    try std.testing.expect(second_prepare.slot_existed);
}

test "postgres libpq live logical poll works after snapshot query on same connection" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) {
        return error.SkipZigTest;
    }
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_snapshot_then_stream_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_snapshot_then_stream_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_snapshot_then_stream_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u0', 'Seed')", .{table_name});
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u1', 'Alice')", .{table_name});
    defer alloc.free(insert_sql);

    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, seed_sql);

    const snapshot_query = sql.PreparedQuery{
        .sql_text = try std.fmt.allocPrint(alloc, "SELECT * FROM {s} LIMIT 16 OFFSET 0", .{table_name}),
    };
    var snapshot_query_result = try executor.queryPreparedAlloc(alloc, dsn, snapshot_query);
    defer snapshot_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), snapshot_query_result.rows.len);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer poll_params.deinit(alloc);

    var empty_poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer empty_poll_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty_poll_result.changes.len);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    var poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer poll_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), poll_result.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.insert, poll_result.changes[0].op);
    try std.testing.expectEqualStrings("u1", poll_result.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("Alice", poll_result.changes[0].row.?.object.get("name").?.string);
}

test "postgres libpq consistent snapshot query holds repeatable read view" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_snapshot_probe_{d}", .{suffix});
    defer alloc.free(table_name);

    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u0', 'Seed')", .{table_name});
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u1', 'Alice')", .{table_name});
    defer alloc.free(insert_sql);
    const select_sql = try std.fmt.allocPrint(alloc, "SELECT id, name FROM {s} ORDER BY id ASC", .{table_name});
    defer alloc.free(select_sql);

    try execCommandForTest(&executor, conn, alloc, drop_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, seed_sql);

    var snapshot = try executor.asQueryExecutor().beginSnapshotQuery(alloc, dsn);
    defer snapshot.deinit(alloc);

    var first_result = try snapshot.queryPrepared(alloc, .{
        .sql_text = try alloc.dupe(u8, select_sql),
    });
    defer first_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), first_result.rows.len);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    var second_result = try snapshot.queryPrepared(alloc, .{
        .sql_text = try alloc.dupe(u8, select_sql),
    });
    defer second_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second_result.rows.len);
    try std.testing.expectEqualStrings("u0", second_result.rows[0].object.get("id").?.string);

    var fresh_result = try executor.queryPreparedAlloc(alloc, dsn, .{
        .sql_text = try alloc.dupe(u8, select_sql),
    });
    defer fresh_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), fresh_result.rows.len);
}

test "postgres libpq prepared replication snapshot bridges initial cutover" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) return error.SkipZigTest;
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_exact_cutover_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_exact_cutover_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_exact_cutover_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u0', 'Seed')", .{table_name});
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u1', 'Alice')", .{table_name});
    defer alloc.free(insert_sql);
    try execCommandForTest(&executor, conn, alloc, drop_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, seed_sql);

    var begin_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
    };
    defer begin_params.deinit(alloc);
    var prepared = try executor.asQueryExecutor().beginPreparedReplicationSnapshot(alloc, dsn, begin_params);
    defer prepared.deinit(alloc);
    try std.testing.expect(prepared.checkpoint.len > 0);

    var snapshot_result = try prepared.snapshot_query.queryPrepared(alloc, .{
        .sql_text = try std.fmt.allocPrint(alloc, "SELECT id, name FROM {s} ORDER BY id ASC", .{table_name}),
    });
    defer snapshot_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), snapshot_result.rows.len);
    try std.testing.expectEqualStrings("u0", snapshot_result.rows[0].object.get("id").?.string);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .checkpoint = try alloc.dupe(u8, prepared.checkpoint),
        .limit = 16,
    };
    defer poll_params.deinit(alloc);
    var poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer poll_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), poll_result.changes.len);
    try std.testing.expectEqualStrings("u1", poll_result.changes[0].row.?.object.get("id").?.string);
}

test "postgres libpq poll resumes from durable checkpoint across multiple transactions" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) return error.SkipZigTest;
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_resume_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_resume_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_resume_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null, tier text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name, tier) values ('u1', 'Alice', 'gold')", .{table_name});
    defer alloc.free(insert_sql);
    const update_sql = try std.fmt.allocPrint(alloc, "update {s} set tier = 'platinum' where id = 'u1'", .{table_name});
    defer alloc.free(update_sql);
    try execCommandForTest(&executor, conn, alloc, drop_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_table_sql);

    var prepare_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer prepare_params.deinit(alloc);
    var prepare_result = try executor.prepareReplicationAlloc(alloc, dsn, prepare_params);
    defer prepare_result.deinit(alloc);
    try std.testing.expect(prepare_result.checkpoint.len > 0);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    var first_poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .checkpoint = try alloc.dupe(u8, prepare_result.checkpoint),
        .limit = 16,
    };
    defer first_poll_params.deinit(alloc);
    var first_poll = try executor.pollChangesAlloc(alloc, dsn, first_poll_params);
    defer first_poll.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), first_poll.changes.len);
    try std.testing.expect(first_poll.checkpoint.len > 0);
    try std.testing.expectEqualStrings("u1", first_poll.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings(first_poll.changes[0].checkpoint, first_poll.checkpoint);

    try execCommandForTest(&executor, conn, alloc, update_sql);

    var second_poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .checkpoint = try alloc.dupe(u8, first_poll.checkpoint),
        .limit = 16,
    };
    defer second_poll_params.deinit(alloc);
    var second_poll = try executor.pollChangesAlloc(alloc, dsn, second_poll_params);
    defer second_poll.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second_poll.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.update, second_poll.changes[0].op);
    try std.testing.expectEqualStrings("u1", second_poll.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("platinum", second_poll.changes[0].row.?.object.get("tier").?.string);
    try std.testing.expect(second_poll.checkpoint.len > 0);
    try std.testing.expectEqualStrings(second_poll.changes[0].checkpoint, second_poll.checkpoint);
}

test "postgres libpq module compiles" {
    _ = Executor;
    _ = registerDefaultExecutor;
}

fn testPgDsnAlloc(alloc: Allocator) ![]u8 {
    if (std.c.getenv("ANTFLY_TEST_PG_DSN")) |value_z| {
        return try alloc.dupe(u8, std.mem.span(value_z));
    }
    if (std.c.getenv("PG_DSN")) |value_z| {
        return try alloc.dupe(u8, std.mem.span(value_z));
    }
    return try alloc.dupe(u8, "postgres://localhost:5432/postgres?sslmode=disable");
}

var live_poll_test_counter: u64 = 0;

fn execCommandForTest(executor: *Executor, conn: ?*PGconn, alloc: Allocator, sql_text: []const u8) !void {
    var prepared = sql.PreparedQuery{
        .sql_text = try alloc.dupe(u8, sql_text),
    };
    defer prepared.deinit(alloc);
    const result = try executor.execPreparedAllowCommand(conn, alloc, prepared);
    executor.pqclear(result);
}
