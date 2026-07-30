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
const filter = @import("filter.zig");
const foreign_source = @import("source.zig");
const postgres_libpq = @import("postgres_libpq.zig");
const sql = @import("sql.zig");
const platform_time = @import("antfly_platform").time;

const Allocator = std.mem.Allocator;

fn ensureExecutionDeadline(deadline_ns: ?u64) !void {
    const deadline = deadline_ns orelse return;
    if (platform_time.monotonicNs() >= deadline) return error.Timeout;
}

pub const QueryExecutor = struct {
    ptr: *anyopaque,
    vtable: *const QueryExecutor.VTable,

    pub const SnapshotQuery = struct {
        ptr: *anyopaque,
        vtable: *const SnapshotQuery.VTable,

        pub const VTable = struct {
            deinit: *const fn (ptr: *anyopaque, alloc: Allocator) void,
            query: *const fn (ptr: *anyopaque, alloc: Allocator, prepared: sql.PreparedQuery, execution_deadline_ns: ?u64) anyerror!foreign_source.QueryResult,
        };

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            self.vtable.deinit(self.ptr, alloc);
            self.* = undefined;
        }

        pub fn queryPrepared(
            self: @This(),
            alloc: Allocator,
            prepared: sql.PreparedQuery,
            execution_deadline_ns: ?u64,
        ) !foreign_source.QueryResult {
            return try self.vtable.query(self.ptr, alloc, prepared, execution_deadline_ns);
        }
    };

    pub const PreparedReplicationSnapshot = struct {
        checkpoint: []u8,
        snapshot_query: SnapshotQuery,

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.free(self.checkpoint);
            self.snapshot_query.deinit(alloc);
            self.* = undefined;
        }
    };

    pub const VTable = struct {
        deinit: ?*const fn (ptr: *anyopaque, alloc: Allocator) void = null,
        query: *const fn (ptr: *anyopaque, alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery, execution_deadline_ns: ?u64) anyerror!foreign_source.QueryResult,
        statistics: *const fn (ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8) anyerror!foreign_source.TableStatistics,
        statistics_with_deadline: ?*const fn (ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) anyerror!foreign_source.TableStatistics = null,
        discover_columns: ?*const fn (ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) anyerror![]foreign_source.Column = null,
        refresh_columns: ?*const fn (ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) anyerror![]foreign_source.Column = null,
        begin_snapshot_query: ?*const fn (ptr: *anyopaque, alloc: Allocator, dsn: []const u8) anyerror!SnapshotQuery = null,
        begin_prepared_replication_snapshot: ?*const fn (ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams, execution_deadline_ns: u64) anyerror!PreparedReplicationSnapshot = null,
        prepare_replication: ?*const fn (ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) anyerror!foreign_source.ReplicationPrepareResult = null,
        poll_changes: ?*const fn (ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) anyerror!foreign_source.ReplicationPollResult = null,
        cleanup_replication: ?*const fn (ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationCleanupParams) anyerror!void = null,
    };

    pub fn deinit(self: @This(), alloc: Allocator) void {
        if (self.vtable.deinit) |deinit_fn| deinit_fn(self.ptr, alloc);
    }

    pub fn query(self: @This(), alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery, execution_deadline_ns: ?u64) !foreign_source.QueryResult {
        return try self.vtable.query(self.ptr, alloc, dsn, prepared, execution_deadline_ns);
    }

    pub fn statistics(self: @This(), alloc: Allocator, dsn: []const u8, table: []const u8) !foreign_source.TableStatistics {
        return try self.vtable.statistics(self.ptr, alloc, dsn, table);
    }

    pub fn statisticsWithDeadline(self: @This(), alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) !foreign_source.TableStatistics {
        if (self.vtable.statistics_with_deadline) |fn_ptr| {
            return try fn_ptr(self.ptr, alloc, dsn, table, execution_deadline_ns);
        }
        if (execution_deadline_ns != null) return error.UnsupportedDeadline;
        try ensureExecutionDeadline(execution_deadline_ns);
        const result = try self.statistics(alloc, dsn, table);
        try ensureExecutionDeadline(execution_deadline_ns);
        return result;
    }

    pub fn discoverColumns(self: @This(), alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) ![]foreign_source.Column {
        const discover_fn = self.vtable.discover_columns orelse return error.UnsupportedColumnDiscovery;
        return try discover_fn(self.ptr, alloc, dsn, table, execution_deadline_ns);
    }

    pub fn refreshColumns(self: @This(), alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) ![]foreign_source.Column {
        if (self.vtable.refresh_columns) |refresh_fn|
            return try refresh_fn(self.ptr, alloc, dsn, table, execution_deadline_ns);
        return try self.discoverColumns(alloc, dsn, table, execution_deadline_ns);
    }

    pub fn beginSnapshotQuery(self: @This(), alloc: Allocator, dsn: []const u8) !SnapshotQuery {
        const begin_fn = self.vtable.begin_snapshot_query orelse return error.UnsupportedConsistentSnapshot;
        return try begin_fn(self.ptr, alloc, dsn);
    }

    pub fn beginPreparedReplicationSnapshot(
        self: @This(),
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationPollParams,
        execution_deadline_ns: u64,
    ) !PreparedReplicationSnapshot {
        const begin_fn = self.vtable.begin_prepared_replication_snapshot orelse return error.UnsupportedExactCutover;
        return try begin_fn(self.ptr, alloc, dsn, params, execution_deadline_ns);
    }

    pub fn prepareReplication(self: @This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPrepareResult {
        const prepare_fn = self.vtable.prepare_replication orelse return error.UnsupportedReplicationStreaming;
        return try prepare_fn(self.ptr, alloc, dsn, params);
    }

    pub fn pollChanges(self: @This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPollResult {
        const poll_fn = self.vtable.poll_changes orelse return error.UnsupportedReplicationStreaming;
        return try poll_fn(self.ptr, alloc, dsn, params);
    }

    pub fn cleanupReplication(self: @This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationCleanupParams) !void {
        const cleanup_fn = self.vtable.cleanup_replication orelse return error.UnsupportedReplicationCleanup;
        return try cleanup_fn(self.ptr, alloc, dsn, params);
    }
};

fn validateAggregations(
    aggregations: []const foreign_source.NamedAggregation,
    columns: []const foreign_source.Column,
) !void {
    for (aggregations) |aggregation| {
        const field_required = !std.mem.eql(u8, aggregation.definition.type_name, "count");
        if (field_required and aggregation.definition.field == null) return error.InvalidQueryRequest;
        if (aggregation.definition.field) |field| {
            if (columns.len == 0) continue;
            if (!isKnownColumn(field, columns)) return error.UnknownColumn;
        }
    }
}

fn cloneObjectFieldValueAlloc(
    alloc: Allocator,
    row: std.json.Value,
    field: []const u8,
) !std.json.Value {
    if (row != .object) return .null;
    const value = row.object.get(field) orelse return .null;
    return try cloneJsonValueAlloc(alloc, value);
}

fn putOwnedJsonObjectValue(
    alloc: Allocator,
    object: *std.json.ObjectMap,
    key: []const u8,
    value: std.json.Value,
) !void {
    var owned_value = value;
    errdefer foreign_source.deinitJsonValue(alloc, &owned_value);
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    try object.put(alloc, owned_key, owned_value);
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
            for (arr.items) |item| try out.append(try cloneJsonValueAlloc(alloc, item));
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
                {
                    const key = try alloc.dupe(u8, entry.key_ptr.*);
                    errdefer alloc.free(key);
                    var item = try cloneJsonValueAlloc(alloc, entry.value_ptr.*);
                    errdefer foreign_source.deinitJsonValue(alloc, &item);
                    try out.put(alloc, key, item);
                }
            }
            break :blk .{ .object = out };
        },
    };
}

pub const RuntimeSource = struct {
    alloc: Allocator,
    executor: QueryExecutor,
    dsn: []u8,

    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.dsn);
        self.* = undefined;
    }

    pub fn asSource(self: *@This()) foreign_source.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .deinit = RuntimeSource.destroy,
                .query = RuntimeSource.query,
                .aggregate = aggregate,
                .statistics = statistics,
                .statistics_with_deadline = statisticsWithDeadline,
                .begin_snapshot_query = beginSnapshotQuery,
                .begin_prepared_replication_snapshot = beginPreparedReplicationSnapshot,
                .prepare_replication = prepareReplication,
                .poll_changes = pollChanges,
                .cleanup_replication = cleanupReplication,
            },
        };
    }

    fn destroy(ptr: *anyopaque, _: Allocator) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const alloc = self.alloc;
        self.deinit();
        alloc.destroy(self);
    }

    fn query(ptr: *anyopaque, alloc: Allocator, params: foreign_source.QueryParams) !foreign_source.QueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try ensureExecutionDeadline(params.execution_deadline_ns);
        var owned_params = try cloneQueryParamsAlloc(alloc, params);
        defer owned_params.deinit(alloc);

        const auto_discovered_columns = owned_params.columns.len == 0 and
            (owned_params.fields.len > 0 or owned_params.order_by.len > 0 or owned_params.filter_query_json != null);
        if (auto_discovered_columns) {
            owned_params.columns = try self.executor.discoverColumns(alloc, self.dsn, owned_params.table, owned_params.execution_deadline_ns);
        }
        var refreshed_columns = false;
        while (true) {
            validateRequestedFields(owned_params.fields, owned_params.columns) catch |err| {
                if (!shouldRefreshDiscoveredColumns(err, auto_discovered_columns, refreshed_columns)) return err;
                try refreshQueryParamsColumns(self.executor, alloc, self.dsn, &owned_params);
                refreshed_columns = true;
                continue;
            };
            validateRequestedOrderBy(owned_params.order_by, owned_params.columns) catch |err| {
                if (!shouldRefreshDiscoveredColumns(err, auto_discovered_columns, refreshed_columns)) return err;
                try refreshQueryParamsColumns(self.executor, alloc, self.dsn, &owned_params);
                refreshed_columns = true;
                continue;
            };

            var translated_filter: ?filter.Translation = null;
            defer if (translated_filter) |*value| value.deinit(alloc);
            if (owned_params.filter_query_json) |filter_query_json| {
                translated_filter = filter.translateAlloc(alloc, sql.postgresDialect(), filter_query_json, owned_params.columns) catch |err| {
                    if (!shouldRefreshDiscoveredColumns(err, auto_discovered_columns, refreshed_columns)) return err;
                    try refreshQueryParamsColumns(self.executor, alloc, self.dsn, &owned_params);
                    refreshed_columns = true;
                    continue;
                };
            }

            const sql_text = try sql.buildSelectStatementAlloc(alloc, sql.postgresDialect(), .{
                .table = owned_params.table,
                .fields = owned_params.fields,
                .where_sql = if (translated_filter) |value| if (value.where_sql.len > 0) value.where_sql else null else null,
                .order_by = owned_params.order_by,
                .limit = owned_params.limit,
                .offset = owned_params.offset,
            });
            ensureExecutionDeadline(owned_params.execution_deadline_ns) catch |err| {
                alloc.free(sql_text);
                return err;
            };
            const args: []sql.ParameterValue = if (translated_filter) |value|
                cloneArgsAlloc(alloc, value.args) catch |err| {
                    alloc.free(sql_text);
                    return err;
                }
            else
                &.{};
            var result = self.executor.query(alloc, self.dsn, .{
                .sql_text = sql_text,
                .args = args,
            }, owned_params.execution_deadline_ns) catch |err| {
                if (!shouldRefreshDiscoveredColumns(err, auto_discovered_columns, refreshed_columns)) return err;
                try refreshQueryParamsColumns(self.executor, alloc, self.dsn, &owned_params);
                refreshed_columns = true;
                continue;
            };
            errdefer result.deinit(alloc);
            try ensureExecutionDeadline(owned_params.execution_deadline_ns);
            return result;
        }
    }

    fn aggregate(ptr: *anyopaque, alloc: Allocator, params: foreign_source.AggregateParams) !foreign_source.AggregateResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try ensureExecutionDeadline(params.execution_deadline_ns);
        var owned_params = try cloneAggregateParamsAlloc(alloc, params);
        defer owned_params.deinit(alloc);

        const auto_discovered_columns = owned_params.columns.len == 0 and
            (owned_params.filter_query_json != null or aggregateParamsNeedColumnDiscovery(owned_params.aggregations));
        if (auto_discovered_columns) {
            owned_params.columns = try self.executor.discoverColumns(alloc, self.dsn, owned_params.table, owned_params.execution_deadline_ns);
        }
        var refreshed_columns = false;
        while (true) {
            validateAggregations(owned_params.aggregations, owned_params.columns) catch |err| {
                if (!shouldRefreshDiscoveredColumns(err, auto_discovered_columns, refreshed_columns)) return err;
                try refreshAggregateParamsColumns(self.executor, alloc, self.dsn, &owned_params);
                refreshed_columns = true;
                continue;
            };

            var translated_filter: ?filter.Translation = null;
            defer if (translated_filter) |*value| value.deinit(alloc);
            if (owned_params.filter_query_json) |filter_query_json| {
                translated_filter = filter.translateAlloc(alloc, sql.postgresDialect(), filter_query_json, owned_params.columns) catch |err| {
                    if (!shouldRefreshDiscoveredColumns(err, auto_discovered_columns, refreshed_columns)) return err;
                    try refreshAggregateParamsColumns(self.executor, alloc, self.dsn, &owned_params);
                    refreshed_columns = true;
                    continue;
                };
            }

            try ensureExecutionDeadline(owned_params.execution_deadline_ns);
            var result = self.aggregateParamsAlloc(
                alloc,
                owned_params,
                if (translated_filter) |value| if (value.where_sql.len > 0) value.where_sql else null else null,
                if (translated_filter) |value| value.args else &.{},
            ) catch |err| {
                if (!shouldRefreshDiscoveredColumns(err, auto_discovered_columns, refreshed_columns)) return err;
                try refreshAggregateParamsColumns(self.executor, alloc, self.dsn, &owned_params);
                refreshed_columns = true;
                continue;
            };
            errdefer result.deinit(alloc);
            try ensureExecutionDeadline(owned_params.execution_deadline_ns);
            return result;
        }
    }

    fn statistics(ptr: *anyopaque, table: []const u8) !foreign_source.TableStatistics {
        return try statisticsWithDeadline(ptr, table, null);
    }

    fn statisticsWithDeadline(ptr: *anyopaque, table: []const u8, execution_deadline_ns: ?u64) !foreign_source.TableStatistics {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try ensureExecutionDeadline(execution_deadline_ns);
        const result = try self.executor.statisticsWithDeadline(self.alloc, self.dsn, table, execution_deadline_ns);
        try ensureExecutionDeadline(execution_deadline_ns);
        return result;
    }

    const RuntimeSnapshotReader = struct {
        alloc: Allocator,
        executor: QueryExecutor,
        dsn: []u8,
        snapshot_query: QueryExecutor.SnapshotQuery,

        fn deinit(self: *@This()) void {
            self.snapshot_query.deinit(self.alloc);
            self.alloc.free(self.dsn);
            self.* = undefined;
        }

        fn asSnapshotReader(self: *@This()) foreign_source.SnapshotReader {
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = RuntimeSnapshotReader.destroy,
                    .query = RuntimeSnapshotReader.query,
                },
            };
        }

        fn destroy(ptr: *anyopaque, _: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const alloc = self.alloc;
            self.deinit();
            alloc.destroy(self);
        }

        fn query(ptr: *anyopaque, alloc: Allocator, params: foreign_source.QueryParams) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try ensureExecutionDeadline(params.execution_deadline_ns);
            var owned_params = try cloneQueryParamsAlloc(alloc, params);
            defer owned_params.deinit(alloc);

            const auto_discovered_columns = owned_params.columns.len == 0 and
                (owned_params.fields.len > 0 or owned_params.order_by.len > 0 or owned_params.filter_query_json != null);
            if (auto_discovered_columns) {
                owned_params.columns = try self.executor.discoverColumns(alloc, self.dsn, owned_params.table, owned_params.execution_deadline_ns);
            }
            var refreshed_columns = false;
            while (true) {
                validateRequestedFields(owned_params.fields, owned_params.columns) catch |err| {
                    if (!shouldRefreshDiscoveredColumns(err, auto_discovered_columns, refreshed_columns)) return err;
                    try refreshQueryParamsColumns(self.executor, alloc, self.dsn, &owned_params);
                    refreshed_columns = true;
                    continue;
                };
                validateRequestedOrderBy(owned_params.order_by, owned_params.columns) catch |err| {
                    if (!shouldRefreshDiscoveredColumns(err, auto_discovered_columns, refreshed_columns)) return err;
                    try refreshQueryParamsColumns(self.executor, alloc, self.dsn, &owned_params);
                    refreshed_columns = true;
                    continue;
                };

                var translated_filter: ?filter.Translation = null;
                defer if (translated_filter) |*value| value.deinit(alloc);
                if (owned_params.filter_query_json) |filter_query_json| {
                    translated_filter = filter.translateAlloc(alloc, sql.postgresDialect(), filter_query_json, owned_params.columns) catch |err| {
                        if (!shouldRefreshDiscoveredColumns(err, auto_discovered_columns, refreshed_columns)) return err;
                        try refreshQueryParamsColumns(self.executor, alloc, self.dsn, &owned_params);
                        refreshed_columns = true;
                        continue;
                    };
                }

                const sql_text = try sql.buildSelectStatementAlloc(alloc, sql.postgresDialect(), .{
                    .table = owned_params.table,
                    .fields = owned_params.fields,
                    .where_sql = if (translated_filter) |value| if (value.where_sql.len > 0) value.where_sql else null else null,
                    .order_by = owned_params.order_by,
                    .limit = owned_params.limit,
                    .offset = owned_params.offset,
                });
                ensureExecutionDeadline(owned_params.execution_deadline_ns) catch |err| {
                    alloc.free(sql_text);
                    return err;
                };
                const args: []sql.ParameterValue = if (translated_filter) |value|
                    cloneArgsAlloc(alloc, value.args) catch |err| {
                        alloc.free(sql_text);
                        return err;
                    }
                else
                    &.{};
                var result = try self.snapshot_query.queryPrepared(alloc, .{
                    .sql_text = sql_text,
                    .args = args,
                }, owned_params.execution_deadline_ns);
                errdefer result.deinit(alloc);
                try ensureExecutionDeadline(owned_params.execution_deadline_ns);
                return result;
            }
        }
    };

    fn beginSnapshotQuery(ptr: *anyopaque, alloc: Allocator) !foreign_source.SnapshotReader {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const snapshot_query = try self.executor.beginSnapshotQuery(alloc, self.dsn);
        errdefer {
            var owned_snapshot_query = snapshot_query;
            owned_snapshot_query.deinit(alloc);
        }

        const reader = try alloc.create(RuntimeSnapshotReader);
        errdefer alloc.destroy(reader);
        reader.* = .{
            .alloc = alloc,
            .executor = self.executor,
            .dsn = try alloc.dupe(u8, self.dsn),
            .snapshot_query = snapshot_query,
        };
        return reader.asSnapshotReader();
    }

    fn beginPreparedReplicationSnapshot(
        ptr: *anyopaque,
        alloc: Allocator,
        params: foreign_source.ReplicationPollParams,
        execution_deadline_ns: u64,
    ) !foreign_source.PreparedReplicationSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var owned_params = try cloneReplicationPollParamsAlloc(alloc, params);
        defer owned_params.deinit(alloc);

        var prepared = try self.executor.beginPreparedReplicationSnapshot(
            alloc,
            self.dsn,
            owned_params,
            execution_deadline_ns,
        );
        errdefer prepared.deinit(alloc);

        const reader = try alloc.create(RuntimeSnapshotReader);
        errdefer alloc.destroy(reader);
        reader.* = .{
            .alloc = alloc,
            .executor = self.executor,
            .dsn = try alloc.dupe(u8, self.dsn),
            .snapshot_query = prepared.snapshot_query,
        };

        return .{
            .checkpoint = prepared.checkpoint,
            .reader = reader.asSnapshotReader(),
        };
    }

    fn prepareReplication(ptr: *anyopaque, alloc: Allocator, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPrepareResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var owned_params = try cloneReplicationPollParamsAlloc(alloc, params);
        defer owned_params.deinit(alloc);
        return try self.executor.prepareReplication(alloc, self.dsn, owned_params);
    }

    fn pollChanges(ptr: *anyopaque, alloc: Allocator, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPollResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var owned_params = try cloneReplicationPollParamsAlloc(alloc, params);
        defer owned_params.deinit(alloc);
        return try self.executor.pollChanges(alloc, self.dsn, owned_params);
    }

    fn cleanupReplication(ptr: *anyopaque, alloc: Allocator, params: foreign_source.ReplicationCleanupParams) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const slot_name = try alloc.dupe(u8, params.slot_name);
        const publication_name = alloc.dupe(u8, params.publication_name) catch |err| {
            alloc.free(slot_name);
            return err;
        };
        var owned_params = foreign_source.ReplicationCleanupParams{
            .slot_name = slot_name,
            .publication_name = publication_name,
            .execution_deadline_ns = params.execution_deadline_ns,
        };
        defer owned_params.deinit(alloc);
        return try self.executor.cleanupReplication(alloc, self.dsn, owned_params);
    }

    fn aggregateParamsAlloc(
        self: *@This(),
        alloc: Allocator,
        params: foreign_source.AggregateParams,
        where_sql: ?[]const u8,
        where_args: []const sql.ParameterValue,
    ) !foreign_source.AggregateResult {
        const simple_aggs = try collectSimpleAggregationsAlloc(alloc, params.aggregations);
        defer if (simple_aggs.len > 0) alloc.free(simple_aggs);

        const out = try alloc.alloc(foreign_source.NamedValue, params.aggregations.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*item| item.deinit(alloc);
            alloc.free(out);
        }

        if (simple_aggs.len > 0) {
            try ensureExecutionDeadline(params.execution_deadline_ns);
            const prepared = try buildSimpleAggregatePreparedQueryAlloc(alloc, params.table, simple_aggs, where_sql, where_args);
            var result = try self.executor.query(alloc, self.dsn, prepared, params.execution_deadline_ns);
            defer result.deinit(alloc);
            const row = if (result.rows.len > 0) result.rows[0] else std.json.Value{ .object = std.json.ObjectMap.empty };
            defer if (result.rows.len == 0) foreign_source.deinitJsonValue(alloc, @constCast(&row));
            for (simple_aggs) |simple_agg| {
                const name = try alloc.dupe(u8, simple_agg.name);
                errdefer alloc.free(name);
                var value = try cloneObjectFieldValueAlloc(alloc, row, simple_agg.name);
                errdefer foreign_source.deinitJsonValue(alloc, &value);
                out[initialized] = .{
                    .name = name,
                    .value = value,
                };
                initialized += 1;
            }
        }

        for (params.aggregations) |aggregation| {
            try ensureExecutionDeadline(params.execution_deadline_ns);
            if (isSimpleAggregation(aggregation.definition.type_name)) continue;
            const name = try alloc.dupe(u8, aggregation.name);
            errdefer alloc.free(name);
            var value = try self.executeComplexAggregationAlloc(alloc, params.table, aggregation.definition, where_sql, where_args, params.execution_deadline_ns);
            errdefer foreign_source.deinitJsonValue(alloc, &value);
            out[initialized] = .{
                .name = name,
                .value = value,
            };
            initialized += 1;
        }

        return .{ .results = out[0..initialized] };
    }

    fn executeComplexAggregationAlloc(
        self: *@This(),
        alloc: Allocator,
        table: []const u8,
        definition: foreign_source.AggregationDef,
        where_sql: ?[]const u8,
        where_args: []const sql.ParameterValue,
        execution_deadline_ns: ?u64,
    ) !std.json.Value {
        if (std.mem.eql(u8, definition.type_name, "stats")) {
            return try self.runStatsAggregateAlloc(alloc, table, definition, where_sql, where_args, execution_deadline_ns);
        }
        if (std.mem.eql(u8, definition.type_name, "terms")) {
            return try self.runTermsAggregateAlloc(alloc, table, definition, where_sql, where_args, execution_deadline_ns);
        }
        return error.UnsupportedAggregate;
    }

    fn runStatsAggregateAlloc(
        self: *@This(),
        alloc: Allocator,
        table: []const u8,
        definition: foreign_source.AggregationDef,
        where_sql: ?[]const u8,
        where_args: []const sql.ParameterValue,
        execution_deadline_ns: ?u64,
    ) !std.json.Value {
        const field = definition.field orelse return error.InvalidQueryRequest;
        const quoted_field = try sql.postgresDialect().quote_identifier(alloc, field);
        defer alloc.free(quoted_field);
        const quoted_table = try sql.postgresDialect().quote_relation(alloc, table);
        defer alloc.free(quoted_table);
        const where_clause = if (where_sql) |value|
            try std.fmt.allocPrint(alloc, " WHERE {s}", .{value})
        else
            try alloc.dupe(u8, "");
        defer alloc.free(where_clause);
        const sql_text = try std.fmt.allocPrint(
            alloc,
            "SELECT COUNT({s}) AS \"count\", MIN({s}) AS \"min\", MAX({s}) AS \"max\", AVG({s}) AS \"avg\", SUM({s}) AS \"sum\" FROM {s}{s}",
            .{
                quoted_field,
                quoted_field,
                quoted_field,
                quoted_field,
                quoted_field,
                quoted_table,
                where_clause,
            },
        );
        defer alloc.free(sql_text);
        const prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, sql_text),
            .args = try cloneArgsAlloc(alloc, where_args),
        };
        var result = try self.executor.query(alloc, self.dsn, prepared, execution_deadline_ns);
        defer result.deinit(alloc);
        if (result.rows.len == 0) {
            var empty = std.json.ObjectMap.empty;
            try putOwnedJsonObjectValue(alloc, &empty, "count", .{ .integer = 0 });
            return .{ .object = empty };
        }
        return try cloneJsonValueAlloc(alloc, result.rows[0]);
    }

    fn runTermsAggregateAlloc(
        self: *@This(),
        alloc: Allocator,
        table: []const u8,
        definition: foreign_source.AggregationDef,
        where_sql: ?[]const u8,
        where_args: []const sql.ParameterValue,
        execution_deadline_ns: ?u64,
    ) !std.json.Value {
        const field = definition.field orelse return error.InvalidQueryRequest;
        const quoted_field = try sql.postgresDialect().quote_identifier(alloc, field);
        defer alloc.free(quoted_field);
        const quoted_table = try sql.postgresDialect().quote_relation(alloc, table);
        defer alloc.free(quoted_table);
        const size = @min(definition.size orelse 10, 1000);
        const where_clause = if (where_sql) |value|
            try std.fmt.allocPrint(alloc, " WHERE {s}", .{value})
        else
            try alloc.dupe(u8, "");
        defer alloc.free(where_clause);
        const sql_text = try std.fmt.allocPrint(
            alloc,
            "SELECT {s} AS \"key\", COUNT(*) AS \"doc_count\" FROM {s}{s} GROUP BY {s} ORDER BY \"doc_count\" DESC LIMIT {d}",
            .{ quoted_field, quoted_table, where_clause, quoted_field, size },
        );
        defer alloc.free(sql_text);
        const prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, sql_text),
            .args = try cloneArgsAlloc(alloc, where_args),
        };
        var result = try self.executor.query(alloc, self.dsn, prepared, execution_deadline_ns);
        defer result.deinit(alloc);

        var buckets = std.json.Array.init(alloc);
        errdefer {
            for (buckets.items) |*item| foreign_source.deinitJsonValue(alloc, item);
            buckets.deinit();
        }
        for (result.rows) |row| {
            try ensureExecutionDeadline(execution_deadline_ns);
            var bucket = std.json.ObjectMap.empty;
            errdefer {
                var it = bucket.iterator();
                while (it.next()) |entry| {
                    alloc.free(@constCast(entry.key_ptr.*));
                    foreign_source.deinitJsonValue(alloc, entry.value_ptr);
                }
                bucket.deinit(alloc);
            }
            try putOwnedJsonObjectValue(alloc, &bucket, "key", try cloneObjectFieldValueAlloc(alloc, row, "key"));
            try putOwnedJsonObjectValue(alloc, &bucket, "doc_count", try cloneObjectFieldValueAlloc(alloc, row, "doc_count"));
            try buckets.append(.{ .object = bucket });
        }
        return .{ .array = buckets };
    }
};

fn cloneColumnsAlloc(alloc: Allocator, columns: []const foreign_source.Column) ![]foreign_source.Column {
    if (columns.len == 0) return &.{};
    const out = try alloc.alloc(foreign_source.Column, columns.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*column| column.deinit(alloc);
        alloc.free(out);
    }
    for (columns, 0..) |column, i| {
        const name = try alloc.dupe(u8, column.name);
        errdefer alloc.free(name);
        const data_type = try alloc.dupe(u8, column.data_type);
        errdefer alloc.free(data_type);
        out[i] = .{
            .name = name,
            .data_type = data_type,
            .nullable = column.nullable,
        };
        initialized += 1;
    }
    return out;
}

fn cloneFieldsAlloc(alloc: Allocator, fields: []const []u8) ![][]u8 {
    if (fields.len == 0) return &.{};
    const out = try alloc.alloc([]u8, fields.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |field| alloc.free(field);
        alloc.free(out);
    }
    for (fields, 0..) |field, i| {
        out[i] = try alloc.dupe(u8, field);
        initialized += 1;
    }
    return out;
}

fn cloneOrderByAlloc(alloc: Allocator, order_by: []const foreign_source.SortField) ![]foreign_source.SortField {
    if (order_by.len == 0) return &.{};
    const out = try alloc.alloc(foreign_source.SortField, order_by.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*field| field.deinit(alloc);
        alloc.free(out);
    }
    for (order_by, 0..) |field, i| {
        out[i] = .{
            .field = try alloc.dupe(u8, field.field),
            .desc = field.desc,
        };
        initialized += 1;
    }
    return out;
}

fn cloneAggregationsAlloc(alloc: Allocator, aggregations: []const foreign_source.NamedAggregation) ![]foreign_source.NamedAggregation {
    if (aggregations.len == 0) return &.{};
    const out = try alloc.alloc(foreign_source.NamedAggregation, aggregations.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*aggregation| aggregation.deinit(alloc);
        alloc.free(out);
    }
    for (aggregations, 0..) |aggregation, i| {
        const name = try alloc.dupe(u8, aggregation.name);
        errdefer alloc.free(name);
        const type_name = try alloc.dupe(u8, aggregation.definition.type_name);
        errdefer alloc.free(type_name);
        const field = if (aggregation.definition.field) |value| try alloc.dupe(u8, value) else null;
        errdefer if (field) |value| alloc.free(value);
        out[i] = .{
            .name = name,
            .definition = .{
                .type_name = type_name,
                .field = field,
                .size = aggregation.definition.size,
            },
        };
        initialized += 1;
    }
    return out;
}

fn cloneQueryParamsAlloc(alloc: Allocator, params: foreign_source.QueryParams) !foreign_source.QueryParams {
    var out = foreign_source.QueryParams{
        .table = try alloc.dupe(u8, params.table),
        .limit = params.limit,
        .offset = params.offset,
        .execution_deadline_ns = params.execution_deadline_ns,
    };
    errdefer out.deinit(alloc);
    out.fields = try cloneFieldsAlloc(alloc, params.fields);
    out.filter_query_json = if (params.filter_query_json) |query| try alloc.dupe(u8, query) else null;
    out.columns = try cloneColumnsAlloc(alloc, params.columns);
    out.order_by = try cloneOrderByAlloc(alloc, params.order_by);
    return out;
}

fn cloneAggregateParamsAlloc(alloc: Allocator, params: foreign_source.AggregateParams) !foreign_source.AggregateParams {
    var out = foreign_source.AggregateParams{
        .table = try alloc.dupe(u8, params.table),
        .execution_deadline_ns = params.execution_deadline_ns,
    };
    errdefer out.deinit(alloc);
    out.filter_query_json = if (params.filter_query_json) |query| try alloc.dupe(u8, query) else null;
    out.columns = try cloneColumnsAlloc(alloc, params.columns);
    out.aggregations = try cloneAggregationsAlloc(alloc, params.aggregations);
    return out;
}

const SimpleAggregation = struct {
    name: []const u8,
    type_name: []const u8,
    field: ?[]const u8,
};

fn aggregateParamsNeedColumnDiscovery(aggregations: []const foreign_source.NamedAggregation) bool {
    for (aggregations) |aggregation| {
        if (aggregation.definition.field != null) return true;
    }
    return false;
}

fn collectSimpleAggregationsAlloc(alloc: Allocator, aggregations: []const foreign_source.NamedAggregation) ![]SimpleAggregation {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (isSimpleAggregation(aggregation.definition.type_name)) count += 1;
    }
    if (count == 0) return &.{};

    const out = try alloc.alloc(SimpleAggregation, count);
    var idx: usize = 0;
    for (aggregations) |aggregation| {
        if (!isSimpleAggregation(aggregation.definition.type_name)) continue;
        out[idx] = .{
            .name = aggregation.name,
            .type_name = aggregation.definition.type_name,
            .field = aggregation.definition.field,
        };
        idx += 1;
    }
    return out;
}

fn isSimpleAggregation(type_name: []const u8) bool {
    return std.mem.eql(u8, type_name, "count") or
        std.mem.eql(u8, type_name, "sum") or
        std.mem.eql(u8, type_name, "avg") or
        std.mem.eql(u8, type_name, "min") or
        std.mem.eql(u8, type_name, "max");
}

fn buildSimpleAggregatePreparedQueryAlloc(
    alloc: Allocator,
    table: []const u8,
    aggregations: []const SimpleAggregation,
    where_sql: ?[]const u8,
    where_args: []const sql.ParameterValue,
) !sql.PreparedQuery {
    const quoted_table = try sql.postgresDialect().quote_relation(alloc, table);
    defer alloc.free(quoted_table);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "SELECT ");
    for (aggregations, 0..) |aggregation, i| {
        if (i != 0) try out.appendSlice(alloc, ", ");
        const fn_name = if (std.mem.eql(u8, aggregation.type_name, "count"))
            "COUNT"
        else if (std.mem.eql(u8, aggregation.type_name, "sum"))
            "SUM"
        else if (std.mem.eql(u8, aggregation.type_name, "avg"))
            "AVG"
        else if (std.mem.eql(u8, aggregation.type_name, "min"))
            "MIN"
        else
            "MAX";
        const alias = try sql.postgresDialect().quote_identifier(alloc, aggregation.name);
        defer alloc.free(alias);
        try out.appendSlice(alloc, fn_name);
        try out.append(alloc, '(');
        if (aggregation.field) |field| {
            const quoted_field = try sql.postgresDialect().quote_identifier(alloc, field);
            defer alloc.free(quoted_field);
            try out.appendSlice(alloc, quoted_field);
        } else {
            try out.append(alloc, '*');
        }
        try out.appendSlice(alloc, ") AS ");
        try out.appendSlice(alloc, alias);
    }
    try out.appendSlice(alloc, " FROM ");
    try out.appendSlice(alloc, quoted_table);
    if (where_sql) |value| {
        try out.appendSlice(alloc, " WHERE ");
        try out.appendSlice(alloc, value);
    }
    return .{
        .sql_text = try out.toOwnedSlice(alloc),
        .args = try cloneArgsAlloc(alloc, where_args),
    };
}

fn validateRequestedFields(fields: []const []const u8, columns: []const foreign_source.Column) !void {
    if (columns.len == 0) return;
    for (fields) |field| {
        if (!isKnownColumn(field, columns)) return error.UnknownColumn;
    }
}

fn shouldRefreshDiscoveredColumns(err: anyerror, auto_discovered: bool, already_refreshed: bool) bool {
    return auto_discovered and
        !already_refreshed and
        (err == error.UnknownColumn or err == error.ForeignTableNotFound);
}

fn replaceOwnedColumns(
    alloc: Allocator,
    destination: *[]foreign_source.Column,
    replacement: []foreign_source.Column,
) void {
    for (destination.*) |*column| column.deinit(alloc);
    if (destination.len > 0) alloc.free(destination.*);
    destination.* = replacement;
}

fn refreshQueryParamsColumns(
    executor: QueryExecutor,
    alloc: Allocator,
    dsn: []const u8,
    params: *foreign_source.QueryParams,
) !void {
    const refreshed = try executor.refreshColumns(
        alloc,
        dsn,
        params.table,
        params.execution_deadline_ns,
    );
    replaceOwnedColumns(alloc, &params.columns, refreshed);
}

fn refreshAggregateParamsColumns(
    executor: QueryExecutor,
    alloc: Allocator,
    dsn: []const u8,
    params: *foreign_source.AggregateParams,
) !void {
    const refreshed = try executor.refreshColumns(
        alloc,
        dsn,
        params.table,
        params.execution_deadline_ns,
    );
    replaceOwnedColumns(alloc, &params.columns, refreshed);
}

fn validateRequestedOrderBy(order_by: []const foreign_source.SortField, columns: []const foreign_source.Column) !void {
    if (columns.len == 0) return;
    for (order_by) |sort_field| {
        if (!isKnownColumn(sort_field.field, columns)) return error.UnknownColumn;
    }
}

fn isKnownColumn(name: []const u8, columns: []const foreign_source.Column) bool {
    if (columns.len == 0) return true;
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return true;
    }
    return false;
}

fn cloneArgsAlloc(alloc: Allocator, args: []const sql.ParameterValue) ![]sql.ParameterValue {
    if (args.len == 0) return &.{};
    const out = try alloc.alloc(sql.ParameterValue, args.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*arg| arg.deinit(alloc);
        alloc.free(out);
    }
    for (args, 0..) |arg, idx| {
        out[idx] = switch (arg) {
            .null => .null,
            .bool => |value| .{ .bool = value },
            .integer => |value| .{ .integer = value },
            .float => |value| .{ .float = value },
            .string => |value| .{ .string = try alloc.dupe(u8, value) },
        };
        initialized += 1;
    }
    return out;
}

fn cloneReplicationPollParamsAlloc(
    alloc: Allocator,
    params: foreign_source.ReplicationPollParams,
) !foreign_source.ReplicationPollParams {
    var out = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, params.table),
        .limit = params.limit,
        .execution_deadline_ns = params.execution_deadline_ns,
        .reclaim_exact_cutover_slot = params.reclaim_exact_cutover_slot,
        .exact_cutover_intent = params.exact_cutover_intent,
    };
    errdefer out.deinit(alloc);
    out.slot_name = if (params.slot_name) |slot_name| try alloc.dupe(u8, slot_name) else null;
    out.publication_name = if (params.publication_name) |publication_name| try alloc.dupe(u8, publication_name) else null;
    out.cutover_lock_name = if (params.cutover_lock_name) |name| try alloc.dupe(u8, name) else null;
    out.retired_slot_name = if (params.retired_slot_name) |name| try alloc.dupe(u8, name) else null;
    out.retired_publication_name = if (params.retired_publication_name) |name| try alloc.dupe(u8, name) else null;
    out.filter_query_json = if (params.filter_query_json) |query| try alloc.dupe(u8, query) else null;
    out.checkpoint = if (params.checkpoint) |checkpoint| try alloc.dupe(u8, checkpoint) else null;
    return out;
}

const ExecutorRegistration = struct {
    executor: QueryExecutor,
};

fn deinitExecutorRegistration(ptr: *anyopaque, alloc: Allocator) void {
    const registration: *ExecutorRegistration = @ptrCast(@alignCast(ptr));
    registration.executor.deinit(alloc);
    alloc.destroy(registration);
}

fn factoryFromExecutor(ptr: *anyopaque, alloc: Allocator, config: foreign_source.Config) !foreign_source.Source {
    const registration: *ExecutorRegistration = @ptrCast(@alignCast(ptr));
    var owned = config;
    defer owned.deinit(alloc);
    const runtime_source = try alloc.create(RuntimeSource);
    errdefer alloc.destroy(runtime_source);
    runtime_source.* = .{
        .alloc = alloc,
        .executor = registration.executor,
        .dsn = try alloc.dupe(u8, owned.dsn),
    };
    return runtime_source.asSource();
}

pub fn registerExecutor(
    alloc: Allocator,
    registry: *foreign_source.Registry,
    executor: QueryExecutor,
) !void {
    const registration = try alloc.create(ExecutorRegistration);
    registration.* = .{
        .executor = executor,
    };
    try registry.registerWithContext(alloc, .postgres, registration, factoryFromExecutor, deinitExecutorRegistration);
}

test "postgres source runtime delegates replication polling through executor" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        last_checkpoint: ?[]u8 = null,
        last_slot_name: ?[]u8 = null,
        last_publication_name: ?[]u8 = null,
        prepare_calls: usize = 0,

        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: sql.PreparedQuery, _: ?u64) !foreign_source.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn prepareReplication(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPrepareResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.prepare_calls += 1;
            if (self.last_slot_name) |slot_name| inner_alloc.free(slot_name);
            if (self.last_publication_name) |publication_name| inner_alloc.free(publication_name);
            self.last_slot_name = if (params.slot_name) |slot_name| try inner_alloc.dupe(u8, slot_name) else null;
            self.last_publication_name = if (params.publication_name) |publication_name| try inner_alloc.dupe(u8, publication_name) else null;
            return .{ .checkpoint = try inner_alloc.dupe(u8, "lsn:prepared") };
        }

        fn pollChanges(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPollResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.last_checkpoint) |checkpoint| inner_alloc.free(checkpoint);
            if (self.last_slot_name) |slot_name| inner_alloc.free(slot_name);
            if (self.last_publication_name) |publication_name| inner_alloc.free(publication_name);
            self.last_checkpoint = if (params.checkpoint) |checkpoint| try inner_alloc.dupe(u8, checkpoint) else null;
            self.last_slot_name = if (params.slot_name) |slot_name| try inner_alloc.dupe(u8, slot_name) else null;
            self.last_publication_name = if (params.publication_name) |publication_name| try inner_alloc.dupe(u8, publication_name) else null;

            const changes = try inner_alloc.alloc(foreign_source.ReplicationChange, 1);
            changes[0] = .{
                .op = .insert,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:9"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:9\"}", .{}),
                .lag_records = 2,
            };
            return .{ .changes = changes, .lag_records = 2 };
        }
    };

    var executor = FakeExecutor{};
    var registry = foreign_source.Registry{};
    defer {
        if (executor.last_checkpoint) |checkpoint| alloc.free(checkpoint);
        if (executor.last_slot_name) |slot_name| alloc.free(slot_name);
        if (executor.last_publication_name) |publication_name| alloc.free(publication_name);
        registry.deinit(alloc);
    }
    try registerExecutor(alloc, &registry, .{
        .ptr = &executor,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .prepare_replication = FakeExecutor.prepareReplication,
            .poll_changes = FakeExecutor.pollChanges,
        },
    });

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://db"),
    });
    defer src.deinit(alloc);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, "customers"),
        .slot_name = try alloc.dupe(u8, "antfly_slot"),
        .publication_name = try alloc.dupe(u8, "antfly_pub"),
        .checkpoint = try alloc.dupe(u8, "lsn:8"),
        .limit = 5,
    };
    defer poll_params.deinit(alloc);

    var result = try src.pollChanges(alloc, poll_params);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.changes.len);
    try std.testing.expectEqual(@as(usize, 0), executor.prepare_calls);
    try std.testing.expectEqualStrings("lsn:8", executor.last_checkpoint.?);
    try std.testing.expectEqualStrings("antfly_slot", executor.last_slot_name.?);
    try std.testing.expectEqualStrings("antfly_pub", executor.last_publication_name.?);
    try std.testing.expectEqualStrings("lsn:9", result.changes[0].checkpoint);
    try std.testing.expectEqual(@as(u64, 2), result.changes[0].lag_records);
}

test "postgres source runtime delegates replication prepare through executor" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        last_slot_name: ?[]u8 = null,
        last_publication_name: ?[]u8 = null,
        prepare_calls: usize = 0,

        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: sql.PreparedQuery, _: ?u64) !foreign_source.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn prepareReplication(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPrepareResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.prepare_calls += 1;
            if (self.last_slot_name) |slot_name| inner_alloc.free(slot_name);
            if (self.last_publication_name) |publication_name| inner_alloc.free(publication_name);
            self.last_slot_name = if (params.slot_name) |slot_name| try inner_alloc.dupe(u8, slot_name) else null;
            self.last_publication_name = if (params.publication_name) |publication_name| try inner_alloc.dupe(u8, publication_name) else null;
            return .{ .checkpoint = try inner_alloc.dupe(u8, "lsn:prepared") };
        }
    };

    var executor = FakeExecutor{};
    var registry = foreign_source.Registry{};
    defer {
        if (executor.last_slot_name) |slot_name| alloc.free(slot_name);
        if (executor.last_publication_name) |publication_name| alloc.free(publication_name);
        registry.deinit(alloc);
    }
    try registerExecutor(alloc, &registry, .{
        .ptr = &executor,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .prepare_replication = FakeExecutor.prepareReplication,
        },
    });

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://db"),
    });
    defer src.deinit(alloc);

    var prepare_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, "customers"),
        .slot_name = try alloc.dupe(u8, "antfly_slot"),
        .publication_name = try alloc.dupe(u8, "antfly_pub"),
    };
    defer prepare_params.deinit(alloc);

    var prepare_result = try src.prepareReplication(alloc, prepare_params);
    defer prepare_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), executor.prepare_calls);
    try std.testing.expectEqualStrings("antfly_slot", executor.last_slot_name.?);
    try std.testing.expectEqualStrings("antfly_pub", executor.last_publication_name.?);
    try std.testing.expectEqualStrings("lsn:prepared", prepare_result.checkpoint);
}

test "postgres source runtime delegates consistent snapshot queries through executor" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        const Parent = @This();

        begin_calls: usize = 0,
        query_calls: usize = 0,
        last_sql: ?[]u8 = null,
        last_deadline_ns: ?u64 = null,

        const SnapshotSession = struct {
            owner: *Parent,

            fn destroy(ptr: *anyopaque, inner_alloc: Allocator) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                inner_alloc.destroy(self);
            }

            fn query(ptr: *anyopaque, inner_alloc: Allocator, prepared: sql.PreparedQuery, execution_deadline_ns: ?u64) !foreign_source.QueryResult {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.owner.query_calls += 1;
                self.owner.last_deadline_ns = execution_deadline_ns;
                if (self.owner.last_sql) |value| inner_alloc.free(value);
                self.owner.last_sql = try inner_alloc.dupe(u8, prepared.sql_text);
                var owned = prepared;
                defer owned.deinit(inner_alloc);
                const rows = try inner_alloc.alloc(std.json.Value, 1);
                rows[0] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"snap:1\"}", .{});
                return .{ .rows = rows, .total = 1 };
            }
        };

        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: sql.PreparedQuery, _: ?u64) !foreign_source.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn beginSnapshotQuery(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8) !QueryExecutor.SnapshotQuery {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.begin_calls += 1;
            const session = try inner_alloc.create(SnapshotSession);
            session.* = .{ .owner = self };
            return .{
                .ptr = session,
                .vtable = &.{
                    .deinit = SnapshotSession.destroy,
                    .query = SnapshotSession.query,
                },
            };
        }
    };

    var executor = FakeExecutor{};
    var registry = foreign_source.Registry{};
    defer {
        if (executor.last_sql) |value| alloc.free(value);
        registry.deinit(alloc);
    }
    try registerExecutor(alloc, &registry, .{
        .ptr = &executor,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .begin_snapshot_query = FakeExecutor.beginSnapshotQuery,
        },
    });

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://db"),
    });
    defer src.deinit(alloc);

    var snapshot = try src.beginSnapshotQuery(alloc);
    defer snapshot.deinit(alloc);

    const execution_deadline_ns = platform_time.monotonicNs() + 60 * std.time.ns_per_s;
    var query_params = foreign_source.QueryParams{
        .table = try alloc.dupe(u8, "customers"),
        .limit = 10,
        .offset = 0,
        .execution_deadline_ns = execution_deadline_ns,
    };
    defer query_params.deinit(alloc);

    var result = try snapshot.query(alloc, query_params);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), executor.begin_calls);
    try std.testing.expectEqual(@as(usize, 1), executor.query_calls);
    try std.testing.expectEqual(@as(?u64, execution_deadline_ns), executor.last_deadline_ns);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(std.mem.indexOf(u8, executor.last_sql.?, "FROM \"customers\"") != null);

    var expired_params = foreign_source.QueryParams{
        .table = try alloc.dupe(u8, "customers"),
        .execution_deadline_ns = platform_time.monotonicNs(),
    };
    defer expired_params.deinit(alloc);
    try std.testing.expectError(error.Timeout, snapshot.query(alloc, expired_params));
    try std.testing.expectEqual(@as(usize, 1), executor.query_calls);
}

test "postgres source runtime delegates prepared replication snapshot through executor" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        const Parent = @This();

        begin_calls: usize = 0,
        query_calls: usize = 0,
        last_deadline_ns: u64 = 0,
        last_slot_name: ?[]u8 = null,

        const SnapshotSession = struct {
            owner: *Parent,

            fn destroy(ptr: *anyopaque, inner_alloc: Allocator) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                inner_alloc.destroy(self);
            }

            fn query(ptr: *anyopaque, inner_alloc: Allocator, prepared: sql.PreparedQuery, _: ?u64) !foreign_source.QueryResult {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.owner.query_calls += 1;
                var owned = prepared;
                defer owned.deinit(inner_alloc);
                const rows = try inner_alloc.alloc(std.json.Value, 1);
                rows[0] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"snap:prepared\"}", .{});
                return .{ .rows = rows, .total = 1 };
            }
        };

        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: sql.PreparedQuery, _: ?u64) !foreign_source.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn beginPreparedReplicationSnapshot(
            ptr: *anyopaque,
            inner_alloc: Allocator,
            _: []const u8,
            params: foreign_source.ReplicationPollParams,
            execution_deadline_ns: u64,
        ) !QueryExecutor.PreparedReplicationSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.begin_calls += 1;
            self.last_deadline_ns = execution_deadline_ns;
            if (self.last_slot_name) |value| inner_alloc.free(value);
            self.last_slot_name = if (params.slot_name) |value| try inner_alloc.dupe(u8, value) else null;
            const session = try inner_alloc.create(SnapshotSession);
            session.* = .{ .owner = self };
            return .{
                .checkpoint = try inner_alloc.dupe(u8, "lsn:cutover"),
                .snapshot_query = .{
                    .ptr = session,
                    .vtable = &.{
                        .deinit = SnapshotSession.destroy,
                        .query = SnapshotSession.query,
                    },
                },
            };
        }
    };

    var executor = FakeExecutor{};
    var registry = foreign_source.Registry{};
    defer {
        if (executor.last_slot_name) |value| alloc.free(value);
        registry.deinit(alloc);
    }
    try registerExecutor(alloc, &registry, .{
        .ptr = &executor,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .begin_prepared_replication_snapshot = FakeExecutor.beginPreparedReplicationSnapshot,
        },
    });

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://db"),
    });
    defer src.deinit(alloc);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, "customers"),
        .slot_name = try alloc.dupe(u8, "antfly_slot"),
        .publication_name = try alloc.dupe(u8, "antfly_pub"),
    };
    defer poll_params.deinit(alloc);

    const execution_deadline_ns = std.math.maxInt(u64);
    var prepared = try src.beginPreparedReplicationSnapshot(
        alloc,
        poll_params,
        execution_deadline_ns,
    );
    defer prepared.deinit(alloc);

    var query_params = foreign_source.QueryParams{
        .table = try alloc.dupe(u8, "customers"),
        .limit = 10,
        .offset = 0,
    };
    defer query_params.deinit(alloc);
    var result = try prepared.reader.query(alloc, query_params);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), executor.begin_calls);
    try std.testing.expectEqual(@as(usize, 1), executor.query_calls);
    try std.testing.expectEqual(execution_deadline_ns, executor.last_deadline_ns);
    try std.testing.expectEqualStrings("antfly_slot", executor.last_slot_name.?);
    try std.testing.expectEqualStrings("lsn:cutover", prepared.checkpoint);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
}

test "postgres source runtime live polling returns inserted row" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    const wal_level = execPsqlScalarAlloc(alloc, dsn, "show wal_level") catch return error.SkipZigTest;
    defer alloc.free(wal_level);
    if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, wal_level, &std.ascii.whitespace), "logical")) return error.SkipZigTest;

    live_runtime_poll_test_counter += 1;
    const suffix = live_runtime_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_runtime_cdc_probe_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_runtime_cdc_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_runtime_cdc_pub_{d}", .{suffix});
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
        execPsqlCommand(alloc, dsn, drop_publication_sql) catch {};
        execPsqlCommand(alloc, dsn, drop_slot_sql) catch {};
        execPsqlCommand(alloc, dsn, drop_table_sql) catch {};
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

    try execPsqlCommand(alloc, dsn, create_table_sql);
    try execPsqlCommand(alloc, dsn, create_publication_sql);
    try execPsqlCommand(alloc, dsn, create_slot_sql);

    var registry = foreign_source.Registry{};
    defer registry.deinit(alloc);
    try postgres_libpq.registerDefaultExecutor(alloc, &registry);

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, dsn),
    });
    defer src.deinit(alloc);

    var empty_poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer empty_poll_params.deinit(alloc);

    var empty_poll_result = try src.pollChanges(alloc, empty_poll_params);
    defer empty_poll_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty_poll_result.changes.len);

    try execPsqlCommand(alloc, dsn, insert_sql);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer poll_params.deinit(alloc);

    var poll_result = try src.pollChanges(alloc, poll_params);
    defer poll_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), poll_result.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.insert, poll_result.changes[0].op);
    try std.testing.expectEqualStrings("u1", poll_result.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("Alice", poll_result.changes[0].row.?.object.get("name").?.string);
}

test "postgres source runtime live polling still sees insert after repeated empty polls with recreated sources" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    const wal_level = execPsqlScalarAlloc(alloc, dsn, "show wal_level") catch return error.SkipZigTest;
    defer alloc.free(wal_level);
    if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, wal_level, &std.ascii.whitespace), "logical")) return error.SkipZigTest;

    live_runtime_poll_test_counter += 1;
    const suffix = live_runtime_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_runtime_cdc_recreate_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_runtime_cdc_recreate_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_runtime_cdc_recreate_pub_{d}", .{suffix});
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
        execPsqlCommand(alloc, dsn, drop_publication_sql) catch {};
        execPsqlCommand(alloc, dsn, drop_slot_sql) catch {};
        execPsqlCommand(alloc, dsn, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(
        alloc,
        "create table {s} (id text primary key, name text not null, tier text not null)",
        .{table_name},
    );
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(
        alloc,
        "insert into {s} (id, name, tier) values ('u0', 'Seed', 'seed')",
        .{table_name},
    );
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(
        alloc,
        "insert into {s} (id, name, tier) values ('u1', 'Alice', 'gold')",
        .{table_name},
    );
    defer alloc.free(insert_sql);

    try execPsqlCommand(alloc, dsn, create_table_sql);
    try execPsqlCommand(alloc, dsn, seed_sql);

    var registry = foreign_source.Registry{};
    defer registry.deinit(alloc);
    try postgres_libpq.registerDefaultExecutor(alloc, &registry);

    var query_source = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, dsn),
    });
    defer query_source.deinit(alloc);
    var snapshot_params = foreign_source.QueryParams{
        .table = try alloc.dupe(u8, table_name),
        .limit = 16,
        .offset = 0,
    };
    defer snapshot_params.deinit(alloc);
    var snapshot_result = try query_source.query(alloc, snapshot_params);
    defer snapshot_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), snapshot_result.rows.len);

    inline for (0..3) |_| {
        var empty_source = try registry.create(alloc, .{
            .kind = .postgres,
            .dsn = try alloc.dupe(u8, dsn),
        });
        defer empty_source.deinit(alloc);
        var empty_poll_params = foreign_source.ReplicationPollParams{
            .table = try alloc.dupe(u8, table_name),
            .slot_name = try alloc.dupe(u8, slot_name),
            .publication_name = try alloc.dupe(u8, publication_name),
            .limit = 16,
        };
        defer empty_poll_params.deinit(alloc);
        var empty_poll_result = try empty_source.pollChanges(alloc, empty_poll_params);
        defer empty_poll_result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), empty_poll_result.changes.len);
    }

    try execPsqlCommand(alloc, dsn, insert_sql);

    var poll_source = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, dsn),
    });
    defer poll_source.deinit(alloc);
    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer poll_params.deinit(alloc);

    var poll_result = try poll_source.pollChanges(alloc, poll_params);
    defer poll_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), poll_result.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.insert, poll_result.changes[0].op);
    try std.testing.expectEqualStrings("u1", poll_result.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("Alice", poll_result.changes[0].row.?.object.get("name").?.string);
    try std.testing.expectEqualStrings("gold", poll_result.changes[0].row.?.object.get("tier").?.string);
}

test "postgres source runtime builds select queries through executor" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        last_sql: ?[]u8 = null,
        last_args: []sql.ParameterValue = &.{},
        last_deadline_ns: ?u64 = null,

        fn query(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: sql.PreparedQuery, execution_deadline_ns: ?u64) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.last_sql) |value| inner_alloc.free(value);
            for (self.last_args) |*arg| arg.deinit(inner_alloc);
            if (self.last_args.len > 0) inner_alloc.free(self.last_args);
            self.last_sql = try inner_alloc.dupe(u8, prepared.sql_text);
            self.last_args = try cloneArgsAlloc(inner_alloc, prepared.args);
            self.last_deadline_ns = execution_deadline_ns;
            var owned = prepared;
            owned.deinit(inner_alloc);
            const rows = try inner_alloc.alloc(std.json.Value, 1);
            rows[0] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"cust:a\"}", .{});
            return .{ .rows = rows, .total = 1 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{ .row_count = 1, .size_bytes = 32 };
        }

        fn discoverColumns(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8, _: ?u64) ![]foreign_source.Column {
            const columns = try inner_alloc.alloc(foreign_source.Column, 2);
            columns[0] = .{
                .name = try inner_alloc.dupe(u8, "id"),
                .data_type = try inner_alloc.dupe(u8, "uuid"),
                .nullable = false,
            };
            columns[1] = .{
                .name = try inner_alloc.dupe(u8, "name"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = true,
            };
            return columns;
        }
    };

    var executor = FakeExecutor{};
    var registry = foreign_source.Registry{};
    defer {
        if (executor.last_sql) |value| alloc.free(value);
        for (executor.last_args) |*arg| arg.deinit(alloc);
        if (executor.last_args.len > 0) alloc.free(executor.last_args);
        registry.deinit(alloc);
    }
    try registerExecutor(alloc, &registry, .{
        .ptr = &executor,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .discover_columns = FakeExecutor.discoverColumns,
        },
    });

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://db"),
    });
    defer src.deinit(alloc);

    const order_by = try alloc.alloc(foreign_source.SortField, 1);
    order_by[0] = .{ .field = try alloc.dupe(u8, "name"), .desc = true };
    const fields = try alloc.alloc([]u8, 2);
    fields[0] = try alloc.dupe(u8, "id");
    fields[1] = try alloc.dupe(u8, "name");
    const execution_deadline_ns = platform_time.monotonicNs() + 60 * std.time.ns_per_s;
    var params = foreign_source.QueryParams{
        .table = try alloc.dupe(u8, "customers"),
        .fields = fields,
        .limit = 5,
        .offset = 2,
        .order_by = order_by,
        .execution_deadline_ns = execution_deadline_ns,
    };
    defer params.deinit(alloc);
    var result = try src.query(alloc, params);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings(
        "SELECT \"id\", \"name\" FROM \"customers\" ORDER BY \"name\" DESC LIMIT 5 OFFSET 2",
        executor.last_sql.?,
    );
    try std.testing.expectEqual(@as(usize, 0), executor.last_args.len);
    try std.testing.expectEqual(@as(?u64, execution_deadline_ns), executor.last_deadline_ns);
}

test "postgres source parameter clones are allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var fields = [_][]u8{@constCast("id")};
            var columns = [_]foreign_source.Column{.{
                .name = @constCast("id"),
                .data_type = @constCast("uuid"),
                .nullable = false,
            }};
            var order_by = [_]foreign_source.SortField{.{
                .field = @constCast("id"),
                .desc = false,
            }};
            const params = foreign_source.QueryParams{
                .table = @constCast("customers"),
                .fields = &fields,
                .filter_query_json = @constCast("{\"term\":{\"id\":\"a\"}}"),
                .columns = &columns,
                .order_by = &order_by,
            };
            var cloned_params = try cloneQueryParamsAlloc(alloc, params);
            defer cloned_params.deinit(alloc);

            var aggregations = [_]foreign_source.NamedAggregation{.{
                .name = @constCast("ids"),
                .definition = .{
                    .type_name = @constCast("terms"),
                    .field = @constCast("id"),
                    .size = 10,
                },
            }};
            const aggregate_params = foreign_source.AggregateParams{
                .table = @constCast("customers"),
                .filter_query_json = @constCast("{\"term\":{\"id\":\"a\"}}"),
                .columns = &columns,
                .aggregations = &aggregations,
            };
            var cloned_aggregate_params = try cloneAggregateParamsAlloc(alloc, aggregate_params);
            defer cloned_aggregate_params.deinit(alloc);

            const replication_params = foreign_source.ReplicationPollParams{
                .table = @constCast("customers"),
                .slot_name = @constCast("slot"),
                .publication_name = @constCast("publication"),
                .filter_query_json = @constCast("{\"match_all\":{}}"),
                .checkpoint = @constCast("0/10"),
            };
            var cloned_replication_params = try cloneReplicationPollParamsAlloc(alloc, replication_params);
            defer cloned_replication_params.deinit(alloc);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
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

var live_runtime_poll_test_counter: u64 = 0;

fn testPsqlBin() ?[]const u8 {
    if (std.c.getenv("ANTFLY_TEST_PSQL_BIN")) |value_z| return std.mem.span(value_z);
    return "/opt/homebrew/opt/postgresql@18/bin/psql";
}

fn execPsqlCommand(alloc: Allocator, dsn: []const u8, sql_text: []const u8) !void {
    const psql_bin = testPsqlBin() orelse return error.FileNotFound;
    const result = try std.process.run(alloc, std.testing.io, .{
        .argv = &.{ psql_bin, dsn, "-v", "ON_ERROR_STOP=1", "-c", sql_text },
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| if (code != 0) return error.ChildProcessFailure,
        else => return error.ChildProcessFailure,
    }
}

fn execPsqlScalarAlloc(alloc: Allocator, dsn: []const u8, sql_text: []const u8) ![]u8 {
    const psql_bin = testPsqlBin() orelse return error.FileNotFound;
    const result = try std.process.run(alloc, std.testing.io, .{
        .argv = &.{ psql_bin, dsn, "-tAc", sql_text },
    });
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| {
            if (code != 0) {
                alloc.free(result.stdout);
                return error.ChildProcessFailure;
            }
        },
        else => {
            alloc.free(result.stdout);
            return error.ChildProcessFailure;
        },
    }
    return result.stdout;
}

test "postgres source runtime translates filter_query_json and validates fields" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        last_sql: ?[]u8 = null,
        last_args: []sql.ParameterValue = &.{},

        fn query(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: sql.PreparedQuery, _: ?u64) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.last_sql) |value| inner_alloc.free(value);
            for (self.last_args) |*arg| arg.deinit(inner_alloc);
            if (self.last_args.len > 0) inner_alloc.free(self.last_args);
            self.last_sql = try inner_alloc.dupe(u8, prepared.sql_text);
            self.last_args = try cloneArgsAlloc(inner_alloc, prepared.args);
            var owned = prepared;
            owned.deinit(inner_alloc);
            return .{ .rows = &.{}, .total = 0 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn discoverColumns(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8, _: ?u64) ![]foreign_source.Column {
            const columns = try inner_alloc.alloc(foreign_source.Column, 2);
            columns[0] = .{
                .name = try inner_alloc.dupe(u8, "id"),
                .data_type = try inner_alloc.dupe(u8, "uuid"),
                .nullable = false,
            };
            columns[1] = .{
                .name = try inner_alloc.dupe(u8, "status"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = false,
            };
            return columns;
        }
    };

    var executor = FakeExecutor{};
    var registry = foreign_source.Registry{};
    defer {
        if (executor.last_sql) |value| alloc.free(value);
        for (executor.last_args) |*arg| arg.deinit(alloc);
        if (executor.last_args.len > 0) alloc.free(executor.last_args);
        registry.deinit(alloc);
    }
    try registerExecutor(alloc, &registry, .{
        .ptr = &executor,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .discover_columns = FakeExecutor.discoverColumns,
        },
    });

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://db"),
    });
    defer src.deinit(alloc);

    var columns = try alloc.alloc(foreign_source.Column, 2);
    columns[0] = .{
        .name = try alloc.dupe(u8, "id"),
        .data_type = try alloc.dupe(u8, "uuid"),
        .nullable = false,
    };
    columns[1] = .{
        .name = try alloc.dupe(u8, "status"),
        .data_type = try alloc.dupe(u8, "text"),
        .nullable = false,
    };
    var params = foreign_source.QueryParams{
        .table = try alloc.dupe(u8, "customers"),
        .filter_query_json = try alloc.dupe(u8, "{\"term\":\"active\",\"field\":\"status\"}"),
        .columns = columns,
    };
    defer params.deinit(alloc);
    var result = try src.query(alloc, params);
    defer result.deinit(alloc);

    try std.testing.expectEqualStrings(
        "SELECT * FROM \"customers\" WHERE \"status\" = $1",
        executor.last_sql.?,
    );
    try std.testing.expectEqual(@as(usize, 1), executor.last_args.len);
    try std.testing.expectEqualStrings("active", executor.last_args[0].string);

    var bad_columns = try alloc.alloc(foreign_source.Column, 1);
    bad_columns[0] = .{
        .name = try alloc.dupe(u8, "id"),
        .data_type = try alloc.dupe(u8, "uuid"),
        .nullable = false,
    };
    var fields = try alloc.alloc([]u8, 1);
    fields[0] = try alloc.dupe(u8, "missing");
    var bad_params = foreign_source.QueryParams{
        .table = try alloc.dupe(u8, "customers"),
        .columns = bad_columns,
        .fields = fields,
    };
    defer bad_params.deinit(alloc);
    try std.testing.expectError(error.UnknownColumn, src.query(alloc, bad_params));
}

test "postgres source runtime discovers columns when config omits them" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        discover_count: usize = 0,
        last_sql: ?[]u8 = null,

        fn query(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: sql.PreparedQuery, _: ?u64) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.last_sql) |value| inner_alloc.free(value);
            self.last_sql = try inner_alloc.dupe(u8, prepared.sql_text);
            var owned = prepared;
            owned.deinit(inner_alloc);
            return .{ .rows = &.{}, .total = 0 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn discoverColumns(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8, _: ?u64) ![]foreign_source.Column {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.discover_count += 1;
            const columns = try inner_alloc.alloc(foreign_source.Column, 2);
            columns[0] = .{
                .name = try inner_alloc.dupe(u8, "id"),
                .data_type = try inner_alloc.dupe(u8, "uuid"),
                .nullable = false,
            };
            columns[1] = .{
                .name = try inner_alloc.dupe(u8, "status"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = false,
            };
            return columns;
        }
    };

    var executor = FakeExecutor{};
    var registry = foreign_source.Registry{};
    defer {
        if (executor.last_sql) |value| alloc.free(value);
        registry.deinit(alloc);
    }
    try registerExecutor(alloc, &registry, .{
        .ptr = &executor,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .discover_columns = FakeExecutor.discoverColumns,
        },
    });

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://db"),
    });
    defer src.deinit(alloc);

    var fields = try alloc.alloc([]u8, 1);
    fields[0] = try alloc.dupe(u8, "id");
    var params = foreign_source.QueryParams{
        .table = try alloc.dupe(u8, "customers"),
        .fields = fields,
        .filter_query_json = try alloc.dupe(u8, "{\"term\":\"active\",\"field\":\"status\"}"),
    };
    defer params.deinit(alloc);
    var result = try src.query(alloc, params);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), executor.discover_count);
    try std.testing.expectEqualStrings(
        "SELECT \"id\" FROM \"customers\" WHERE \"status\" = $1",
        executor.last_sql.?,
    );
}

test "postgres source runtime refreshes stale discovered columns once" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        discover_count: usize = 0,
        refresh_count: usize = 0,
        last_sql: ?[]u8 = null,

        fn query(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: sql.PreparedQuery, _: ?u64) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.last_sql) |value| inner_alloc.free(value);
            self.last_sql = try inner_alloc.dupe(u8, prepared.sql_text);
            var owned = prepared;
            owned.deinit(inner_alloc);
            return .{ .rows = &.{}, .total = 0 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{};
        }

        fn columns(inner_alloc: Allocator, include_added: bool) ![]foreign_source.Column {
            const out = try inner_alloc.alloc(foreign_source.Column, if (include_added) 2 else 1);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |*column| column.deinit(inner_alloc);
                inner_alloc.free(out);
            }
            {
                const id_name = try inner_alloc.dupe(u8, "id");
                errdefer inner_alloc.free(id_name);
                const id_type = try inner_alloc.dupe(u8, "uuid");
                errdefer inner_alloc.free(id_type);
                out[0] = .{
                    .name = id_name,
                    .data_type = id_type,
                    .nullable = false,
                };
            }
            initialized = 1;
            if (include_added) {
                {
                    const added_name = try inner_alloc.dupe(u8, "added");
                    errdefer inner_alloc.free(added_name);
                    const added_type = try inner_alloc.dupe(u8, "text");
                    errdefer inner_alloc.free(added_type);
                    out[1] = .{
                        .name = added_name,
                        .data_type = added_type,
                        .nullable = true,
                    };
                }
                initialized = 2;
            }
            return out;
        }

        fn discoverColumns(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8, _: ?u64) ![]foreign_source.Column {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.discover_count += 1;
            return try columns(inner_alloc, false);
        }

        fn refreshColumns(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8, _: ?u64) ![]foreign_source.Column {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.refresh_count += 1;
            return try columns(inner_alloc, true);
        }
    };

    var executor = FakeExecutor{};
    var registry = foreign_source.Registry{};
    defer {
        if (executor.last_sql) |value| alloc.free(value);
        registry.deinit(alloc);
    }
    try registerExecutor(alloc, &registry, .{
        .ptr = &executor,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .discover_columns = FakeExecutor.discoverColumns,
            .refresh_columns = FakeExecutor.refreshColumns,
        },
    });

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://db"),
    });
    defer src.deinit(alloc);

    var fields = try alloc.alloc([]u8, 1);
    fields[0] = try alloc.dupe(u8, "added");
    var params = foreign_source.QueryParams{
        .table = try alloc.dupe(u8, "customers"),
        .fields = fields,
    };
    defer params.deinit(alloc);
    var result = try src.query(alloc, params);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), executor.discover_count);
    try std.testing.expectEqual(@as(usize, 1), executor.refresh_count);
    try std.testing.expectEqualStrings("SELECT \"added\" FROM \"customers\"", executor.last_sql.?);
}

test "postgres source runtime batches simple aggregations into one query" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        last_sql: ?[]u8 = null,
        last_args: []sql.ParameterValue = &.{},

        fn query(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: sql.PreparedQuery, _: ?u64) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.last_sql) |value| inner_alloc.free(value);
            for (self.last_args) |*arg| arg.deinit(inner_alloc);
            if (self.last_args.len > 0) inner_alloc.free(self.last_args);
            self.last_sql = try inner_alloc.dupe(u8, prepared.sql_text);
            self.last_args = try cloneArgsAlloc(inner_alloc, prepared.args);
            var owned = prepared;
            owned.deinit(inner_alloc);

            const rows = try inner_alloc.alloc(std.json.Value, 1);
            var row = std.json.ObjectMap.empty;
            try row.put(inner_alloc, try inner_alloc.dupe(u8, "doc_count"), .{ .integer = 2 });
            try row.put(inner_alloc, try inner_alloc.dupe(u8, "version_sum"), .{ .integer = 7 });
            rows[0] = .{ .object = row };
            return .{ .rows = rows, .total = 1 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }
    };

    var executor = FakeExecutor{};
    var registry = foreign_source.Registry{};
    defer {
        if (executor.last_sql) |value| alloc.free(value);
        for (executor.last_args) |*arg| arg.deinit(alloc);
        if (executor.last_args.len > 0) alloc.free(executor.last_args);
        registry.deinit(alloc);
    }
    try registerExecutor(alloc, &registry, .{
        .ptr = &executor,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
        },
    });

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://db"),
    });
    defer src.deinit(alloc);

    const columns = try alloc.alloc(foreign_source.Column, 2);
    columns[0] = .{
        .name = try alloc.dupe(u8, "version"),
        .data_type = try alloc.dupe(u8, "bigint"),
        .nullable = false,
    };
    columns[1] = .{
        .name = try alloc.dupe(u8, "status"),
        .data_type = try alloc.dupe(u8, "text"),
        .nullable = false,
    };

    const aggregations = try alloc.alloc(foreign_source.NamedAggregation, 2);
    aggregations[0] = .{
        .name = try alloc.dupe(u8, "doc_count"),
        .definition = .{
            .type_name = try alloc.dupe(u8, "count"),
        },
    };
    aggregations[1] = .{
        .name = try alloc.dupe(u8, "version_sum"),
        .definition = .{
            .type_name = try alloc.dupe(u8, "sum"),
            .field = try alloc.dupe(u8, "version"),
        },
    };

    var params = foreign_source.AggregateParams{
        .table = try alloc.dupe(u8, "customers"),
        .filter_query_json = try alloc.dupe(u8, "{\"term\":\"active\",\"field\":\"status\"}"),
        .columns = columns,
        .aggregations = aggregations,
    };
    defer params.deinit(alloc);
    var result = try src.aggregate(alloc, params);
    defer result.deinit(alloc);

    try std.testing.expectEqualStrings(
        "SELECT COUNT(*) AS \"doc_count\", SUM(\"version\") AS \"version_sum\" FROM \"customers\" WHERE \"status\" = $1",
        executor.last_sql.?,
    );
    try std.testing.expectEqual(@as(usize, 1), executor.last_args.len);
    try std.testing.expectEqualStrings("active", executor.last_args[0].string);
    try std.testing.expectEqual(@as(usize, 2), result.results.len);
    try std.testing.expectEqualStrings("doc_count", result.results[0].name);
    try std.testing.expectEqual(@as(i64, 2), result.results[0].value.integer);
    try std.testing.expectEqualStrings("version_sum", result.results[1].name);
    try std.testing.expectEqual(@as(i64, 7), result.results[1].value.integer);
}

test "postgres source runtime executes stats and terms aggregations with translated filters" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        sql_texts: std.ArrayListUnmanaged([]u8) = .empty,
        args_list: std.ArrayListUnmanaged([]sql.ParameterValue) = .empty,

        fn deinit(self: *@This(), inner_alloc: Allocator) void {
            for (self.sql_texts.items) |value| inner_alloc.free(value);
            self.sql_texts.deinit(inner_alloc);
            for (self.args_list.items) |args| {
                for (args) |*arg| arg.deinit(inner_alloc);
                if (args.len > 0) inner_alloc.free(args);
            }
            self.args_list.deinit(inner_alloc);
        }

        fn query(ptr: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: sql.PreparedQuery, _: ?u64) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.sql_texts.append(inner_alloc, try inner_alloc.dupe(u8, prepared.sql_text));
            try self.args_list.append(inner_alloc, try cloneArgsAlloc(inner_alloc, prepared.args));

            const query_index = self.sql_texts.items.len - 1;
            var owned = prepared;
            owned.deinit(inner_alloc);

            if (query_index == 0) {
                const rows = try inner_alloc.alloc(std.json.Value, 1);
                var row = std.json.ObjectMap.empty;
                try row.put(inner_alloc, try inner_alloc.dupe(u8, "count"), .{ .integer = 2 });
                try row.put(inner_alloc, try inner_alloc.dupe(u8, "min"), .{ .integer = 1 });
                try row.put(inner_alloc, try inner_alloc.dupe(u8, "max"), .{ .integer = 3 });
                try row.put(inner_alloc, try inner_alloc.dupe(u8, "avg"), .{ .float = 2.0 });
                try row.put(inner_alloc, try inner_alloc.dupe(u8, "sum"), .{ .integer = 4 });
                rows[0] = .{ .object = row };
                return .{ .rows = rows, .total = 1 };
            }

            const rows = try inner_alloc.alloc(std.json.Value, 2);
            var bucket_a = std.json.ObjectMap.empty;
            try bucket_a.put(inner_alloc, try inner_alloc.dupe(u8, "key"), .{ .string = try inner_alloc.dupe(u8, "Alice") });
            try bucket_a.put(inner_alloc, try inner_alloc.dupe(u8, "doc_count"), .{ .integer = 1 });
            rows[0] = .{ .object = bucket_a };

            var bucket_b = std.json.ObjectMap.empty;
            try bucket_b.put(inner_alloc, try inner_alloc.dupe(u8, "key"), .{ .string = try inner_alloc.dupe(u8, "Bob") });
            try bucket_b.put(inner_alloc, try inner_alloc.dupe(u8, "doc_count"), .{ .integer = 1 });
            rows[1] = .{ .object = bucket_b };
            return .{ .rows = rows, .total = 2 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }
    };

    var executor = FakeExecutor{};
    defer executor.deinit(alloc);

    var registry = foreign_source.Registry{};
    defer registry.deinit(alloc);
    try registerExecutor(alloc, &registry, .{
        .ptr = &executor,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
        },
    });

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://db"),
    });
    defer src.deinit(alloc);

    const columns = try alloc.alloc(foreign_source.Column, 3);
    columns[0] = .{
        .name = try alloc.dupe(u8, "version"),
        .data_type = try alloc.dupe(u8, "bigint"),
        .nullable = false,
    };
    columns[1] = .{
        .name = try alloc.dupe(u8, "name"),
        .data_type = try alloc.dupe(u8, "text"),
        .nullable = false,
    };
    columns[2] = .{
        .name = try alloc.dupe(u8, "status"),
        .data_type = try alloc.dupe(u8, "text"),
        .nullable = false,
    };

    const aggregations = try alloc.alloc(foreign_source.NamedAggregation, 2);
    aggregations[0] = .{
        .name = try alloc.dupe(u8, "version_stats"),
        .definition = .{
            .type_name = try alloc.dupe(u8, "stats"),
            .field = try alloc.dupe(u8, "version"),
        },
    };
    aggregations[1] = .{
        .name = try alloc.dupe(u8, "name_terms"),
        .definition = .{
            .type_name = try alloc.dupe(u8, "terms"),
            .field = try alloc.dupe(u8, "name"),
            .size = 5,
        },
    };

    var params = foreign_source.AggregateParams{
        .table = try alloc.dupe(u8, "customers"),
        .filter_query_json = try alloc.dupe(u8, "{\"term\":\"active\",\"field\":\"status\"}"),
        .columns = columns,
        .aggregations = aggregations,
    };
    defer params.deinit(alloc);
    var result = try src.aggregate(alloc, params);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), executor.sql_texts.items.len);
    try std.testing.expectEqualStrings(
        "SELECT COUNT(\"version\") AS \"count\", MIN(\"version\") AS \"min\", MAX(\"version\") AS \"max\", AVG(\"version\") AS \"avg\", SUM(\"version\") AS \"sum\" FROM \"customers\" WHERE \"status\" = $1",
        executor.sql_texts.items[0],
    );
    try std.testing.expectEqualStrings(
        "SELECT \"name\" AS \"key\", COUNT(*) AS \"doc_count\" FROM \"customers\" WHERE \"status\" = $1 GROUP BY \"name\" ORDER BY \"doc_count\" DESC LIMIT 5",
        executor.sql_texts.items[1],
    );
    try std.testing.expectEqual(@as(usize, 1), executor.args_list.items[0].len);
    try std.testing.expectEqualStrings("active", executor.args_list.items[0][0].string);
    try std.testing.expectEqual(@as(usize, 1), executor.args_list.items[1].len);
    try std.testing.expectEqualStrings("active", executor.args_list.items[1][0].string);

    try std.testing.expectEqual(@as(usize, 2), result.results.len);
    try std.testing.expectEqualStrings("version_stats", result.results[0].name);
    try std.testing.expect(result.results[0].value == .object);
    try std.testing.expectEqualStrings("name_terms", result.results[1].name);
    try std.testing.expect(result.results[1].value == .array);
    try std.testing.expectEqual(@as(usize, 2), result.results[1].value.array.items.len);
}
