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
const Allocator = std.mem.Allocator;

pub const SourceKind = enum {
    postgres,
};

pub const Config = struct {
    kind: SourceKind,
    dsn: []u8,

    pub fn deinit(self: *Config, alloc: Allocator) void {
        alloc.free(self.dsn);
        self.* = undefined;
    }
};

pub const Column = struct {
    name: []u8,
    data_type: []u8,
    nullable: bool,

    pub fn deinit(self: *Column, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.data_type);
        self.* = undefined;
    }
};

pub const SortField = struct {
    field: []u8,
    desc: bool = false,

    pub fn deinit(self: *SortField, alloc: Allocator) void {
        alloc.free(self.field);
        self.* = undefined;
    }
};

pub const TableStatistics = struct {
    row_count: i64 = 0,
    size_bytes: i64 = 0,
};

pub const QueryParams = struct {
    table: []u8,
    fields: [][]u8 = &.{},
    filter_query_json: ?[]u8 = null,
    columns: []Column = &.{},
    limit: ?usize = null,
    offset: usize = 0,
    order_by: []SortField = &.{},

    pub fn deinit(self: *QueryParams, alloc: Allocator) void {
        alloc.free(self.table);
        for (self.fields) |field| alloc.free(field);
        if (self.fields.len > 0) alloc.free(self.fields);
        if (self.filter_query_json) |query| alloc.free(query);
        for (self.columns) |*column| column.deinit(alloc);
        if (self.columns.len > 0) alloc.free(self.columns);
        for (self.order_by) |*sort| sort.deinit(alloc);
        if (self.order_by.len > 0) alloc.free(self.order_by);
        self.* = undefined;
    }
};

pub const AggregationDef = struct {
    type_name: []u8,
    field: ?[]u8 = null,
    size: ?usize = null,

    pub fn deinit(self: *AggregationDef, alloc: Allocator) void {
        alloc.free(self.type_name);
        if (self.field) |field| alloc.free(field);
        self.* = undefined;
    }
};

pub const AggregateParams = struct {
    table: []u8,
    filter_query_json: ?[]u8 = null,
    columns: []Column = &.{},
    aggregations: []NamedAggregation = &.{},

    pub fn deinit(self: *AggregateParams, alloc: Allocator) void {
        alloc.free(self.table);
        if (self.filter_query_json) |query| alloc.free(query);
        for (self.columns) |*column| column.deinit(alloc);
        if (self.columns.len > 0) alloc.free(self.columns);
        for (self.aggregations) |*aggregation| aggregation.deinit(alloc);
        if (self.aggregations.len > 0) alloc.free(self.aggregations);
        self.* = undefined;
    }
};

pub const NamedAggregation = struct {
    name: []u8,
    definition: AggregationDef,

    pub fn deinit(self: *NamedAggregation, alloc: Allocator) void {
        alloc.free(self.name);
        self.definition.deinit(alloc);
        self.* = undefined;
    }
};

pub const QueryResult = struct {
    rows: []std.json.Value = &.{},
    total: usize = 0,

    pub fn deinit(self: *QueryResult, alloc: Allocator) void {
        for (self.rows) |*row| deinitJsonValue(alloc, row);
        if (self.rows.len > 0) alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const SnapshotReader = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (ptr: *anyopaque, alloc: Allocator) void,
        query: *const fn (ptr: *anyopaque, alloc: Allocator, params: QueryParams) anyerror!QueryResult,
    };

    pub fn deinit(self: *SnapshotReader, alloc: Allocator) void {
        self.vtable.deinit(self.ptr, alloc);
        self.* = undefined;
    }

    pub fn query(self: SnapshotReader, alloc: Allocator, params: QueryParams) !QueryResult {
        return try self.vtable.query(self.ptr, alloc, params);
    }
};

pub const PreparedReplicationSnapshot = struct {
    checkpoint: []u8,
    reader: SnapshotReader,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.checkpoint);
        self.reader.deinit(alloc);
        self.* = undefined;
    }
};

pub const ReplicationOp = enum {
    insert,
    update,
    delete,
};

pub const ReplicationChange = struct {
    op: ReplicationOp,
    checkpoint: []u8,
    row: ?std.json.Value = null,
    key: ?[]u8 = null,
    lag_records: u64 = 0,
    commit_timestamp_ms: u64 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.checkpoint);
        if (self.row) |*row| deinitJsonValue(alloc, row);
        if (self.key) |key| alloc.free(key);
        self.* = undefined;
    }
};

pub const ReplicationPollParams = struct {
    table: []u8,
    slot_name: ?[]u8 = null,
    publication_name: ?[]u8 = null,
    filter_query_json: ?[]u8 = null,
    checkpoint: ?[]u8 = null,
    limit: ?usize = null,

    pub fn deinit(self: *ReplicationPollParams, alloc: Allocator) void {
        alloc.free(self.table);
        if (self.slot_name) |slot_name| alloc.free(slot_name);
        if (self.publication_name) |publication_name| alloc.free(publication_name);
        if (self.filter_query_json) |query| alloc.free(query);
        if (self.checkpoint) |checkpoint| alloc.free(checkpoint);
        self.* = undefined;
    }
};

pub const ReplicationPollResult = struct {
    changes: []ReplicationChange = &.{},
    checkpoint: []u8 = &.{},
    lag_records: u64 = 0,
    lag_millis: u64 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.changes) |*change| change.deinit(alloc);
        if (self.changes.len > 0) alloc.free(self.changes);
        if (self.checkpoint.len > 0) alloc.free(self.checkpoint);
        self.* = undefined;
    }
};

pub const ReplicationPrepareResult = struct {
    checkpoint: []u8 = "",
    slot_existed: bool = false,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.checkpoint);
        self.* = undefined;
    }
};

pub const ReplicationCleanupParams = struct {
    slot_name: []u8,
    publication_name: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.slot_name);
        alloc.free(self.publication_name);
        self.* = undefined;
    }
};

pub const NamedValue = struct {
    name: []u8,
    value: std.json.Value,

    pub fn deinit(self: *NamedValue, alloc: Allocator) void {
        alloc.free(self.name);
        deinitJsonValue(alloc, &self.value);
        self.* = undefined;
    }
};

pub const AggregateResult = struct {
    results: []NamedValue = &.{},

    pub fn deinit(self: *AggregateResult, alloc: Allocator) void {
        for (self.results) |*result| result.deinit(alloc);
        if (self.results.len > 0) alloc.free(self.results);
        self.* = undefined;
    }
};

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (ptr: *anyopaque, alloc: Allocator) void,
        query: *const fn (ptr: *anyopaque, alloc: Allocator, params: QueryParams) anyerror!QueryResult,
        aggregate: ?*const fn (ptr: *anyopaque, alloc: Allocator, params: AggregateParams) anyerror!AggregateResult = null,
        statistics: *const fn (ptr: *anyopaque, table: []const u8) anyerror!TableStatistics,
        begin_snapshot_query: ?*const fn (ptr: *anyopaque, alloc: Allocator) anyerror!SnapshotReader = null,
        begin_prepared_replication_snapshot: ?*const fn (ptr: *anyopaque, alloc: Allocator, params: ReplicationPollParams) anyerror!PreparedReplicationSnapshot = null,
        prepare_replication: ?*const fn (ptr: *anyopaque, alloc: Allocator, params: ReplicationPollParams) anyerror!ReplicationPrepareResult = null,
        poll_changes: ?*const fn (ptr: *anyopaque, alloc: Allocator, params: ReplicationPollParams) anyerror!ReplicationPollResult = null,
        cleanup_replication: ?*const fn (ptr: *anyopaque, alloc: Allocator, params: ReplicationCleanupParams) anyerror!void = null,
    };

    pub fn deinit(self: *Source, alloc: Allocator) void {
        self.vtable.deinit(self.ptr, alloc);
        self.* = undefined;
    }

    pub fn query(self: Source, alloc: Allocator, params: QueryParams) !QueryResult {
        return try self.vtable.query(self.ptr, alloc, params);
    }

    pub fn aggregate(self: Source, alloc: Allocator, params: AggregateParams) !AggregateResult {
        const aggregate_fn = self.vtable.aggregate orelse return error.UnsupportedAggregate;
        return try aggregate_fn(self.ptr, alloc, params);
    }

    pub fn statistics(self: Source, table: []const u8) !TableStatistics {
        return try self.vtable.statistics(self.ptr, table);
    }

    pub fn beginSnapshotQuery(self: Source, alloc: Allocator) !SnapshotReader {
        const begin_fn = self.vtable.begin_snapshot_query orelse return error.UnsupportedConsistentSnapshot;
        return try begin_fn(self.ptr, alloc);
    }

    pub fn beginPreparedReplicationSnapshot(self: Source, alloc: Allocator, params: ReplicationPollParams) !PreparedReplicationSnapshot {
        const begin_fn = self.vtable.begin_prepared_replication_snapshot orelse return error.UnsupportedExactCutover;
        return try begin_fn(self.ptr, alloc, params);
    }

    pub fn prepareReplication(self: Source, alloc: Allocator, params: ReplicationPollParams) !ReplicationPrepareResult {
        const prepare_fn = self.vtable.prepare_replication orelse return error.UnsupportedReplicationStreaming;
        return try prepare_fn(self.ptr, alloc, params);
    }

    pub fn pollChanges(self: Source, alloc: Allocator, params: ReplicationPollParams) !ReplicationPollResult {
        const poll_fn = self.vtable.poll_changes orelse return error.UnsupportedReplicationStreaming;
        return try poll_fn(self.ptr, alloc, params);
    }

    pub fn cleanupReplication(self: Source, alloc: Allocator, params: ReplicationCleanupParams) !void {
        const cleanup_fn = self.vtable.cleanup_replication orelse return error.UnsupportedReplicationCleanup;
        return try cleanup_fn(self.ptr, alloc, params);
    }
};

pub const Factory = *const fn (alloc: Allocator, config: Config) anyerror!Source;
pub const ContextFactory = *const fn (ctx: *anyopaque, alloc: Allocator, config: Config) anyerror!Source;
pub const ContextDeinit = *const fn (ctx: *anyopaque, alloc: Allocator) void;

pub const FactoryEntry = union(enum) {
    plain: Factory,
    with_ctx: struct {
        ctx: *anyopaque,
        factory: ContextFactory,
        deinit_ctx: ?ContextDeinit = null,
    },

    pub fn create(self: @This(), alloc: Allocator, config: Config) !Source {
        return switch (self) {
            .plain => |factory| try factory(alloc, config),
            .with_ctx => |value| try value.factory(value.ctx, alloc, config),
        };
    }

    pub fn deinit(self: @This(), alloc: Allocator) void {
        switch (self) {
            .plain => {},
            .with_ctx => |value| if (value.deinit_ctx) |deinit_ctx| deinit_ctx(value.ctx, alloc),
        }
    }
};

pub const Registry = struct {
    factories: std.AutoHashMapUnmanaged(SourceKind, FactoryEntry) = .empty,

    pub fn deinit(self: *Registry, alloc: Allocator) void {
        var it = self.factories.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        self.factories.deinit(alloc);
        self.* = undefined;
    }

    pub fn register(self: *Registry, alloc: Allocator, kind: SourceKind, factory: Factory) !void {
        try self.factories.put(alloc, kind, .{ .plain = factory });
    }

    pub fn registerWithContext(
        self: *Registry,
        alloc: Allocator,
        kind: SourceKind,
        ctx: *anyopaque,
        factory: ContextFactory,
        deinit_ctx: ?ContextDeinit,
    ) !void {
        try self.factories.put(alloc, kind, .{
            .with_ctx = .{
                .ctx = ctx,
                .factory = factory,
                .deinit_ctx = deinit_ctx,
            },
        });
    }

    pub fn create(self: Registry, alloc: Allocator, config: Config) !Source {
        const entry = self.factories.get(config.kind) orelse return error.UnsupportedSourceKind;
        return try entry.create(alloc, config);
    }
};

pub fn deinitJsonValue(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .string => |text| alloc.free(text),
        .number_string => |text| alloc.free(text),
        .array => |*arr| {
            for (arr.items) |*item| deinitJsonValue(alloc, item);
            arr.deinit();
        },
        .object => |*obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                deinitJsonValue(alloc, entry.value_ptr);
            }
            obj.deinit(alloc);
        },
        else => {},
    }
    value.* = undefined;
}

test "foreign source types compile" {
    _ = Config;
    _ = SourceKind;
    _ = Column;
    _ = SortField;
    _ = TableStatistics;
    _ = QueryParams;
    _ = AggregationDef;
    _ = NamedAggregation;
    _ = AggregateParams;
    _ = QueryResult;
    _ = NamedValue;
    _ = AggregateResult;
    _ = ReplicationOp;
    _ = ReplicationChange;
    _ = ReplicationPollParams;
    _ = ReplicationPollResult;
    _ = Source;
    _ = Factory;
    _ = ContextFactory;
    _ = ContextDeinit;
    _ = FactoryEntry;
    _ = Registry;
}

test "foreign source registry creates registered source" {
    const alloc = std.testing.allocator;

    const Dummy = struct {
        value: u32,

        fn destroy(ptr: *anyopaque, inner_alloc: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(ptr: *anyopaque, inner_alloc: Allocator, _: QueryParams) !QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var rows = try inner_alloc.alloc(std.json.Value, 1);
            rows[0] = .{ .integer = self.value };
            return .{ .rows = rows, .total = 1 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !TableStatistics {
            return .{ .row_count = 7, .size_bytes = 128 };
        }

        fn factory(inner_alloc: Allocator, config: Config) !Source {
            var owned_config = config;
            defer owned_config.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{ .value = 42 };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = Registry{};
    defer registry.deinit(alloc);
    try registry.register(alloc, .postgres, Dummy.factory);

    const config = Config{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://example"),
    };
    var source_instance = try registry.create(alloc, config);
    defer source_instance.deinit(alloc);

    var params = QueryParams{
        .table = try alloc.dupe(u8, "customers"),
    };
    defer params.deinit(alloc);
    var result = try source_instance.query(alloc, params);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.total);
    try std.testing.expectEqual(@as(i64, 42), result.rows[0].integer);
    var poll_params = ReplicationPollParams{
        .table = try alloc.dupe(u8, "customers"),
    };
    defer poll_params.deinit(alloc);
    try std.testing.expectError(error.UnsupportedReplicationStreaming, source_instance.pollChanges(alloc, poll_params));
}

test "foreign source registry rejects unsupported source kinds" {
    const alloc = std.testing.allocator;
    var registry = Registry{};
    defer registry.deinit(alloc);

    var config = Config{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://example"),
    };
    defer config.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSourceKind, registry.create(alloc, config));
}

test "foreign source registry creates context-bound source" {
    const alloc = std.testing.allocator;

    const Dummy = struct {
        label: []const u8,

        fn destroy(ptr: *anyopaque, inner_alloc: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(ptr: *anyopaque, inner_alloc: Allocator, params: QueryParams) !QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned_params = params;
            defer owned_params.deinit(inner_alloc);
            const row_text = try std.fmt.allocPrint(inner_alloc, "{{\"label\":\"{s}\"}}", .{self.label});
            defer inner_alloc.free(row_text);
            const rows = try inner_alloc.alloc(std.json.Value, 1);
            rows[0] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, row_text, .{});
            return .{ .rows = rows, .total = 1 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !TableStatistics {
            return .{ .row_count = 1, .size_bytes = 32 };
        }

        const Context = struct {
            label: []u8,
        };

        fn contextDeinit(ptr: *anyopaque, inner_alloc: Allocator) void {
            const ctx: *Context = @ptrCast(@alignCast(ptr));
            inner_alloc.free(ctx.label);
            inner_alloc.destroy(ctx);
        }

        fn factory(ctx_ptr: *anyopaque, inner_alloc: Allocator, config: Config) !Source {
            const ctx: *Context = @ptrCast(@alignCast(ctx_ptr));
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{ .label = ctx.label };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = Registry{};
    defer registry.deinit(alloc);

    const ctx = try alloc.create(Dummy.Context);
    ctx.* = .{ .label = try alloc.dupe(u8, "ctx") };
    try registry.registerWithContext(alloc, .postgres, ctx, Dummy.factory, Dummy.contextDeinit);

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://example"),
    });
    defer src.deinit(alloc);

    var result = try src.query(alloc, .{
        .table = try alloc.dupe(u8, "customers"),
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("ctx", result.rows[0].object.get("label").?.string);
}
