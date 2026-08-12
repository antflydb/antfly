// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Data-only row execution types shared by SQL planning and storage adapters.
//! Keep parser, planner, routed execution, and concrete DB imports out of this
//! module so callback interfaces do not root the mixed SQL executor.

const std = @import("std");
const db_types = @import("../storage/db/types.zig");

pub const OwnedRowsBatchRequest = struct {
    writes: []db_types.BatchWrite = &.{},
    deletes: [][]const u8 = &.{},
    relational_identity_rewrites: []db_types.RelationalIdentityRewrite = &.{},
    transforms: []db_types.DocumentTransform = &.{},
    predicates: []db_types.TransactionVersionPredicate = &.{},
    returning_rows: [][]const u8 = &.{},
    returning_version_keys: [][]const u8 = &.{},
    returning_version_prewrite: []u64 = &.{},
    returning_version_outputs: [][]const u8 = &.{},
    req: db_types.BatchRequest = .{},
    inserted: u32 = 0,
    deleted: u32 = 0,
    transformed: u32 = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.writes.len > 0) alloc.free(self.writes);
        for (self.deletes) |key| alloc.free(key);
        if (self.deletes.len > 0) alloc.free(self.deletes);
        for (self.relational_identity_rewrites) |rewrite| {
            alloc.free(@constCast(rewrite.old_key));
            alloc.free(@constCast(rewrite.new_key));
            alloc.free(@constCast(rewrite.value));
        }
        if (self.relational_identity_rewrites.len > 0) alloc.free(self.relational_identity_rewrites);
        for (self.transforms) |transform| {
            alloc.free(@constCast(transform.key));
            for (transform.operations) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (transform.operations.len > 0) alloc.free(transform.operations);
        }
        if (self.transforms.len > 0) alloc.free(self.transforms);
        for (self.predicates) |predicate| alloc.free(@constCast(predicate.key));
        if (self.predicates.len > 0) alloc.free(self.predicates);
        for (self.returning_rows) |row| alloc.free(@constCast(row));
        if (self.returning_rows.len > 0) alloc.free(self.returning_rows);
        for (self.returning_version_keys) |key| alloc.free(key);
        if (self.returning_version_keys.len > 0) alloc.free(self.returning_version_keys);
        if (self.returning_version_prewrite.len > 0) alloc.free(self.returning_version_prewrite);
        for (self.returning_version_outputs) |output| alloc.free(output);
        if (self.returning_version_outputs.len > 0) alloc.free(self.returning_version_outputs);
        self.* = undefined;
    }
};

pub const SequenceDefaultRequest = struct {
    sequence: []const u8,
    database: []const u8 = "",
    schema: []const u8 = "",
};

pub const SequenceDefaultResolver = struct {
    ptr: *anyopaque,
    next_value_json_alloc: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, request: SequenceDefaultRequest) anyerror![]u8,

    pub fn nextValueJsonAlloc(self: @This(), alloc: std.mem.Allocator, request: SequenceDefaultRequest) ![]u8 {
        return try self.next_value_json_alloc(self.ptr, alloc, request);
    }
};

pub const ScalarSubqueryDefaultRequest = struct {
    query_json: []const u8,
};

pub const ScalarSubqueryDefaultResolver = struct {
    ptr: *anyopaque,
    value_json_alloc: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, request: ScalarSubqueryDefaultRequest) anyerror![]u8,

    pub fn valueJsonAlloc(self: @This(), alloc: std.mem.Allocator, request: ScalarSubqueryDefaultRequest) ![]u8 {
        return try self.value_json_alloc(self.ptr, alloc, request);
    }
};

pub const DefaultValueContext = struct {
    sequence_resolver: ?SequenceDefaultResolver = null,
    scalar_subquery_resolver: ?ScalarSubqueryDefaultResolver = null,
};

pub const ResolvedPrimaryRow = struct {
    json: []u8,
    version: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

pub const UniqueSelectorResolver = struct {
    ptr: *anyopaque,
    resolve: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
    ) anyerror!?[]u8,
    resolve_temporal: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
    ) anyerror!?[]u8 = null,
    resolve_temporal_overlap: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
    ) anyerror!?[]u8 = null,
    resolve_primary: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!bool = null,
    lookup_primary: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!?ResolvedPrimaryRow = null,

    pub fn resolveUnique(
        self: UniqueSelectorResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
    ) !?[]u8 {
        return try self.resolve(self.ptr, alloc, table_name, constraint_name, encoded_value);
    }

    pub fn resolveTemporalUnique(
        self: UniqueSelectorResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
    ) !?[]u8 {
        const func = self.resolve_temporal orelse return error.UnsupportedRowsSelector;
        return try func(self.ptr, alloc, table_name, constraint_name, encoded_value, encoded_point);
    }

    pub fn resolveTemporalUniqueOverlap(
        self: UniqueSelectorResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
    ) !?[]u8 {
        const func = self.resolve_temporal_overlap orelse return error.UnsupportedRowsSelector;
        return try func(self.ptr, alloc, table_name, constraint_name, encoded_value, encoded_start, encoded_end);
    }

    pub fn primaryExists(
        self: UniqueSelectorResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) !bool {
        const func = self.resolve_primary orelse return error.UnsupportedRowsSelector;
        return try func(self.ptr, alloc, table_name, physical_key);
    }

    pub fn lookupPrimary(
        self: UniqueSelectorResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) !?ResolvedPrimaryRow {
        const func = self.lookup_primary orelse return error.UnsupportedRowsSelector;
        return try func(self.ptr, alloc, table_name, physical_key);
    }
};

test "row execution contract owns batch and lookup results" {
    const alloc = std.testing.allocator;
    var batch = OwnedRowsBatchRequest{
        .writes = try alloc.dupe(db_types.BatchWrite, &.{.{
            .key = try alloc.dupe(u8, "key"),
            .value = try alloc.dupe(u8, "value"),
        }}),
    };
    batch.deinit(alloc);

    var row = ResolvedPrimaryRow{
        .json = try alloc.dupe(u8, "{}"),
        .version = 1,
    };
    row.deinit(alloc);
}
