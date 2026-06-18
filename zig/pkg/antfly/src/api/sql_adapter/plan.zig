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

const ast = @import("ast.zig");
const db_mod = @import("../../storage/db/mod.zig");
const grammar = @import("grammar.zig");
const relational_rows = @import("../relational_rows.zig");
const runtime_schema = @import("../../storage/schema.zig");

pub const RelationLifetimeKind = grammar.RelationLifetimeKind;
pub const RelationPopulationMode = grammar.RelationPopulationMode;
pub const SelectOutputRef = ast.SelectOutputRef;

pub const LoweredSelect = struct {
    table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    query: db_mod.types.RelationalRowsQueryRequest,
    select_outputs: []const SelectOutputRef = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        self.query.deinit(alloc);
        self.clearSelectOutputs(alloc);
        self.* = undefined;
    }

    pub fn clearSelectOutputs(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.select_outputs.len > 0) alloc.free(self.select_outputs);
        self.select_outputs = &.{};
    }
};

pub const LoweredQueryPlan = struct {
    table_name: []const u8,
    plan: db_mod.types.RelationalRowsQueryPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredWindowPlan = struct {
    table_name: []const u8,
    plan: db_mod.types.RelationalRowsWindowPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredInsert = struct {
    table_name: []const u8,
    batch: relational_rows.OwnedRowsBatchRequest,
    returning_expression_count: usize = 0,
    returning_all: bool = false,
    conflict_where: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredInsertSource = struct {
    table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    insert_source: relational_rows.OwnedRowsInsertSourceRequest,
    returning_expression_count: usize = 0,
    returning_all: bool = false,
    conflict_where: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        self.insert_source.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredMutation = struct {
    table_name: []const u8,
    batch: relational_rows.OwnedRowsBatchRequest,
    returning_expression_count: usize = 0,
    returning_all: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredMutationSource = struct {
    table_name: []const u8,
    mutation: relational_rows.OwnedRowsMutationSourceRequest,
    restart_identity: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.mutation.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredJoinedMutationSource = struct {
    target_table_name: []const u8,
    source_table_name: []const u8,
    mutation: relational_rows.OwnedRowsJoinedMutationSourceRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.target_table_name);
        alloc.free(self.source_table_name);
        self.mutation.deinit(alloc);
        self.* = undefined;
    }
};

pub const LowerWritePlanOptions = struct {
    unique_resolver: ?relational_rows.UniqueSelectorResolver = null,
    row_claim: ?db_mod.types.RowClaimRequest = null,
    joined_source_schema: ?runtime_schema.TableSchema = null,
    insert_source_schema: ?runtime_schema.TableSchema = null,
};

pub const LoweredAggregate = struct {
    table_name: []const u8,
    aggregate: db_mod.types.RelationalRowsAggregateRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.aggregate.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredAggregatePlan = struct {
    table_name: []const u8,
    plan: db_mod.types.RelationalRowsAggregatePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredJoin = struct {
    left_table_name: []const u8,
    right_table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    left_ranges: []const db_mod.types.RelationalRowsDocKeyRange = &.{},
    right_ranges: []const db_mod.types.RelationalRowsDocKeyRange = &.{},
    join: db_mod.types.RelationalRowsJoinRequest,

    pub fn asPlan(self: @This()) db_mod.types.RelationalRowsJoinPlan {
        return .{
            .ctes = self.ctes,
            .left_table = self.left_table_name,
            .right_table = self.right_table_name,
            .left_ranges = self.left_ranges,
            .right_ranges = self.right_ranges,
            .join = self.join,
        };
    }

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        var owned = self.asPlan();
        owned.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredLateralPlan = struct {
    left_table_name: []const u8,
    right_table_name: []const u8,
    plan: db_mod.types.RelationalRowsLateralPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.left_table_name);
        alloc.free(self.right_table_name);
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredReadPlan = union(enum) {
    query: LoweredQueryPlan,
    aggregate: LoweredAggregatePlan,
    join: LoweredJoin,
    lateral: LoweredLateralPlan,
    window: LoweredWindowPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .query => |*query| query.deinit(alloc),
            .aggregate => |*aggregate| aggregate.deinit(alloc),
            .join => |*join| join.deinit(alloc),
            .lateral => |*lateral| lateral.deinit(alloc),
            .window => |*window| window.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const LoweredRelationPopulationPlan = struct {
    mode: RelationPopulationMode,
    target_table_name: []const u8,
    target_lifetime: ?RelationLifetimeKind = null,
    if_not_exists: bool = false,
    source: LoweredReadPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.target_table_name);
        self.source.deinit(alloc);
        self.* = undefined;
    }
};

test "sql adapter lowered read plans own nested storage plan memory" {
    const alloc = std.testing.allocator;

    var lowered = LoweredReadPlan{
        .join = .{
            .left_table_name = try alloc.dupe(u8, "left_table"),
            .right_table_name = try alloc.dupe(u8, "right_table"),
            .join = .{
                .on = try alloc.dupe(db_mod.types.RelationalRowsJoinOn, &[_]db_mod.types.RelationalRowsJoinOn{.{
                    .left_field = try alloc.dupe(u8, "left_id"),
                    .right_field = try alloc.dupe(u8, "right_id"),
                }}),
            },
        },
    };
    lowered.deinit(alloc);
}

test "sql adapter lowered write containers own nested request memory" {
    const alloc = std.testing.allocator;

    var insert = LoweredInsert{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .batch = .{
            .writes = try alloc.dupe(db_mod.types.BatchWrite, &[_]db_mod.types.BatchWrite{.{
                .key = try alloc.dupe(u8, "pk"),
                .value = try alloc.dupe(u8, "{\"id\":\"u1\"}"),
            }}),
        },
    };
    insert.deinit(alloc);

    var mutation_source = LoweredMutationSource{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .mutation = .{ .req = .{ .kind = .delete } },
    };
    mutation_source.deinit(alloc);
}
