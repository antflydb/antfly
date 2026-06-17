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

//! Antfly-owned algebraic materialization artifact types.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const SourceKind = enum(u8) {
    serverless_fragment = 1,
    external_parquet = 2,
    external_iceberg = 3,
    external_lance = 4,
    relational_store = 5,
};

pub const AggregateOp = enum(u8) {
    count = 1,
    sum_i64 = 2,
    min_i64 = 3,
    max_i64 = 4,
};

pub const AggregateValue = union(AggregateOp) {
    count: u64,
    sum_i64: i64,
    min_i64: i64,
    max_i64: i64,
};

pub const SourceRef = struct {
    kind: SourceKind,
    snapshot_id: []u8,
    schema_fingerprint: []u8,
    source_id: []u8 = &.{},

    pub fn deinit(self: *SourceRef, alloc: Allocator) void {
        alloc.free(self.snapshot_id);
        alloc.free(self.schema_fingerprint);
        if (self.source_id.len != 0) alloc.free(self.source_id);
        self.* = undefined;
    }

    pub fn validate(self: SourceRef) !void {
        if (self.snapshot_id.len == 0) return error.InvalidAlgebraicSegment;
        if (self.schema_fingerprint.len == 0) return error.InvalidAlgebraicSegment;
    }
};

pub const GroupFold = struct {
    key: []u8,
    value: AggregateValue,

    pub fn deinit(self: *GroupFold, alloc: Allocator) void {
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub const GroupByAggregate = struct {
    group_column: []u8,
    value_column: []u8 = &.{},
    op: AggregateOp,
    groups: []GroupFold,

    pub fn deinit(self: *GroupByAggregate, alloc: Allocator) void {
        alloc.free(self.group_column);
        if (self.value_column.len != 0) alloc.free(self.value_column);
        for (self.groups) |*group| group.deinit(alloc);
        alloc.free(self.groups);
        self.* = undefined;
    }

    pub fn validate(self: GroupByAggregate) !void {
        if (self.group_column.len == 0) return error.InvalidAlgebraicSegment;
        if (self.op != .count and self.value_column.len == 0) return error.InvalidAlgebraicSegment;
        for (self.groups, 0..) |group, idx| {
            if (group.key.len == 0) return error.InvalidAlgebraicSegment;
            for (self.groups[0..idx]) |previous| {
                if (std.mem.eql(u8, previous.key, group.key)) return error.InvalidAlgebraicSegment;
            }
            if (std.meta.activeTag(group.value) != self.op) return error.InvalidAlgebraicSegment;
        }
    }
};

pub const Segment = struct {
    source: SourceRef,
    aggregate: GroupByAggregate,

    pub fn deinit(self: *Segment, alloc: Allocator) void {
        self.source.deinit(alloc);
        self.aggregate.deinit(alloc);
        self.* = undefined;
    }

    pub fn validate(self: Segment) !void {
        try self.source.validate();
        try self.aggregate.validate();
    }
};

pub fn freeSegment(alloc: Allocator, segment: *Segment) void {
    segment.deinit(alloc);
}

test "algebraic segment validates group-by aggregate folds" {
    const alloc = std.testing.allocator;
    var segment = Segment{
        .source = .{
            .kind = .serverless_fragment,
            .snapshot_id = try alloc.dupe(u8, "manifest-1"),
            .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
            .source_id = try alloc.dupe(u8, "orders"),
        },
        .aggregate = .{
            .group_column = try alloc.dupe(u8, "tenant"),
            .value_column = try alloc.dupe(u8, "amount"),
            .op = .sum_i64,
            .groups = try alloc.alloc(GroupFold, 1),
        },
    };
    defer segment.deinit(alloc);
    segment.aggregate.groups[0] = .{
        .key = try alloc.dupe(u8, "t1"),
        .value = .{ .sum_i64 = 42 },
    };
    try segment.validate();
}
