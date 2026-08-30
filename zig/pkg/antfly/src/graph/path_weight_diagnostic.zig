// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const graph_query = @import("query.zig");

pub const Objective = enum {
    min_hops,
    min_weight_sum,
    max_weight_product,
};

pub const Violation = enum {
    negative_edge_weight,
    edge_weight_above_one,
    path_sum_overflow,
};

pub const Diagnostic = struct {
    operation: []const u8,
    objective: Objective,
    violation: Violation,
};

pub const Storage = struct {
    diagnostic: ?Diagnostic = null,
    operation_buf: [graph_query.max_identifier_bytes]u8 = undefined,
};

threadlocal var active_storage: ?*Storage = null;

pub const Binding = struct {
    previous: ?*Storage,

    pub fn deinit(self: Binding) void {
        active_storage = self.previous;
    }
};

pub fn bind(storage: *Storage) Binding {
    const previous = active_storage;
    active_storage = storage;
    return .{ .previous = previous };
}

pub fn reset() void {
    if (active_storage) |storage| storage.diagnostic = null;
}

pub fn take() ?Diagnostic {
    const storage = active_storage orelse return null;
    const diagnostic = storage.diagnostic;
    storage.diagnostic = null;
    return diagnostic;
}

pub fn isDomainError(err: anyerror) bool {
    return err == error.GraphMinWeightDomainViolation or
        err == error.GraphMaxWeightDomainViolation or
        err == error.GraphPathWeightOverflow;
}

pub fn record(operation: []const u8, query: graph_query.GraphQuery, err: anyerror) void {
    const storage = active_storage orelse return;
    if (operation.len == 0 or operation.len > storage.operation_buf.len or !isDomainError(err)) {
        storage.diagnostic = null;
        return;
    }
    @memcpy(storage.operation_buf[0..operation.len], operation);
    const objective: Objective = switch (query.params.weight_mode) {
        .min_hops => .min_hops,
        .min_weight => .min_weight_sum,
        .max_weight => .max_weight_product,
    };
    const violation: Violation = if (err == error.GraphMinWeightDomainViolation)
        .negative_edge_weight
    else if (err == error.GraphMaxWeightDomainViolation)
        .edge_weight_above_one
    else
        .path_sum_overflow;
    storage.diagnostic = .{
        .operation = storage.operation_buf[0..operation.len],
        .objective = objective,
        .violation = violation,
    };
}

test "path weight overflow has a stable public diagnostic" {
    var storage: Storage = .{};
    const binding = bind(&storage);
    defer binding.deinit();
    record("route", .{
        .query_type = .neighbors,
        .index_name = "relationships",
        .start_nodes = .{ .keys = &.{} },
        .params = .{ .weight_mode = .min_weight },
    }, error.GraphPathWeightOverflow);
    const diagnostic = take().?;
    try std.testing.expectEqualStrings("route", diagnostic.operation);
    try std.testing.expectEqual(Objective.min_weight_sum, diagnostic.objective);
    try std.testing.expectEqual(Violation.path_sum_overflow, diagnostic.violation);
}
