// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const graph_query = @import("query.zig");
const pattern = @import("pattern.zig");

pub const Dimension = pattern.DistinctBudget.Dimension;
pub const Exhaustion = pattern.DistinctBudget.Exhaustion;

pub const Diagnostic = struct {
    operation: []const u8,
    dimension: Dimension,
    maximum: usize,
};

threadlocal var last_diagnostic: ?Diagnostic = null;
threadlocal var operation_buf: [graph_query.max_identifier_bytes]u8 = undefined;

pub fn reset() void {
    last_diagnostic = null;
}

pub fn take() ?Diagnostic {
    const diagnostic = last_diagnostic;
    last_diagnostic = null;
    return diagnostic;
}

pub fn record(operation: []const u8, exhaustion: Exhaustion) void {
    if (operation.len == 0 or operation.len > operation_buf.len) {
        last_diagnostic = null;
        return;
    }
    @memcpy(operation_buf[0..operation.len], operation);
    last_diagnostic = .{
        .operation = operation_buf[0..operation.len],
        .dimension = exhaustion.dimension,
        .maximum = exhaustion.maximum,
    };
}

pub fn recordBudget(operation: []const u8, budget: *const pattern.DistinctBudget) void {
    if (budget.exhaustion()) |exhaustion| record(operation, exhaustion);
}

pub fn dimensionName(dimension: Dimension) []const u8 {
    return switch (dimension) {
        .distinct_identities => "distinct_identities",
        .distinct_state_bytes => "distinct_state_bytes",
    };
}

test "distinct budget diagnostic owns the operation name" {
    var operation = [_]u8{ 'm', 'a', 't', 'c', 'h' };
    record(&operation, .{ .dimension = .distinct_state_bytes, .maximum = 4096 });
    @memset(&operation, 'x');
    const diagnostic = take().?;
    try std.testing.expectEqualStrings("match", diagnostic.operation);
    try std.testing.expectEqual(Dimension.distinct_state_bytes, diagnostic.dimension);
    try std.testing.expectEqual(@as(usize, 4096), diagnostic.maximum);
}
