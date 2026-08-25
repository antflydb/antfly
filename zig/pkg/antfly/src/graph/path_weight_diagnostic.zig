// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const graph_query = @import("query.zig");

pub const Diagnostic = struct {
    operation: []const u8,
    mode: []const u8,
    allowed_range: []const u8,
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

pub fn isDomainError(err: anyerror) bool {
    return err == error.GraphMinWeightDomainViolation or
        err == error.GraphMaxWeightDomainViolation or
        err == error.GraphPathWeightOverflow;
}

pub fn record(operation: []const u8, err: anyerror) void {
    if (operation.len == 0 or operation.len > operation_buf.len or !isDomainError(err)) {
        last_diagnostic = null;
        return;
    }
    @memcpy(operation_buf[0..operation.len], operation);
    const mode: []const u8 = if (err == error.GraphMinWeightDomainViolation)
        "min_weight"
    else if (err == error.GraphMaxWeightDomainViolation)
        "max_weight"
    else
        "weight_sum";
    const allowed_range: []const u8 = if (err == error.GraphMinWeightDomainViolation)
        "[0,+inf) with a finite path sum"
    else if (err == error.GraphMaxWeightDomainViolation)
        "[0,1]"
    else
        "finite f64";
    last_diagnostic = .{
        .operation = operation_buf[0..operation.len],
        .mode = mode,
        .allowed_range = allowed_range,
    };
}

test "path weight overflow has a stable public diagnostic" {
    record("route", error.GraphPathWeightOverflow);
    const diagnostic = take().?;
    try std.testing.expectEqualStrings("route", diagnostic.operation);
    try std.testing.expectEqualStrings("weight_sum", diagnostic.mode);
    try std.testing.expectEqualStrings("finite f64", diagnostic.allowed_range);
}
