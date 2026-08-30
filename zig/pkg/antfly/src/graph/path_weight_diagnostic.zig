// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const graph_query = @import("query.zig");

pub const Mode = enum {
    min_weight,
    max_weight,
    weighted_path,
};

pub const Violation = enum {
    negative_edge_weight,
    edge_weight_above_one,
    path_sum_overflow,
};

pub const Diagnostic = struct {
    operation: []const u8,
    mode: Mode,
    violation: Violation,
    allowed_range: []const u8,
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

pub fn record(operation: []const u8, err: anyerror) void {
    const storage = active_storage orelse return;
    if (operation.len == 0 or operation.len > storage.operation_buf.len or !isDomainError(err)) {
        storage.diagnostic = null;
        return;
    }
    @memcpy(storage.operation_buf[0..operation.len], operation);
    const mode: Mode = if (err == error.GraphMinWeightDomainViolation)
        .min_weight
    else if (err == error.GraphMaxWeightDomainViolation)
        .max_weight
    else
        .weighted_path;
    const violation: Violation = if (err == error.GraphMinWeightDomainViolation)
        .negative_edge_weight
    else if (err == error.GraphMaxWeightDomainViolation)
        .edge_weight_above_one
    else
        .path_sum_overflow;
    const allowed_range: []const u8 = if (err == error.GraphMinWeightDomainViolation)
        "[0,+inf) with a finite path sum"
    else if (err == error.GraphMaxWeightDomainViolation)
        "[0,1]"
    else
        "finite f64";
    storage.diagnostic = .{
        .operation = storage.operation_buf[0..operation.len],
        .mode = mode,
        .violation = violation,
        .allowed_range = allowed_range,
    };
}

test "path weight overflow has a stable public diagnostic" {
    var storage: Storage = .{};
    const binding = bind(&storage);
    defer binding.deinit();
    record("route", error.GraphPathWeightOverflow);
    const diagnostic = take().?;
    try std.testing.expectEqualStrings("route", diagnostic.operation);
    try std.testing.expectEqual(Mode.weighted_path, diagnostic.mode);
    try std.testing.expectEqual(Violation.path_sum_overflow, diagnostic.violation);
    try std.testing.expectEqualStrings("finite f64", diagnostic.allowed_range);
}
