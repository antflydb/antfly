// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! One admission budget: original execution lifetime and independent catalog
//! and fence controls. Clock domains remain owned by their original budgets.
const std = @import("std");
const sources = @import("antfly_local_sources");
const runtime = @import("antfly_runtime_abi");
const Context = sources.api_local_transaction_contract.PreDecisionContext;
const Controls = struct {
    require_checked_reentry: bool = true,
    context: Context,
    catalog: sources.api_routing_budget.RoutingBudget,
    fence_cancellation: sources.storage_db_types.CancellationToken,
    execution_runnable: ?runtime.runnable_abi.Borrowed,
};
fn check(controls: Controls) !void {
    try sources.api_local_table_writes.ensurePreDecisionContextActive(controls.context);
    try controls.fence_cancellation.check();
    try controls.catalog.checkpoint();
}
pub const Budget = sources.common_read_execution_budget.Budget(Controls, check);
pub fn init(context: Context, catalog: sources.api_routing_budget.RoutingBudget, fence_cancellation: sources.storage_db_types.CancellationToken, io: std.Io) Budget {
    return .{ .io = io, .controls = .{ .context = context, .catalog = catalog, .fence_cancellation = fence_cancellation, .execution_runnable = context.execution_runnable } };
}

test "routed write admission keeps independent original clocks and sticky cancellation" {
    const Clock = struct {
        value: i96,
        fn now(raw: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return .{ .nanoseconds = self.value };
        }
        const vtable = blk: {
            var v = std.Io.failing.vtable.*;
            v.now = now;
            break :blk v;
        };
        fn io(self: *@This()) std.Io {
            return .{ .userdata = self, .vtable = &vtable };
        }
    };
    var original: Clock = .{ .value = 100 };
    var catalog: Clock = .{ .value = 10_000 };
    const original_io = original.io();
    const catalog_io = catalog.io();
    const context: Context = .{ .deadline_ns = 200, .deadline_io = runtime.io_abi.Borrow.init(&original_io) };
    const routing: sources.api_routing_budget.RoutingBudget = .{ .deadline_ns = 10_100, .io = runtime.io_abi.Borrow.init(&catalog_io) };
    var budget = init(context, routing, .none, std.testing.io);
    try budget.checkpoint();
    original.value = 200;
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, budget.checkpoint());
    original.value = 100;
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, budget.checkpoint());
    budget = init(context, routing, .none, std.testing.io);
    catalog.value = 10_100;
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, budget.checkpoint());
    catalog.value = 10_000;
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, budget.checkpoint());
    for ([_]bool{ false, true }) |fence| {
        var canceled = std.atomic.Value(bool).init(false);
        const token = sources.storage_db_types.CancellationToken.fromAtomic(&canceled);
        var request = context;
        if (!fence) request.cancellation = token;
        budget = init(request, routing, if (fence) token else .none, std.testing.io);
        try budget.checkpoint();
        canceled.store(true, .release);
        try std.testing.expectError(error.Canceled, budget.checkpoint());
        canceled.store(false, .release);
        try std.testing.expectError(error.Canceled, budget.checkpoint());
    }
}
