// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const graph_query = @import("graph_query_diagnostic.zig");
const distinct_budget = @import("../graph/distinct_budget_diagnostic.zig");
const path_weight = @import("../graph/path_weight_diagnostic.zig");
const work_budget = @import("../graph/work_budget_diagnostic.zig");

/// Owns every diagnostic that can be produced while executing one public
/// graph request. Keeping this value on the request stack prevents payloads
/// from leaking between reused worker threads.
pub const Context = struct {
    query: graph_query.Storage = .{},
    distinct: distinct_budget.Storage = .{},
    path_weight: path_weight.Storage = .{},
    work: work_budget.Storage = .{},
};

/// Binds request-owned storage for the duration of synchronous execution.
/// Bindings are stackable so internal/nested requests restore their caller.
pub const Scope = struct {
    query: graph_query.Binding,
    distinct: distinct_budget.Binding,
    path_weight: path_weight.Binding,
    work: work_budget.Binding,

    pub fn init(context: *Context) Scope {
        return .{
            .query = graph_query.bind(&context.query),
            .distinct = distinct_budget.bind(&context.distinct),
            .path_weight = path_weight.bind(&context.path_weight),
            .work = work_budget.bind(&context.work),
        };
    }

    pub fn deinit(self: Scope) void {
        self.work.deinit();
        self.path_weight.deinit();
        self.distinct.deinit();
        self.query.deinit();
    }
};

test "nested request scopes restore their caller diagnostics" {
    var outer: Context = .{};
    const outer_scope = Scope.init(&outer);
    defer outer_scope.deinit();
    graph_query.record("outer", "match", .external_alias_source_not_supported);

    var inner: Context = .{};
    const inner_scope = Scope.init(&inner);
    graph_query.record("inner", "match", .reverse_variable_path_not_supported);
    try std.testing.expectEqualStrings("inner", graph_query.take().?.operation);
    inner_scope.deinit();

    try std.testing.expectEqualStrings("outer", graph_query.take().?.operation);
}
