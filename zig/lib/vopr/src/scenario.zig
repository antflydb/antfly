// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const outcome = @import("outcome.zig");

pub fn assertContract(comptime Scenario: type) void {
    const required_declarations = .{ "World", "name", "version", "properties", "init", "deinit", "enumerate", "execute", "observe", "evaluate", "done" };
    inline for (required_declarations) |declaration| {
        if (!@hasDecl(Scenario, declaration)) @compileError("simulation scenario is missing required declaration: " ++ declaration);
    }
    if (@TypeOf(Scenario.name) != []const u8) @compileError("Scenario.name must be []const u8");
    if (@TypeOf(Scenario.version) != u32) @compileError("Scenario.version must be u32");
    const execute_return = @typeInfo(@TypeOf(Scenario.execute)).@"fn".return_type orelse
        @compileError("Scenario.execute must have an explicit return type");
    const execute_payload = switch (@typeInfo(execute_return)) {
        .error_union => |error_union| error_union.payload,
        else => @compileError("Scenario.execute must return an error union containing vopr.outcome.TransitionOutcome"),
    };
    if (execute_payload != outcome.TransitionOutcome)
        @compileError("Scenario.execute must return !vopr.outcome.TransitionOutcome");
}
