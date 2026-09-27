// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const outcome = @import("outcome.zig");

pub const NoTransition = union(enum) {
    clean_quiescence,
    expected_blocked,
    liveness_failure: []const u8,
    harness_deadlock: []const u8,
};

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
