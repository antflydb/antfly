// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Focused tests for production mutation, aggregation and query contracts.
const std = @import("std");
const Imports = @import("antfly/imports.zig").AntflyRootImports;

pub fn add(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize, imports: Imports) void {
    _ = @import("focused_tests.zig").addPagingTests(b, target, optimize);
    const plan_step = b.step("mutation-plan-test", "Test concrete mutation and transform read planning without storage");
    inline for (.{ "transform_read_plan", "mutation_plan" }) |name| {
        const plan = b.addTest(.{ .name = name ++ "-tests", .root_module = b.createModule(.{ .root_source_file = b.path("pkg/antfly-embedded/src/storage/db/" ++ name ++ ".zig"), .target = target, .optimize = optimize }) });
        const plan_run = b.addRunArtifact(plan);
        plan_step.dependOn(&plan_run.step);
    }

    const aggregation_module = @import("focused_tests.zig").aggregationModule(b, target, optimize);
    const aggregation_tests = @import("../pkg/antfly/build/linked_tests.zig").add(b, .{ .name = "aggregation-plan-tests", .root_module = aggregation_module, .filters = &.{"aggregation domain"}, .max_rss = 2 * 1024 * 1024 * 1024 });
    // This source-free final link and nine pure tests do not inherit the
    // product integration gate's 7 GiB/6 GiB compile/run defaults.
    aggregation_tests.executable.step.max_rss = 1024 * 1024 * 1024;
    const aggregation_run = aggregation_tests.run(b);
    aggregation_run.step.max_rss = 128 * 1024 * 1024;
    b.step("aggregation-plan-test", "Test concrete query aggregation planning and execution without a database").dependOn(&aggregation_run.step);

    const query_contract_tests = b.addTest(.{ .name = "local-query-contract-tests", .root_module = imports.storage_boundary.query, .filters = &.{"query DB borrow"} });
    const query_contract_run = b.addRunArtifact(query_contract_tests);
    b.step("local-query-contract-test", "Check fail-closed native query borrows without a database").dependOn(&query_contract_run.step);
}
