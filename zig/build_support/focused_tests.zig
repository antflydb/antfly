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

//! Minimal authored dependencies for focused value/plan tests. No server build
//! graph, inference runtime, source catalog, test sharding or physical owner.
const std = @import("std");
const platform_build = @import("antfly_platform");

fn module(b: *std.Build, path: []const u8, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) *std.Build.Module {
    return b.createModule(.{ .root_source_file = b.path(path), .target = target, .optimize = optimize });
}

pub fn aggregationModule(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) *std.Build.Module {
    const root = module(b, "pkg/antfly-embedded/src/aggregation_plan_test_root.zig", target, optimize);
    root.link_libc = true;
    const runtime = module(b, "lib/runtime/src/root.zig", target, optimize);
    const cancellation = module(b, "lib/runtime/src/cancellation.zig", target, optimize);
    cancellation.addImport("antfly_runtime_abi", runtime);
    const platform = platform_build.createModule(b, .{ .root_source_file = b.path("lib/platform/src/root.zig"), .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"), .target = target, .optimize = optimize, .link_libc = true });
    const reranking = module(b, "lib/reranking/src/mod.zig", target, optimize);
    const reranking_contract = module(b, "pkg/antfly-embedded/src/openapi/generated/antfly_reranking_openapi/root.zig", target, optimize);
    reranking_contract.addImport("antfly_provider_openapi", module(b, "pkg/antfly-embedded/src/openapi/generated/antfly_provider_openapi/root.zig", target, optimize));
    reranking.addImport("antfly_reranking_openapi", reranking_contract);
    reranking.addImport("antfly-json", module(b, "lib/json/src/mod.zig", target, optimize));
    const cloudflare = module(b, "lib/cloudflare/routing.zig", target, optimize);
    cloudflare.addImport("cloudflare_api", module(b, "pkg/antfly-embedded/src/openapi/generated/cloudflare_api/root.zig", target, optimize));
    reranking.addImport("antfly_cloudflare_config", cloudflare);
    const regex = module(b, "lib/regex/src/mod.zig", target, optimize);
    regex.addImport("antfly_fst", module(b, "lib/fst/src/mod.zig", target, optimize));
    regex.addImport("antfly_grapheme", module(b, "lib/tokenizer/src/grapheme.zig", target, optimize));
    regex.addImport("antfly_capture_regex", module(b, "lib/regex/src/captures.zig", target, optimize));
    regex.addImport("antfly_platform", platform);
    root.addImport("antfly_platform", platform);
    root.addImport("antfly_runtime_abi", runtime);
    root.addImport("antfly_cancellation", cancellation);
    root.addImport("antfly_reranking", reranking);
    root.addImport("antfly_regex", regex);
    const cache_budget = module(b, "lib/runtime/src/cache_budget.zig", target, optimize);
    cache_budget.addImport("antfly_platform", platform);
    root.addImport("antfly_cache_budget", cache_budget);
    platform_build.addMacosSdkPaths(b, root, target);
    return root;
}

pub fn addPagingTests(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) *std.Build.Step.Run {
    const tests = b.addTest(.{ .name = "query-paging-tests", .root_module = module(b, "pkg/antfly-embedded/src/query_paging_test_root.zig", target, optimize), .filters = &.{"query paging plan"}, .max_rss = 1024 * 1024 * 1024, .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple } });
    const run = b.addRunArtifact(tests);
    run.step.max_rss = 128 * 1024 * 1024;
    run.addArgs(&.{ "--suite-filter", "query paging plan" });
    run.addPassthruArgs();
    b.step("query-paging-test", "Run dependency-free production query-window and domain plans").dependOn(&run.step);
    return run;
}

pub fn build(b: *std.Build) void {
    defer platform_build.finalizeMacosSdk(b);
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const all = b.step("test", "Run focused mutation and aggregation plans without constructing the product graph");
    b.default_step = all;
    all.dependOn(&addPagingTests(b, target, optimize).step);
    const plans = b.step("mutation-plan-test", "Run concrete mutation and transform read plans");
    inline for (.{ "transform_read_plan", "mutation_plan" }) |name| {
        const tests = b.addTest(.{ .name = name ++ "-tests", .root_module = module(b, "pkg/antfly-embedded/src/storage/db/" ++ name ++ ".zig", target, optimize), .max_rss = 1024 * 1024 * 1024 });
        const run = b.addRunArtifact(tests);
        run.step.max_rss = 128 * 1024 * 1024;
        plans.dependOn(&run.step);
    }
    all.dependOn(plans);
    const tests = b.addTest(.{ .name = "aggregation-plan-tests", .root_module = aggregationModule(b, target, optimize), .filters = &.{"aggregation domain"}, .max_rss = 2 * 1024 * 1024 * 1024, .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple } });
    const run = b.addRunArtifact(tests);
    run.step.max_rss = 128 * 1024 * 1024;
    run.addArgs(&.{ "--suite-filter", "aggregation domain" });
    run.addPassthruArgs();
    b.step("aggregation-plan-test", "Run concrete aggregation planning and complete-component collection").dependOn(&run.step);
    all.dependOn(&run.step);
}
