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

//! Opt-in production-operation probes. They do not change product topology.
const std = @import("std");
const Imports = @import("antfly/imports.zig").AntflyRootImports;

pub fn add(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize, imports: Imports, linalg: *std.Build.Module) void {
    const aggregate = b.step("architecture-probe-test", "Test pure mutation plans and independent production-operation owners");
    const plan_step = b.step("mutation-plan-test", "Test concrete mutation and transform read planning without storage");
    inline for (.{ "transform_read_plan", "mutation_plan" }) |name| {
        const plan = b.addTest(.{ .name = name ++ "-tests", .root_module = b.createModule(.{ .root_source_file = b.path("pkg/antfly-embedded/src/storage/db/" ++ name ++ ".zig"), .target = target, .optimize = optimize }) });
        const plan_run = b.addRunArtifact(plan);
        plan_step.dependOn(&plan_run.step);
        aggregate.dependOn(&plan_run.step);
    }

    const abi = b.createModule(.{ .root_source_file = b.path("build_support/architecture_probe_abi.zig"), .target = target, .optimize = optimize });
    abi.addImport("runtime_failure_abi", imports.storage_boundary.failure);
    inline for (.{ "index", "fused_cpu" }) |name| {
        const provider = b.createModule(.{
            .root_source_file = b.path(if (comptime std.mem.eql(u8, name, "index")) "pkg/antfly/src/index_owner_probe.zig" else "pkg/inference/src/fused_cpu_probe.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        provider.addImport("architecture_probe_abi", abi);
        provider.addImport("runtime_failure_identity", imports.storage_boundary.identity);
        if (comptime std.mem.eql(u8, name, "index")) {
            imports.configureStorage(b, provider, true);
            imports.storage_boundary.configureSources(provider, false, false);
            // This archive owns the selected implementation. It must not call
            // back into or silently link the complete shared storage engine.
            const implementation_options = b.addOptions();
            implementation_options.addOption(bool, "shared_storage_core", true);
            implementation_options.addOption(bool, "storage_engine_owner", true);
            provider.addOptions("storage_engine_options", implementation_options);
        } else provider.addImport("inference_linalg", linalg);
        const library = b.addLibrary(.{ .name = "antfly-probe-" ++ name, .root_module = provider, .linkage = .static, .use_llvm = true, .max_rss = 4 * 1024 * 1024 * 1024 });
        const consumer = b.createModule(.{ .root_source_file = b.path(if (comptime std.mem.eql(u8, name, "index")) "tools/fixtures/index_owner_probe_client.zig" else "tools/fixtures/fused_cpu_probe_client.zig"), .target = target, .optimize = optimize, .link_libc = true });
        consumer.addImport("architecture_probe_abi", abi);
        consumer.addImport("runtime_failure_identity", imports.storage_boundary.identity);
        const tests = @import("../pkg/antfly/build/linked_tests.zig").add(b, .{ .name = name ++ "-owner-probe-tests", .root_module = consumer, .max_rss = 1024 * 1024 * 1024 });
        tests.executable.root_module.linkLibrary(library);
        const run = tests.run(b);
        b.step(if (comptime std.mem.eql(u8, name, "index")) "index-owner-probe-test" else "inference-kernel-probe-test", "Run separately compiled " ++ name ++ " production-operation contracts").dependOn(&run.step);
        aggregate.dependOn(&run.step);
    }
}
