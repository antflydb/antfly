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

// Installed as build.zig only in the source-overlay regression checkout.
const std = @import("std");
const project = @import("project_build.zig");
const profiles = @import("tools/fixtures/build_profiles.zig");

// This bounded cache audit measures these owners and consumers. Other owner
// fixtures are covered by their normal test targets; adding one must not grow
// this compilation matrix or invalidate its discovery contract.
const audited_tests = [_][]const u8{
    "storage-owner-tests",
    "storage-owner-source-tests",
    "storage-owner-enrichment-tests",
    "api-table-read-tests",
    "api-table-write-tests",
    "api-table-write-lifecycle-tests",
    "data-runtime-tests",
};

pub fn build(b: *std.Build) void {
    const measured_memory = b.option(bool, "native-linux-ownership-memory", "Use the qualified Nix glibc stripped Debug audit envelope") orelse false;
    _ = project.create(b) orelse return;
    var steps = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    var modules = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
    for (b.top_level_steps.values()) |top| profiles.collectSteps(&top.step, &steps, &modules);
    const check = b.step("check-storage-compilation", "Compile real runtime archives and their owner tests");
    var found: [audited_tests.len]bool = @splat(false);
    var iterator = steps.keyIterator();
    while (iterator.next()) |entry| {
        const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
        if (artifact.kind != .exe) continue;
        for (audited_tests, 0..) |name, index| {
            if (std.mem.eql(u8, artifact.name, name)) {
                if (found[index]) std.debug.panic("duplicate audited storage test artifact: {s}", .{name});
                check.dependOn(&artifact.step);
                found[index] = true;
            }
        }
    }
    for (audited_tests, found) |name, present| {
        if (!present) std.debug.panic("missing audited storage test artifact: {s}", .{name});
    }
    inline for (.{ "cli", "distributed", "storage_kernel", "enrichment_compute", "serverless", "inference", "api_kernel" }) |unit| {
        check.dependOn(&b.top_level_steps.get("runtime-unit-" ++ unit).?.step);
    }
    if (measured_memory) {
        const host = b.graph.host.result;
        if (host.os.tag != .linux or host.cpu.arch != .x86_64 or
            !std.mem.eql(u8, @import("builtin").zig_version_string, "0.17.0"))
            @panic("native Linux ownership memory profile requires the qualified host/compiler");
        var audited_steps = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
        var audited_modules = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
        profiles.collectSteps(check, &audited_steps, &audited_modules);
        var selected = audited_steps.keyIterator();
        while (selected.next()) |entry| {
            const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
            const cap = profiles.native_linux_audit_compile_cap;
            if (artifact.step.max_rss <= cap) continue;
            const module = artifact.root_module;
            const target = module.resolved_target.?.result;
            const baseline = std.Target.Cpu.baseline(target.cpu.arch, target.os);
            if (target.os.tag != .linux or target.cpu.arch != .x86_64 or target.abi != .gnu or
                target.cpu.model != baseline.model or !target.cpu.features.eql(baseline.features) or
                module.optimize != .debug or module.strip != true)
                @panic("native Linux ownership memory profile requires baseline GNU stripped Debug artifacts");
            // Applies only to this audit's transitive graph. Keep smaller
            // authored reservations and all production build profiles intact.
            artifact.step.max_rss = cap;
        }
    }
}
