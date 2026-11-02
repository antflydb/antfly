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

// Zig 0.17 keys relative C inputs by their working directory even with a
// relocatable --build-root. The audit overlays share immutable authored C
// providers, so retain those providers' canonical identities across workers.
// Mutable Zig sources and generated paths keep their private workspace roots.
fn canonicalAuthoredPath(b: *std.Build, path: std.Build.LazyPath) std.Build.LazyPath {
    const relative = switch (path) {
        .src_path => |source| source.owner.root.joinString(b.allocator, source.sub_path) catch @panic("OOM"),
        .dependency => |source| source.dependency.builder.root.joinString(b.allocator, source.sub_path) catch @panic("OOM"),
        else => return path,
    };
    b.dependOnFileMetadata(path);
    const canonical = std.Io.Dir.cwd().realPathFileAlloc(b.graph.io, relative, b.allocator) catch |err|
        std.debug.panic("cannot identify authored audit input {s}: {t}", .{ relative, err });
    return b.graph.cwdRelativePath(canonical);
}

// Anonymous schema imports such as ../openapi.yaml lie outside the explicit
// Zig build root. Zig hashes those paths absolutely, so relocation alone
// otherwise recompiles the API owner. These overlays share immutable authored
// schemas; keep their canonical checkout identity just like C providers.
fn authoredExternalPath(b: *std.Build, path: std.Build.LazyPath) std.Build.LazyPath {
    switch (path) {
        .src_path => |source| {
            if (std.mem.startsWith(u8, source.sub_path, "../") or
                std.mem.startsWith(u8, source.sub_path, "..\\"))
                return canonicalAuthoredPath(b, path);
        },
        else => {},
    }
    return path;
}

fn retainAuthoredIdentities(b: *std.Build, modules: *std.AutoHashMap(*std.Build.Module, void)) void {
    var iterator = modules.keyIterator();
    while (iterator.next()) |entry| {
        const module = entry.*;
        if (module.root_source_file) |path| module.root_source_file = authoredExternalPath(b, path);
        var objects: std.ArrayList(std.Build.Module.LinkObject) = .empty;
        for (module.link_objects.items) |object| switch (object) {
            .c_source_file => |source| {
                source.file = canonicalAuthoredPath(b, source.file);
                objects.append(b.allocator, object) catch @panic("OOM");
            },
            .c_source_files => |sources| for (sources.files) |file| {
                const source = b.allocator.create(std.Build.Module.CSourceFile) catch @panic("OOM");
                source.* = .{
                    .file = canonicalAuthoredPath(b, sources.root.path(b, file)),
                    .flags = sources.flags,
                    .language = sources.language,
                };
                objects.append(b.allocator, .{ .c_source_file = source }) catch @panic("OOM");
            },
            else => objects.append(b.allocator, object) catch @panic("OOM"),
        };
        module.link_objects = objects;
    }
}

pub fn build(b: *std.Build) void {
    const measured_memory = b.option(bool, "native-linux-ownership-memory", "Use the qualified Nix glibc stripped Debug audit envelope") orelse false;
    _ = project.create(b) orelse return;
    var steps = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    var modules = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
    for (b.top_level_steps.values()) |top| profiles.collectSteps(&top.step, &steps, &modules);
    retainAuthoredIdentities(b, &modules);
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
            if (artifact.step.max_rss <= cap) {
                std.debug.print("storage audit claim artifact={s} bytes={d} authored=true\n", .{ artifact.name, artifact.step.max_rss });
                continue;
            }
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
            std.debug.print("storage audit claim artifact={s} bytes={d} authored=false\n", .{ artifact.name, artifact.step.max_rss });
        }
    }
}
