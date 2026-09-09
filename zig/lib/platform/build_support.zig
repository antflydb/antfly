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

const std = @import("std");

pub const ModuleOptions = struct {
    root_source_file: std.Build.LazyPath,
    filesystem_capacity_source_file: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    link_libc: bool,
    single_threaded: ?bool = null,
};

pub fn createModule(b: *std.Build, options: ModuleOptions) *std.Build.Module {
    return configureModule(b.createModule(createOptions(options)), options);
}

pub fn addModule(b: *std.Build, name: []const u8, options: ModuleOptions) *std.Build.Module {
    return configureModule(b.addModule(name, createOptions(options)), options);
}

fn createOptions(options: ModuleOptions) std.Build.Module.CreateOptions {
    return .{
        .root_source_file = options.root_source_file,
        .target = options.target,
        .optimize = options.optimize,
        .link_libc = options.link_libc,
        .single_threaded = options.single_threaded,
    };
}

pub fn addFilesystemCapacitySource(
    module: *std.Build.Module,
    source_file: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
) void {
    if (!filesystemCapacitySupported(target)) return;
    module.addCSourceFile(.{
        .file = source_file,
        .flags = &.{"-std=c11"},
    });
}

fn filesystemCapacitySupported(target: std.Build.ResolvedTarget) bool {
    return switch (target.result.os.tag) {
        .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => true,
        else => false,
    };
}

fn configureModule(module: *std.Build.Module, options: ModuleOptions) *std.Build.Module {
    if (options.link_libc) {
        addFilesystemCapacitySource(module, options.filesystem_capacity_source_file, options.target);
    }
    return module;
}

/// Register the same unit and process-lifecycle checks in either build graph.
pub fn addTests(b: *std.Build, options: struct {
    root: std.Build.LazyPath,
    name: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    link_libc: bool,
}) *std.Build.Step {
    const target = options.target;
    const optimize = options.optimize;
    const link_libc = options.link_libc;
    const supervisor = b.createModule(.{
        .root_source_file = options.root.path(b, "src/inference_process_supervisor.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    const unit = b.addTest(.{ .root_module = supervisor });
    const test_step = b.step(options.name, "Run supervisor unit and process-lifecycle tests (Python 3 on POSIX)");
    test_step.dependOn(&b.addRunArtifact(unit).step);
    if (target.result.os.tag == .linux or target.result.os.tag == .macos) {
        const fixture = b.addExecutable(.{
            .name = "inference-supervisor-fixture",
            .root_module = b.createModule(.{
                .root_source_file = options.root.path(b, "tests/inference_supervisor_fixture.zig"),
                .target = target,
                .optimize = optimize,
                .link_libc = link_libc,
                .imports = &.{.{ .name = "supervisor", .module = supervisor }},
            }),
        });
        const integration = b.addSystemCommand(&.{"python3"});
        integration.addFileArg(options.root.path(b, "tests/test_inference_supervisor.py"));
        integration.addArtifactArg(fixture);
        test_step.dependOn(&integration.step);
    }
    return test_step;
}

pub fn addMacosSdkPaths(b: *std.Build, module: *std.Build.Module, target: std.Build.ResolvedTarget) void {
    if (target.result.os.tag != .macos) return;
    const sdk_root = b.sysroot orelse
        b.graph.environ_map.get("SDK_PATH") orelse
        std.zig.system.darwin.getSdk(b.allocator, b.graph.io, &target.result) orelse
        return;
    module.addSystemIncludePath(.{ .cwd_relative = b.fmt("{s}/usr/include", .{sdk_root}) });
    module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/usr/lib", .{sdk_root}) });
    module.addFrameworkPath(.{ .cwd_relative = b.fmt("{s}/System/Library/Frameworks", .{sdk_root}) });
}
