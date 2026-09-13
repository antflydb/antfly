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
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    link_libc: bool,
}) struct {
    unit: *std.Build.Step.Run,
    process: ?*std.Build.Step,
    one_shot_unit: *std.Build.Step.Run,
    one_shot_process: ?*std.Build.Step,
} {
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
    const one_shot = b.createModule(.{
        .root_source_file = options.root.path(b, "src/one_shot_process.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    const one_shot_unit = b.addTest(.{ .root_module = one_shot });
    var process: ?*std.Build.Step = null;
    var one_shot_process: ?*std.Build.Step = null;
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
        process = addNativeProcessTest(b, fixture, options.root.path(b, "tests/test_inference_supervisor.py"));
        const platform = createModule(b, .{
            .root_source_file = options.root.path(b, "src/root.zig"),
            .filesystem_capacity_source_file = options.root.path(b, "src/filesystem_capacity.c"),
            .target = target,
            .optimize = optimize,
            .link_libc = link_libc,
        });
        const one_shot_fixture = b.addExecutable(.{
            .name = "one-shot-process-fixture",
            .root_module = b.createModule(.{
                .root_source_file = options.root.path(b, "tests/one_shot_process_fixture.zig"),
                .target = target,
                .optimize = optimize,
                .link_libc = link_libc,
                .imports = &.{.{ .name = "platform", .module = platform }},
            }),
        });
        one_shot_process = addNativeProcessTest(b, one_shot_fixture, options.root.path(b, "tests/test_one_shot_process.py"));
    }
    return .{
        .unit = b.addRunArtifact(unit),
        .process = process,
        .one_shot_unit = b.addRunArtifact(one_shot_unit),
        .one_shot_process = one_shot_process,
    };
}

/// Python owns these POSIX process groups and signals. Compile foreign fixtures,
/// but leave emulation of ordinary unit tests to std.Build.addRunArtifact.
pub fn addNativeProcessTest(b: *std.Build, fixture: *std.Build.Step.Compile, script: std.Build.LazyPath) *std.Build.Step {
    const target = fixture.root_module.resolved_target.?.result;
    // Static libc does not require the target's dynamic linker to be installed
    // on the host. Zig defaults musl executables to static linkage.
    const dynamic_libc = (fixture.root_module.link_libc orelse false) and
        fixture.linkage != .static and (!target.isMuslLibC() or fixture.linkage == .dynamic);
    const executor = std.zig.system.getExternalExecutor(b.graph.io, &b.graph.host.result, &target, .{
        .link_libc = dynamic_libc,
        .allow_rosetta = false,
        .allow_qemu = false,
        .allow_wine = false,
        .allow_wasmtime = false,
        .allow_darling = false,
    });
    if (executor == .native) {
        const run = b.addSystemCommand(&.{"python3"});
        run.addFileArg(script);
        run.addArtifactArg(fixture);
        return &run.step;
    }
    const skipped = b.allocator.create(std.Build.Step) catch @panic("OOM");
    skipped.* = std.Build.Step.init(.{
        .id = .custom,
        .name = b.fmt("skip {s} process checks (requires a native host target)", .{fixture.name}),
        .owner = b,
        .makeFn = struct {
            fn make(_: *std.Build.Step, _: std.Build.Step.MakeOptions) !void {
                return error.MakeSkipped;
            }
        }.make,
    });
    skipped.dependOn(&fixture.step);
    return skipped;
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
