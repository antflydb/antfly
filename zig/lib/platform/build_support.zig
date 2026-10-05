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
    optimize: std.lang.Optimize,
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
    optimize: std.lang.Optimize,
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
    const atomic_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = options.root.path(b, "src/atomic.zig"),
        .target = target,
        .optimize = optimize,
    }) });
    const run_atomic_tests = b.addRunArtifact(atomic_tests);
    const entropy_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = options.root.path(b, "src/entropy.zig"),
        .target = target,
        .optimize = optimize,
    }) });
    const run_entropy_tests = b.addRunArtifact(entropy_tests);
    const run_unit = b.addRunArtifact(unit);
    run_unit.step.dependOn(&run_atomic_tests.step);
    run_unit.step.dependOn(&run_entropy_tests.step);
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
        .unit = run_unit,
        .process = process,
        .one_shot_unit = b.addRunArtifact(one_shot_unit),
        .one_shot_process = one_shot_process,
    };
}

/// Fixtures that spawn target executables directly require a native executor.
/// Ordinary unit tests retain std.Build.addRunArtifact emulator support.
pub fn canRunNativeProcess(b: *std.Build, fixture: *std.Build.Step.Compile) bool {
    const target = fixture.root_module.resolved_target.?.result;
    // Static libc does not require the target's dynamic linker to be installed
    // on the host. Zig defaults musl executables to static linkage.
    const dynamic_libc = (fixture.root_module.link_libc orelse false) and
        fixture.linkage != .static and (!target.isMuslLibC() or fixture.linkage == .dynamic);
    const executor = std.zig.system.getExternalExecutor(b.graph.io, &target, .{
        .link_libc = dynamic_libc,
        .link_mode = fixture.linkage orelse if (target.isMuslLibC()) .static else .dynamic,
        .host_cpu_arch = b.graph.host.result.cpu.arch,
        .host_os_tag = b.graph.host.result.os.tag,
    });
    return executor == .native;
}

pub fn addNativeProcessTest(b: *std.Build, fixture: *std.Build.Step.Compile, script: std.Build.LazyPath) *std.Build.Step {
    if (canRunNativeProcess(b, fixture)) {
        const run = b.addSystemCommand(&.{"python3"});
        run.addFileArg2(script, .{ .make_absolute = true });
        run.addArtifactArg2(fixture, .{ .make_absolute = true });
        return &run.step;
    }
    const skipped = b.step(b.fmt("skip {s} process checks (requires a native host target)", .{fixture.name}), "Requires a native executor");
    skipped.dependOn(&fixture.step);
    return skipped;
}

pub fn addMacosSdkPaths(b: *std.Build, module: *std.Build.Module, target: std.Build.ResolvedTarget) void {
    if (target.result.os.tag != .macos) return;
    const sdk_root = b.graph.environ_map.get("SDK_PATH") orelse sdk: {
        // xcrun observes the selected Xcode installation outside configure inputs.
        b.graph.poisonCache();
        break :sdk std.zig.system.darwin.getSdk(b.allocator, b.graph.io, &target.result) orelse return;
    };
    module.addSystemIncludePath(b.graph.cwdRelativePath(b.fmt("{s}/usr/include", .{sdk_root})));
    module.addLibraryPath(b.graph.cwdRelativePath(b.fmt("{s}/usr/lib", .{sdk_root})));
    module.addFrameworkPath(b.graph.cwdRelativePath(b.fmt("{s}/System/Library/Frameworks", .{sdk_root})));
}
