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
    if (options.target.result.os.tag == .windows) {
        // Independent runtime archives cannot propagate inferred extern-library
        // dependencies to their final executable. Bind platform imports here.
        module.linkSystemLibrary("bcrypt", .{});
        module.linkSystemLibrary("ws2_32", .{});
    }
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
    const clock_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = options.root.path(b, "src/time.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    }) });
    const run_clocks = b.addRunArtifact(clock_tests);
    // Always compile the Linux syscall variant, including on macOS hosts.
    // This catches an accidental libc dependency without a cross-runner.
    const syscall_clocks = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = options.root.path(b, "src/time.zig"),
        .target = b.resolveTargetQuery(.{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .gnu }),
        .optimize = optimize,
        .link_libc = false,
    }) });
    const io_platform = createModule(b, .{
        .root_source_file = options.root.path(b, "src/root.zig"),
        .filesystem_capacity_source_file = options.root.path(b, "src/filesystem_capacity.c"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    const io_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = options.root.path(b, "tests/io_namespace_test.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
        .imports = &.{.{ .name = "antfly_platform", .module = io_platform }},
    }) });
    const run_io_tests = b.addRunArtifact(io_tests);
    const run_unit = b.addRunArtifact(unit);
    run_unit.step.dependOn(&run_io_tests.step);
    run_unit.step.dependOn(&run_clocks.step);
    run_unit.step.dependOn(&syscall_clocks.step);
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
    const sdk_root = macosSdkRoot(b, target) orelse return;
    module.addSystemIncludePath(sdk_root.path(b, "usr/include"));
    module.addLibraryPath(sdk_root.path(b, "usr/lib"));
    module.addFrameworkPath(sdk_root.path(b, "System/Library/Frameworks"));
}

fn macosSdkRoot(b: *std.Build, target: std.Build.ResolvedTarget) ?std.Build.LazyPath {
    const key = "antfly_macos_sdk_root";
    if (b.named_lazy_paths.get(key)) |root| return root;
    // Declare once even when discovery fails and another owner retries it.
    const explicit = if (!b.available_options_map.contains("macos-sdk"))
        b.option([]const u8, "macos-sdk", "Explicit macOS SDK root (otherwise SDK_PATH or xcrun)")
    else
        null;
    const sdk_root = explicit orelse b.graph.environ_map.get("SDK_PATH") orelse sdk: {
        // Automatic selection depends on Xcode's external configuration. Keep
        // rediscovering it; explicit SDK inputs can reuse configure results.
        b.graph.poisonCache();
        break :sdk std.zig.system.darwin.getSdk(b.allocator, b.graph.io, &target.result) orelse return null;
    };
    const root = b.graph.cwdRelativePath(sdk_root);
    b.dependOnDirectoryMetadata(root);
    b.addNamedLazyPath(key, root);
    return root;
}

/// Pass the selected SDK to the compiler and translator, rather than adding
/// fallback search paths behind Zig's automatically discovered libc.
pub fn macosSdkLibCFile(b: *std.Build, target: std.Build.ResolvedTarget) ?std.Build.LazyPath {
    if (target.result.os.tag != .macos) return null;
    const key = "antfly_macos_sdk_libc";
    if (b.named_lazy_paths.get(key)) |file| return file;
    const root = macosSdkRoot(b, target) orelse return null;
    const sdk = std.Io.Dir.cwd().realPathFileAlloc(b.graph.io, root.relative.sub_path, b.allocator) catch |err|
        std.debug.panic("cannot resolve macOS SDK: {t}", .{err});
    const file = b.addWriteFiles().add("macos-sdk-libc.txt", b.fmt(
        "include_dir={s}/usr/include\nsys_include_dir={s}/usr/include\ncrt_dir={s}/usr/lib\ncc_dir=\nmsvc_lib_dir=\nkernel32_lib_dir=\ndarwin_sdk_dir={s}\n",
        .{ sdk, sdk, sdk, sdk },
    ));
    b.addNamedLazyPath(key, file);
    return file;
}

/// Configure every macOS artifact after its owners have constructed the graph,
/// including linked libraries and generated host tools.
pub fn finalizeMacosSdk(b: *std.Build) void {
    // Host generators can target macOS even when the product targets Linux or
    // wasm. Select the SDK lazily for those artifacts too, so their libc input
    // does not change with the product target.
    if (!b.named_lazy_paths.contains("antfly_macos_sdk_root") and b.graph.host.result.os.tag != .macos) return;
    var steps: std.AutoHashMap(*std.Build.Step, void) = .init(b.allocator);
    var modules: std.AutoHashMap(*std.Build.Module, void) = .init(b.allocator);
    for (b.top_level_steps.values()) |top| visitSdkStep(b, &top.step, &steps, &modules);
}

fn visitSdkStep(b: *std.Build, step: *std.Build.Step, steps: *std.AutoHashMap(*std.Build.Step, void), modules: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((steps.getOrPut(step) catch @panic("OOM")).found_existing) return;
    // setLibCFile adds a dependency; traverse existing dependencies first.
    for (step.dependencies.items) |dependency| visitSdkStep(b, dependency, steps, modules);
    if (step.cast(std.Build.Step.Compile)) |artifact| {
        if (artifact.root_module.resolved_target) |target| {
            if (target.result.os.tag == .macos and artifact.libc_file == null)
                artifact.setLibCFile(macosSdkLibCFile(b, target));
        }
        visitSdkModule(b, artifact.root_module, steps, modules);
    }
}

fn visitSdkModule(b: *std.Build, module: *std.Build.Module, steps: *std.AutoHashMap(*std.Build.Step, void), modules: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((modules.getOrPut(module) catch @panic("OOM")).found_existing) return;
    if (module.root_source_file) |source| switch (source) {
        .generated => |generated| visitSdkStep(b, b.graph.generated_files.items[@backingInt(generated.index)], steps, modules),
        else => {},
    };
    for (module.link_objects.items) |object| switch (object) {
        .other_step => |artifact| visitSdkStep(b, &artifact.step, steps, modules),
        else => {},
    };
    for (module.import_table.values()) |dependency| visitSdkModule(b, dependency, steps, modules);
}

/// Bind native platform APIs throughout a composed product graph. Borrowed Io
/// types remain std.Io; only executor construction and native primitives use
/// this dependency. Host-tool graphs keep their own target and platform.
pub fn bindPlatform(value: anytype, platform: *std.Build.Module) void {
    const T = @TypeOf(value);
    if (T == *std.Build.Module) {
        bindPlatformModule(value, platform);
    } else switch (@typeInfo(T)) {
        .@"struct" => |info| inline for (info.field_names) |name| {
            bindPlatform(@field(value, name), platform);
        },
        .optional => if (value) |present| bindPlatform(present, platform),
        else => {},
    }
}

fn bindPlatformModule(root: *std.Build.Module, platform: *std.Build.Module) void {
    if (root == platform) return;
    if (root.resolved_target) |target| {
        const expected = platform.resolved_target.?.result;
        if (target.result.os.tag != expected.os.tag or
            target.result.cpu.arch != expected.cpu.arch or target.result.abi != expected.abi) return;
    }
    const arena = root.owner.graph.arena;
    var seen: std.AutoHashMap(*std.Build.Module, void) = .init(arena);
    var pending: std.ArrayList(*std.Build.Module) = .empty;
    pending.append(arena, root) catch @panic("OOM");
    var index: usize = 0;
    while (index < pending.items.len) : (index += 1) {
        const module = pending.items[index];
        if (module == platform) continue;
        if (module.resolved_target) |target| {
            const expected = platform.resolved_target.?.result;
            if (target.result.os.tag != expected.os.tag or
                target.result.cpu.arch != expected.cpu.arch or target.result.abi != expected.abi) continue;
        }
        const entry = seen.getOrPut(module) catch @panic("OOM");
        if (entry.found_existing) continue;
        // Do not override an explicitly configured dependency.
        if (!module.import_table.contains("antfly_platform")) {
            module.addImport("antfly_platform", platform);
        }
        module.cached_graph = .{ .modules = &.{}, .names = &.{} };
        for (module.import_table.values()) |dependency| {
            if (dependency != platform) pending.append(arena, dependency) catch @panic("OOM");
        }
    }
}
