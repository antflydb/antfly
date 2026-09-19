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

pub const Import = enum {
    antfly_image,
    antfly_platform,
    build_options,
    build_info,
    jinja,
    ml,
    onnx_graph,
    pjrt,
    protobuf,
    termite_c_file,
    inference_finetune_data,
    inference_finetune_tokenizer_batch,
    inference_hf_tokenizer,
    inference_internal,
    termite_io_compat,
    inference_linalg,
    inference_tokenizer,
};

pub const NativeLink = enum {
    none,
    default,
    no_accel,
};

pub const Context = struct {
    b: *std.Build,
    root: ?std.Build.LazyPath = null,
    args: ?[]const []const u8 = null,
    publish_targets: bool = true,
    // CPU finetune roots measured under 3 GiB. Accelerator configurations keep
    // the previous conservative aggregate allowance until measured separately.
    test_compile_max_rss: usize = 7 * 1024 * 1024 * 1024,

    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    build_info_mod: *std.Build.Module,
    build_info_object: *std.Build.Step.Compile,
    identities: @import("../jit_identity.zig").Modules,
    build_options_mod: *std.Build.Module,
    jinja_mod: *std.Build.Module,
    ml_mod: *std.Build.Module,
    onnx: @import("../runtime.zig").OnnxModules,
    inference_internal_mod: *std.Build.Module,
    inference_tokenizer_mod: *std.Build.Module,
    inference_hf_tokenizer_mod: *std.Build.Module,
    antfly_image_mod: *std.Build.Module,
    pjrt_mod: ?*std.Build.Module,
    qualification_pjrt_mod: *std.Build.Module,
    protobuf_mod: *std.Build.Module,
    inference_linalg_mod: *std.Build.Module,
    antfly_platform_mod: *std.Build.Module,
    enable_system_blas: bool,
    blas_root: ?[]const u8,
    enable_metal: bool,

    pub fn path(ctx: Context, sub_path: []const u8) std.Build.LazyPath {
        return (ctx.root orelse ctx.b.path(".")).path(ctx.b, sub_path);
    }

    pub fn moduleFor(ctx: Context, import: Import) *std.Build.Module {
        return switch (import) {
            .antfly_image => ctx.antfly_image_mod,
            .antfly_platform => ctx.antfly_platform_mod,
            .build_options => ctx.build_options_mod,
            .build_info => ctx.build_info_mod,
            .jinja => ctx.jinja_mod,
            .ml => ctx.ml_mod,
            .onnx_graph => ctx.onnx.graph,
            .pjrt => ctx.qualification_pjrt_mod,
            .protobuf => ctx.protobuf_mod,
            .termite_c_file => ctx.b.createModule(.{
                .root_source_file = ctx.path("src/util/c_file.zig"),
                .target = ctx.target,
                .optimize = ctx.optimize,
            }),
            // These roots intentionally live directly under src/. Their
            // transitive imports need src as the Zig module boundary.
            .inference_finetune_data => ctx.b.createModule(.{
                .root_source_file = ctx.path("src/finetune_data_root.zig"),
                .target = ctx.target,
                .optimize = ctx.optimize,
            }),
            .inference_finetune_tokenizer_batch => blk: {
                const mod = ctx.b.createModule(.{
                    .root_source_file = ctx.path("src/finetune_tokenizer_batch_root.zig"),
                    .target = ctx.target,
                    .optimize = ctx.optimize,
                });
                mod.addImport("inference_tokenizer", ctx.inference_tokenizer_mod);
                mod.addImport("inference_hf_tokenizer", ctx.inference_hf_tokenizer_mod);
                break :blk mod;
            },
            .inference_hf_tokenizer => ctx.inference_hf_tokenizer_mod,
            .inference_internal => ctx.inference_internal_mod,
            .termite_io_compat => ctx.b.createModule(.{
                .root_source_file = ctx.path("src/io/compat.zig"),
                .target = ctx.target,
                .optimize = ctx.optimize,
            }),
            .inference_linalg => ctx.inference_linalg_mod,
            .inference_tokenizer => ctx.inference_tokenizer_mod,
        };
    }
};

pub const CommandSpec = struct {
    name: []const u8,
    root_source_file: []const u8,
    description: []const u8,
    imports: []const Import = &.{},
    assets: ?@import("assets.zig").Owner = null,
    native_link: NativeLink = .none,
    link_libc: bool = false,
    /// This command writes a release version to its output or training manifest.
    release_metadata: bool = false,
    // Entrypoints that import another CLI through a relative source path must
    // retain one module boundary (Zig rejects the file in two modules).
    shared_check: bool = true,
};

pub const TestSpec = struct {
    /// This import-only root is completely covered by the full inference unit gate.
    covered_by_inference: bool = false,
    step_name: []const u8,
    root_source_file: []const u8,
    description: []const u8,
    imports: []const Import = &.{},
    native_link: NativeLink = .none,
    filters: []const []const u8 = &.{},
    /// Runtime family selection when a focused target includes imported tests.
    focused_filters: []const []const u8 = &.{},
};

pub const Command = struct {
    executable: *std.Build.Step.Compile,
    run: *std.Build.Step.Run,
};

/// Return the actual artifacts so entrypoints can compose compile checks from
/// the same registry that publishes commands, without running model workloads.
pub fn addCommands(ctx: Context, specs: []const CommandSpec) []const Command {
    const commands = ctx.b.allocator.alloc(Command, specs.len) catch @panic("OOM");
    for (specs, commands) |spec, *command| command.* = addCommand(ctx, spec);
    return commands;
}

pub fn addCommand(ctx: Context, spec: CommandSpec) Command {
    const b = ctx.b;
    const exe = b.addExecutable(.{
        .name = spec.name,
        .root_module = b.createModule(.{
            .root_source_file = ctx.path(spec.root_source_file),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    configureCommand(ctx, spec, exe);

    const run = b.addRunArtifact(exe);
    run.setCwd(ctx.root orelse b.path("."));
    if (ctx.args orelse b.args) |args| run.addArgs(args);
    if (ctx.publish_targets) {
        const step = b.step(spec.name, spec.description);
        step.dependOn(&run.step);
    }
    return .{ .executable = exe, .run = run };
}

fn configureCommand(ctx: Context, spec: CommandSpec, exe: *std.Build.Step.Compile) void {
    const b = ctx.b;
    addImports(ctx, exe.root_module, spec.imports, ctx.pjrt_mod);
    if (spec.assets) |owner| exe.root_module.addImport("inference_finetune_assets", @import("assets.zig").create(.{
        .b = b,
        .root = ctx.root orelse b.path("."),
        .target = ctx.target,
        .optimize = ctx.optimize,
        .owner = owner,
        .onnx_data = ctx.onnx.data,
        .jinja = ctx.jinja_mod,
        .platform = ctx.antfly_platform_mod,
    }));
    configureNative(ctx, exe, spec.native_link, spec.imports);
    if (spec.release_metadata) {
        exe.root_module.addImport("build_info", ctx.build_info_mod);
        exe.root_module.addObject(ctx.build_info_object);
    }
    if (spec.link_libc) exe.root_module.link_libc = true;
}

fn sameCommandConfiguration(a: CommandSpec, b: CommandSpec) bool {
    // inference_internal owns the Metal translation unit when imported; otherwise
    // configureNative supplies it at the executable root. Keep that ownership.
    return a.shared_check and b.shared_check and a.assets == b.assets and a.native_link == b.native_link and
        a.link_libc == b.link_libc and a.release_metadata == b.release_metadata and
        containsImport(a.imports, .inference_internal) == containsImport(b.imports, .inference_internal);
}

/// Check every registered entrypoint without recompiling its shared inference
/// implementation for every CLI. Compatible link profiles share a compilation;
/// each command module still receives only its declared imports.
/// Individual command artifacts and run targets remain independently buildable.
pub fn addCommandChecks(ctx: Context, specs: []const CommandSpec) *std.Build.Step {
    const b = ctx.b;
    const step = b.step(if (ctx.publish_targets) "test-finetune-command-check" else "inference-finetune-command-check", "Compile and link all finetuning CLI entrypoints in shared configuration groups");
    const assigned = b.allocator.alloc(bool, specs.len) catch @panic("OOM");
    @memset(assigned, false);
    var group_index: usize = 0;
    for (specs, 0..) |spec, first| {
        if (assigned[first]) continue;
        var names: std.ArrayList([]const u8) = .empty;
        var commands: std.ArrayList(CommandSpec) = .empty;
        for (specs, 0..) |candidate, index| {
            if (assigned[index] or (index != first and !sameCommandConfiguration(spec, candidate))) continue;
            std.debug.assert(std.mem.startsWith(u8, candidate.root_source_file, "src/"));
            names.append(b.allocator, candidate.name) catch @panic("OOM");
            commands.append(b.allocator, candidate) catch @panic("OOM");
            assigned[index] = true;
        }
        const generated = b.addWriteFiles();
        var source: std.Io.Writer.Allocating = .init(b.allocator);
        source.writer.writeAll(@embedFile("command_check_preamble.zig.txt")) catch @panic("OOM");
        for (names.items, 0..) |name, index| {
            source.writer.print("    if (std.mem.eql(u8, name, \"{s}\")) return @import(\"command_{d}\").main(init);\n", .{ name, index }) catch @panic("OOM");
        }
        source.writer.writeAll("    return error.CompileCheckOnly;\n}\n") catch @panic("OOM");
        const check = b.addExecutable(.{
            .name = b.fmt("finetune-command-check-{d}", .{group_index}),
            .max_rss = ctx.test_compile_max_rss,
            .root_module = b.createModule(.{
                .root_source_file = generated.add("check.zig", source.written()),
                .target = ctx.target,
                .optimize = ctx.optimize,
            }),
        });
        configureCommand(ctx, spec, check);
        // Share each named dependency within the group, while retaining the
        // original source boundary for each command module.
        for (commands.items, 0..) |command, index| {
            const module = b.createModule(.{
                .root_source_file = ctx.path(command.root_source_file),
                .target = ctx.target,
                .optimize = ctx.optimize,
            });
            // Commands keep their declared imports even when their link-compatible
            // checks share an executable. A union of imports would mask missing
            // dependency declarations in standalone commands.
            for (command.imports) |dependency| {
                const name = @tagName(dependency);
                if (!check.root_module.import_table.contains(name))
                    addImports(ctx, check.root_module, &.{dependency}, ctx.qualification_pjrt_mod);
                if (check.root_module.import_table.get(name)) |shared|
                    module.addImport(name, shared);
                if (dependency == .onnx_graph) module.addImport("onnx_data", ctx.onnx.data);
            }
            if (command.native_link != .none) ctx.identities.addImports(module);
            if (command.assets != null)
                module.addImport("inference_finetune_assets", check.root_module.import_table.get("inference_finetune_assets").?);
            if (command.release_metadata)
                module.addImport("build_info", ctx.build_info_mod);
            check.root_module.addImport(b.fmt("command_{d}", .{index}), module);
        }
        _ = check.getEmittedBin();
        step.dependOn(&check.step);
        group_index += 1;
    }
    return step;
}

/// One source boundary and compile artifact for ordinary finetuning tests.
/// Reuse inference's dependency identities so implementations have one owner.
pub fn sharedTests(ctx: Context, specs: []const TestSpec) *std.Build.Step.Compile {
    const b = ctx.b;
    const root = b.createModule(.{
        .root_source_file = ctx.path("src/finetune_test_root.zig"),
        .target = ctx.target,
        .optimize = ctx.optimize,
    });
    const exe = b.addTest(.{
        .name = "finetune-tests",
        .max_rss = ctx.test_compile_max_rss,
        .root_module = root,
        .test_runner = .{ .path = ctx.path("src/test_runner_filter.zig"), .mode = .simple },
    });
    root.addImport("antfly_platform", ctx.antfly_platform_mod);
    for (specs) |spec| {
        if (spec.covered_by_inference) continue;
        std.debug.assert(spec.filters.len == 0);
        std.debug.assert(spec.native_link != .no_accel);
        for (spec.imports) |dependency| {
            if (!root.import_table.contains(@tagName(dependency)))
                addImports(ctx, root, &.{dependency}, ctx.qualification_pjrt_mod);
        }
    }
    configureNative(ctx, exe, .default, &.{.inference_internal});
    return exe;
}

pub fn addTest(ctx: Context, spec: TestSpec) *std.Build.Step {
    const b = ctx.b;
    const test_exe = b.addTest(.{
        .max_rss = ctx.test_compile_max_rss,
        .root_module = b.createModule(.{
            .root_source_file = ctx.path(spec.root_source_file),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
        .filters = spec.filters,
        // The build-protocol runner executes each test in a fresh process.
        // Several trainer modules own process-scoped backend state and are
        // intentionally validated with the inference runner's explicit
        // per-test std.Io/allocator lifecycle instead.
        .test_runner = .{
            .path = ctx.path("src/test_runner_filter.zig"),
            .mode = .simple,
        },
    });
    addImports(ctx, test_exe.root_module, spec.imports, ctx.qualification_pjrt_mod);
    if (!containsImport(spec.imports, .antfly_platform))
        test_exe.root_module.addImport("antfly_platform", ctx.antfly_platform_mod);
    configureNative(ctx, test_exe, spec.native_link, spec.imports);

    const run = b.addRunArtifact(test_exe);
    run.setCwd(ctx.root orelse b.path("."));
    if (!ctx.publish_targets) return &run.step;
    const step = b.step(spec.step_name, spec.description);
    step.dependOn(&run.step);
    return step;
}

fn addImports(ctx: Context, module: *std.Build.Module, imports: []const Import, pjrt: ?*std.Build.Module) void {
    for (imports) |import| {
        const dependency = if (import == .pjrt) (pjrt orelse continue) else ctx.moduleFor(import);
        module.addImport(@tagName(import), dependency);
        if (import == .onnx_graph) module.addImport("onnx_data", ctx.onnx.data);
    }
}

fn configureNative(
    ctx: Context,
    artifact: *std.Build.Step.Compile,
    native_link: NativeLink,
    imports: []const Import,
) void {
    if (native_link != .none) ctx.identities.addImports(artifact.root_module);
    // inference_internal already owns the Metal translation unit. Adding it to
    // the importing executable as well makes Zig pass the same object to the
    // linker twice. Keep frameworks on the final artifact, but compile the
    // kernels at exactly one module boundary.
    const owns_metal_kernels = !containsImport(imports, .inference_internal);
    switch (native_link) {
        .none => {},
        .default => configureNativeTool(ctx, artifact, ctx.enable_metal, owns_metal_kernels),
        .no_accel => configureNativeTool(ctx, artifact, false, false),
    }
}

fn containsImport(imports: []const Import, needle: Import) bool {
    for (imports) |import| {
        if (import == needle) return true;
    }
    return false;
}

fn configureNativeTool(
    ctx: Context,
    artifact: *std.Build.Step.Compile,
    enable_metal: bool,
    owns_metal_kernels: bool,
) void {
    if (ctx.enable_system_blas) {
        configureSystemBlas(ctx, artifact.root_module);
    }
    configureMetal(ctx, artifact.root_module, enable_metal, owns_metal_kernels);
    artifact.root_module.link_libc = true;
}

fn configureSystemBlas(ctx: Context, module: *std.Build.Module) void {
    if (ctx.target.result.os.tag == .macos) {
        addMacosSdkPaths(ctx, module);
        module.linkFramework("Accelerate", .{});
        return;
    }
    if (ctx.blas_root) |root| {
        module.addIncludePath(.{ .cwd_relative = ctx.b.fmt("{s}/include", .{root}) });
        module.addLibraryPath(.{ .cwd_relative = ctx.b.fmt("{s}/lib", .{root}) });
        module.addRPath(.{ .cwd_relative = ctx.b.fmt("{s}/lib", .{root}) });
    }
    module.linkSystemLibrary("openblas", .{});
}

fn configureMetal(
    ctx: Context,
    module: *std.Build.Module,
    enable_metal: bool,
    owns_metal_kernels: bool,
) void {
    if (!enable_metal or ctx.target.result.os.tag != .macos) return;
    addMacosSdkPaths(ctx, module);
    module.linkFramework("Foundation", .{});
    module.linkFramework("Metal", .{});
    module.linkFramework("MetalPerformanceShaders", .{});
    if (owns_metal_kernels) {
        module.addCSourceFile(.{ .file = ctx.path("src/backends/metal_kernels.m"), .flags = &.{"-fobjc-arc"} });
    }
}

fn addMacosSdkPaths(ctx: Context, module: *std.Build.Module) void {
    if (ctx.target.result.os.tag != .macos) return;
    const sdk_root = ctx.b.sysroot orelse
        ctx.b.graph.environ_map.get("SDK_PATH") orelse
        std.zig.system.darwin.getSdk(ctx.b.allocator, ctx.b.graph.io, &ctx.target.result) orelse
        return;
    module.addSystemIncludePath(.{ .cwd_relative = ctx.b.fmt("{s}/usr/include", .{sdk_root}) });
    module.addLibraryPath(.{ .cwd_relative = ctx.b.fmt("{s}/usr/lib", .{sdk_root}) });
    module.addFrameworkPath(.{ .cwd_relative = ctx.b.fmt("{s}/System/Library/Frameworks", .{sdk_root}) });
}

pub fn fromWorkflow(ctx: @import("../context.zig").Context) Context {
    const b = ctx.b;
    return Context{
        .root = ctx.path("."),
        .args = ctx.args,
        .b = b,
        .target = ctx.target,
        .optimize = ctx.optimize,
        .build_info_mod = ctx.graph.build_info_mod,
        .build_info_object = ctx.graph.build_info_object,
        .identities = ctx.graph.identities,
        .build_options_mod = ctx.graph.build_options_mod,
        .jinja_mod = ctx.graph.jinja_mod,
        .ml_mod = ctx.graph.ml_mod,
        .onnx = ctx.graph.onnx,
        .inference_internal_mod = ctx.graph.inference_internal_mod,
        .inference_tokenizer_mod = ctx.graph.inference_tokenizer_mod,
        .inference_hf_tokenizer_mod = ctx.graph.inference_hf_tokenizer_mod,
        .antfly_image_mod = ctx.graph.image_mod,
        .pjrt_mod = ctx.graph.pjrt_mod,
        .qualification_pjrt_mod = ctx.graph.qualification_pjrt_mod,
        .protobuf_mod = ctx.graph.protobuf_mod,
        .inference_linalg_mod = ctx.graph.inference_linalg_mod,
        .antfly_platform_mod = ctx.graph.platform_mod,
        .enable_system_blas = ctx.backend.enable_system_blas,
        .blas_root = ctx.backend.blas_root,
        .enable_metal = ctx.backend.enable_metal,
    };
}
