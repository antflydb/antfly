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
    jinja,
    ml,
    onnx_graph,
    pjrt,
    protobuf,
    termite_c_file,
    termite_finetune_data,
    termite_finetune_tokenizer_batch,
    termite_hf_tokenizer,
    termite_internal,
    termite_io_compat,
    termite_linalg,
    termite_tokenizer,
};

pub const NativeLink = enum {
    none,
    default,
    no_accel,
};

pub const Context = struct {
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    build_options_mod: *std.Build.Module,
    jinja_mod: *std.Build.Module,
    ml_mod: *std.Build.Module,
    onnx_graph_mod: *std.Build.Module,
    termite_internal_mod: *std.Build.Module,
    termite_tokenizer_mod: *std.Build.Module,
    termite_hf_tokenizer_mod: *std.Build.Module,
    antfly_image_mod: *std.Build.Module,
    pjrt_mod: *std.Build.Module,
    protobuf_mod: *std.Build.Module,
    termite_linalg_mod: *std.Build.Module,
    antfly_platform_mod: *std.Build.Module,
    enable_system_blas: bool,
    blas_root: ?[]const u8,
    enable_mlx: bool,
    mlx_root: ?[]const u8,
    enable_metal: bool,

    pub fn moduleFor(ctx: Context, import: Import) *std.Build.Module {
        return switch (import) {
            .antfly_image => ctx.antfly_image_mod,
            .antfly_platform => ctx.antfly_platform_mod,
            .build_options => ctx.build_options_mod,
            .jinja => ctx.jinja_mod,
            .ml => ctx.ml_mod,
            .onnx_graph => ctx.onnx_graph_mod,
            .pjrt => ctx.pjrt_mod,
            .protobuf => ctx.protobuf_mod,
            .termite_c_file => ctx.b.createModule(.{
                .root_source_file = ctx.b.path("src/util/c_file.zig"),
                .target = ctx.target,
                .optimize = ctx.optimize,
            }),
            // These roots intentionally live directly under src/. Their
            // transitive imports need src as the Zig module boundary.
            .termite_finetune_data => ctx.b.createModule(.{
                .root_source_file = ctx.b.path("src/finetune_data_root.zig"),
                .target = ctx.target,
                .optimize = ctx.optimize,
            }),
            .termite_finetune_tokenizer_batch => blk: {
                const mod = ctx.b.createModule(.{
                    .root_source_file = ctx.b.path("src/finetune_tokenizer_batch_root.zig"),
                    .target = ctx.target,
                    .optimize = ctx.optimize,
                });
                mod.addImport("termite_tokenizer", ctx.termite_tokenizer_mod);
                mod.addImport("termite_hf_tokenizer", ctx.termite_hf_tokenizer_mod);
                break :blk mod;
            },
            .termite_hf_tokenizer => ctx.termite_hf_tokenizer_mod,
            .termite_internal => ctx.termite_internal_mod,
            .termite_io_compat => ctx.b.createModule(.{
                .root_source_file = ctx.b.path("src/io/compat.zig"),
                .target = ctx.target,
                .optimize = ctx.optimize,
            }),
            .termite_linalg => ctx.termite_linalg_mod,
            .termite_tokenizer => ctx.termite_tokenizer_mod,
        };
    }
};

pub const CommandSpec = struct {
    name: []const u8,
    root_source_file: []const u8,
    description: []const u8,
    imports: []const Import = &.{},
    native_link: NativeLink = .none,
    link_libc: bool = false,
};

pub const TestSpec = struct {
    step_name: []const u8,
    root_source_file: []const u8,
    description: []const u8,
    imports: []const Import = &.{},
    native_link: NativeLink = .none,
};

pub fn addCommand(ctx: Context, spec: CommandSpec) void {
    const b = ctx.b;
    const exe = b.addExecutable(.{
        .name = spec.name,
        .root_module = b.createModule(.{
            .root_source_file = b.path(spec.root_source_file),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    addImports(ctx, exe.root_module, spec.imports);
    configureNative(ctx, exe, spec.native_link);
    if (spec.link_libc) exe.root_module.link_libc = true;

    const run = b.addRunArtifact(exe);
    if (b.args) |args| run.addArgs(args);
    const step = b.step(spec.name, spec.description);
    step.dependOn(&run.step);
}

pub fn addTest(ctx: Context, spec: TestSpec) *std.Build.Step {
    const b = ctx.b;
    const test_exe = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(spec.root_source_file),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    addImports(ctx, test_exe.root_module, spec.imports);
    configureNative(ctx, test_exe, spec.native_link);

    const run = b.addRunArtifact(test_exe);
    const step = b.step(spec.step_name, spec.description);
    step.dependOn(&run.step);
    return step;
}

fn addImports(ctx: Context, module: *std.Build.Module, imports: []const Import) void {
    for (imports) |import| {
        module.addImport(@tagName(import), ctx.moduleFor(import));
    }
}

fn configureNative(ctx: Context, artifact: *std.Build.Step.Compile, native_link: NativeLink) void {
    switch (native_link) {
        .none => {},
        .default => configureNativeTool(ctx, artifact, ctx.enable_mlx, ctx.enable_metal),
        .no_accel => configureNativeTool(ctx, artifact, false, false),
    }
}

fn configureNativeTool(ctx: Context, artifact: *std.Build.Step.Compile, enable_mlx: bool, enable_metal: bool) void {
    if (ctx.enable_system_blas) {
        configureSystemBlas(ctx, artifact.root_module);
    }
    configureMetal(ctx, artifact.root_module, enable_metal);
    configureMlx(ctx, artifact.root_module, enable_mlx);
    artifact.root_module.link_libc = true;
}

fn configureSystemBlas(ctx: Context, module: *std.Build.Module) void {
    if (ctx.target.result.os.tag == .macos) {
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

fn configureMetal(ctx: Context, module: *std.Build.Module, enable_metal: bool) void {
    if (!enable_metal or ctx.target.result.os.tag != .macos) return;
    module.linkFramework("Foundation", .{});
    module.linkFramework("Metal", .{});
    module.linkFramework("MetalPerformanceShaders", .{});
    module.linkFramework("MetalPerformanceShadersGraph", .{});
    module.addCSourceFile(.{ .file = ctx.b.path("src/backends/metal_kernels.m"), .flags = &.{"-fobjc-arc"} });
}

fn configureMlx(ctx: Context, module: *std.Build.Module, enable_mlx: bool) void {
    if (!enable_mlx or ctx.target.result.os.tag != .macos) return;
    if (ctx.mlx_root) |root| {
        module.addLibraryPath(.{ .cwd_relative = ctx.b.fmt("{s}/lib", .{root}) });
        module.addRPath(.{ .cwd_relative = ctx.b.fmt("{s}/lib", .{root}) });
    }
    module.linkSystemLibrary("mlxc", .{});
}
