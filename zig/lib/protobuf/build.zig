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

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const protobuf_mod = b.addModule("protobuf", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // protoc-zig: build-time code generator. Downstream build.zig files run
    // this artifact with `addRunArtifact`, so it must be compiled for the host
    // even when the protobuf runtime module is being cross-compiled.
    //
    // The root source is a thin wrapper at src/ level (see src/codegen_main.zig)
    // so the executable's module root is src/, letting src/codegen/*.zig
    // reach src/descriptor.zig through `../` imports.
    const codegen_exe = b.addExecutable(.{
        .name = "protoc-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/codegen_main.zig"),
            .target = b.graph.host,
            .optimize = optimize,
        }),
    });
    b.installArtifact(codegen_exe);

    const test_step = b.step("test", "Run unit tests");

    const wire_test = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/wire.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(wire_test).step);

    const message_test = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/message.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(message_test).step);

    const descriptor_test = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/descriptor.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(descriptor_test).step);

    // codegen tests use a src/-level aggregator so src/codegen/*.zig can
    // import ../descriptor.zig and ../message.zig without leaving the module.
    const codegen_test = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tests_codegen.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(codegen_test).step);

    // End-to-end smoke test: run protoc-zig against our testdata descriptor,
    // treat the output as a Zig module, and compile/round-trip a test file
    // against the generated types. This is the only way to catch bugs that
    // only show up when the emitted source hits the real Zig compiler +
    // protobuf runtime together (e.g. cross-file imports, enum defaults,
    // `_pb_field_map` shape mismatches).
    const gen_run = b.addRunArtifact(codegen_exe);
    gen_run.addArg("--desc");
    gen_run.addFileArg(b.path("src/testdata/quantize.desc"));
    gen_run.addArg("--output");
    const gen_dir = gen_run.addOutputDirectoryArg("generated");

    const generated_mod = b.createModule(.{
        .root_source_file = gen_dir.path(b, "root.zig"),
        .target = target,
        .optimize = optimize,
    });
    generated_mod.addImport("protobuf", protobuf_mod);

    const smoke_mod = b.createModule(.{
        .root_source_file = b.path("src/tests_generated_smoke.zig"),
        .target = target,
        .optimize = optimize,
    });
    smoke_mod.addImport("generated", generated_mod);

    const smoke_test = b.addTest(.{ .root_module = smoke_mod });
    test_step.dependOn(&b.addRunArtifact(smoke_test).step);
}

/// Build helper: compile a binary `.desc` file into a Zig module.
///
/// Downstream `build.zig` usage (after adding protobuf as a dependency):
///
/// ```
/// const protobuf_dep = b.dependency("protobuf", .{ .target = target, .optimize = optimize });
/// const onnx_mod = @import("protobuf").addProtoModule(b, protobuf_dep, b.path("proto/onnx.desc"), "onnx_proto");
/// exe.root_module.addImport("onnx_proto", onnx_mod);
/// ```
///
/// Produces a module whose root is `<gen_dir>/root.zig`, which re-exports one
/// Zig submodule per proto package. Cross-file type references are wired up
/// via `@import` with package-based file names.
pub fn addProtoModule(
    b: *std.Build,
    protobuf_dep: *std.Build.Dependency,
    desc_file: std.Build.LazyPath,
    module_name: []const u8,
    extra_args: []const []const u8,
) *std.Build.Module {
    const codegen = b.addRunArtifact(protobuf_dep.artifact("protoc-zig"));
    codegen.addArg("--desc");
    codegen.addFileArg(desc_file);
    codegen.addArg("--output");
    const gen_dir = codegen.addOutputDirectoryArg(module_name);
    for (extra_args) |arg| codegen.addArg(arg);

    const mod = b.addModule(module_name, .{
        .root_source_file = gen_dir.path(b, "root.zig"),
    });
    mod.addImport("protobuf", protobuf_dep.module("protobuf"));
    return mod;
}
