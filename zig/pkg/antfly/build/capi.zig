// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const antfly_tests_build = @import("tests.zig");

pub const CApiSteps = struct {
    capi_lib: *std.Build.Step.Compile,
    lite_capi_lib: *std.Build.Step.Compile,
    install_capi_lib: *std.Build.Step.InstallArtifact,
    install_antfly_capi_lib: *std.Build.Step.InstallArtifact,
    install_capi_header: *std.Build.Step.InstallFile,
    run_lite_capi_smoke: *std.Build.Step.Run,
    run_lite_go_tests: *std.Build.Step.Run,
    run_lite_go_example: *std.Build.Step.Run,
    run_lite_go_retrieval_template: *std.Build.Step.Run,
    run_cabi_packaging_tests: *std.Build.Step.Run,
    run_capi_tests: *std.Build.Step.Run,
};

pub fn addCApiSteps(ctx: anytype) CApiSteps {
    const b = ctx.b;
    const target = ctx.target;
    const optimize = ctx.optimize;
    const strip = ctx.strip;
    const lib_mod = ctx.lib_mod;
    const platform_mod = ctx.platform_mod;
    const structlog_mod = ctx.structlog_mod;
    const reuse_runtime_storage = ctx.reuse_runtime_storage;

    const capi_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/db.zig"),
        .target = target,
        .optimize = optimize,
    });
    capi_mod.addImport("antfly-zig", lib_mod);
    capi_mod.addImport("antfly_platform", platform_mod);
    capi_mod.addImport("structlog", structlog_mod);
    capi_mod.strip = strip;

    const capi_link_mod = if (reuse_runtime_storage) b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/link_anchor.zig"),
        .target = target,
        .optimize = optimize,
        .strip = strip,
    }) else capi_mod;
    const capi_lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "antfly_zig_capi",
        .root_module = capi_link_mod,
    });
    capi_lib.link_gc_sections = true;
    const install_capi_lib = b.addInstallArtifact(capi_lib, .{});

    const lite_capi_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/db.zig"),
        .target = target,
        .optimize = optimize,
    });
    lite_capi_mod.addImport("antfly-zig", lib_mod);
    lite_capi_mod.addImport("antfly_platform", platform_mod);
    lite_capi_mod.addImport("structlog", structlog_mod);
    lite_capi_mod.strip = strip;

    const lite_capi_link_mod = if (reuse_runtime_storage) b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/link_anchor.zig"),
        .target = target,
        .optimize = optimize,
        .strip = strip,
    }) else lite_capi_mod;
    const lite_capi_lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "antfly",
        .root_module = lite_capi_link_mod,
    });
    lite_capi_lib.link_gc_sections = true;
    const install_antfly_capi_lib = b.addInstallArtifact(lite_capi_lib, .{});
    const install_capi_header = b.addInstallFileWithDir(
        b.path("pkg/antfly/include/antfly.h"),
        .header,
        "antfly.h",
    );

    const capi_step = b.step("capi", "Build the Zig C API shared libraries");
    capi_step.dependOn(&install_capi_lib.step);
    capi_step.dependOn(&install_antfly_capi_lib.step);
    capi_step.dependOn(&install_capi_header.step);

    const lite_capi_step = b.step("lite-capi", "Build the libantfly C ABI shared library");
    lite_capi_step.dependOn(&install_antfly_capi_lib.step);
    lite_capi_step.dependOn(&install_capi_header.step);

    const lite_capi_smoke_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    lite_capi_smoke_mod.link_libc = true;
    lite_capi_smoke_mod.addIncludePath(b.path("pkg/antfly/include"));
    lite_capi_smoke_mod.addCSourceFile(.{
        .file = b.path("examples/antfly_lite_c_smoke.c"),
        .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" },
    });
    const lite_capi_smoke = b.addExecutable(.{
        .name = "antfly-lite-c-smoke",
        .root_module = lite_capi_smoke_mod,
    });
    lite_capi_smoke.root_module.linkLibrary(lite_capi_lib);
    const run_lite_capi_smoke = b.addRunArtifact(lite_capi_smoke);

    const run_lite_go_tests = b.addSystemCommand(&.{
        "env",
        "GOWORK=off",
        "go",
        "test",
        "-tags",
        "antflylite_capi",
        "-count=1",
        "./...",
    });
    run_lite_go_tests.setCwd(b.path("../go/pkg/antflylite"));
    run_lite_go_tests.step.dependOn(&install_antfly_capi_lib.step);
    run_lite_go_tests.step.dependOn(&install_capi_header.step);

    const run_lite_go_example = b.addSystemCommand(&.{
        "env",
        "GOWORK=off",
        "go",
        "run",
        ".",
        "--reset",
        "--db",
        "../../zig/.zig-cache/antfly-lite-go-example.aflite",
        "--backup",
        "../../zig/.zig-cache/antfly-lite-go-example.afb",
    });
    run_lite_go_example.setCwd(b.path("../examples/antfly-lite-go"));
    run_lite_go_example.step.dependOn(&install_antfly_capi_lib.step);
    run_lite_go_example.step.dependOn(&install_capi_header.step);

    const run_lite_go_retrieval_template = b.addSystemCommand(&.{
        "env",
        "GOWORK=off",
        "go",
        "run",
        ".",
        "--reset",
        "--db",
        "../../zig/.zig-cache/antfly-lite-retrieval-go.aflite",
        "--backup",
        "../../zig/.zig-cache/antfly-lite-retrieval-go.afb",
    });
    run_lite_go_retrieval_template.setCwd(b.path("../examples/antfly-lite-retrieval-go"));
    run_lite_go_retrieval_template.step.dependOn(&install_antfly_capi_lib.step);
    run_lite_go_retrieval_template.step.dependOn(&install_capi_header.step);

    const run_cabi_packaging_tests = b.addSystemCommand(&.{
        "env",
        "PYTHONPYCACHEPREFIX=/tmp/antfly-pycache",
        "python3",
        "scripts/packaging/test_cabi_packaging.py",
    });
    run_cabi_packaging_tests.setCwd(b.path(".."));

    const capi_test = antfly_tests_build.addModuleTestStep(b, capi_mod, "capi-test", "Run C API tests", .{
        .filters = &antfly_tests_build.capi_default_filters,
        .simple_runner = true,
    });
    capi_test.step.dependOn(&run_lite_capi_smoke.step);

    return .{
        .capi_lib = capi_lib,
        .lite_capi_lib = lite_capi_lib,
        .install_capi_lib = install_capi_lib,
        .install_antfly_capi_lib = install_antfly_capi_lib,
        .install_capi_header = install_capi_header,
        .run_lite_capi_smoke = run_lite_capi_smoke,
        .run_lite_go_tests = run_lite_go_tests,
        .run_lite_go_example = run_lite_go_example,
        .run_lite_go_retrieval_template = run_lite_go_retrieval_template,
        .run_cabi_packaging_tests = run_cabi_packaging_tests,
        .run_capi_tests = capi_test.run,
    };
}
