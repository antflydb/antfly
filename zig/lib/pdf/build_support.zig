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
const addMacosSdkPaths = @import("../platform/build_support.zig").addMacosSdkPaths;

pub const AddTestsOptions = struct {
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    image_mod: *std.Build.Module,
    pdf_standard_fonts_mod: *std.Build.Module,
    font_mod: *std.Build.Module,
};
pub const AddTestsResult = struct {
    run_lib_pdf_tests: *std.Build.Step.Run,
};

pub fn addTests(b: *std.Build, options: AddTestsOptions) AddTestsResult {
    const target = options.target;
    const optimize = options.optimize;
    const image_mod = options.image_mod;
    const pdf_standard_fonts_mod = options.pdf_standard_fonts_mod;
    const font_mod = options.font_mod;
    const pdf_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/pdf_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    pdf_test_mod.addImport("antfly_image", image_mod);
    pdf_test_mod.addImport("antfly_font", font_mod);
    pdf_test_mod.addImport("pdf_standard_fonts", pdf_standard_fonts_mod);
    if (target.result.os.tag == .macos) {
        addMacosSdkPaths(b, pdf_test_mod, target);
        pdf_test_mod.linkFramework("CoreFoundation", .{});
        pdf_test_mod.linkFramework("CoreGraphics", .{});
    }
    const lib_pdf_tests = b.addTest(.{
        .root_module = pdf_test_mod,
    });
    lib_pdf_tests.root_module.link_libc = true;
    const run_lib_pdf_tests = b.addRunArtifact(lib_pdf_tests);
    const lib_pdf_test_step = b.step("lib-pdf-test", "Run shared PDF tests");
    lib_pdf_test_step.dependOn(&run_lib_pdf_tests.step);

    return .{
        .run_lib_pdf_tests = run_lib_pdf_tests,
    };
}
const addFilteredTestRunArtifact = @import("../../pkg/antfly/build/tests.zig").addFilteredTestRunArtifact;

pub const AddBenchmarkOptions = struct {
    target: std.Build.ResolvedTarget,
    hash_bench_mod: *std.Build.Module,
    pdf_mod: *std.Build.Module,
};

pub fn addBenchmark(b: *std.Build, options: AddBenchmarkOptions) void {
    const target = options.target;
    const hash_bench_mod = options.hash_bench_mod;
    const pdf_mod = options.pdf_mod;
    const pdf_bench_optimize = b.option(
        std.builtin.OptimizeMode,
        "pdf-optimize",
        "Optimization for the isolated PDF executable",
    ) orelse .ReleaseFast;
    const pdf_bench_image_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = target,
        .optimize = pdf_bench_optimize,
    });
    pdf_bench_image_mod.addImport("antfly_hash", hash_bench_mod);
    const pdf_bench_font_mod = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = target,
        .optimize = pdf_bench_optimize,
    });
    const pdf_bench_pdf_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/mod.zig"),
        .target = target,
        .optimize = pdf_bench_optimize,
    });
    const pdf_bench_standard_fonts_mod = b.createModule(.{
        .root_source_file = b.path("pdf_standard_fonts.zig"),
        .target = target,
        .optimize = pdf_bench_optimize,
    });
    pdf_bench_pdf_mod.addImport("antfly_image", pdf_bench_image_mod);
    pdf_bench_pdf_mod.addImport("antfly_font", pdf_bench_font_mod);
    pdf_bench_pdf_mod.addImport("pdf_standard_fonts", pdf_bench_standard_fonts_mod);
    if (target.result.os.tag == .macos) {
        addMacosSdkPaths(b, pdf_bench_pdf_mod, target);
        pdf_bench_pdf_mod.linkFramework("CoreFoundation", .{});
        pdf_bench_pdf_mod.linkFramework("CoreGraphics", .{});
    }
    const pdf_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/pdf_bench.zig"),
        .target = target,
        .optimize = pdf_bench_optimize,
    });
    pdf_bench_mod.addImport("antfly_pdf", pdf_bench_pdf_mod);
    const lib_pdf_bench = b.addExecutable(.{
        .name = "lib-pdf-bench",
        .root_module = pdf_bench_mod,
    });

    const lib_pdf_bench_step = b.step("lib-pdf-bench", "Build and install lib-pdf-bench");
    lib_pdf_bench_step.dependOn(&b.addInstallArtifact(lib_pdf_bench, .{}).step);

    const lib_pdf_safety_tests = b.addTest(.{
        .root_module = pdf_mod,
        .filters = &.{
            "native backend renders simple pdf first page png",
            "stream decoders enforce the decoded byte budget before growth",
            "xref parser rejects a cyclic Prev chain",
        },
    });
    const run_lib_pdf_safety_tests = addFilteredTestRunArtifact(b, lib_pdf_safety_tests);
    const lib_pdf_safety_test_step = b.step("lib-pdf-safety-test", "Run focused PDF OCR rendering and parser safety tests");
    lib_pdf_safety_test_step.dependOn(&run_lib_pdf_safety_tests.step);
}

pub fn createModule(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, image: *std.Build.Module, font: *std.Build.Module, standard_fonts: *std.Build.Module) *std.Build.Module {
    const module = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    module.addImport("antfly_image", image);
    module.addImport("antfly_font", font);
    module.addImport("pdf_standard_fonts", standard_fonts);
    if (target.result.os.tag == .macos) {
        addMacosSdkPaths(b, module, target);
        module.linkFramework("CoreFoundation", .{});
        module.linkFramework("CoreGraphics", .{});
    }
    return module;
}
