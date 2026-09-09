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
    root: std.Build.LazyPath,
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
        .root_source_file = options.root.path(b, "pdf_test_root.zig"),
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

    return .{
        .run_lib_pdf_tests = run_lib_pdf_tests,
    };
}
pub fn addBenchmark(b: *std.Build, options: struct {
    root: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    pdf_mod: *std.Build.Module,
}) *std.Build.Step.Compile {
    const module = b.createModule(.{
        .root_source_file = options.root.path(b, "src/pdf_bench.zig"),
        .target = options.target,
        .optimize = options.optimize,
    });
    module.addImport("antfly_pdf", options.pdf_mod);
    return b.addExecutable(.{ .name = "lib-pdf-bench", .root_module = module });
}

pub fn addSafetyTests(b: *std.Build, pdf_mod: *std.Build.Module) *std.Build.Step.Compile {
    return b.addTest(.{
        .root_module = pdf_mod,
        .filters = &.{
            "native backend renders simple pdf first page png",
            "stream decoders enforce the decoded byte budget before growth",
            "xref parser rejects a cyclic Prev chain",
        },
    });
}

pub fn createModule(b: *std.Build, root: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, image: *std.Build.Module, font: *std.Build.Module, standard_fonts: *std.Build.Module) *std.Build.Module {
    const module = b.createModule(.{
        .root_source_file = root.path(b, "src/mod.zig"),
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
