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

pub const SpngPaths = struct {
    include_dir: []const u8,
    lib_dir: []const u8,
};

pub fn detectSpngPaths(b: *std.Build, target: std.Build.ResolvedTarget) ?SpngPaths {
    const macos_candidates = [_]SpngPaths{
        .{ .include_dir = "/opt/homebrew/include", .lib_dir = "/opt/homebrew/lib" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib" },
    };
    const linux_candidates = [_]SpngPaths{
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib/x86_64-linux-gnu" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib/aarch64-linux-gnu" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib64" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib64" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib" },
    };
    const candidates: []const SpngPaths = switch (target.result.os.tag) {
        .macos => macos_candidates[0..],
        .linux => linux_candidates[0..],
        else => return null,
    };

    for (candidates) |candidate| {
        const header = b.fmt("{s}/spng.h", .{candidate.include_dir});
        const dylib = b.fmt("{s}/libspng.dylib", .{candidate.lib_dir});
        const so = b.fmt("{s}/libspng.so", .{candidate.lib_dir});
        const static_lib = b.fmt("{s}/libspng.a", .{candidate.lib_dir});
        if (pathExists(b, header) and (pathExists(b, dylib) or pathExists(b, so) or pathExists(b, static_lib))) return candidate;
    }
    return null;
}

fn pathExists(b: *std.Build, path: []const u8) bool {
    std.Io.Dir.cwd().access(b.graph.io, path, .{}) catch return false;
    return true;
}

pub const AddTestsOptions = struct {
    root: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    hash_mod: *std.Build.Module,
    image_mod: *std.Build.Module,
};
pub const AddTestsResult = struct {
    run_lib_image_tests: *std.Build.Step.Run,
    run_png_tests: *std.Build.Step.Run,
    run_jpeg2000_decode_tests: *std.Build.Step.Run,
};

pub fn addTests(b: *std.Build, options: AddTestsOptions) AddTestsResult {
    const target = options.target;
    const optimize = options.optimize;
    const hash_mod = options.hash_mod;
    const image_mod = options.image_mod;
    const image_test_mod = b.createModule(.{
        .root_source_file = options.root.path(b, "image_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    image_test_mod.addImport("antfly_image", image_mod);
    const lib_image_tests = b.addTest(.{
        .root_module = image_test_mod,
    });
    const run_lib_image_tests = b.addRunArtifact(lib_image_tests);
    // A named image dependency does not discover its internal PNG tests.
    // Compile PNG as a test root to verify its checksum/container integration.
    const png_test_mod = b.createModule(.{
        .root_source_file = options.root.path(b, "src/png.zig"),
        .target = target,
        .optimize = optimize,
    });
    png_test_mod.addImport("antfly_hash", hash_mod);
    const png_tests = b.addTest(.{ .root_module = png_test_mod });
    const run_png_tests = b.addRunArtifact(png_tests);

    const jpeg2000_decode_test_mod = b.createModule(.{
        .root_source_file = options.root.path(b, "src/jpeg2000/decode.zig"),
        .target = target,
        .optimize = optimize,
    });
    const jpeg2000_decode_tests = b.addTest(.{
        .root_module = jpeg2000_decode_test_mod,
    });
    const run_jpeg2000_decode_tests = b.addRunArtifact(jpeg2000_decode_tests);
    return .{
        .run_lib_image_tests = run_lib_image_tests,
        .run_png_tests = run_png_tests,
        .run_jpeg2000_decode_tests = run_jpeg2000_decode_tests,
    };
}

pub const AddBenchmarkOptions = struct {
    root: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    hash_bench_mod: *std.Build.Module,
    spng_paths: ?SpngPaths,
};
pub fn addBenchmark(b: *std.Build, options: AddBenchmarkOptions) *std.Build.Step.Compile {
    const target = options.target;
    const hash_bench_mod = options.hash_bench_mod;
    const lib_image_bench_build_options = b.addOptions();
    const lib_image_spng_paths = options.spng_paths;
    const lib_image_enable_spng = lib_image_spng_paths != null;
    lib_image_bench_build_options.addOption(bool, "enable_spng", lib_image_enable_spng);
    const lib_image_bench_mod = b.createModule(.{
        .root_source_file = options.root.path(b, "src/image_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lib_image_bench_mod.addImport("antfly_hash", hash_bench_mod);
    lib_image_bench_mod.addOptions("build_options", lib_image_bench_build_options);
    if (lib_image_spng_paths) |spng_paths| {
        lib_image_bench_mod.addIncludePath(.{ .cwd_relative = spng_paths.include_dir });
    }
    const lib_image_bench = b.addExecutable(.{
        .name = "lib-image-bench",
        .root_module = lib_image_bench_mod,
    });
    if (lib_image_spng_paths) |spng_paths| {
        lib_image_bench.root_module.addLibraryPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_bench.root_module.addRPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_bench.root_module.linkSystemLibrary("spng", .{});
        lib_image_bench.root_module.link_libc = true;
    }

    return lib_image_bench;
}

pub const AddConformanceOptions = struct {
    root: std.Build.LazyPath,
    add_test_run: *const fn (*std.Build, *std.Build.Step.Compile) *std.Build.Step.Run = std.Build.addRunArtifact,
    conformance_fetch: bool,
    conformance_fixtures: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    hash_mod: *std.Build.Module,
    image_mod: *std.Build.Module,
    spng_paths: ?SpngPaths,
};
pub const AddConformanceResult = struct {
    runs: [8]*std.Build.Step.Run,
    jpeg_seed_corpora: *std.Build.Step.Compile,
    jpeg2000_fuzz: *std.Build.Step.Compile,
};

pub fn addConformance(b: *std.Build, options: AddConformanceOptions) AddConformanceResult {
    const conformance_fetch = options.conformance_fetch;
    const conformance_fixtures = options.conformance_fixtures;
    const target = options.target;
    const optimize = options.optimize;
    const hash_mod = options.hash_mod;
    const image_mod = options.image_mod;
    const lib_image_spng_paths = options.spng_paths;
    const lib_image_enable_spng = lib_image_spng_paths != null;
    const lib_image_conformance_test_mod = b.createModule(.{
        .root_source_file = options.root.path(b, "src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_image_conformance_test_mod.addImport("antfly_hash", hash_mod);
    const lib_image_conformance_tests = b.addTest(.{
        .root_module = lib_image_conformance_test_mod,
        .filters = &.{"conformance corpus"},
    });
    const run_lib_image_conformance_tests = options.add_test_run(b, lib_image_conformance_tests);

    const lib_image_corpus_build_options = b.addOptions();
    lib_image_corpus_build_options.addOption(bool, "enable_spng", lib_image_enable_spng);
    const lib_image_corpus_mod = b.createModule(.{
        .root_source_file = options.root.path(b, "src/image_corpus.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_image_corpus_mod.addImport("antfly_hash", hash_mod);
    lib_image_corpus_mod.addOptions("build_options", lib_image_corpus_build_options);
    if (lib_image_spng_paths) |spng_paths| {
        lib_image_corpus_mod.addIncludePath(.{ .cwd_relative = spng_paths.include_dir });
    }
    const lib_image_corpus = b.addExecutable(.{
        .name = "lib-image-corpus",
        .root_module = lib_image_corpus_mod,
    });
    if (lib_image_spng_paths) |spng_paths| {
        lib_image_corpus.root_module.addLibraryPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_corpus.root_module.addRPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_corpus.root_module.linkSystemLibrary("spng", .{});
        lib_image_corpus.root_module.link_libc = true;
    }
    const run_lib_image_corpus_verify_jpeg = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_jpeg.addArg("verify-jpeg");

    const run_lib_image_corpus_verify_png = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_png.addArg("verify-png");

    const run_lib_image_corpus_verify_png_spng = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_png_spng.addArg("verify-png-spng");

    const run_lib_image_corpus_verify_gif = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_gif.addArg("verify-gif");

    const run_lib_image_corpus_verify_bmp = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_bmp.addArg("verify-bmp");

    const run_lib_image_corpus_verify_webp = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_webp.addArg("verify-webp");

    const image_jpeg_seed_corpora_e2e = b.addExecutable(.{
        .name = "image-jpeg-seed-corpora-e2e",
        .root_module = b.createModule(.{
            .root_source_file = options.root.path(b, "src/image_jpeg_seed_corpora_e2e.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_image_jpeg_seed_corpora_e2e = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    run_image_jpeg_seed_corpora_e2e.addArgs(&.{ "run", b.pathJoin(&.{ conformance_fixtures, "libjpeg-turbo-seed-corpora" }) });
    if (!conformance_fetch) run_image_jpeg_seed_corpora_e2e.addArg("--no-fetch");

    const jpeg2000_fuzz = b.addExecutable(.{
        .name = "jpeg2000-fuzz",
        .root_module = b.createModule(.{
            .root_source_file = options.root.path(b, "src/jpeg2000_fuzz.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    jpeg2000_fuzz.root_module.addImport("antfly_image", image_mod);
    // External lib/image conformance fixtures. The fetcher shallow-clones
    // openjpeg-data into /tmp; normal tests skip gracefully when the checkout
    // is missing.
    const lib_image_conformance_fetcher = b.addExecutable(.{
        .name = "lib-image-conformance-fetch",
        .root_module = b.createModule(.{
            .root_source_file = options.root.path(b, "src/jpeg2000_conformance_fixtures.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const prepare_lib_image_conformance = b.addRunArtifact(lib_image_conformance_fetcher);
    prepare_lib_image_conformance.addArgs(&.{ "fetch", b.pathJoin(&.{ conformance_fixtures, "openjpeg-data" }) });
    if (!conformance_fetch) prepare_lib_image_conformance.addArg("--no-fetch");
    run_lib_image_conformance_tests.step.dependOn(&prepare_lib_image_conformance.step);
    run_lib_image_conformance_tests.setEnvironmentVariable("OPENJPEG_DATA_DIR", b.pathJoin(&.{ conformance_fixtures, "openjpeg-data" }));

    return .{
        .runs = .{ run_lib_image_conformance_tests, run_lib_image_corpus_verify_jpeg, run_lib_image_corpus_verify_png, run_lib_image_corpus_verify_png_spng, run_lib_image_corpus_verify_gif, run_lib_image_corpus_verify_bmp, run_lib_image_corpus_verify_webp, run_image_jpeg_seed_corpora_e2e },
        .jpeg_seed_corpora = image_jpeg_seed_corpora_e2e,
        .jpeg2000_fuzz = jpeg2000_fuzz,
    };
}

pub fn createModule(b: *std.Build, root: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, hash: *std.Build.Module) *std.Build.Module {
    const module = b.createModule(.{
        .root_source_file = root.path(b, "src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    module.addImport("antfly_hash", hash);
    return module;
}
