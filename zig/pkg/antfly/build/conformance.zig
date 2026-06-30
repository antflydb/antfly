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
const antfly_tests_build = @import("tests.zig");

pub const ImageConformanceSteps = struct {
    tests_after_fetch_quiet: *std.Build.Step.Run,
    corpus_verify_jpeg_quiet: *std.Build.Step,
    corpus_verify_png_quiet: *std.Build.Step,
    corpus_verify_png_spng_quiet: *std.Build.Step,
    corpus_verify_gif_quiet: *std.Build.Step,
    jpeg_seed_corpora_after_fetch_quiet: *std.Build.Step,
};

pub fn addToonConformanceSteps(ctx: anytype) *std.Build.Step {
    const b = ctx.b;
    const target = ctx.target;
    const optimize = ctx.optimize;
    const toon_mod = ctx.toon_mod;

    const lib_toon_conformance = b.addExecutable(.{
        .name = "lib-toon-conformance",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/toon/toon_conformance.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    lib_toon_conformance.root_module.addImport("antfly_toon", toon_mod);

    const fetch_lib_toon_conformance = b.addRunArtifact(lib_toon_conformance);
    fetch_lib_toon_conformance.addArg("fetch");
    fetch_lib_toon_conformance.addArg("/tmp/toon-format-spec");
    const lib_toon_conformance_fetch_step = b.step("lib-toon-conformance-fetch", "Fetch the lib/toon upstream conformance fixtures");
    lib_toon_conformance_fetch_step.dependOn(&fetch_lib_toon_conformance.step);

    const fetch_lib_toon_conformance_quiet = b.addRunArtifact(lib_toon_conformance);
    fetch_lib_toon_conformance_quiet.addArg("fetch");
    fetch_lib_toon_conformance_quiet.addArg("/tmp/toon-format-spec");
    const fetch_lib_toon_conformance_quiet_step = expectQuietSuccess(fetch_lib_toon_conformance_quiet);

    const run_lib_toon_conformance = b.addRunArtifact(lib_toon_conformance);
    run_lib_toon_conformance.addArg("run");
    run_lib_toon_conformance.addArg("/tmp/toon-format-spec");
    run_lib_toon_conformance.addArg("--no-fetch");
    const lib_toon_conformance_run_step = b.step("lib-toon-conformance-run", "Run lib/toon conformance suite without fetching fixtures");
    lib_toon_conformance_run_step.dependOn(&run_lib_toon_conformance.step);

    const run_lib_toon_conformance_after_fetch = b.addRunArtifact(lib_toon_conformance);
    run_lib_toon_conformance_after_fetch.addArg("run");
    run_lib_toon_conformance_after_fetch.addArg("/tmp/toon-format-spec");
    run_lib_toon_conformance_after_fetch.addArg("--no-fetch");
    run_lib_toon_conformance_after_fetch.step.dependOn(&fetch_lib_toon_conformance.step);
    const lib_toon_conformance_step = b.step("lib-toon-conformance", "Fetch and run lib/toon conformance suite");
    lib_toon_conformance_step.dependOn(&run_lib_toon_conformance_after_fetch.step);

    const run_lib_toon_conformance_after_fetch_quiet = b.addRunArtifact(lib_toon_conformance);
    run_lib_toon_conformance_after_fetch_quiet.addArg("run");
    run_lib_toon_conformance_after_fetch_quiet.addArg("/tmp/toon-format-spec");
    run_lib_toon_conformance_after_fetch_quiet.addArg("--no-fetch");
    run_lib_toon_conformance_after_fetch_quiet.step.dependOn(fetch_lib_toon_conformance_quiet_step);
    return expectQuietSuccess(run_lib_toon_conformance_after_fetch_quiet);
}

pub fn addAudioConformanceSteps(ctx: anytype) void {
    const b = ctx.b;
    const target = ctx.target;
    const ffmpeg_paths_opt = ctx.ffmpeg_paths;
    const inference_build_options_mod = ctx.inference_build_options_mod;
    const conformance_test_step = ctx.conformance_test_step;

    const lib_audio_xiph_conformance = b.addExecutable(.{
        .name = "lib-audio-xiph-conformance",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/audio/audio_xiph_corpora_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    lib_audio_xiph_conformance.root_module.addImport("build_options", inference_build_options_mod);
    if (ffmpeg_paths_opt) |ffmpeg_paths| {
        lib_audio_xiph_conformance.root_module.addIncludePath(.{ .cwd_relative = ffmpeg_paths.include_dir });
    }
    lib_audio_xiph_conformance.root_module.link_libc = true;

    const lib_audio_misc_conformance = b.addExecutable(.{
        .name = "lib-audio-misc-conformance",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/audio/audio_misc_corpora_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    lib_audio_misc_conformance.root_module.addImport("build_options", inference_build_options_mod);
    if (ffmpeg_paths_opt) |ffmpeg_paths| {
        lib_audio_misc_conformance.root_module.addIncludePath(.{ .cwd_relative = ffmpeg_paths.include_dir });
    }
    lib_audio_misc_conformance.root_module.link_libc = true;

    const fetch_lib_audio_xiph_conformance = b.addRunArtifact(lib_audio_xiph_conformance);
    fetch_lib_audio_xiph_conformance.addArg("fetch");
    fetch_lib_audio_xiph_conformance.addArg("/tmp/termite-audio-xiph-corpora");

    const fetch_lib_audio_misc_conformance = b.addRunArtifact(lib_audio_misc_conformance);
    fetch_lib_audio_misc_conformance.addArg("fetch");
    fetch_lib_audio_misc_conformance.addArg("/tmp/termite-audio-misc-corpora");

    const lib_audio_conformance_fetch_step = b.step("lib-audio-conformance-fetch", "Fetch the lib/audio external conformance fixtures");
    lib_audio_conformance_fetch_step.dependOn(&fetch_lib_audio_xiph_conformance.step);
    lib_audio_conformance_fetch_step.dependOn(&fetch_lib_audio_misc_conformance.step);

    const fetch_lib_audio_xiph_conformance_quiet = b.addRunArtifact(lib_audio_xiph_conformance);
    fetch_lib_audio_xiph_conformance_quiet.addArg("fetch");
    fetch_lib_audio_xiph_conformance_quiet.addArg("/tmp/termite-audio-xiph-corpora");
    const fetch_lib_audio_xiph_conformance_quiet_step = expectQuietSuccess(fetch_lib_audio_xiph_conformance_quiet);

    const fetch_lib_audio_misc_conformance_quiet = b.addRunArtifact(lib_audio_misc_conformance);
    fetch_lib_audio_misc_conformance_quiet.addArg("fetch");
    fetch_lib_audio_misc_conformance_quiet.addArg("/tmp/termite-audio-misc-corpora");
    const fetch_lib_audio_misc_conformance_quiet_step = expectQuietSuccess(fetch_lib_audio_misc_conformance_quiet);

    const run_lib_audio_xiph_conformance = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance.addArg("run");
    run_lib_audio_xiph_conformance.addArg("/tmp/termite-audio-xiph-corpora");
    run_lib_audio_xiph_conformance.addArg("--no-fetch");

    const run_lib_audio_misc_conformance = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance.addArg("run");
    run_lib_audio_misc_conformance.addArg("/tmp/termite-audio-misc-corpora");
    run_lib_audio_misc_conformance.addArg("--no-fetch");

    const lib_audio_conformance_run_step = b.step("lib-audio-conformance-run", "Run lib/audio conformance suites without fetching fixtures");
    lib_audio_conformance_run_step.dependOn(&run_lib_audio_xiph_conformance.step);
    lib_audio_conformance_run_step.dependOn(&run_lib_audio_misc_conformance.step);

    const run_lib_audio_xiph_conformance_after_fetch = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance_after_fetch.addArg("run");
    run_lib_audio_xiph_conformance_after_fetch.addArg("/tmp/termite-audio-xiph-corpora");
    run_lib_audio_xiph_conformance_after_fetch.addArg("--no-fetch");
    run_lib_audio_xiph_conformance_after_fetch.step.dependOn(&fetch_lib_audio_xiph_conformance.step);

    const run_lib_audio_misc_conformance_after_fetch = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance_after_fetch.addArg("run");
    run_lib_audio_misc_conformance_after_fetch.addArg("/tmp/termite-audio-misc-corpora");
    run_lib_audio_misc_conformance_after_fetch.addArg("--no-fetch");
    run_lib_audio_misc_conformance_after_fetch.step.dependOn(&fetch_lib_audio_misc_conformance.step);

    const lib_audio_conformance_step = b.step("lib-audio-conformance", "Fetch and run lib/audio conformance suites");
    lib_audio_conformance_step.dependOn(&run_lib_audio_xiph_conformance_after_fetch.step);
    lib_audio_conformance_step.dependOn(&run_lib_audio_misc_conformance_after_fetch.step);

    const run_lib_audio_xiph_conformance_after_fetch_quiet = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance_after_fetch_quiet.addArg("run");
    run_lib_audio_xiph_conformance_after_fetch_quiet.addArg("/tmp/termite-audio-xiph-corpora");
    run_lib_audio_xiph_conformance_after_fetch_quiet.addArg("--no-fetch");
    run_lib_audio_xiph_conformance_after_fetch_quiet.step.dependOn(fetch_lib_audio_xiph_conformance_quiet_step);
    const run_lib_audio_xiph_conformance_after_fetch_quiet_step = expectQuietSuccess(run_lib_audio_xiph_conformance_after_fetch_quiet);

    const run_lib_audio_misc_conformance_after_fetch_quiet = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance_after_fetch_quiet.addArg("run");
    run_lib_audio_misc_conformance_after_fetch_quiet.addArg("/tmp/termite-audio-misc-corpora");
    run_lib_audio_misc_conformance_after_fetch_quiet.addArg("--no-fetch");
    run_lib_audio_misc_conformance_after_fetch_quiet.step.dependOn(fetch_lib_audio_misc_conformance_quiet_step);
    const run_lib_audio_misc_conformance_after_fetch_quiet_step = expectQuietSuccess(run_lib_audio_misc_conformance_after_fetch_quiet);
    conformance_test_step.dependOn(run_lib_audio_xiph_conformance_after_fetch_quiet_step);
    conformance_test_step.dependOn(run_lib_audio_misc_conformance_after_fetch_quiet_step);
}

pub fn addImageConformanceSteps(ctx: anytype) ImageConformanceSteps {
    const b = ctx.b;
    const target = ctx.target;
    const optimize = ctx.optimize;
    const image_mod = ctx.image_mod;
    const spng_paths_opt = ctx.spng_paths;
    const enable_spng = ctx.enable_spng;

    const lib_image_conformance_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lib_image_conformance_tests = b.addTest(.{
        .root_module = lib_image_conformance_test_mod,
        .filters = antfly_tests_build.selectTestFilters(b, &antfly_tests_build.PackageTestFilters.image_conformance),
    });
    const run_lib_image_conformance_tests = b.addRunArtifact(lib_image_conformance_tests);
    const lib_image_conformance_run_step = b.step("lib-image-conformance-run", "Run lib/image conformance suites without fetching fixtures");
    lib_image_conformance_run_step.dependOn(&run_lib_image_conformance_tests.step);

    const lib_image_corpus_build_options = b.addOptions();
    lib_image_corpus_build_options.addOption(bool, "enable_spng", enable_spng);
    const lib_image_corpus_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/image_corpus.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_image_corpus_mod.addOptions("build_options", lib_image_corpus_build_options);
    if (spng_paths_opt) |spng_paths| {
        lib_image_corpus_mod.addIncludePath(.{ .cwd_relative = spng_paths.include_dir });
    }
    const lib_image_corpus = b.addExecutable(.{
        .name = "lib-image-corpus",
        .root_module = lib_image_corpus_mod,
    });
    if (spng_paths_opt) |spng_paths| {
        lib_image_corpus.root_module.addLibraryPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_corpus.root_module.addRPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_corpus.root_module.linkSystemLibrary("spng", .{});
        lib_image_corpus.root_module.link_libc = true;
    }
    const run_lib_image_corpus_verify_jpeg = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_jpeg.addArg("verify-jpeg");
    lib_image_conformance_run_step.dependOn(&run_lib_image_corpus_verify_jpeg.step);

    const run_lib_image_corpus_verify_jpeg_quiet = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_jpeg_quiet.addArg("verify-jpeg");
    const run_lib_image_corpus_verify_jpeg_quiet_step = expectQuietSuccess(run_lib_image_corpus_verify_jpeg_quiet);

    const run_lib_image_corpus_verify_png = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_png.addArg("verify-png");
    lib_image_conformance_run_step.dependOn(&run_lib_image_corpus_verify_png.step);

    const run_lib_image_corpus_verify_png_quiet = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_png_quiet.addArg("verify-png");
    const run_lib_image_corpus_verify_png_quiet_step = expectQuietSuccess(run_lib_image_corpus_verify_png_quiet);

    const run_lib_image_corpus_verify_png_spng = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_png_spng.addArg("verify-png-spng");
    lib_image_conformance_run_step.dependOn(&run_lib_image_corpus_verify_png_spng.step);

    const run_lib_image_corpus_verify_png_spng_quiet = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_png_spng_quiet.addArg("verify-png-spng");
    const run_lib_image_corpus_verify_png_spng_quiet_step = expectQuietSuccess(run_lib_image_corpus_verify_png_spng_quiet);

    const run_lib_image_corpus_verify_gif = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_gif.addArg("verify-gif");
    lib_image_conformance_run_step.dependOn(&run_lib_image_corpus_verify_gif.step);

    const run_lib_image_corpus_verify_gif_quiet = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_gif_quiet.addArg("verify-gif");
    const run_lib_image_corpus_verify_gif_quiet_step = expectQuietSuccess(run_lib_image_corpus_verify_gif_quiet);

    const image_jpeg_seed_corpora_e2e = b.addExecutable(.{
        .name = "image-jpeg-seed-corpora-e2e",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/image/src/image_jpeg_seed_corpora_e2e.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const image_jpeg_seed_corpora_e2e_step = b.step("image-jpeg-seed-corpora-e2e", "Build the lib/image upstream JPEG seed-corpora e2e runner");
    image_jpeg_seed_corpora_e2e_step.dependOn(&image_jpeg_seed_corpora_e2e.step);

    const fetch_image_jpeg_seed_corpora_e2e = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    fetch_image_jpeg_seed_corpora_e2e.addArg("fetch");
    fetch_image_jpeg_seed_corpora_e2e.addArg("/tmp/libjpeg-turbo-seed-corpora");
    const image_jpeg_seed_corpora_e2e_fetch_step = b.step("image-jpeg-seed-corpora-e2e-fetch", "Fetch or refresh the upstream lib/image JPEG seed-corpora checkout");
    image_jpeg_seed_corpora_e2e_fetch_step.dependOn(&fetch_image_jpeg_seed_corpora_e2e.step);

    const fetch_image_jpeg_seed_corpora_e2e_quiet = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    fetch_image_jpeg_seed_corpora_e2e_quiet.addArg("fetch");
    fetch_image_jpeg_seed_corpora_e2e_quiet.addArg("/tmp/libjpeg-turbo-seed-corpora");
    const fetch_image_jpeg_seed_corpora_e2e_quiet_step = expectQuietSuccess(fetch_image_jpeg_seed_corpora_e2e_quiet);

    const run_image_jpeg_seed_corpora_e2e = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    run_image_jpeg_seed_corpora_e2e.addArg("run");
    run_image_jpeg_seed_corpora_e2e.addArg("/tmp/libjpeg-turbo-seed-corpora");
    run_image_jpeg_seed_corpora_e2e.addArg("--no-fetch");
    const image_jpeg_seed_corpora_e2e_run_step = b.step("image-jpeg-seed-corpora-e2e-run", "Run the lib/image upstream JPEG seed-corpora e2e runner");
    image_jpeg_seed_corpora_e2e_run_step.dependOn(&run_image_jpeg_seed_corpora_e2e.step);

    const run_image_jpeg_seed_corpora_e2e_after_fetch_quiet = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    run_image_jpeg_seed_corpora_e2e_after_fetch_quiet.addArg("run");
    run_image_jpeg_seed_corpora_e2e_after_fetch_quiet.addArg("/tmp/libjpeg-turbo-seed-corpora");
    run_image_jpeg_seed_corpora_e2e_after_fetch_quiet.addArg("--no-fetch");
    run_image_jpeg_seed_corpora_e2e_after_fetch_quiet.addArg("--quiet-failures");
    run_image_jpeg_seed_corpora_e2e_after_fetch_quiet.step.dependOn(fetch_image_jpeg_seed_corpora_e2e_quiet_step);
    const run_image_jpeg_seed_corpora_e2e_after_fetch_quiet_step = expectQuietSuccess(run_image_jpeg_seed_corpora_e2e_after_fetch_quiet);

    const triage_image_jpeg_seed_corpora_e2e = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    triage_image_jpeg_seed_corpora_e2e.addArg("triage-djpeg");
    triage_image_jpeg_seed_corpora_e2e.addArg("/tmp/libjpeg-turbo-seed-corpora");
    triage_image_jpeg_seed_corpora_e2e.addArg("--no-fetch");
    const image_jpeg_seed_corpora_e2e_triage_step = b.step("image-jpeg-seed-corpora-e2e-triage", "Triage upstream JPEG decode failures against local djpeg");
    image_jpeg_seed_corpora_e2e_triage_step.dependOn(&triage_image_jpeg_seed_corpora_e2e.step);

    const jpeg2000_fuzz = b.addExecutable(.{
        .name = "jpeg2000-fuzz",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/image/src/jpeg2000_fuzz.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    jpeg2000_fuzz.root_module.addImport("antfly_image", image_mod);
    const install_jpeg2000_fuzz = b.addInstallArtifact(jpeg2000_fuzz, .{});
    const jpeg2000_fuzz_step = b.step("image-jpeg2000-fuzz", "Build the JPEG 2000 fuzz runner");
    jpeg2000_fuzz_step.dependOn(&install_jpeg2000_fuzz.step);

    const lib_image_conformance_fetcher = b.addExecutable(.{
        .name = "lib-image-conformance-fetch",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/image/src/jpeg2000_conformance_fixtures.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const fetch_lib_image_conformance_fixtures = b.addRunArtifact(lib_image_conformance_fetcher);
    fetch_lib_image_conformance_fixtures.addArg("fetch");
    fetch_lib_image_conformance_fixtures.addArg("/tmp/openjpeg-data");
    const lib_image_conformance_fetch_step = b.step(
        "lib-image-conformance-fetch",
        "Fetch the lib/image external conformance fixtures",
    );
    lib_image_conformance_fetch_step.dependOn(&fetch_lib_image_conformance_fixtures.step);

    const fetch_lib_image_conformance_fixtures_quiet = b.addRunArtifact(lib_image_conformance_fetcher);
    fetch_lib_image_conformance_fixtures_quiet.addArg("fetch");
    fetch_lib_image_conformance_fixtures_quiet.addArg("/tmp/openjpeg-data");
    const fetch_lib_image_conformance_fixtures_quiet_step = expectQuietSuccess(fetch_lib_image_conformance_fixtures_quiet);

    const run_lib_image_conformance_tests_after_fetch = b.addRunArtifact(lib_image_conformance_tests);
    run_lib_image_conformance_tests_after_fetch.step.dependOn(&fetch_lib_image_conformance_fixtures.step);
    const lib_image_conformance_step = b.step("lib-image-conformance", "Fetch and run lib/image conformance suites");
    lib_image_conformance_step.dependOn(&run_lib_image_conformance_tests_after_fetch.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_jpeg.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_png.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_png_spng.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_gif.step);

    const run_lib_image_conformance_tests_after_fetch_quiet = b.addRunArtifact(lib_image_conformance_tests);
    run_lib_image_conformance_tests_after_fetch_quiet.step.dependOn(fetch_lib_image_conformance_fixtures_quiet_step);

    return .{
        .tests_after_fetch_quiet = run_lib_image_conformance_tests_after_fetch_quiet,
        .corpus_verify_jpeg_quiet = run_lib_image_corpus_verify_jpeg_quiet_step,
        .corpus_verify_png_quiet = run_lib_image_corpus_verify_png_quiet_step,
        .corpus_verify_png_spng_quiet = run_lib_image_corpus_verify_png_spng_quiet_step,
        .corpus_verify_gif_quiet = run_lib_image_corpus_verify_gif_quiet_step,
        .jpeg_seed_corpora_after_fetch_quiet = run_image_jpeg_seed_corpora_e2e_after_fetch_quiet_step,
    };
}

fn expectQuietSuccess(run: *std.Build.Step.Run) *std.Build.Step {
    run.has_side_effects = true;
    run.expectExitCode(0);
    run.expectStdErrMatch("");
    return &run.step;
}
