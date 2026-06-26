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

fn expectQuietSuccess(run: *std.Build.Step.Run) *std.Build.Step {
    run.has_side_effects = true;
    run.expectExitCode(0);
    run.expectStdErrMatch("");
    return &run.step;
}
