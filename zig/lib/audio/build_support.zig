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

pub const FfmpegPaths = struct {
    include_dir: []const u8,
    lib_dir: []const u8,
};

pub const AddConformanceOptions = struct {
    conformance_fetch: bool,
    conformance_fixtures: []const u8,
    target: std.Build.ResolvedTarget,
    inference_ffmpeg_paths: ?FfmpegPaths,
    inference_build_options_mod: *std.Build.Module,
    conformance_test_step: *std.Build.Step,
};

pub fn addConformance(b: *std.Build, options: AddConformanceOptions) void {
    const conformance_fetch = options.conformance_fetch;
    const conformance_fixtures = options.conformance_fixtures;
    const target = options.target;
    const inference_ffmpeg_paths = options.inference_ffmpeg_paths;
    const inference_build_options_mod = options.inference_build_options_mod;
    const conformance_test_step = options.conformance_test_step;
    const lib_audio_xiph_conformance = b.addExecutable(.{
        .name = "lib-audio-xiph-conformance",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/audio/audio_xiph_corpora_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    lib_audio_xiph_conformance.root_module.addImport("build_options", inference_build_options_mod);
    if (inference_ffmpeg_paths) |ffmpeg_paths| {
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
    if (inference_ffmpeg_paths) |ffmpeg_paths| {
        lib_audio_misc_conformance.root_module.addIncludePath(.{ .cwd_relative = ffmpeg_paths.include_dir });
    }
    lib_audio_misc_conformance.root_module.link_libc = true;

    const run_lib_audio_xiph_conformance = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance.addArgs(&.{ "run", b.pathJoin(&.{ conformance_fixtures, "audio-xiph-corpora" }) });
    if (!conformance_fetch) run_lib_audio_xiph_conformance.addArg("--no-fetch");
    const run_lib_audio_misc_conformance = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance.addArgs(&.{ "run", b.pathJoin(&.{ conformance_fixtures, "audio-misc-corpora" }) });
    if (!conformance_fetch) run_lib_audio_misc_conformance.addArg("--no-fetch");
    const lib_audio_conformance_step = b.step("lib-audio-conformance", "Run lib/audio conformance (fetch missing fixtures)");
    lib_audio_conformance_step.dependOn(&run_lib_audio_xiph_conformance.step);
    lib_audio_conformance_step.dependOn(&run_lib_audio_misc_conformance.step);
    conformance_test_step.dependOn(lib_audio_conformance_step);
}
