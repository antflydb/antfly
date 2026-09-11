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

pub const AddConformanceOptions = struct {
    root: std.Build.LazyPath,
    conformance_fetch: bool,
    conformance_fixtures: []const u8,
    target: std.Build.ResolvedTarget,
};

pub fn addConformance(b: *std.Build, options: AddConformanceOptions) [2]*std.Build.Step.Run {
    const conformance_fetch = options.conformance_fetch;
    const conformance_fixtures = options.conformance_fixtures;
    const target = options.target;
    const lib_audio_xiph_conformance = b.addExecutable(.{
        .name = "lib-audio-xiph-conformance",
        .root_module = b.createModule(.{
            .root_source_file = options.root.path(b, "audio_xiph_corpora_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    lib_audio_xiph_conformance.root_module.link_libc = true;

    const lib_audio_misc_conformance = b.addExecutable(.{
        .name = "lib-audio-misc-conformance",
        .root_module = b.createModule(.{
            .root_source_file = options.root.path(b, "audio_misc_corpora_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    lib_audio_misc_conformance.root_module.link_libc = true;

    const run_lib_audio_xiph_conformance = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance.addArgs(&.{ "run", b.pathJoin(&.{ conformance_fixtures, "audio-xiph-corpora" }) });
    if (!conformance_fetch) run_lib_audio_xiph_conformance.addArg("--no-fetch");
    const run_lib_audio_misc_conformance = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance.addArgs(&.{ "run", b.pathJoin(&.{ conformance_fixtures, "audio-misc-corpora" }) });
    if (!conformance_fetch) run_lib_audio_misc_conformance.addArg("--no-fetch");
    return .{ run_lib_audio_xiph_conformance, run_lib_audio_misc_conformance };
}
