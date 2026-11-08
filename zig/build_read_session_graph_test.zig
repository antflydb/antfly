// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Standalone regression: linked provider archives are not necessarily present
//! in Step.dependencies until Zig prepares the compilation graph.
const std = @import("std");
const owner = @import("build_support/embedded/source_owner.zig");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const files = b.addWriteFiles();
    const provider = b.createModule(.{
        .root_source_file = files.add("provider.zig", "export fn session_provider() u32 { _ = @import(\"read_session_backend\"); _ = @import(\"read_session_types\"); return 42; }\n"),
        .target = target,
        .optimize = .debug,
        .pic = true,
    });
    const options = b.addOptions();
    options.addOption(bool, "shared_storage_core", true);
    options.addOption(bool, "storage_engine_owner", true);
    provider.addOptions("storage_engine_options", options);
    owner.configureReadSessionModules(provider);
    const archive = b.addLibrary(.{ .name = "session-provider", .linkage = .static, .root_module = provider });
    // Exercise links on imported modules as well as on the compile root.
    const links = b.createModule(.{ .root_source_file = files.add("links.zig", ""), .target = target, .optimize = .debug });
    links.linkLibrary(archive);
    const consumer = b.createModule(.{
        .root_source_file = files.add("consumer.zig", "extern fn session_provider() u32;\nexport fn session_consumer() u32 { return session_provider(); }\n"),
        .target = target,
        .optimize = .debug,
    });
    consumer.addImport("provider_links", links);
    const library = b.addLibrary(.{ .name = "session-consumer", .linkage = .dynamic, .root_module = consumer });
    b.installArtifact(library);
    owner.finalize(b);
    // The production graph finalizes both before test ownership and on return.
    owner.finalize(b);
}
