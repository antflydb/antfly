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

const std = @import("std");
const Imports = @import("../../../build_support/antfly/imports.zig").AntflyRootImports;

pub fn add(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize, imports: Imports, vopr: *std.Build.Module, lmdb_engine: *std.Build.Module) void {
    const source = @import("lake_execution_tests.zig").sourceImports(b, imports);
    const support = @import("../../../build_support/antfly/test_support.zig").Imports{ .runtime = source, .vopr = vopr, .lmdb_engine = lmdb_engine };
    const client = b.createModule(.{ .root_source_file = b.path("pkg/antfly/src/read_session_client_probe_root.zig"), .target = target, .optimize = optimize, .pic = true });
    support.configure(b, client, false, true);
    client.addImport("antfly_storage_root", client);
    const options = b.addOptions();
    options.addOption(bool, "shared_storage_core", true);
    options.addOption(bool, "storage_engine_owner", false);
    client.addOptions("storage_engine_options", options);
    const archive = @import("../../../build_support/embedded/shared_library.zig").addProviderArchive(b, client, "read-session-compiled-client-probe", 4 * 1024 * 1024 * 1024);
    b.step("opaque-read-session-client-compile", "Compile only the production retained-read client probe").dependOn(&archive.step);
    const consumer = b.createModule(.{ .root_source_file = b.path("pkg/antfly/src/read_session_compiled_client_test_root.zig"), .target = target, .optimize = optimize });
    support.configure(b, consumer, true, true);
    const provider = b.createModule(.{ .root_source_file = b.path("pkg/antfly-embedded/src/storage/read_session_provider.zig"), .target = target, .optimize = optimize });
    @import("../../../build_support/embedded/source_owner.zig").registerReadSessionProvider(provider);
    consumer.addImport("read_session_provider", provider);
    const tests = @import("linked_tests.zig").add(b, .{ .name = "read-session-compiled-client-tests", .root_module = consumer, .filters = &.{"opaque read session"} });
    tests.executable.root_module.linkLibrary(archive);
    imports.build_info.link(tests.executable.root_module);
    b.step("opaque-read-session-abi-integration-test", "Exercise production session handles across independently compiled allocator and error domains")
        .dependOn(&tests.run(b).step);
}
