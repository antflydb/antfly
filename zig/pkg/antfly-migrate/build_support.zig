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

pub fn add(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize, package_path: []const u8, header_path: []const u8, default_lib_path: []const u8) void {
    const module = b.createModule(.{
        .root_source_file = b.path(b.fmt("{s}/src/main.zig", .{package_path})),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const bindings = @import("translate_c").Translator.init(b.dependency("translate_c", .{}), .{
        .c_source_file = b.path(b.fmt("{s}/src/ffi.h", .{package_path})),
        .libc_file = @import("antfly_platform").macosSdkLibCFile(b, target),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    bindings.addIncludePath(.{ .cwd_relative = (b.root.joinString(b.allocator, header_path) catch @panic("OOM")) });
    module.addImport("migration_c", bindings.mod);
    if (b.option([]const u8, "sqlite-root", "SQLite installation prefix (headers and library)")) |root| {
        bindings.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include", .{root}) });
        module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/lib", .{root}) });
        module.addRPath(.{ .cwd_relative = b.fmt("{s}/lib", .{root}) });
    }
    const library = b.option([]const u8, "migrate-antfly-lib-dir", "Directory containing a built libantfly (build capi first)") orelse (b.root.joinString(b.allocator, default_lib_path) catch @panic("OOM"));
    module.addLibraryPath(.{ .cwd_relative = library });
    module.addRPath(.{ .cwd_relative = library });
    @import("antfly_platform").addMacosSdkPaths(b, module, target);
    module.linkSystemLibrary("antfly", .{});
    module.linkSystemLibrary("sqlite3", .{});
    const executable = b.addExecutable(.{ .name = "antfly-migrate", .root_module = module });
    const install = b.addInstallArtifact(executable, .{});
    b.step("migrate", "Build the standalone SQLite to aflite migration tool (requires libantfly)").dependOn(&install.step);
    const tests = b.addTest(.{ .root_module = module });
    b.step("migrate-test", "Test migration plans and typed source conversions").dependOn(&b.addRunArtifact(tests).step);
    const acceptance = b.addSystemCommand(&.{"python3"});
    acceptance.addFileArg(b.path(b.fmt("{s}/tests/test_migrate.py", .{package_path})));
    acceptance.addArtifactArg(executable);
    acceptance.addArg("-v");
    acceptance.setEnvironmentVariable("ANTFLY_MIGRATE_LIB", library);
    b.step("migrate-e2e", "Run real SQLite and aflite migration acceptance tests").dependOn(&acceptance.step);
    const profile_tests = b.addSystemCommand(&.{"python3"});
    profile_tests.addFileArg(b.path(b.fmt("{s}/tests/test_codex_profile.py", .{package_path})));
    profile_tests.addArtifactArg(executable);
    profile_tests.addArg("-v");
    b.top_level_steps.get("migrate-e2e").?.step.dependOn(&profile_tests.step);
}
