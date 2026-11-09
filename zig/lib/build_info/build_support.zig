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

pub const BuildInfo = struct {
    module: *std.Build.Module,
    object: *std.Build.Step.Compile,
    test_object: *std.Build.Step.Compile,

    /// Call only for final release-metadata consumers. Runtime archives and
    /// unit tests import `module` directly and must not depend on `object`.
    pub fn link(self: BuildInfo, module: *std.Build.Module) void {
        linkObject(module, self.module, self.object);
    }

    /// Independently compiled providers still call the metadata ABI in tests.
    /// Resolve it to a stable value without importing the release incarnation.
    pub fn linkTest(self: BuildInfo, module: *std.Build.Module) void {
        linkObject(module, self.module, self.test_object);
    }

    fn linkObject(module: *std.Build.Module, interface: *std.Build.Module, object: *std.Build.Step.Compile) void {
        // A link-only executable must have no Zig import root: otherwise the
        // first imported module is compiled as main (and owns startup/TLS).
        if (module.root_source_file != null) module.addImport("build_info", interface);
        // Composition layers can share a final module. Deduplicate within
        // that module, while each independent image still owns its metadata.
        for (module.link_objects.items) |input| {
            if (input == .other_step and input.other_step == object) return;
        }
        module.addObject(object);
    }
};

pub fn create(b: *std.Build, options: struct {
    root: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    version: []const u8,
}) BuildInfo {
    const metadata = b.addOptions();
    metadata.addOption([]const u8, "version", options.version);
    const value = b.createModule(.{
        .root_source_file = options.root.path(b, "src/value.zig"),
        .target = options.target,
        .optimize = options.optimize,
        .pic = true,
    });
    value.addOptions("metadata", metadata);
    const test_metadata = b.addOptions();
    test_metadata.addOption([]const u8, "version", "test");
    const test_value = b.createModule(.{
        .root_source_file = options.root.path(b, "src/value.zig"),
        .target = options.target,
        .optimize = options.optimize,
        .pic = true,
    });
    test_value.addOptions("metadata", test_metadata);
    return .{
        .module = b.createModule(.{ .root_source_file = options.root.path(b, "src/root.zig") }),
        .object = b.addObject(.{ .name = "antfly-build-info", .root_module = value }),
        .test_object = b.addObject(.{ .name = "antfly-build-info-test", .root_module = test_value }),
    };
}
