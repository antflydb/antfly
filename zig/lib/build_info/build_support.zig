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

pub const BuildInfo = struct {
    module: *std.Build.Module,
    object: *std.Build.Step.Compile,

    /// Call only for final release-metadata consumers. Runtime archives and
    /// unit tests import `module` directly and must not depend on `object`.
    pub fn link(self: BuildInfo, module: *std.Build.Module) void {
        module.addImport("build_info", self.module);
        module.addObject(self.object);
    }
};

pub fn create(b: *std.Build, options: struct {
    root: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
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
    return .{
        .module = b.createModule(.{ .root_source_file = options.root.path(b, "src/root.zig") }),
        .object = b.addObject(.{ .name = "antfly-build-info", .root_module = value }),
    };
}
