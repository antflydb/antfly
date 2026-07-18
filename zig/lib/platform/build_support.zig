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

pub const ModuleOptions = struct {
    root_source_file: std.Build.LazyPath,
    filesystem_capacity_source_file: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    link_libc: bool,
    single_threaded: ?bool = null,
};

pub fn createModule(b: *std.Build, options: ModuleOptions) *std.Build.Module {
    return configureModule(b.createModule(createOptions(options)), options);
}

pub fn addModule(b: *std.Build, name: []const u8, options: ModuleOptions) *std.Build.Module {
    return configureModule(b.addModule(name, createOptions(options)), options);
}

fn createOptions(options: ModuleOptions) std.Build.Module.CreateOptions {
    return .{
        .root_source_file = options.root_source_file,
        .target = options.target,
        .optimize = options.optimize,
        .link_libc = options.link_libc,
        .single_threaded = options.single_threaded,
    };
}

pub fn addFilesystemCapacitySource(
    module: *std.Build.Module,
    source_file: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
) void {
    if (!filesystemCapacitySupported(target)) return;
    module.addCSourceFile(.{
        .file = source_file,
        .flags = &.{"-std=c11"},
    });
}

fn filesystemCapacitySupported(target: std.Build.ResolvedTarget) bool {
    return switch (target.result.os.tag) {
        .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => true,
        else => false,
    };
}

fn configureModule(module: *std.Build.Module, options: ModuleOptions) *std.Build.Module {
    if (options.link_libc) {
        addFilesystemCapacitySource(module, options.filesystem_capacity_source_file, options.target);
    }
    return module;
}
