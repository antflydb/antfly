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

//! Install only Apache product and third-party notices for Lite artifacts.
const std = @import("std");

pub fn installApache(b: *std.Build, repo_root: std.Build.LazyPath, name: []const u8, directory: []const u8) *std.Build.Step {
    const step = b.step(b.fmt("licenses-{s}", .{name}), b.fmt("Install {s} licenses and source map", .{name}));
    const primary = b.addInstallFile(repo_root.path(b, "LICENSES/Apache-2.0.txt"), b.fmt("{s}/LICENSE", .{directory}));
    step.dependOn(&primary.step);
    for ([_][]const u8{
        "LICENSES/Apache-2.0.txt",
        "THIRD_PARTY_NOTICES.md",
        "scripts/apache_engine_files.txt",
        "scripts/source_license_roots.json",
        "scripts/embedded_asset_licenses.json",
    }) |file| {
        const copy = b.addInstallFile(repo_root.path(b, file), b.fmt("{s}/{s}", .{ directory, file }));
        step.dependOn(&copy.step);
    }
    const upstream = b.addInstallDirectory(.{
        .source_dir = repo_root.path(b, "LICENSES/third-party"),
        .install_dir = .prefix,
        .install_subdir = b.fmt("{s}/LICENSES/third-party", .{directory}),
    });
    step.dependOn(&upstream.step);
    return step;
}

pub fn build(b: *std.Build) void {
    _ = b;
}
