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

/// Inspect authored roots during configuration. Generated files are make-phase
/// inputs and must be passed to build steps as LazyPath values.
pub fn authored(b: *std.Build, path: std.Build.LazyPath) ?[]const u8 {
    return switch (path) {
        .src_path => |source| source.owner.root.joinString(b.allocator, source.sub_path) catch @panic("OOM"),
        .dependency => |source| source.dependency.builder.root.joinString(b.allocator, source.sub_path) catch @panic("OOM"),
        .cwd_relative => |source| source,
        else => null,
    };
}

pub fn producer(b: *std.Build, path: std.Build.LazyPath) ?*std.Build.Step {
    if (path != .generated) return null;
    return b.graph.generated_files.items[@backingInt(path.generated.index)];
}
