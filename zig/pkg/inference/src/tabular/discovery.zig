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

//! Walks `~/.antfly/inference/ml/<name>/tabular_model.json` and
//! registers each discovered model with the registry. Seeds the builtin
//! iris classifier from `builtin/models/iris-classifier/` on first run.

const std = @import("std");
const tabular = @import("ml_tabular");
const limits = @import("limits.zig");
const registry_mod = @import("registry.zig");

const builtin_iris_bytes = @embedFile("builtin/models/iris-classifier/tabular_model.json");

pub const Error = error{ OutOfMemory, IoError } || registry_mod.Error;

/// Scan a base directory for predictor models. The directory layout is
/// `<base>/<model-name>/tabular_model.json`. Models with safe `name`
/// metadata override the directory name; unsafe effective names are skipped.
pub fn discover(io: std.Io, alloc: std.mem.Allocator, reg: *registry_mod.Registry, base_dir: []const u8) Error!u32 {
    var found: u32 = 0;
    var dir = std.Io.Dir.cwd().openDir(io, base_dir, .{ .iterate = true }) catch return found;
    defer dir.close(io);

    var it = dir.iterate();
    while (it.next(io) catch return Error.IoError) |entry| {
        if (entry.kind != .directory) continue;
        const json_path = try std.fs.path.join(alloc, &.{ base_dir, entry.name, "tabular_model.json" });
        defer alloc.free(json_path);

        const bytes = std.Io.Dir.cwd().readFileAlloc(io, json_path, alloc, .limited(limits.max_model_json_bytes)) catch continue;
        defer alloc.free(bytes);

        var loaded = tabular.loader.parseFromSlice(alloc, bytes) catch continue;
        defer loaded.deinit();

        const md = loaded.model.metadata;
        const name = if (md.name.len > 0) md.name else entry.name;
        if (!registry_mod.isSafeName(name)) continue;
        const model_dir = try std.fs.path.join(alloc, &.{ base_dir, entry.name });
        defer alloc.free(model_dir);

        try reg.register(.{
            .name = name,
            .path = model_dir,
            .task = md.task,
            .num_features = md.num_features,
            .num_outputs = loaded.model.output.num_outputs,
            .feature_names = md.feature_names,
            .source_framework = md.source_framework,
        });
        found += 1;
    }
    return found;
}

/// Seed the builtin iris classifier into `<base>/iris-classifier/` if it
/// doesn't already exist.
pub fn seedBuiltins(io: std.Io, base_dir: []const u8) Error!void {
    const dir_path = std.fs.path.join(std.heap.page_allocator, &.{ base_dir, "iris-classifier" }) catch return Error.OutOfMemory;
    defer std.heap.page_allocator.free(dir_path);
    std.Io.Dir.cwd().createDirPath(io, dir_path) catch return Error.IoError;
    const file_path = std.fs.path.join(std.heap.page_allocator, &.{ dir_path, "tabular_model.json" }) catch return Error.OutOfMemory;
    defer std.heap.page_allocator.free(file_path);

    if (std.Io.Dir.cwd().access(io, file_path, .{})) {
        return; // already exists
    } else |_| {}

    std.Io.Dir.cwd().writeFile(io, .{ .sub_path = file_path, .data = builtin_iris_bytes }) catch return Error.IoError;
}
