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

//! Audit the real named module graph for each embedded target/profile.
const std = @import("std");

fn dependencyOwned(b: *std.Build, source: std.Build.LazyPath) bool {
    const owner = switch (source) {
        .src_path => |path| path.owner,
        .generated => |path| b.graph.generated_files.items[@backingInt(path.index)].owner,
        .dependency => |path| path.dependency.builder,
        // Host paths have no dependency provenance; audit them conservatively.
        .cwd_relative, .relative => return false,
    };
    const project_root = b.root.subPathOpt() orelse return false;
    const owner_root = owner.root.subPathOpt() orelse return false;
    const relative = std.fs.path.relative(b.allocator, project_root, null, project_root, owner_root) catch @panic("OOM");
    return std.fs.path.isAbsolute(relative) or std.mem.eql(u8, relative, "..") or std.mem.startsWith(u8, relative, "../") or std.mem.startsWith(u8, relative, "..\\");
}

pub fn add(b: *std.Build, module: *std.Build.Module) *std.Build.Step.Run {
    const run = b.addSystemCommand(&.{"python3"});
    run.addFileArg2(b.path("tools/audit_embedded_source_boundary.py"), .{ .make_absolute = true });
    run.addArg("--project");
    run.addDirectoryArg2(b.path("."), .{ .make_absolute = true });
    run.addArgs(&.{ "--entry-module", "M0", "--target-os", @tagName(module.resolved_target.?.result.os.tag) });
    var modules: std.ArrayList(*std.Build.Module) = .empty;
    var ids = std.AutoHashMap(*std.Build.Module, usize).init(b.allocator);
    modules.append(b.allocator, module) catch @panic("OOM");
    ids.put(module, 0) catch @panic("OOM");
    var cursor: usize = 0;
    while (cursor < modules.items.len) : (cursor += 1) {
        const current = modules.items[cursor];
        const source = current.root_source_file orelse @panic("embedded module must have a source owner");
        run.addArgs(&.{ "--module", b.fmt("M{d}", .{cursor}) });
        if (dependencyOwned(b, source))
            run.addArgs(&.{ "--external-module", b.fmt("M{d}", .{cursor}) });
        // Authored roots can be absent in a staged tree when their declared
        // module is unused. Resolve them only if the source audit reaches them.
        switch (source) {
            .src_path => |path| run.addArg(path.owner.root.joinString(b.allocator, path.sub_path) catch @panic("OOM")),
            .cwd_relative => |path| run.addArg(path),
            else => run.addFileArg2(source, .{ .make_absolute = true }),
        }
        var imports = current.import_table.iterator();
        while (imports.next()) |import| {
            const dependency = import.value_ptr.*;
            const entry = ids.getOrPut(dependency) catch @panic("OOM");
            if (!entry.found_existing) {
                entry.value_ptr.* = modules.items.len;
                modules.append(b.allocator, dependency) catch @panic("OOM");
            }
            run.addArgs(&.{ "--module-import", b.fmt("M{d}", .{cursor}), import.key_ptr.*, b.fmt("M{d}", .{entry.value_ptr.*}) });
        }
    }
    return run;
}
