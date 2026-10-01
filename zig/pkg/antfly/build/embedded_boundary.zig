// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Audit the real named module graph for each embedded target/profile.
const std = @import("std");

pub fn add(b: *std.Build, module: *std.Build.Module) *std.Build.Step.Run {
    const run = b.addSystemCommand(&.{"python3"});
    run.addFileArg(b.path("tools/audit_embedded_source_boundary.py"));
    run.addArg("--project");
    run.addDirectoryArg(b.path("."));
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
        // Authored roots can be absent in a staged tree when their declared
        // module is unused. Resolve them only if the source audit reaches them.
        switch (source) {
            .src_path, .cwd_relative => run.addArg(source.getPath(b)),
            else => run.addFileArg(source),
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
