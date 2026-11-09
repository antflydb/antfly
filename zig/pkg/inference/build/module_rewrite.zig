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

//! Fork only modules affected by explicit implementation/client substitutions.
//! Original import tables and platform/link configuration remain independent.
const std = @import("std");
pub const Replacement = struct { source: *std.Build.Module, target: *std.Build.Module };
pub const Rewriter = struct {
    b: *std.Build,
    replacements: []const Replacement,
    on_copy: ?*const fn (*std.Build.Module, *std.Build.Module) void = null,
    affected: std.AutoHashMapUnmanaged(*std.Build.Module, bool) = .{},
    copies: std.AutoHashMapUnmanaged(*std.Build.Module, *std.Build.Module) = .{},

    fn target(self: *const Rewriter, source: *std.Build.Module) ?*std.Build.Module {
        for (self.replacements) |item| if (item.source == source and item.source != item.target) return item.target;
        return null;
    }
    fn collect(self: *Rewriter, source: *std.Build.Module) void {
        if (self.affected.contains(source)) return;
        self.affected.put(self.b.allocator, source, self.target(source) != null) catch @panic("OOM");
        if (self.target(source) != null) return;
        for (source.import_table.values()) |child| self.collect(child);
    }
    pub fn rewrite(self: *Rewriter, source: *std.Build.Module) *std.Build.Module {
        self.collect(source);
        var changed = true;
        while (changed) {
            changed = false;
            var entries = self.affected.iterator();
            while (entries.next()) |entry| {
                if (entry.value_ptr.*) continue;
                for (entry.key_ptr.*.import_table.values()) |child| {
                    if (self.affected.get(child) orelse false) {
                        entry.value_ptr.* = true;
                        changed = true;
                        break;
                    }
                }
            }
        }
        return self.copy(source);
    }
    fn copy(self: *Rewriter, source: *std.Build.Module) *std.Build.Module {
        if (self.target(source)) |replacement| return replacement;
        if (!(self.affected.get(source) orelse false)) return source;
        if (self.copies.get(source)) |existing| return existing;
        const out = self.b.allocator.create(std.Build.Module) catch @panic("OOM");
        out.* = source.*;
        out.import_table = .empty;
        out.cached_graph = .{ .modules = &.{}, .names = &.{} };
        // Subsequent entrypoint configuration must not mutate source modules.
        inline for (.{ "c_macros", "include_dirs", "lib_paths", "rpaths", "link_objects" }) |field|
            @field(out, field) = @field(source, field).clone(self.b.allocator) catch @panic("OOM");
        out.frameworks = source.frameworks.clone(self.b.allocator) catch @panic("OOM");
        self.copies.put(self.b.allocator, source, out) catch @panic("OOM");
        var imports = source.import_table.iterator();
        while (imports.next()) |entry| out.addImport(entry.key_ptr.*, self.copy(entry.value_ptr.*));
        if (self.on_copy) |callback| callback(source, out);
        return out;
    }
};
