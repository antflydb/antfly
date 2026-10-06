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
const Binding = struct { consumer: *std.Build.Module, local: *std.Build.Module };
var bindings: std.ArrayList(Binding) = .empty;

/// One local source owner per consumer profile. Source files are never copied
/// into a server root; source selection remains explicit at the module boundary.
pub fn attach(consumer: *std.Build.Module) void {
    if (consumer.import_table.contains("antfly_local_sources")) return;
    const b = consumer.owner;
    const root = consumer.root_source_file orelse return;
    const path = @import("../antfly/source_paths.zig").authored(b, root) orelse return;
    if (std.mem.indexOf(u8, path, "pkg/antfly-embedded/src/local/") != null) return;
    const local = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/source_catalog.zig"),
        .target = consumer.resolved_target,
        .optimize = consumer.optimize,
    });
    const empty = b.addOptions();
    empty.addOption([]const []const u8, "names", &.{});
    local.addOptions("antfly_local_test_sources", empty);
    consumer.addImport("antfly_local_sources", local);
    bindings.append(b.allocator, .{ .consumer = consumer, .local = local }) catch @panic("OOM");
}

/// Run after composition: late-bound options and test capabilities must be the
/// same declarations as the consumer's, rather than a second configuration.
pub fn finalize(b: *std.Build) void {
    for (bindings.items) |binding| {
        var imports = binding.consumer.import_table.iterator();
        while (imports.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.*, "antfly_source_root") or
                std.mem.eql(u8, entry.key_ptr.*, "antfly_local_sources")) continue;
            binding.local.addImport(entry.key_ptr.*, entry.value_ptr.*);
        }
        binding.local.addImport("antfly_source_root", binding.local);
        binding.local.addImport("antfly_test_source_root", binding.consumer);
        binding.local.addImport("antfly_server_test_sources", binding.consumer);
        binding.local.link_libc = binding.consumer.link_libc;
    }
    @import("../antfly/test_partitions.zig").add(b);
}

/// Cloned consumer graphs retain their own late-bound source configuration.
pub fn adopt(consumer: *std.Build.Module) void {
    const local = consumer.import_table.get("antfly_local_sources") orelse return;
    for (bindings.items) |binding| if (binding.consumer == consumer) return;
    bindings.append(consumer.owner.allocator, .{ .consumer = consumer, .local = local }) catch @panic("OOM");
}

pub fn localFor(consumer: *std.Build.Module) ?*std.Build.Module {
    for (bindings.items) |binding| if (binding.consumer == consumer) return binding.local;
    return null;
}
