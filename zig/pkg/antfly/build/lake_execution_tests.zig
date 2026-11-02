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
const Imports = @import("../../../build_support/antfly/imports.zig").AntflyRootImports;

/// A test-only provider archive proves error/executor translation. Production
/// continues to use the existing serverless owner; no new serving unit is added.
pub fn add(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize, imports: Imports, vopr: *std.Build.Module, lmdb_engine: *std.Build.Module) void {
    const source_imports = sourceImports(b, imports);
    const test_imports = @import("../../../build_support/antfly/test_support.zig").Imports{ .runtime = source_imports, .vopr = vopr, .lmdb_engine = lmdb_engine };
    const provider = b.createModule(.{ .root_source_file = b.path("pkg/antfly/src/lake_execution_provider_probe_root.zig"), .target = target, .optimize = optimize, .pic = true });
    test_imports.configure(b, provider, false, true);
    // This fixture owns source implementations rather than production storage
    // function tables. Only the final consumer supplies native C dependencies.
    const provider_options = b.addOptions();
    provider_options.addOption(bool, "shared_storage_core", false);
    provider_options.addOption(bool, "storage_engine_owner", true);
    provider.addOptions("storage_engine_options", provider_options);
    const archive = @import("../../../build_support/embedded/shared_library.zig").addProviderArchive(b, provider, "lake-execution-abi-provider-probe", 4 * 1024 * 1024 * 1024);
    const consumer = b.createModule(.{ .root_source_file = b.path("pkg/antfly/src/lake_execution_linked_test_root.zig"), .target = target, .optimize = optimize });
    test_imports.configure(b, consumer, true, true);
    const consumer_options = b.addOptions();
    consumer_options.addOption(bool, "shared_lake_execution", false);
    consumer_options.addOption(bool, "provide_lake_execution", false);
    consumer_options.addOption(bool, "external_test_provider", true);
    consumer.addOptions("lake_execution_options", consumer_options);
    const tests = @import("linked_tests.zig").add(b, .{ .name = "lake-execution-abi-integration", .root_module = consumer, .filters = &.{"external lake execution"} });
    tests.executable.root_module.linkLibrary(archive);
    imports.build_info.link(tests.executable.root_module);
    const run = tests.run(b);
    b.step("lake-execution-abi-integration-test", "Run complete lake builds across independently compiled error and executor domains").dependOn(&run.step);
}

/// The fixture owns raw media implementations transitively. A global product
/// toggle may have installed PDF/image facades inside inference/readers before
/// this source root is composed; restore one shared source graph for all fields.
pub fn sourceImports(b: *std.Build, original: Imports) Imports {
    const graph = @import("../../../build_support/module_rewrite.zig");
    var seen: std.AutoHashMap(*std.Build.Module, void) = .init(b.allocator);
    var replacements: std.ArrayList(graph.Replacement) = .empty;
    inline for (@typeInfo(Imports).@"struct".field_names) |field| {
        const T = @FieldType(Imports, field);
        if (T == *std.Build.Module) collectFacades(b, @field(original, field), original, &seen, &replacements);
        if (T == ?*std.Build.Module) if (@field(original, field)) |module| collectFacades(b, module, original, &seen, &replacements);
    }
    var source: graph.Rewriter = .{ .b = b, .replacements = replacements.items, .on_copy = @import("../../../build_support/embedded/source_owner.zig").forkContext };
    var result = original;
    inline for (@typeInfo(Imports).@"struct".field_names) |field| {
        const T = @FieldType(Imports, field);
        if (T == *std.Build.Module) @field(result, field) = source.rewrite(@field(original, field));
        if (T == ?*std.Build.Module) if (@field(original, field)) |module| {
            @field(result, field) = source.rewrite(module);
        };
    }
    inline for (@typeInfo(@TypeOf(result.storage_boundary)).@"struct".field_names) |field| {
        if (@FieldType(@TypeOf(result.storage_boundary), field) == *std.Build.Module)
            @field(result.storage_boundary, field) = source.rewrite(@field(original.storage_boundary, field));
    }
    const options = b.addOptions();
    options.addOption(bool, "shared_lake_execution", false);
    options.addOption(bool, "provide_lake_execution", false);
    result.lake_execution_options = options.createModule();
    result.lake_provider_options = result.lake_execution_options;
    result.shared_lake_execution = false;
    return result;
}
fn collectFacades(b: *std.Build, module: *std.Build.Module, original: Imports, seen: *std.AutoHashMap(*std.Build.Module, void), replacements: *std.ArrayList(@import("../../../build_support/module_rewrite.zig").Replacement)) void {
    if ((seen.getOrPut(module) catch @panic("OOM")).found_existing) return;
    if (module.root_source_file) |root| if (@import("../../../build_support/antfly/source_paths.zig").authored(b, root)) |path| {
        if (std.mem.endsWith(u8, path, "/storage/pdf_facade.zig")) {
            replacements.append(b.allocator, .{ .source = module, .target = original.pdf_implementation orelse original.pdf }) catch @panic("OOM");
            return;
        }
        if (std.mem.endsWith(u8, path, "/storage/image_facade.zig")) {
            replacements.append(b.allocator, .{ .source = module, .target = original.image_implementation orelse original.image }) catch @panic("OOM");
            return;
        }
    };
    for (module.import_table.values()) |child| collectFacades(b, child, original, seen, replacements);
}
