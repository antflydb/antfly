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
const AntflyRootImports = @import("../../../build_support/antfly/imports.zig").AntflyRootImports;
const runtime = @import("runtime.zig");

/// These sources require separately linked owner suites, not source shards.
pub const test_sources = [_][]const u8{
    "kernel_owner_test.zig",
    "kernel_owner_provisioned_source_test.zig",
    "enrichment_compute_test.zig",
    "kernel_owner_handoff_reopen_test.zig",
};

/// Exercise the same storage archive linked by serving and embedded consumers.
/// Root composition attaches these runs to the owning test aggregates.
pub const Result = struct { runs: []const *std.Build.Step.Run, benchmark: *std.Build.Step.Compile };

pub fn add(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    imports: AntflyRootImports,
    vopr: *std.Build.Module,
    lmdb_engine: *std.Build.Module,
    artifacts: [@typeInfo(runtime.RuntimeLibraryUnit).@"enum".field_names.len]?*std.Build.Step.Compile,
) Result {
    const owner_filter = b.option([]const u8, "storage-owner-test-filter", "Compile and run one matching storage owner test subset");
    const source_filter = b.option([]const u8, "storage-owner-source-test-filter", "Compile and run matching provisioned storage owner tests");
    const test_metadata = @import("../../../lib/build_info/build_support.zig").create(b, .{
        .root = b.path("lib/build_info"),
        .target = target,
        .optimize = optimize,
        .version = "test",
    });
    var runs: [5]*std.Build.Step.Run = undefined;
    var additional_runs: std.ArrayList(*std.Build.Step.Run) = .empty;
    var benchmark: *std.Build.Step.Compile = undefined;
    inline for (.{ "storage_kernel_owner_test_root.zig", "storage_kernel_provisioned_source_test_root.zig", "enrichment_compute_test_root.zig", "storage_boundary_bench.zig" }, 0..) |root, index| {
        const module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/" ++ root),
            .target = target,
            .optimize = optimize,
        });
        if (index == 0) module.addImport("vopr", vopr);
        var owner_imports = imports;
        owner_imports.boundary_profile = if (index == 2) .enrichment else .owner;
        if (index == 2)
            owner_imports.configureEnrichment(b, module, true)
        else
            owner_imports.configureStorage(b, module, true);
        owner_imports.storage_boundary.configureProfile(module, true, true, owner_imports.boundary_profile);
        if (comptime index < 3) {
            const tests = @import("linked_tests.zig").add(b, .{
                .name = if (index == 0) "storage-owner-tests" else if (index == 1) "storage-owner-source-tests" else "storage-owner-enrichment-tests",
                .root_module = module,
                .filters = if (index == 0 and owner_filter != null)
                    &.{owner_filter.?}
                else if (index == 1 and source_filter != null)
                    &.{source_filter.?}
                else if (index == 1)
                    &.{
                        b.fmt("storage.{s}.", .{std.fs.path.stem(test_sources[index])}),
                        "compiled structural reconciliation publishes",
                        "api.kernel_owner_source.",
                        "compiled startup catch-up retains exact retries",
                        "compiled owner coordinated ttl admission",
                        "source owner deadlines normalize executor clock epochs",
                        "distributed txn native lookup read-index rejects leader loss before storage execution",
                    }
                else
                    &.{b.fmt("storage.{s}.", .{std.fs.path.stem(test_sources[index])})},
                .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
            });
            tests.executable.root_module.addObject(test_metadata.object);
            runs[index] = tests.run(b);
            if (index == 2) {
                b.step("enrichment-compute-owner-test", "Run the compiled media owner contract regressions").dependOn(&runs[index].step);
                tests.executable.root_module.linkLibrary(artifacts[@backingInt(runtime.RuntimeLibraryUnit.enrichment_compute)].?);
            } else {
                inline for (.{ .storage_kernel, .local_query, .enrichment_compute, .inference }) |unit|
                    tests.executable.root_module.linkLibrary(artifacts[@backingInt(@as(runtime.RuntimeLibraryUnit, unit))].?);
            }
        } else {
            test_metadata.link(module);
            benchmark = b.addExecutable(.{
                .name = "storage_boundary_bench",
                .root_module = module,
                // The physical engine has its own reservation; account for
                // this independently compiled boundary consumer as well.
                .max_rss = 4 * 1024 * 1024 * 1024,
            });
        }
        if (index == 3) {
            inline for (.{ .storage_kernel, .local_query, .enrichment_compute, .inference }) |unit|
                module.linkLibrary(artifacts[@backingInt(@as(runtime.RuntimeLibraryUnit, unit))].?);
        }
    }
    const pdf_client = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/storage/pdf_session_client.zig"),
        .target = target,
        .optimize = optimize,
    });
    pdf_client.addImport("enrichment_compute_abi", imports.storage_boundary.enrichment);
    pdf_client.addImport("kernel_error_identity", imports.storage_boundary.identity);
    if (imports.image_native_contract) |contract| {
        const step = b.step("compiled-image-native-test", "Verify source and compiled image transport including retained VP8");
        inline for (.{ false, true }) |native| {
            const client = b.createModule(.{ .root_source_file = b.path("pkg/antfly/src/image_native_owner_test_root.zig"), .target = target, .optimize = optimize, .link_libc = true });
            client.addImport("image", if (native) imports.image_facade.? else imports.image_implementation.?);
            client.addImport("media", b.createModule(.{ .root_source_file = b.path("lib/media/src/mod.zig"), .target = target, .optimize = optimize }));
            client.addImport("image_native_contract", contract);
            if (native) client.addImport("fixtures", b.createModule(.{ .root_source_file = b.path("lib/image/src/test_support.zig"), .target = target, .optimize = optimize }));
            const options = b.addOptions();
            options.addOption([]const u8, "mode", if (native) "native" else "source");
            client.addOptions("test_options", options);
            @import("../../../build_support/embedded/source_owner.zig").registerStorageIndependent(client);
            const tests = @import("linked_tests.zig").add(b, .{ .name = if (native) "image-native-owner-tests" else "image-source-oracle-tests", .root_module = client, .filters = &.{"compiled image native"}, .max_rss = 2 * 1024 * 1024 * 1024 });
            if (native) tests.executable.root_module.linkLibrary(artifacts[@backingInt(runtime.RuntimeLibraryUnit.enrichment_compute)].?);
            const run = tests.run(b);
            run.has_side_effects = true;
            additional_runs.append(b.allocator, run) catch @panic("OOM");
            step.dependOn(&run.step);
        }
    }
    if (imports.pdf_native_contract) |contract| {
        const client = b.createModule(.{ .root_source_file = b.path("pkg/antfly/src/pdf_native_owner_test_root.zig"), .target = target, .optimize = optimize });
        client.link_libc = true;
        client.addImport("pdf", imports.pdf);
        client.addImport("pdf_native_contract", contract);
        @import("../../../build_support/embedded/source_owner.zig").registerStorageIndependent(client);
        const tests = @import("linked_tests.zig").add(b, .{ .name = "pdf-native-owner-tests", .root_module = client, .filters = &.{"compiled PDF native"}, .max_rss = 2 * 1024 * 1024 * 1024 });
        tests.executable.root_module.linkLibrary(artifacts[@backingInt(runtime.RuntimeLibraryUnit.enrichment_compute)].?);
        const run = tests.run(b);
        additional_runs.append(b.allocator, run) catch @panic("OOM");
        b.step("compiled-pdf-native-test", "Test retained analysis, fork caches, raster and bounded PDF batches").dependOn(&run.step);
    }
    const pdf_consumer = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/opaque_pdf_session_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    pdf_consumer.addImport("pdf_session_client", pdf_client);
    pdf_consumer.addImport("pdf_backend_client", imports.storage_boundary.pdf_backend_client);
    pdf_consumer.addImport("enrichment_compute_abi", imports.storage_boundary.enrichment);
    const pdf_tests = @import("linked_tests.zig").add(b, .{
        .name = "opaque-pdf-session-tests",
        .root_module = pdf_consumer,
        .filters = &.{ "opaque PDF session", "opaque PDF backend" },
        .max_rss = 1024 * 1024 * 1024,
    });
    pdf_tests.executable.root_module.linkLibrary(artifacts[@backingInt(runtime.RuntimeLibraryUnit.enrichment_compute)].?);
    runs[4] = pdf_tests.run(b);
    b.step("opaque-pdf-session-test", "Test retained PDF state from a source-free client").dependOn(&runs[4].step);
    b.step("opaque-pdf-consumer-compile", "Compile the PDF client without building its implementation owner").dependOn(&pdf_tests.object.step);
    const template_module = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/template_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    var template_imports = imports;
    template_imports.shared_pdf_templates = true;
    const template_options = @import("../../../build_support/embedded/storage.zig").makeRootBuildOptions(b, false, false, true, false, false);
    template_options.addOption(bool, "shared_pdf_templates", true);
    template_imports.build_options = template_options;
    template_imports.configureEmbedded(b, template_module, true);
    @import("../../../build_support/embedded/source_owner.zig").registerStorageIndependent(template_module);
    const template_tests = @import("linked_tests.zig").add(b, .{
        .name = "compiled-template-pdf-tests",
        .root_module = template_module,
        .filters = &.{"compiled template PDF"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    template_tests.executable.root_module.linkLibrary(artifacts[@backingInt(runtime.RuntimeLibraryUnit.enrichment_compute)].?);
    const template_run = template_tests.run(b);
    additional_runs.append(b.allocator, template_run) catch @panic("OOM");
    b.step("compiled-template-pdf-test", "Check compiled template PDF default, exact errors and backend overrides").dependOn(&template_run.step);
    const opaque_module = b.createModule(.{ .root_source_file = b.path("pkg/antfly/src/opaque_query_owner_test_root.zig"), .target = target, .optimize = optimize });
    opaque_module.link_libc = true;
    opaque_module.addImport("kernel_owner_abi", imports.storage_boundary.owner);
    opaque_module.addImport("kernel_error_identity", imports.storage_boundary.identity);
    opaque_module.addImport("local_query_client", imports.storage_boundary.query_client);
    opaque_module.addImport("antfly_platform", imports.platform);
    const opaque_tests = @import("linked_tests.zig").add(b, .{
        .name = "opaque-query-owner-tests",
        .root_module = opaque_module,
        .filters = &.{"opaque query owner"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    opaque_tests.executable.root_module.addObject(test_metadata.object);
    inline for (.{ .storage_kernel, .local_query, .enrichment_compute, .inference }) |unit|
        opaque_tests.executable.root_module.linkLibrary(artifacts[@backingInt(@as(runtime.RuntimeLibraryUnit, unit))].?);
    runs[3] = opaque_tests.run(b);
    b.step("opaque-query-owner-test", "Test complete queries, retained generations and controls from an opaque consumer").dependOn(&runs[3].step);
    b.step("local-query-owner-test", "Check retained-generation continuation through the production query owner").dependOn(&runs[3].step);
    b.step("opaque-query-consumer-compile", "Compile the query integration caller without compiling any physical provider").dependOn(&opaque_tests.object.step);

    const physical_module = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_kernel_owner_physical_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    physical_module.addImport("vopr", vopr);
    var physical_imports = imports;
    physical_imports.boundary_profile = .owner;
    physical_imports.configureStorage(b, physical_module, true);
    @import("../../../build_support/embedded/storage.zig").configureLmdb(b, physical_module, lmdb_engine, true);
    physical_imports.storage_boundary.configureProfile(physical_module, true, true, .owner);
    const handoff_tests = @import("linked_tests.zig").add(b, .{
        .name = "storage-owner-handoff-reopen-tests",
        .root_module = physical_module,
        .filters = &.{"storage owner handoff receipt survives shared-context hidden to public reopen"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    handoff_tests.executable.root_module.addObject(test_metadata.object);
    inline for (.{ .storage_kernel, .local_query, .enrichment_compute, .inference }) |unit|
        handoff_tests.executable.root_module.linkLibrary(artifacts[@backingInt(@as(runtime.RuntimeLibraryUnit, unit))].?);
    b.step("antfly-storage-owner-handoff-reopen-test", "Run physical shared-context owner handoff receipt reopen regression")
        .dependOn(&handoff_tests.run(b).step);
    return .{ .runs = std.mem.concat(b.allocator, *std.Build.Step.Run, &.{ &runs, additional_runs.items }) catch @panic("OOM"), .benchmark = benchmark };
}
