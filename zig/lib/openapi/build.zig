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

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const httpx_dep = b.dependency("httpx", .{});
    const httpx_mod = httpx_dep.module("httpx");

    // Main library module
    const openapi_mod = b.createModule(.{
        .root_source_file = b.path("src/openapi.zig"),
        .target = target,
        .optimize = optimize,
    });

    // CLI executable
    const exe = b.addExecutable(.{
        .name = "openapi-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("openapi", openapi_mod);
    exe.root_module.addImport("httpx", httpx_mod);
    b.installArtifact(exe);

    const run_exe = b.addRunArtifact(exe);
    run_exe.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_exe.addArgs(args);
    }
    const run_step = b.step("run", "Run the openapi-zig code generator");
    run_step.dependOn(&run_exe.step);

    // Tests
    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/openapi.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);

    // E2E test for modular code generation
    const e2e_modular = b.addSystemCommand(&.{ "bash", "test/e2e_modular.sh" });
    e2e_modular.step.dependOn(b.getInstallStep());
    const e2e_step = b.step("e2e", "Run end-to-end tests");
    e2e_step.dependOn(&e2e_modular.step);
    e2e_step.dependOn(&run_tests.step);
}

// ─── Build helper for consumers ──────────────────────────────────────────────
//
// Usage in consumer build.zig:
//
//   const openapi_dep = b.dependency("openapi_zig", .{});
//
//   // Generate types-only module from a library spec:
//   const schema_mod = openapi_dep.builder.addOpenApiModule(b, .{
//       .spec = b.path("lib/schema/openapi.json"),
//       .package_name = "schema",
//       .generate = .{ .types = true },
//   });
//
//   // Generate types+server from the main API spec with import mappings:
//   const api_mod = openapi_dep.builder.addOpenApiModule(b, .{
//       .spec = b.path("src/api.json"),
//       .package_name = "api",
//       .generate = .{ .types = true, .server = true },
//       .import_mappings = &.{
//           .{ "schema.json", "schema" },
//           .{ "embeddings.json", "embeddings" },
//       },
//   });
//   api_mod.addImport("schema", schema_mod);
//   api_mod.addImport("embeddings", embeddings_mod);
//   api_mod.addImport("httpx", httpx_mod);

pub const OpenApiModuleOptions = struct {
    spec: std.Build.LazyPath,
    package_name: []const u8 = "api",
    generate: struct {
        types: bool = true,
        client: bool = false,
        server: bool = false,
    } = .{},
    import_mappings: []const struct { []const u8, []const u8 } = &.{},
};

/// Create a Zig module from an OpenAPI spec using the openapi-zig code generator.
///
/// This runs the openapi-zig CLI as a build step, captures the generated output
/// directory, and creates a module rooted at the generated root.zig.
///
/// The caller is responsible for adding any needed imports to the returned module
/// (httpx, external type modules referenced via import_mappings, etc.).
pub fn addOpenApiModule(dep: *std.Build.Dependency, b: *std.Build, opts: OpenApiModuleOptions) *std.Build.Module {
    const codegen = b.addRunArtifact(dep.artifact("openapi-zig"));

    codegen.addArgs(&.{ "--spec" });
    codegen.addFileArg(opts.spec);
    codegen.addArgs(&.{ "--package", opts.package_name });

    // Build --generate flag
    var gen_parts = std.ArrayListUnmanaged(u8).empty;
    var need_comma = false;
    if (opts.generate.types) {
        gen_parts.appendSlice(b.allocator, "types") catch @panic("OOM");
        need_comma = true;
    }
    if (opts.generate.client) {
        if (need_comma) gen_parts.append(b.allocator, ',') catch @panic("OOM");
        gen_parts.appendSlice(b.allocator, "client") catch @panic("OOM");
        need_comma = true;
    }
    if (opts.generate.server) {
        if (need_comma) gen_parts.append(b.allocator, ',') catch @panic("OOM");
        gen_parts.appendSlice(b.allocator, "server") catch @panic("OOM");
    }
    if (gen_parts.items.len > 0) {
        codegen.addArgs(&.{ "--generate", gen_parts.items });
    }

    // Add import mappings
    for (opts.import_mappings) |mapping| {
        const arg = std.fmt.allocPrint(b.allocator, "{s}={s}", .{ mapping[0], mapping[1] }) catch @panic("OOM");
        codegen.addArgs(&.{ "--import-mapping", arg });
    }

    codegen.addArgs(&.{ "--output" });
    const gen_dir = codegen.addOutputDirectoryArg(opts.package_name);

    return b.addModule(opts.package_name, .{
        .root_source_file = gen_dir.path(b, "root.zig"),
    });
}
