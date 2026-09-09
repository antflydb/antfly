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

//! Construct the real runtime import graph with empty external dependencies.
//! Replace only the runtime source bodies so cache regressions need no full
//! Antfly compilation. All owner-installed imports and options remain intact.
const std = @import("std");
const runtime = @import("pkg/antfly/build/runtime.zig");
const AntflyRootImports = @import("pkg/antfly/build/imports.zig").AntflyRootImports;
const codegen = @import("pkg/antfly/build/codegen.zig");
const storage = @import("pkg/antfly/build/storage.zig");

pub fn build(b: *std.Build) void {
    const sources = b.addWriteFiles();
    const empty_source = sources.add("empty.zig", "");
    const unrelated_source = sources.add("unrelated.zig", "export fn probe() u64 { return 0; }\n");
    const api_source = sources.add("api.zig",
        \\const std = @import("std");
        \\const specs = @import("antfly_openapi_specs");
        \\export fn probe() u64 {
        \\    return comptime std.hash.Wyhash.hash(0, specs.ard ++ specs.antfly ++
        \\        specs.metadata ++ specs.extensions ++ specs.auth ++ specs.inference_config);
        \\}
    );
    const stub = b.createModule(.{ .root_source_file = empty_source, .target = b.graph.host });
    const build_options = storage.makeRootBuildOptions(b, .zig, false, false, false, true, false, false, true, "cache-test");
    var imports: AntflyRootImports = undefined;
    inline for (std.meta.fields(AntflyRootImports)) |field| {
        @field(imports, field.name) = switch (field.type) {
            *std.Build.Module => stub,
            *std.Build.Step.Options => build_options,
            std.Build.ResolvedTarget => b.graph.host,
            std.Build.LazyPath => empty_source,
            bool => true,
            else => @compileError("initialize new Antfly import configuration in the cache fixture"),
        };
    }
    imports.embedded_openapi = codegen.addEmbeddedSpecs(b, .{
        .root_source_file = b.path("pkg/antfly/src/openapi/embedded_specs.zig"),
        .schema_root = b.path("schemas"),
        .public_spec = b.path("public.yaml"),
    });
    const artifacts = runtime.addRuntime(b, .{
        .target = b.graph.host,
        .optimize = .Debug,
        .strip = false,
        .link_libc = true,
        .sanitize_thread = false,
        .runtime_artifact_role = null,
        .production_build_options = build_options,
        .structlog_mod = stub,
        .platform_mod = stub,
        .hash_mod = stub,
        .production_antfly_imports = imports,
        .antfly_client_pkg_mod = stub,
        .capi_mod = b.createModule(.{ .root_source_file = empty_source }),
        .libantfly_link_mod = b.createModule(.{ .root_source_file = empty_source }),
    });
    inline for (std.meta.tags(runtime.RuntimeLibraryUnit)) |unit| {
        const artifact = artifacts.runtime_library_artifacts[@intFromEnum(unit)].?;
        artifact.root_module.root_source_file = if (unit == .api_kernel) api_source else unrelated_source;
        // The fixture compiles tiny bodies, so production memory reservations
        // would needlessly serialize it on small CI machines.
        artifact.step.max_rss = 0;
        b.getInstallStep().dependOn(&artifact.step);
    }
}
