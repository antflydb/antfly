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

//! Run the real root composition, assert transitive ownership contracts, then
//! replace only expensive runtime bodies. External modules, options, generated
//! files, C sources, and final link edges all remain production declarations.
const std = @import("std");
const project = @import("project_build.zig");
const runtime = @import("pkg/antfly/build/runtime.zig");
const profiles = @import("tools/fixtures/build_profiles.zig");
const collectSteps = profiles.collectSteps;

pub fn build(b: *std.Build) void {
    const artifacts = project.create(b) orelse return;
    const inference = artifacts.inference;
    if (inference.inference_audio_mod.import_table.contains("build_options"))
        @panic("audio depends on inference build options");
    if (inference.inference_tokenizer_mod.import_table.get("protobuf") != inference.protobuf_mod or
        inference.sentencepiece_proto_mod.import_table.get("protobuf") != inference.protobuf_mod or
        inference.inference_hf_tokenizer_mod.import_table.get("inference_tokenizer") != inference.inference_tokenizer_mod)
        @panic("tokenizer dependencies do not share the configured modules");
    const sources = b.addWriteFiles();
    var steps = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    var modules = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
    for (b.top_level_steps.values()) |top| collectSteps(&top.step, &steps, &modules);
    const host_tools = b.step("cache-host-tools", "Compile the actual host generators");
    const openapi = b.step("cache-openapi", "Exercise the actual schema joins and their discovered inputs");
    const unit_tests = b.step("cache-unit-tests", "Exercise actual test imports with stable metadata");
    unit_tests.dependOn(&b.addRunArtifact(artifacts.runtime.antfly_main_tests).step);
    var template: ?*std.Build.Step.Compile = null;
    var vopr_test_found = false;
    var lmdb_test_found = false;
    var pjrt_test_found = false;
    var host_count: usize = 0;
    var test_count: usize = 0;
    var iterator = steps.keyIterator();
    while (iterator.next()) |entry| {
        if (entry.*.cast(std.Build.Step.Run)) |run| {
            if (std.mem.eql(u8, run.step.name, "run uv (openapi.public.joined.yaml)") or
                std.mem.eql(u8, run.step.name, "run uv (openapi.public.prefixed.yaml)"))
                openapi.dependOn(&run.step);
        }
        const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
        profiles.check(artifact);
        profiles.addInferenceWasmProbe(b, artifact);
        profiles.addBenchmarkProbe(b, artifact);
        pjrt_test_found = profiles.addPjrtQualificationProbe(b, artifact) or pjrt_test_found;
        const arch = artifact.root_module.resolved_target.?.result.cpu.arch;
        if (arch == .wasm32 or arch == .wasm64) {
            var wasm_modules = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
            inspectWasmProfile(artifact.root_module, artifact.root_module.resolved_target.?, &wasm_modules);
        }
        if (artifact.kind.isTest()) {
            var seen = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
            rejectMetadata(artifact.root_module, inference.build_info_object, &seen);
            test_count += 1;
        }
        if (artifact.root_module.root_source_file) |source| switch (source) {
            .src_path => |path| {
                if (artifact.kind.isTest() and artifact.filters.len == 0 and std.mem.endsWith(u8, path.sub_path, "/storage/lmdb.zig")) {
                    artifact.root_module.root_source_file = b.addWriteFiles().add("lmdb_test.zig",
                        \\test "LMDB cache probe" {
                        \\    const lmdb = @import("lmdb_engine");
                        \\    try @import("std").testing.expectEqualStrings(@import("build_options").lmdb_backend, lmdb.selected_backend_name);
                        \\    @import("std").debug.print("LMDB_PROBE {s} {} {d}\n", .{lmdb.selected_backend_name, lmdb.cache_test_evented, lmdb.cache_test_revision});
                        \\}
                    );
                    artifact.filters = &.{"LMDB cache probe"};
                    b.step("cache-lmdb-tests", "Exercise actual LMDB consumer imports and options").dependOn(&b.addRunArtifact(artifact).step);
                    lmdb_test_found = true;
                }
                if (artifact.kind.isTest() and std.mem.endsWith(u8, path.sub_path, "/api_http_runtime_test_root.zig")) {
                    // Keep the actual API test's imports, flags, and runner.
                    artifact.root_module.root_source_file = sources.add("vopr_test.zig",
                        \\test "VOPR cache probe" {
                        \\    const revision = @import("vopr").cache_test_revision;
                        \\    try @import("std").testing.expect(revision > 0);
                        \\    @import("std").debug.print("VOPR_REVISION {d}\n", .{revision});
                        \\}
                    );
                    artifact.filters = &.{"VOPR cache probe"};
                    b.step("cache-vopr-tests", "Exercise actual simulation test imports").dependOn(&b.addRunArtifact(artifact).step);
                    vopr_test_found = true;
                }
                if (std.mem.endsWith(u8, path.sub_path, "/template_test_root.zig")) template = artifact;
                if (artifact.kind.isTest() and std.mem.endsWith(u8, path.sub_path, "/lite_main.zig"))
                    unit_tests.dependOn(&b.addRunArtifact(artifact).step);
            },
            else => {},
        };
        if (std.mem.eql(u8, artifact.name, "antfly-inference-audio-bench") and
            artifact.root_module.import_table.contains("build_options"))
            @panic("audio benchmark depends on inference build options");
        for ([_][]const u8{ "openapi-zig", "antfly-quant-kernel-codegen", "protoc-zig", "yacc-zig" }) |name| {
            if (!std.mem.eql(u8, artifact.name, name)) continue;
            // yacc-zig also has an installed, product-configured executable.
            // Select the instance actually used for SQL generation.
            if (std.mem.eql(u8, name, "yacc-zig") and !isSqlGenerator(b, artifact)) continue;
            if (artifact.root_module.optimize != .ReleaseSafe or
                !artifact.root_module.resolved_target.?.query.eql(b.graph.host.query))
                std.debug.panic("{s} inherits product configuration", .{name});
            if (artifact.root_module.import_table.contains("build_options"))
                std.debug.panic("{s} inherits backend options", .{name});
            host_tools.dependOn(&artifact.step);
            host_count += 1;
        }
    }
    profiles.addDataToolChecks(b, &steps);
    profiles.addAssetToolChecks(b, &steps);
    if (host_count != 4 or test_count == 0 or !vopr_test_found or !lmdb_test_found or !pjrt_test_found or openapi.dependencies.items.len != 2) @panic("cache fixture did not inspect the expected production graph");
    const wasm = artifacts.wasm;
    inline for (.{ .{ "httpx_profile", "lib/httpx/src/httpx.zig" }, .{ "json_profile", "lib/json/src/mod.zig" } }) |probe| {
        var visited = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
        const module = findSourceModule(wasm.root_module, probe[1], &visited) orelse @panic("WASM runtime dependency missing");
        wasm.root_module.addImport(probe[0], module);
    }
    // Keep the actual runtime import graph. Only replace its expensive entry body;
    // the Python test adds a builtin-mode probe to the real HTTPX/JSON sources.
    wasm.root_module.root_source_file = sources.add("wasm_profile.zig",
        \\export fn profile_ok() void {
        \\    comptime {
        \\        if (@import("httpx_profile").cache_test_profile != .ReleaseSafe or
        \\            @import("json_profile").cache_test_profile != .ReleaseSafe)
        \\            @compileError("WASM dependencies must use ReleaseSafe");
        \\    }
        \\}
    );
    b.step("cache-wasm", "Check production WASM profiles and cache independence").dependOn(&wasm.step);
    const template_tests = template orelse @panic("missing Antfly template suite");
    template_tests.root_module.root_source_file = sources.add("template_test.zig",
        \\test "unit metadata is stable without a release object" {
        \\    try @import("std").testing.expectEqualStrings("test", @import("build_info").version());
        \\}
    );
    unit_tests.dependOn(&b.addRunArtifact(template_tests).step);

    inline for (.{ .{ "cache-product-options", inference.build_options_mod }, .{ "cache-qualification-options", inference.qualification_build_options_mod } }) |probe| {
        const executable = b.addExecutable(.{
            .name = probe[0],
            .root_module = b.createModule(.{
                .root_source_file = sources.add("options_probe.zig",
                    \\pub fn main() void {
                    \\    const options = @import("options");
                    \\    @import("std").debug.print("OPTIONS_PROBE {s} {s} {s} {}\n", .{
                    \\        options.cuda_artifacts, options.cuda_libraries, options.wasm_memory_model, options.enable_webgpu,
                    \\    });
                    \\}
                ),
                .target = b.graph.host,
                .imports = &.{.{ .name = "options", .module = probe[1] }},
            }),
        });
        b.step(probe[0], "Read actual configured backend options").dependOn(&b.addRunArtifact(executable).step);
    }
    inline for (std.meta.tags(runtime.RuntimeLibraryUnit)) |unit| {
        const artifact = artifacts.runtime.runtime_library_artifacts[@intFromEnum(unit)].?;
        var seen = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
        inspect(artifact.root_module, unit, artifacts.inference.build_info_object, &seen);
        if (unit == .distributed) artifact.root_module.addImport("cache_lite_capabilities", b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/storage/lite/capabilities.zig"),
            .target = artifact.root_module.resolved_target,
            .optimize = artifact.root_module.optimize,
            .imports = &.{.{ .name = "antfly_lite_options", .module = artifact.root_module.import_table.get("antfly_lite_options").? }},
        }));
        const expression = if (unit == .api_kernel)
            "@import(\"antfly_hash\").Adler32.hash(\"cache probe\") ^ std.hash.Wyhash.hash(0, specs.ard) ^ std.hash.Wyhash.hash(0, specs.antfly) ^ " ++
                "std.hash.Wyhash.hash(0, specs.metadata) ^ std.hash.Wyhash.hash(0, specs.extensions) ^ " ++
                "std.hash.Wyhash.hash(0, specs.auth) ^ std.hash.Wyhash.hash(0, specs.inference_config)"
        else if (unit == .inference)
            "@import(\"antfly_hash\").Adler32.hash(\"cache probe\") ^ @sizeOf(@import(\"inference_server\").execution_control.Cancellation)"
        else if (unit == .distributed)
            "@import(\"antfly_hash\").Adler32.hash(\"cache probe\") ^ @intFromBool(@import(\"cache_lite_capabilities\").capabilitiesForProfile(.native).local_inference_runtime)"
        else
            "@import(\"antfly_hash\").Adler32.hash(\"cache probe\")";
        artifact.root_module.root_source_file = sources.add(b.fmt("{s}.zig", .{@tagName(unit)}), b.fmt(
            "const std = @import(\"std\");\n{s}export fn probe_{s}() u64 {{ return {s}; }}\n",
            .{ if (unit == .api_kernel) "const specs = @import(\"antfly_openapi_specs\");\n" else "", @tagName(unit), expression },
        ));
        artifact.step.max_rss = 0;
    }
    artifacts.runtime.antfly_main.root_module.root_source_file = sources.add("main.zig",
        \\const std = @import("std");
        \\extern fn probe_cli() u64;
        \\extern fn probe_distributed() u64;
        \\extern fn probe_serverless() u64;
        \\extern fn probe_inference() u64;
        \\extern fn probe_api_kernel() u64;
        \\pub fn main() void {
        \\    std.debug.print("CACHE_PROBE {s} {x} {x} {x} {x} {x}\n", .{
        \\        @import("build_info").version(), probe_cli(), probe_distributed(),
        \\        probe_serverless(), probe_inference(), probe_api_kernel(),
        \\    });
        \\}
    );
    b.step("cache-probe", "Exercise production archive and final-link cache boundaries").dependOn(&b.addRunArtifact(artifacts.runtime.antfly_main).step);

    const tokenizer = b.addExecutable(.{
        .name = "cache-tokenizer",
        .root_module = b.createModule(.{
            .root_source_file = sources.add("tokenizer.zig",
                \\const std = @import("std");
                \\pub fn main() void {
                \\    std.debug.print("TOKENIZER_PROBE {x}\n", .{std.hash.Wyhash.hash(0, @import("data").tokenizer_json)});
                \\}
            ),
            .target = b.graph.host,
            .imports = &.{.{ .name = "data", .module = artifacts.inference.inference_fixed_tokenizer_data_mod }},
        }),
    });
    b.step("cache-tokenizer", "Read the actual generated tokenizer data").dependOn(&b.addRunArtifact(tokenizer).step);

    const identities = artifacts.inference.identities;
    if (identities.metal != null or identities.cuda != null) {
        const identity_probe = b.addExecutable(.{
            .name = "cache-identity",
            .root_module = b.createModule(.{
                .root_source_file = sources.add("identity.zig", b.fmt(
                    "const std = @import(\"std\"); pub fn main() void {{{s}{s}}}\n",
                    .{
                        if (identities.metal != null) "std.debug.print(\"METAL_IDENTITY {s} {s}\\n\", .{ @import(\"metal_jit_identity\").baseline, @import(\"metal_jit_identity\").qualification });" else "",
                        if (identities.cuda != null) "std.debug.print(\"CUDA_IDENTITY {s} {s} {s}\\n\", .{ @import(\"cuda_jit_identity\").baseline, @import(\"cuda_jit_identity\").qualification, @import(\"cuda_jit_identity\").dispatch });" else "",
                    },
                )),
                .target = b.graph.host,
            }),
        });
        identities.addImports(identity_probe.root_module);
        b.step("cache-identity", "Read enabled backends' actual source identities").dependOn(&b.addRunArtifact(identity_probe).step);
    }
}

fn findSourceModule(module: *std.Build.Module, suffix: []const u8, seen: *std.AutoHashMap(*std.Build.Module, void)) ?*std.Build.Module {
    if ((seen.getOrPut(module) catch @panic("OOM")).found_existing) return null;
    if (module.root_source_file) |source| switch (source) {
        .src_path => |path| if (std.mem.endsWith(u8, path.sub_path, suffix)) {
            return module;
        },
        else => {},
    };
    for (module.import_table.values()) |dependency| {
        if (findSourceModule(dependency, suffix, seen)) |found| return found;
    }
    return null;
}

fn inspectWasmProfile(module: *std.Build.Module, target: std.Build.ResolvedTarget, seen: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((seen.getOrPut(module) catch @panic("OOM")).found_existing) return;
    if (module.optimize) |optimize| if (optimize != .ReleaseSafe) @panic("WASM module inherits native optimization");
    if (module.resolved_target) |actual| {
        if (!std.Target.Query.fromTarget(&actual.result).eql(std.Target.Query.fromTarget(&target.result)))
            @panic("WASM module inherits a foreign runtime target");
    }
    if (module.link_libc == true) @panic("WASM module links native libc");
    for (module.import_table.values()) |dependency| inspectWasmProfile(dependency, target, seen);
}

fn inspect(module: *std.Build.Module, unit: runtime.RuntimeLibraryUnit, metadata: *std.Build.Step.Compile, seen: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((seen.getOrPut(module) catch @panic("OOM")).found_existing) return;
    for (module.link_objects.items) |object| switch (object) {
        .other_step => |artifact| {
            if (artifact == metadata) std.debug.panic("{s} archive depends on version metadata object", .{@tagName(unit)});
            inspect(artifact.root_module, unit, metadata, seen);
        },
        else => {},
    };
    var imports = module.import_table.iterator();
    while (imports.next()) |entry| {
        const name = entry.key_ptr.*;
        if (std.mem.eql(u8, name, "vopr"))
            std.debug.panic("{s} archive depends on simulation test support", .{@tagName(unit)});
        if (std.mem.eql(u8, name, "lmdb_engine"))
            std.debug.panic("{s} archive depends on disabled LMDB", .{@tagName(unit)});
        if (unit != .distributed and std.mem.eql(u8, name, "antfly_lite_options"))
            std.debug.panic("{s} archive depends on Lite capability settings", .{@tagName(unit)});
        if (unit != .api_kernel and (std.mem.eql(u8, name, "antfly_mcp") or std.mem.eql(u8, name, "antfly_a2a")))
            std.debug.panic("{s} archive depends on API protocol adapters", .{@tagName(unit)});
        if (unit == .serverless and std.mem.eql(u8, name, "raft_engine"))
            @panic("serverless archive depends on Raft");
        if (unit != .api_kernel and std.mem.eql(u8, name, "antfly_openapi_specs"))
            std.debug.panic("{s} archive depends on served schemas", .{@tagName(unit)});
        if (unit != .inference and (std.mem.eql(u8, name, "metal_jit_identity") or std.mem.eql(u8, name, "cuda_jit_identity")))
            std.debug.panic("{s} archive depends on inference backend identity", .{@tagName(unit)});
        if (unit == .cli and (std.mem.startsWith(u8, name, "inference_") or std.mem.eql(u8, name, "sentencepiece_proto") or
            std.mem.eql(u8, name, "lmdb_engine") or std.mem.eql(u8, name, "raft_engine")))
            std.debug.panic("remote CLI depends on local implementation: {s}", .{name});
        inspect(entry.value_ptr.*, unit, metadata, seen);
    }
}

fn rejectMetadata(module: *std.Build.Module, metadata: *std.Build.Step.Compile, seen: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((seen.getOrPut(module) catch @panic("OOM")).found_existing) return;
    for (module.link_objects.items) |object| switch (object) {
        .other_step => |artifact| {
            if (artifact == metadata) @panic("unit test depends on release metadata");
            rejectMetadata(artifact.root_module, metadata, seen);
        },
        else => {},
    };
    for (module.import_table.values()) |dependency| rejectMetadata(dependency, metadata, seen);
}

fn isSqlGenerator(b: *std.Build, artifact: *std.Build.Step.Compile) bool {
    var steps = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    var modules = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
    collectSteps(&b.top_level_steps.get("sql-grammar-generated-check").?.step, &steps, &modules);
    return steps.contains(&artifact.step);
}
