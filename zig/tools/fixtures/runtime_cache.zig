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
    inline for (std.meta.tags(runtime.RuntimeLibraryUnit)) |unit| {
        const artifact = artifacts.runtime.runtime_library_artifacts[@intFromEnum(unit)].?;
        var seen = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
        inspect(artifact.root_module, unit, artifacts.inference.build_info_object, &seen);
        const expression = if (unit == .api_kernel)
            "@import(\"antfly_hash\").Adler32.hash(\"cache probe\") ^ std.hash.Wyhash.hash(0, specs.ard) ^ std.hash.Wyhash.hash(0, specs.antfly) ^ " ++
                "std.hash.Wyhash.hash(0, specs.metadata) ^ std.hash.Wyhash.hash(0, specs.extensions) ^ " ++
                "std.hash.Wyhash.hash(0, specs.auth) ^ std.hash.Wyhash.hash(0, specs.inference_config)"
        else if (unit == .inference)
            "@import(\"antfly_hash\").Adler32.hash(\"cache probe\") ^ @sizeOf(@import(\"inference_server\").execution_control.Cancellation)"
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
