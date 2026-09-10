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

//! Inspect production module profiles before fixtures substitute entry bodies.
const std = @import("std");

pub fn check(artifact: *std.Build.Step.Compile) void {
    var seen = std.AutoHashMap(*std.Build.Module, void).init(artifact.step.owner.allocator);
    inspect(artifact, artifact.root_module, &seen);
}

fn inspect(artifact: *std.Build.Step.Compile, module: *std.Build.Module, seen: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((seen.getOrPut(module) catch @panic("OOM")).found_existing) return;
    const root = artifact.root_module;
    if (module.resolved_target) |target| {
        if (!std.Target.Query.fromTarget(&target.result).eql(std.Target.Query.fromTarget(&root.resolved_target.?.result)))
            std.debug.panic("{s}: runtime dependency has a different target", .{artifact.name});
    }
    if (module.optimize) |optimize| {
        if (optimize != (root.optimize orelse .Debug))
            std.debug.panic("{s}: runtime dependency uses {s}, expected {s}: {s}", .{
                artifact.name,                                                                             @tagName(optimize), @tagName(root.optimize orelse .Debug),
                if (module.root_source_file) |source| source.getPath(artifact.step.owner) else "C module",
            });
    }
    for (module.link_objects.items) |object| switch (object) {
        .system_lib => |lib| {
            for ([_][]const u8{ "avformat", "avcodec", "avutil", "swresample" }) |name| {
                if (std.mem.eql(u8, lib.name, name)) @panic("runtime links an unused external FFmpeg library");
            }
        },
        // Linked artifacts and generated-source host tools own separate profiles.
        else => {},
    };
    for (module.import_table.values()) |dependency| inspect(artifact, dependency, seen);
}

/// Preserve the browser dependency graph while avoiding unrelated baseline
/// runtime compilation failures. Real supported options affect the emitted code.
pub fn addInferenceWasmProbe(b: *std.Build, artifact: *std.Build.Step.Compile) void {
    if (!std.mem.startsWith(u8, artifact.name, "antfly-inference-wasm")) return;
    artifact.root_module.root_source_file = b.addWriteFiles().add("inference_wasm_profile.zig",
        \\export fn browser_capabilities() u32 {
        \\    const options = @import("build_options");
        \\    comptime {
        \\        if (options.enable_native or options.link_libc or options.enable_native_quant_dispatch_stats or options.skip_openapi)
        \\            @compileError("browser profile inherits native/server configuration");
        \\    }
        \\    return @intFromBool(options.enable_webgpu) + @as(u32, @intFromBool(@import("builtin").cpu.arch == .wasm64)) * 2;
        \\}
    );
    b.step("cache-inference-wasm", "Check the actual inference browser profile").dependOn(&artifact.step);
}

/// Exercise real CPU benchmark bodies; use small profile probes for audio/linalg.
pub fn addBenchmarkProbe(b: *std.Build, artifact: *std.Build.Step.Compile) void {
    const training = std.mem.eql(u8, artifact.name, "antfly-inference-training-bench");
    if (training or std.mem.eql(u8, artifact.name, "antfly-inference-paged-attention-bench")) {
        const run = b.addRunArtifact(artifact);
        run.addArgs(if (training) &.{
            "--mode",        "both", "--optimizer-len",       "64", "--optimizer-steps", "2",
            "--graph-batch", "2",    "--graph-width",         "8",  "--graph-depth",     "2",
            "--graph-steps", "2",    "--checkpoint-interval", "1",
        } else &.{
            "--backend",   "native", "--prompt-len",   "4", "--decode-steps",  "2",
            "--page-size", "4",      "--num-heads",    "2", "--num-kv-heads",  "1",
            "--head-dim",  "32",     "--warmup-iters", "0", "--measure-iters", "1",
        });
        b.step(b.fmt("cache-{s}", .{artifact.name}), "Run an actual bounded CPU benchmark workload").dependOn(&run.step);
        return;
    }
    const dependency: []const u8 = if (std.mem.eql(u8, artifact.name, "antfly-inference-audio-bench")) "inference_audio" else if (std.mem.eql(u8, artifact.name, "antfly-inference-linalg-bench")) "inference_linalg" else return;
    const files = b.addWriteFiles();
    artifact.root_module.root_source_file = files.add(b.fmt("{s}.zig", .{artifact.name}), b.fmt("const std = @import(\"std\"); pub fn main() void {{ std.debug.print(\"BENCH_PROFILE {{s}} {{s}}\\n\", .{{ @tagName(@import(\"builtin\").mode), @tagName(@import(\"{s}\").cache_test_profile) }}); }}", .{dependency}));
    b.step(b.fmt("cache-{s}", .{artifact.name}), "Read the actual benchmark/library profile").dependOn(&b.addRunArtifact(artifact).step);
}

/// Run the real text and multimodal data pipeline with two examples each.
pub fn addDataToolChecks(b: *std.Build, steps: *std.AutoHashMap(*std.Build.Step, void)) void {
    const names = [_][]const u8{
        "generate-gemma4-pilot-dataset", "generate-gemma4-multimodal-pilot-dataset",
        "prepare-gemma4-text-dataset",   "prepare-gemma4-multimodal-dataset",
    };
    var artifacts: [names.len]?*std.Build.Step.Compile = @splat(null);
    var iterator = steps.keyIterator();
    while (iterator.next()) |entry| {
        const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
        for (names, 0..) |name, index| {
            if (std.mem.eql(u8, artifact.name, name)) artifacts[index] = artifact;
        }
    }
    const data_check = b.step("cache-finetune-data", "Run actual bounded finetune data pipelines");
    for ([_][]const u8{ "text", "multimodal" }, 0..) |kind, index| {
        const generator = artifacts[index] orelse @panic("missing registered data generator");
        const converter = artifacts[index + 2] orelse @panic("missing registered data converter");
        for ([_]*std.Build.Step.Compile{ generator, converter }) |artifact| {
            if (artifact.root_module.import_table.contains("inference_internal") or
                artifact.root_module.import_table.contains("build_options"))
                @panic("data tools depend on inference runtime settings");
        }
        const generate = b.addRunArtifact(generator);
        const data = generate.addOutputFileArg(b.fmt("{s}-pilot.jsonl", .{kind}));
        generate.addArg("2");
        if (index == 1) generate.addFileArg(b.addWriteFiles().add("image.ppm", "P3\n1 1\n255\n0 0 0\n"));
        const convert = b.addRunArtifact(converter);
        // The legacy text CSV converter accepts prompt/response records; the
        // chat pilot generator is used directly by the chat training workflow.
        convert.addFileArg(if (index == 0) b.addWriteFiles().add("instructions.jsonl", "{\"prompt\":\"first\",\"response\":\"one\"}\n{\"prompt\":\"second\",\"response\":\"two\"}\n") else data);
        convert.addArg("train");
        _ = convert.addOutputFileArg(b.fmt("{s}-pilot.csv", .{kind}));
        _ = convert.addOutputFileArg(b.fmt("{s}-summary.json", .{kind}));
        data_check.dependOn(&convert.step);
        data_check.dependOn(&generate.step);
    }
}

/// Exercise the actual offline I/O consumers; inspect every registered asset
/// command so a newly added one cannot silently acquire runtime dependencies.
pub fn addAssetToolChecks(b: *std.Build, steps: *std.AutoHashMap(*std.Build.Step, void)) void {
    var assets = std.StringHashMap(*std.Build.Step.Compile).init(b.allocator);
    var iterator = steps.keyIterator();
    while (iterator.next()) |entry| {
        const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
        if (!artifact.root_module.import_table.contains("inference_finetune_assets")) continue;
        var seen = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
        checkAssetModule(b, artifact.root_module, &seen);
        assets.put(artifact.name, artifact) catch @panic("OOM");
    }
    if (assets.count() == 0) @panic("no offline asset commands registered");
    const check_step = b.step("cache-finetune-assets", "Run actual bounded offline checkpoint operations");
    // One real executable per source owner exercises every extracted boundary.
    // Reuse the three commands with bounded I/O workloads below. Other owners
    // choose a deterministic representative from the production registry.
    var owners = std.StringHashMap(*std.Build.Step.Compile).init(b.allocator);
    var asset_iterator = assets.valueIterator();
    while (asset_iterator.next()) |entry| {
        const artifact = entry.*;
        const source = artifact.root_module.import_table.get("inference_finetune_assets").?.root_source_file.?.getPath(b);
        const owner = owners.getOrPut(source) catch @panic("OOM");
        if (!owner.found_existing or std.mem.lessThan(u8, artifact.name, owner.value_ptr.*.name)) owner.value_ptr.* = artifact;
    }
    for ([_][]const u8{ "compose-lora-adapters", "inspect-reranker-lora-bundle", "materialize-reranker-head" }) |name| {
        const artifact = assets.get(name).?;
        const source = artifact.root_module.import_table.get("inference_finetune_assets").?.root_source_file.?.getPath(b);
        owners.put(source, artifact) catch @panic("OOM");
    }
    var owner_iterator = owners.valueIterator();
    while (owner_iterator.next()) |entry| {
        const artifact = entry.*;
        std.debug.print("ASSET_COMMAND {s}\n", .{artifact.name});
        check_step.dependOn(&artifact.step);
    }
    const compose = b.addRunArtifact(assets.get("compose-lora-adapters").?);
    compose.addArg("--out");
    _ = compose.addOutputFileArg("composed-adapter.safetensors");
    compose.addFileArg(b.path("cache_asset_inputs/adapter/adapter_model.safetensors"));
    check_step.dependOn(&compose.step);

    const inspect_run = b.addRunArtifact(assets.get("inspect-reranker-lora-bundle").?);
    inspect_run.addDirectoryArg(b.path("cache_asset_inputs/base"));
    inspect_run.addDirectoryArg(b.path("cache_asset_inputs/adapter"));
    _ = inspect_run.captureStdOut(.{ .basename = "asset-inspection.json" });
    check_step.dependOn(&inspect_run.step);

    const materialize = b.addRunArtifact(assets.get("materialize-reranker-head").?);
    materialize.addDirectoryArg(b.path("cache_asset_inputs/base"));
    materialize.addFileArg(b.path("cache_asset_inputs/head.safetensors"));
    _ = materialize.addOutputDirectoryArg("materialized-head");
    check_step.dependOn(&materialize.step);
    const rejected = b.addRunArtifact(assets.get("compose-lora-adapters").?);
    rejected.addArgs(&.{ "--out", "unused.safetensors" });
    rejected.addFileArg(b.path("cache_asset_inputs/adapter/adapter_model.safetensors"));
    rejected.addArg("cache_asset_inputs/missing.safetensors");
    rejected.expectExitCode(1);
    _ = rejected.captureStdErr(.{});
    b.step("cache-finetune-assets-error", "Reject an unreadable adapter without corrupting cleanup").dependOn(&rejected.step);
}

fn checkAssetModule(b: *std.Build, module: *std.Build.Module, seen: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((seen.getOrPut(module) catch @panic("OOM")).found_existing) return;
    var imports = module.import_table.iterator();
    while (imports.next()) |entry| {
        const name = entry.key_ptr.*;
        if (std.mem.eql(u8, name, "inference_internal") or std.mem.eql(u8, name, "build_info") or
            std.mem.endsWith(u8, name, "jit_identity")) @panic("offline assets import runtime dependencies");
        if (std.mem.eql(u8, name, "ml") or std.mem.eql(u8, name, "onnx_graph"))
            @panic("offline assets import graph conversion or training");
        if (std.mem.eql(u8, name, "build_options")) {
            const source = entry.value_ptr.*.root_source_file.?.getPath(b);
            if (!std.mem.endsWith(u8, source, "/finetune/assets_options.zig"))
                @panic("offline assets inherit product build options");
        }
        checkAssetModule(b, entry.value_ptr.*, seen);
    }
    for (module.link_objects.items) |object| switch (object) {
        .system_lib => @panic("offline assets link an optional native library"),
        .other_step => @panic("offline assets link a runtime artifact"),
        else => {},
    };
    if (module.frameworks.count() != 0) @panic("offline assets link native frameworks");
}

/// Keep the actual inference qualification test's imports and runner.
pub fn addPjrtQualificationProbe(b: *std.Build, artifact: *std.Build.Step.Compile) bool {
    if (!artifact.kind.isTest() or artifact.test_runner == null) return false;
    const source = artifact.root_module.root_source_file orelse return false;
    switch (source) {
        .src_path => |path| if (!std.mem.eql(u8, path.sub_path, "src/inference.zig") and
            !std.mem.endsWith(u8, path.sub_path, "/src/inference.zig")) return false,
        else => return false,
    }
    artifact.root_module.root_source_file = b.addWriteFiles().add("pjrt_test.zig",
        \\test "PJRT cache probe" {
        \\    const revision = @import("pjrt").cache_test_revision;
        \\    try @import("std").testing.expect(revision > 0);
        \\    @import("std").debug.print("PJRT_REVISION {d}\n", .{revision});
        \\}
    );
    artifact.filters = &.{"PJRT cache probe"};
    b.step("cache-pjrt-tests", "Exercise actual PJRT qualification imports").dependOn(&b.addRunArtifact(artifact).step);
    return true;
}

// Follow generated sources as well as explicit steps, without freezing module
// graphs before the fixture replaces the expensive compilation bodies.
pub fn collectSteps(step: *std.Build.Step, steps: *std.AutoHashMap(*std.Build.Step, void), modules: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((steps.getOrPut(step) catch @panic("OOM")).found_existing) return;
    for (step.dependencies.items) |dependency| collectSteps(dependency, steps, modules);
    if (step.cast(std.Build.Step.Compile)) |artifact| collectModules(artifact.root_module, steps, modules);
}

fn collectModules(module: *std.Build.Module, steps: *std.AutoHashMap(*std.Build.Step, void), modules: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((modules.getOrPut(module) catch @panic("OOM")).found_existing) return;
    if (module.root_source_file) |source| switch (source) {
        .generated => |generated| collectSteps(generated.file.step, steps, modules),
        else => {},
    };
    for (module.link_objects.items) |object| switch (object) {
        .other_step => |artifact| collectSteps(&artifact.step, steps, modules),
        else => {},
    };
    for (module.import_table.values()) |dependency| collectModules(dependency, steps, modules);
}
