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
const Context = @import("context.zig").Context;
const runtime_build = @import("runtime.zig");

pub fn resolveTarget(ctx: Context) std.Build.ResolvedTarget {
    const model = ctx.backend.wasm_memory_model;
    if (!std.mem.eql(u8, model, "wasm32") and !std.mem.eql(u8, model, "wasm64")) @panic("invalid -Dwasm-memory-model (expected wasm32 or wasm64)");
    return ctx.b.resolveTargetQuery(.{
        .cpu_arch = if (std.mem.eql(u8, model, "wasm64")) .wasm64 else .wasm32,
        .os_tag = .freestanding,
        .cpu_features_add = std.Target.wasm.featureSet(&.{ .atomics, .bulk_memory, .simd128 }),
    });
}

fn wasmBackend(native: runtime_build.BackendOptions) runtime_build.BackendOptions {
    var backend = native;
    backend.enable_wasm = true;
    backend.enable_native = false;
    backend.link_libc = false;
    backend.enable_metal = false;
    backend.enable_cuda = false;
    backend.enable_onnx = false;
    backend.enable_pjrt = false;
    backend.enable_system_blas = false;
    backend.blas_root = null;
    return backend;
}

pub fn addWasm(ctx: Context, wasm_jinja_mod: *std.Build.Module, wasm_platform_mod: *std.Build.Module) *std.Build.Step {
    const b = ctx.b;
    const is_wasm64 = std.mem.eql(u8, ctx.backend.wasm_memory_model, "wasm64");
    const wasm_target = resolveTarget(ctx);
    const wasm_root = if (is_wasm64)
        "src/wasm_entry_wasm64.zig"
    else
        "src/wasm_entry_wasm32.zig";
    const wasm_install_name = if (is_wasm64) "antfly-inference-wasm64.wasm" else "antfly-inference-wasm32.wasm";
    const wasm_lib = b.addExecutable(.{
        .name = if (is_wasm64) "antfly-inference-wasm64" else "antfly-inference-wasm32",
        .root_module = b.createModule(.{
            .root_source_file = ctx.path(wasm_root),
            .target = wasm_target,
            .optimize = .ReleaseSafe,
            .single_threaded = true,
        }),
    });
    wasm_lib.root_module.addOptions("build_options", runtime_build.addBuildOptions(b, wasmBackend(ctx.backend)));
    wasm_lib.entry = .disabled;
    wasm_lib.rdynamic = true;
    // ReleaseSafe: works around LLVM WASM backend miscompilation at -Os/-O3
    // that produces NaN in BERT encoder FFN linear ops. 1.2 MB binary.

    // Tokenizer modules for WASM target (pure Zig, no C deps)
    const wasm_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/tokenizer/src/tokenizer.zig" })),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_hf_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/tokenizer/src/hf_root.zig" })),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_audio_mod = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/audio/src/mod.zig" })),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_image_mod = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/image/src/mod.zig" })),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_hash_mod = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/hash/src/mod.zig" })),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    wasm_image_mod.addImport("antfly_hash", wasm_hash_mod);
    wasm_platform_mod.single_threaded = true;
    const wasm_linalg_mod = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/linalg/src/mod.zig" })),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_ml_mod = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/ml/src/root.zig" })),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    wasm_ml_mod.addImport("antfly_platform", wasm_platform_mod);
    const wasm_onnx_graph_mod = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/onnx/src/root.zig" })),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_protobuf = b.dependency("protobuf", .{ .target = wasm_target, .optimize = .ReleaseSafe }).module("protobuf");
    // Reuse generated source without importing the native runtime module.
    const wasm_sentencepiece_proto = b.createModule(.{
        .root_source_file = ctx.graph.sentencepiece_proto_mod.root_source_file,
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .imports = &.{.{ .name = "protobuf", .module = wasm_protobuf }},
    });
    wasm_onnx_graph_mod.addImport("protobuf", wasm_protobuf);
    wasm_onnx_graph_mod.addImport("ml", wasm_ml_mod);
    wasm_tokenizer_mod.addImport("sentencepiece_proto", wasm_sentencepiece_proto);
    wasm_hf_tokenizer_mod.addImport("inference_tokenizer", wasm_tokenizer_mod);
    wasm_lib.root_module.addImport("jinja", wasm_jinja_mod);
    wasm_lib.root_module.addImport("inference_audio", wasm_audio_mod);
    wasm_lib.root_module.addImport("inference_tokenizer", wasm_tokenizer_mod);
    wasm_lib.root_module.addImport("inference_hf_tokenizer", wasm_hf_tokenizer_mod);
    wasm_lib.root_module.addImport("inference_linalg", wasm_linalg_mod);
    wasm_lib.root_module.addImport("antfly_image", wasm_image_mod);
    wasm_lib.root_module.addImport("antfly_platform", wasm_platform_mod);
    wasm_lib.root_module.addImport("ml", wasm_ml_mod);
    wasm_lib.root_module.addImport("onnx_graph", wasm_onnx_graph_mod);

    const wasm_install = b.addInstallArtifact(wasm_lib, .{
        .dest_sub_path = wasm_install_name,
    });

    const wasm_step = ctx.step("wasm", "Build WASM module for browser inference");
    wasm_step.dependOn(&wasm_install.step);
    if (!is_wasm64) {
        const wasm_compat_install = b.addInstallFile(wasm_lib.getEmittedBin(), "antfly-inference.wasm");
        wasm_step.dependOn(&wasm_compat_install.step);
    }
    return wasm_step;
}
