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
const dependencies = @import("build_support/antfly/dependencies.zig");
const embedded_owner = @import("build_support/embedded/embedded.zig");
const wasm_owner = @import("build_support/embedded/wasm.zig");
const source_owner = @import("build_support/embedded/source_owner.zig");

pub fn build(b: *std.Build) void {
    buildDependency(b, @This());
}

pub fn buildDependency(b: *std.Build, comptime asking_build_zig: type) void {
    const joinable_threaded = @import("build_support/joinable_threaded.zig");
    const owned_worker_library = joinable_threaded.prepare(b);
    defer joinable_threaded.attach(b, owned_worker_library);
    defer @import("antfly_platform").finalizeMacosSdk(b);
    const shared = dependencies.create(b, asking_build_zig) orelse return;
    defer {
        source_owner.finalize(b);
        // Shared providers and their callers must use the same debug/tracing
        // profile, including the independent embedded packaging composition.
        if (shared.strip) @import("build_support/embedded/shared_library.zig").stripBuildGraph(b);
    }
    b.modules.put(b.allocator, "antfly-inference", shared.inference_graph.inference_mod) catch @panic("OOM");
    const local_module = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/embedded_root.zig"),
        .target = shared.target,
        .optimize = shared.optimize,
    });
    shared.production_antfly_imports.configureEmbedded(b, local_module, shared.link_libc);
    const embedded = embedded_owner.addEmbedded(b, .{
        .version = shared.antfly_version,
        .vopr = shared.vopr_mod,
        .lmdb_engine = shared.lmdb_engine_mod,
        .optimize = shared.optimize,
        .strip = shared.strip,
        .antfly_imports = shared.production_antfly_imports,
        .antfly_mod = local_module,
    });
    const library_step = b.step("libantfly", "Build the embedding library, public header, and pkg-config metadata");
    library_step.dependOn(embedded.install_libantfly);
    library_step.dependOn(&embedded.install_capi_header.step);
    library_step.dependOn(&b.top_level_steps.get("licenses-libantfly").?.step);
    b.getInstallStep().dependOn(library_step);
    b.default_step = library_step;

    const wasm = wasm_owner.add(b, shared.sentencepiece_proto_source);
    b.installArtifact(wasm.artifact);
    const wasm_step = b.step("wasm", "Build the embedded WASM bundle");
    for (wasm.install) |step| wasm_step.dependOn(step);
    wasm.smoke.step.dependOn(wasm_step);
    b.step("wasm-test", "Run the embedded WASM smoke test").dependOn(&wasm.smoke.step);
}
