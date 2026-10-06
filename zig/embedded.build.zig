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
    defer @import("antfly_platform").finalizeMacosSdk(b);
    defer source_owner.finalize(b);
    const shared = dependencies.create(b, asking_build_zig) orelse return;
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
    shared.build_info.link(embedded.libantfly_link_mod);
    const lite_module = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/lite_main.zig"),
        .target = shared.target,
        .optimize = shared.optimize,
    });
    shared.antfly_imports.configureEmbedded(b, lite_module, shared.link_libc);
    lite_module.addImport("antfly-client", embedded.antfly_client_pkg_mod);
    lite_module.addImport("antfly_inference_host", shared.antfly_imports.inference_host);
    lite_module.linkLibrary(embedded.native_inference);
    lite_module.linkLibrary(embedded.native_enrichment);
    shared.build_info.link(lite_module);
    const lite = b.addExecutable(.{
        .name = "antfly-lite",
        .root_module = lite_module,
        .max_rss = 12 * 1024 * 1024 * 1024,
    });
    const install_lite = b.addInstallArtifact(lite, .{});
    const lite_step = b.step("lite", "Build the independent file-oriented Lite CLI and public C ABI");
    lite_step.dependOn(&install_lite.step);
    lite_step.dependOn(&embedded.install_libantfly.step);
    lite_step.dependOn(&embedded.install_capi_header.step);
    lite_step.dependOn(&b.top_level_steps.get("licenses-antfly-lite").?.step);

    b.getInstallStep().dependOn(&install_lite.step);
    b.getInstallStep().dependOn(&embedded.install_libantfly.step);
    b.getInstallStep().dependOn(&embedded.install_capi_header.step);
    b.default_step = lite_step;

    const wasm = wasm_owner.add(b, shared.sentencepiece_proto_source);
    b.installArtifact(wasm.artifact);
    const wasm_step = b.step("wasm", "Build the embedded WASM bundle");
    for (wasm.install) |step| wasm_step.dependOn(step);
    wasm.smoke.step.dependOn(wasm_step);
    b.step("wasm-test", "Run the embedded WASM smoke test").dependOn(&wasm.smoke.step);
}
