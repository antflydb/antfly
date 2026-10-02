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
const dependencies = @import("build_support/antfly/dependencies.zig");
const embedded_owner = @import("pkg/antfly-embedded/build/embedded.zig");
const wasm_owner = @import("pkg/antfly-embedded/build/wasm.zig");
const source_owner = @import("pkg/antfly-embedded/build/source_owner.zig");

pub fn build(b: *std.Build) void {
    defer source_owner.finalize(b);
    const shared = dependencies.create(b) orelse return;
    const local_module = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/embedded_root.zig"),
        .target = shared.target,
        .optimize = shared.optimize,
    });
    shared.production_antfly_imports.configureEmbedded(b, local_module, shared.link_libc);
    const embedded = embedded_owner.addEmbedded(b, .{
        .vopr = shared.vopr_mod,
        .lmdb_engine = shared.lmdb_engine_mod,
        .optimize = shared.optimize,
        .strip = shared.strip,
        .antfly_imports = shared.production_antfly_imports,
        .antfly_mod = local_module,
    });
    shared.build_info.link(embedded.libantfly_link_mod);
    const lite_module = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/lite_main.zig"),
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
    b.step("lite", "Build the independent file-oriented Lite CLI").dependOn(&install_lite.step);

    const wasm = wasm_owner.add(b, shared.sentencepiece_proto_source);
    const wasm_step = b.step("wasm", "Build the embedded WASM bundle");
    for (wasm.install) |step| wasm_step.dependOn(step);
    wasm.smoke.step.dependOn(wasm_step);
    b.step("wasm-test", "Run the embedded WASM smoke test").dependOn(&wasm.smoke.step);
}
