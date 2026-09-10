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

//! Offline command dependencies, independent of product backend configuration.
const std = @import("std");

pub const Owner = enum { peft, gliner2, gemma4, colqwen2, layoutlmv3, reranker_head, reranker_lora, gliner2_run_validation };

pub fn create(options: struct {
    b: *std.Build,
    root: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    owner: Owner,
    onnx_data: *std.Build.Module,
    jinja: *std.Build.Module,
    platform: *std.Build.Module,
}) *std.Build.Module {
    const b = options.b;
    const module = b.createModule(.{
        .root_source_file = options.root.path(b, b.fmt("src/finetune_assets_{s}.zig", .{@tagName(options.owner)})),
        .target = options.target,
        .optimize = options.optimize,
        .link_libc = true,
    });
    module.addImport("build_options", b.createModule(.{
        .root_source_file = options.root.path(b, "src/finetune/assets_options.zig"),
        .target = options.target,
        .optimize = options.optimize,
    }));
    switch (options.owner) {
        .peft, .gliner2_run_validation => {},
        else => {
            module.addImport("onnx_data", options.onnx_data);
            module.addImport("jinja", options.jinja);
        },
    }
    if (options.owner == .gemma4) module.addImport("antfly_platform", options.platform);
    return module;
}
