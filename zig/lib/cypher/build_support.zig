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
pub fn createModule(b: *std.Build, root: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) *std.Build.Module {
    const module = b.createModule(.{ .root_source_file = root.path(b, "root.zig"), .target = target, .optimize = optimize });
    module.addImport("cypher_regex_capture", b.createModule(.{ .root_source_file = root.path(b, "../regex/src/captures.zig"), .target = target, .optimize = optimize }));
    return module;
}
pub fn addSteps(b: *std.Build, options: struct { root: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize, codegen: *std.Build.Step.Compile, compare_tool: *std.Build.Step.Compile }) *std.Build.Module {
    const generate = b.addRunArtifact(options.codegen);
    generate.addFileArg2(options.root.path(b, "grammar/antfly_cypher.y"), .{ .make_absolute = true });
    const output = generate.addOutputFileArg2("cypher_grammar.zig", .{ .make_absolute = true });
    generate.addArg("lib/cypher/grammar/antfly_cypher.y");
    const checked = options.root.path(b, "grammar/generated/root.zig");
    const update = b.addUpdateSourceFiles();
    update.addCopyFileToSource(output, checked.src_path.sub_path);
    b.step("regen-cypher-grammar", "Regenerate the Antfly graph language parser").dependOn(&update.step);
    const compare = b.addRunArtifact(options.compare_tool);
    compare.addFileArg2(output, .{ .make_absolute = true });
    compare.addFileArg2(checked, .{ .make_absolute = true });
    const check = b.step("cypher-grammar-generated-check", "Verify graph grammar provenance and generated output");
    check.dependOn(&compare.step);
    const module = createModule(b, options.root, options.target, options.optimize);
    const tests = b.addRunArtifact(b.addTest(.{ .root_module = module }));
    b.step("lib-cypher-test", "Run graph compiler, binding, execution, and semantic tests").dependOn(&tests.step);
    check.dependOn(&tests.step);
    return module;
}
