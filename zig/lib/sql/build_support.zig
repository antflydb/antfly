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
pub const Steps = struct {
    regen: *std.Build.Step.UpdateSourceFiles,
    compare: *std.Build.Step.Run,
    run_generated: *std.Build.Step.Run,
    benchmark: *std.Build.Step.Compile,
    run_parser_tests: *std.Build.Step.Run,
};

pub fn addSteps(b: *std.Build, options: struct {
    root: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    codegen: *std.Build.Step.Compile,
    compare_tool: *std.Build.Step.Compile,
    /// Stable source label embedded in the generated metadata.
    grammar_label: []const u8,
}) Steps {
    const target = options.target;
    const optimize = options.optimize;
    const yacc_codegen = options.codegen;
    const grammar = options.root.path(b, "grammar/antfly_sql.y");
    const generated = options.root.path(b, "grammar/generated/root.zig");
    const generate = b.addRunArtifact(yacc_codegen);
    generate.addFileArg2(grammar, .{ .make_absolute = true });
    const output = generate.addOutputFileArg2("sql_grammar_root.zig", .{ .make_absolute = true });
    generate.addArg(options.grammar_label);
    const update = b.addUpdateSourceFiles();
    update.addCopyFileToSource(output, generated.src_path.sub_path);
    const compare = b.addRunArtifact(options.compare_tool);
    compare.addFileArg2(output, .{ .make_absolute = true });
    compare.addFileArg2(generated, .{ .make_absolute = true });

    const generated_compile = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = generated,
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_generated_compile = b.addRunArtifact(generated_compile);

    const parser_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = options.root.path(b, "root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_parser_tests = b.addRunArtifact(parser_tests);

    const parser_bench = b.addExecutable(.{
        .name = "lib-sql-parser-bench",
        .root_module = b.createModule(.{
            .root_source_file = options.root.path(b, "parser_bench.zig"),
            .target = target,
            .optimize = .fast,
        }),
    });

    return .{
        .regen = update,
        .compare = compare,
        .run_generated = run_generated_compile,
        .benchmark = parser_bench,
        .run_parser_tests = run_parser_tests,
    };
}
