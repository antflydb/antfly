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
const addFileCompareTool = @import("../../tools/build_support.zig").addFileCompareTool;

pub const sql_grammar_source = "lib/sql/grammar/antfly_sql.y";

pub const sql_grammar_generated_root = "lib/sql/grammar/generated/root.zig";

pub fn addLocalYaccCodegen(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step.Compile {
    const yacc_mod = b.createModule(.{
        .root_source_file = b.path("lib/yacc/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "yacc-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/yacc/src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("yacc", yacc_mod);
    return exe;
}

pub const YaccSteps = struct {
    run_yacc_tests: *std.Build.Step.Run,
    run_parser_tests: *std.Build.Step.Run,
};

pub fn addYaccSteps(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) YaccSteps {
    const yacc_codegen = addLocalYaccCodegen(b, target, optimize);
    const install_yacc_codegen = b.addInstallArtifact(yacc_codegen, .{});
    const yacc_codegen_step = b.step("yacc-zig", "Build and install the standalone Zig yacc generator");
    yacc_codegen_step.dependOn(&install_yacc_codegen.step);

    const yacc_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/yacc/src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_yacc_tests = b.addRunArtifact(yacc_tests);
    const yacc_test_step = b.step("lib-yacc-test", "Run standalone lib/yacc parser generator tests");
    yacc_test_step.dependOn(&run_yacc_tests.step);

    const regen_run = b.addRunArtifact(yacc_codegen);
    regen_run.addFileArg(b.path(sql_grammar_source));
    const regen_output = regen_run.addOutputFileArg("regen_sql_grammar_root.zig");
    regen_run.addArg(sql_grammar_source);
    const update = b.addUpdateSourceFiles();
    update.addCopyFileToSource(regen_output, sql_grammar_generated_root);
    const regen_fmt = b.addSystemCommand(&.{ b.graph.zig_exe, "fmt", sql_grammar_generated_root });
    regen_fmt.step.dependOn(&update.step);
    const regen_step = b.step("regen-sql-grammar", "Regenerate checked-in Antfly SQL grammar metadata");
    regen_step.dependOn(&regen_fmt.step);

    const check_run = b.addRunArtifact(yacc_codegen);
    check_run.addFileArg(b.path(sql_grammar_source));
    const check_output = check_run.addOutputFileArg("check_sql_grammar_root.zig");
    check_run.addArg(sql_grammar_source);
    const check_fmt = b.addSystemCommand(&.{ b.graph.zig_exe, "fmt" });
    check_fmt.addFileArg(check_output);
    const compare = b.addRunArtifact(addFileCompareTool(b));
    compare.step.dependOn(&check_fmt.step);
    compare.addFileArg(check_output);
    compare.addFileArg(b.path(sql_grammar_generated_root));

    const generated_compile = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(sql_grammar_generated_root),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_generated_compile = b.addRunArtifact(generated_compile);
    const check_step = b.step("sql-grammar-generated-check", "Check and compile the generated Antfly SQL grammar metadata");
    check_step.dependOn(&compare.step);
    check_step.dependOn(&run_generated_compile.step);

    const parser_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/sql/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_parser_tests = b.addRunArtifact(parser_tests);
    const parser_test_step = b.step("lib-sql-parser-test", "Run the storage-independent SQL lexer and parser tests");
    parser_test_step.dependOn(&run_parser_tests.step);

    const parser_bench = b.addExecutable(.{
        .name = "lib-sql-parser-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/sql/parser_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });

    const parser_bench_step = b.step("lib-sql-parser-bench", "Build and install lib-sql-parser-bench");
    parser_bench_step.dependOn(&b.addInstallArtifact(parser_bench, .{}).step);

    return .{
        .run_yacc_tests = run_yacc_tests,
        .run_parser_tests = run_parser_tests,
    };
}
