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

const std = @import("std");

pub const snowball_languages = [_][]const u8{
    "danish",
    "dutch",
    "finnish",
    "french",
    "german",
    "italian",
    "norwegian",
    "portuguese",
    "spanish",
    "swedish",
};

pub const snowball_generated_root = "pkg/antfly/src/search/snowball/generated";

pub const snowball_compiler_sources = [_][]const u8{
    "compiler/analyser.c",
    "compiler/driver.c",
    "compiler/generator.c",
    "compiler/generator_ada.c",
    "compiler/generator_csharp.c",
    "compiler/generator_dart.c",
    "compiler/generator_go.c",
    "compiler/generator_java.c",
    "compiler/generator_js.c",
    "compiler/generator_pascal.c",
    "compiler/generator_php.c",
    "compiler/generator_python.c",
    "compiler/generator_rust.c",
    "compiler/generator_zig.c",
    "compiler/space.c",
    "compiler/tokeniser.c",
};

pub fn addSnowballModule(b: *std.Build, antfly_mod: *std.Build.Module) void {
    const snowball_mod = b.addModule("snowball", .{
        .root_source_file = b.path(snowball_generated_root ++ "/root.zig"),
    });

    antfly_mod.addImport("snowball", snowball_mod);
}

pub fn snowballGeneratedPath(b: *std.Build, comptime fmt: []const u8, args: anytype) []const u8 {
    return b.fmt(snowball_generated_root ++ "/" ++ fmt, args);
}

pub fn snowballRootContents(b: *std.Build) []const u8 {
    const fragments = b.allocator.alloc([]const u8, 1 + snowball_languages.len) catch @panic("OOM");
    fragments[0] =
        "pub const Env = @import(\"env.zig\").Env;\n" ++
        "pub const Among = @import(\"env.zig\").Among;\n";
    for (snowball_languages, 0..) |lang, idx| {
        fragments[1 + idx] = b.fmt("pub const {s} = @import(\"{s}_stemmer.zig\");\n", .{ lang, lang });
    }
    return std.mem.concat(b.allocator, u8, fragments) catch @panic("OOM");
}

pub fn addSnowballCompiler(b: *std.Build) *std.Build.Step.Compile {
    const snowball_dep = b.path("deps/snowball");

    const snowball_compiler = b.addExecutable(.{
        .name = "snowball",
        .root_module = b.createModule(.{
            .root_source_file = null,
            .target = b.graph.host,
        }),
    });
    snowball_compiler.root_module.link_libc = true;
    for (snowball_compiler_sources) |src| {
        snowball_compiler.root_module.addCSourceFiles(.{
            .root = snowball_dep,
            .files = &.{src},
            .flags = &.{ "-O2", "-W", "-Wall" },
        });
    }

    return snowball_compiler;
}

const addFileCompareTool = @import("../../../tools/build_support.zig").addFileCompareTool;

pub fn addSnowballGeneratedOutputs(
    b: *std.Build,
    snowball_compiler: *std.Build.Step.Compile,
) struct {
    root: std.Build.LazyPath,
    env: std.Build.LazyPath,
    stemmers: [snowball_languages.len]std.Build.LazyPath,
} {
    const snowball_dep = b.path("deps/snowball");

    const wf = b.addWriteFiles();
    const root = wf.add("root.zig", snowballRootContents(b));
    const env = wf.addCopyFile(snowball_dep.path(b, "zig/env.zig"), "env.zig");

    var stemmers: [snowball_languages.len]std.Build.LazyPath = undefined;
    inline for (snowball_languages, 0..) |lang, idx| {
        const run = b.addRunArtifact(snowball_compiler);
        run.addFileArg(snowball_dep.path(b, b.fmt("algorithms/{s}.sbl", .{lang})));
        run.addArg("-zig");
        run.addArg("-o");
        stemmers[idx] = run.addOutputFileArg(b.fmt("{s}_stemmer.zig", .{lang}));
    }

    return .{
        .root = root,
        .env = env,
        .stemmers = stemmers,
    };
}

pub fn addSnowballRegenStep(b: *std.Build) void {
    const regen_step = b.step("regen-snowball", "Regenerate checked-in Zig Snowball stemmers");
    const snowball_compiler = addSnowballCompiler(b);
    const generated = addSnowballGeneratedOutputs(b, snowball_compiler);

    const update = b.addUpdateSourceFiles();
    update.addCopyFileToSource(generated.root, snowball_generated_root ++ "/root.zig");
    update.addCopyFileToSource(generated.env, snowball_generated_root ++ "/env.zig");
    for (snowball_languages, 0..) |lang, idx| {
        update.addCopyFileToSource(generated.stemmers[idx], snowballGeneratedPath(b, "{s}_stemmer.zig", .{lang}));
    }

    const fmt = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "fmt",
        snowball_generated_root,
    });
    fmt.step.dependOn(&update.step);
    regen_step.dependOn(&fmt.step);
}

pub fn addSnowballCheckStep(b: *std.Build) void {
    const check_step = b.step("check-snowball", "Check checked-in Zig Snowball stemmers are current");
    const snowball_compiler = addSnowballCompiler(b);
    const generated = addSnowballGeneratedOutputs(b, snowball_compiler);

    const fmt = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "fmt",
    });
    fmt.addFileArg(generated.root);
    fmt.addFileArg(generated.env);
    for (snowball_languages, 0..) |_, idx| {
        fmt.addFileArg(generated.stemmers[idx]);
    }

    const compare_tool = addFileCompareTool(b);
    const compare = b.addRunArtifact(compare_tool);
    compare.step.dependOn(&fmt.step);
    compare.addFileArg(generated.root);
    compare.addFileArg(b.path(snowball_generated_root ++ "/root.zig"));
    compare.addFileArg(generated.env);
    compare.addFileArg(b.path(snowball_generated_root ++ "/env.zig"));
    for (snowball_languages, 0..) |lang, idx| {
        compare.addFileArg(generated.stemmers[idx]);
        compare.addFileArg(b.path(snowballGeneratedPath(b, "{s}_stemmer.zig", .{lang})));
    }
    check_step.dependOn(&compare.step);
}
