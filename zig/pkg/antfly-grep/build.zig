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

pub fn add(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) void {
    const fst = b.createModule(.{ .root_source_file = b.path("lib/fst/src/mod.zig"), .target = target, .optimize = optimize });
    const regex = b.createModule(.{ .root_source_file = b.path("lib/regex/src/mod.zig"), .target = target, .optimize = optimize });
    regex.addImport("antfly_fst", fst);
    const grapheme = b.createModule(.{ .root_source_file = b.path("lib/tokenizer/src/grapheme.zig"), .target = target, .optimize = optimize });
    regex.addImport("antfly_grapheme", grapheme);
    const json = b.createModule(.{ .root_source_file = b.path("lib/json/src/mod.zig"), .target = target, .optimize = optimize });
    const http = b.createModule(.{ .root_source_file = b.path("lib/httpx/src/httpx.zig"), .target = target, .optimize = optimize });
    http.addImport("antfly-json", json);
    const lib = b.addModule("antfly-grep", .{ .root_source_file = b.path("pkg/antfly-grep/src/root.zig"), .target = target, .optimize = optimize });
    lib.addImport("antfly_encoding", b.createModule(.{ .root_source_file = b.path("lib/encoding/src/mod.zig"), .target = target, .optimize = optimize }));
    const hash = b.createModule(.{ .root_source_file = b.path("lib/hash/src/mod.zig"), .target = target, .optimize = optimize, .link_libc = true });
    const compress = b.createModule(.{ .root_source_file = b.path("lib/compress/src/mod.zig"), .target = target, .optimize = optimize });
    compress.addImport("antfly_hash", hash);
    lib.addImport("antfly_hash", hash);
    lib.addImport("antfly_compress", compress);
    const syntax = b.createModule(.{ .root_source_file = b.path("lib/syntax/src/root.zig"), .target = target, .optimize = optimize });
    syntax.addImport("util", b.createModule(.{ .root_source_file = b.path("lib/syntax/src/vendor/yuku/util/root.zig"), .target = target, .optimize = optimize }));
    syntax.addImport("dialect", b.addOptions().createModule());
    const syntax_codegen = b.addOptions();
    syntax_codegen.addOption(bool, "source_maps", false);
    syntax.addImport("codegen_options", syntax_codegen.createModule());
    lib.addImport("antfly_syntax", syntax);
    lib.addImport("antfly_regex", regex);
    lib.addImport("httpx", http);
    const profile_mod = b.createModule(.{ .root_source_file = b.path("pkg/antfly-grep/bench/regex_profile.zig"), .target = target, .optimize = optimize, .link_libc = true });
    profile_mod.addImport("antfly_regex", regex);
    const profile = b.addExecutable(.{ .name = "antfly-grep-regex-profile", .root_module = profile_mod });
    b.step("grep-profile", "Build in-process VM/JIT profiling utility").dependOn(&b.addInstallArtifact(profile, .{}).step);
    const main = b.createModule(.{ .root_source_file = b.path("pkg/antfly-grep/src/main.zig"), .target = target, .optimize = optimize });
    main.addImport("antfly-grep", lib);
    main.link_libc = true;
    lib.link_libc = true;
    const exe = b.addExecutable(.{ .name = "antfly-grep", .root_module = main });
    const install = b.addInstallArtifact(exe, .{});
    b.step("grep", "Build the native regex and decision-search CLI").dependOn(&install.step);
    b.getInstallStep().dependOn(&install.step);
    const run = b.addRunArtifact(exe);
    run.addPassthruArgs();
    b.step("grep-run", "Run antfly-grep").dependOn(&run.step);
    const bench = b.addSystemCommand(&.{"python3"});
    bench.addFileArg(b.path("pkg/antfly-grep/bench/compare.py"));
    bench.addArtifactArg(exe);
    bench.addPassthruArgs();
    b.step("grep-bench", "Benchmark grep throughput and peak RSS against optional ripgrep").dependOn(&bench.step);
    const tests = b.addTest(.{ .root_module = lib });
    const test_run = b.addRunArtifact(tests);
    const test_step = b.step("grep-test", "Run native grep unit and integration tests");
    const unit_step = b.step("grep-unit", "Run native grep, regex, grammar and dictionary unit tests");
    test_step.dependOn(unit_step);
    unit_step.dependOn(&test_run.step);
    const search_mod = b.createModule(.{ .root_source_file = b.path("lib/regex/src/search.zig"), .target = target, .optimize = optimize, .link_libc = true });
    search_mod.addImport("antfly_grapheme", grapheme);
    const regex_search_tests = b.addTest(.{ .root_module = search_mod });
    unit_step.dependOn(&b.addRunArtifact(regex_search_tests).step);
    const grammar_tests = b.addTest(.{ .root_module = b.createModule(.{ .root_source_file = b.path("lib/syntax/src/grammar.zig"), .target = target, .optimize = optimize }) });
    unit_step.dependOn(&b.addRunArtifact(grammar_tests).step);
    const dictionary_tests = b.addTest(.{ .root_module = b.createModule(.{ .root_source_file = b.path("lib/compress/src/brotli/shared_dictionary.zig"), .target = target, .optimize = optimize }) });
    unit_step.dependOn(&b.addRunArtifact(dictionary_tests).step);
    const harden_mod = b.createModule(.{ .root_source_file = b.path("pkg/antfly-grep/tests/harden.zig"), .target = target, .optimize = .safe, .link_libc = true });
    const harden_regex = b.createModule(.{ .root_source_file = b.path("lib/regex/src/mod.zig"), .target = target, .optimize = .safe, .link_libc = true });
    harden_regex.addImport("antfly_fst", fst);
    harden_regex.addImport("antfly_grapheme", b.createModule(.{ .root_source_file = b.path("lib/tokenizer/src/grapheme.zig"), .target = target, .optimize = .safe }));
    const harden_syntax = b.createModule(.{ .root_source_file = b.path("lib/syntax/src/root.zig"), .target = target, .optimize = .safe });
    harden_syntax.addImport("util", b.createModule(.{ .root_source_file = b.path("lib/syntax/src/vendor/yuku/util/root.zig"), .target = target, .optimize = .safe }));
    harden_syntax.addImport("dialect", b.addOptions().createModule());
    harden_syntax.addImport("codegen_options", syntax_codegen.createModule());
    const harden_compress = b.createModule(.{ .root_source_file = b.path("lib/compress/src/mod.zig"), .target = target, .optimize = .safe });
    harden_compress.addImport("antfly_hash", b.createModule(.{ .root_source_file = b.path("lib/hash/src/mod.zig"), .target = target, .optimize = .safe, .link_libc = true }));
    harden_mod.addImport("antfly_regex", harden_regex);
    harden_mod.addImport("antfly_syntax", harden_syntax);
    harden_mod.addImport("antfly_compress", harden_compress);
    const fuzz_target = b.option(enum { regex, parsers, dictionaries }, "grep-fuzz-target", "Select one coverage-guided hardening target");
    const fuzz_filters: []const []const u8 = if (fuzz_target) |value| switch (value) {
        .regex => &.{"coverage fuzz regex"},
        .parsers => &.{"coverage fuzz pinned"},
        .dictionaries => &.{"coverage fuzz dictionaries"},
    } else &.{};
    const harden = b.addTest(.{ .name = "antfly-grep-harden-test", .root_module = harden_mod, .filters = fuzz_filters });
    b.step("grep-harden-build", "Build portable hardening test executable without running it").dependOn(&b.addInstallArtifact(harden, .{}).step);
    const harden_run = b.addRunArtifact(harden);
    const harden_step = b.step("grep-harden", "Run bounded malformed-input mutation and coverage fuzz seeds");
    harden_step.dependOn(&harden_run.step);
    test_step.dependOn(harden_step);
    const integration = b.addSystemCommand(&.{"python3"});
    integration.addFileArg(b.path("pkg/antfly-grep/tests/test_cli.py"));
    integration.addArtifactArg(exe);
    const fixture_mod = b.createModule(.{ .root_source_file = b.path("pkg/antfly-grep/tests/local_inference_fixture.zig"), .target = target, .optimize = optimize, .link_libc = true });
    const fixture = b.addLibrary(.{ .name = "antfly-grep-inference-fixture", .linkage = .dynamic, .root_module = fixture_mod });
    integration.addArtifactArg(fixture);
    test_step.dependOn(&integration.step);
    const differential_tools = b.addSystemCommand(&.{"python3"});
    differential_tools.addFileArg(b.path("pkg/antfly-grep/tests/test_differential_tools.py"));
    test_step.dependOn(&differential_tools.step);
    const compatibility = b.addSystemCommand(&.{"python3"});
    compatibility.addFileArg(b.path("pkg/antfly-grep/tests/check_compatibility.py"));
    compatibility.addArg("--check");
    test_step.dependOn(&compatibility.step);
    const evaluation = b.addSystemCommand(&.{"python3"});
    evaluation.addFileArg(b.path("pkg/antfly-grep/bench/evaluate.py"));
    evaluation.addArtifactArg(exe);
    evaluation.addArgs(&.{ "--fixture", "--min-recall", "1", "--min-precision", "1" });
    b.step("grep-eval", "Evaluate labeled coding searches using local protocol fixtures").dependOn(&evaluation.step);
}
