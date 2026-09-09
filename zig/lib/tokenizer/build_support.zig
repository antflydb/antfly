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

pub const Modules = struct {
    tokenizer: *std.Build.Module,
    huggingface: *std.Build.Module,
    fixed_data: *std.Build.Module,
};

pub fn create(b: *std.Build, options: struct {
    root: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    protobuf: *std.Build.Module,
    sentencepiece_proto: *std.Build.Module,
}) Modules {
    const tokenizer = b.createModule(.{
        .root_source_file = options.root.path(b, "src/tokenizer.zig"),
        .target = options.target,
        .optimize = options.optimize,
        .imports = &.{
            .{ .name = "protobuf", .module = options.protobuf },
            .{ .name = "sentencepiece_proto", .module = options.sentencepiece_proto },
        },
    });
    const huggingface = b.createModule(.{
        .root_source_file = options.root.path(b, "src/hf_root.zig"),
        .target = options.target,
        .optimize = options.optimize,
        .imports = &.{.{ .name = "inference_tokenizer", .module = tokenizer }},
    });
    const files = b.addWriteFiles();
    _ = files.addCopyFile(options.root.path(b, "testdata/embedder/tokenizer.json"), "tokenizer.json");
    const fixed_data = b.createModule(.{
        .root_source_file = files.add("root.zig", "pub const tokenizer_json = @embedFile(\"tokenizer.json\");\n"),
    });
    return .{ .tokenizer = tokenizer, .huggingface = huggingface, .fixed_data = fixed_data };
}

/// Generation is a declared dependency of the tokenizer module; configuring
/// unrelated consumers does not execute protoc or the compatibility fixup.
pub fn addSentencePieceProtoModule(b: *std.Build, protobuf: *std.Build.Dependency, root: std.Build.LazyPath) *std.Build.Module {
    const codegen = b.addRunArtifact(protobuf.artifact("protoc-zig"));
    codegen.addArg("--desc");
    codegen.addFileArg(root.path(b, "proto/sentencepiece_model.desc"));
    codegen.addArg("--output");
    const raw_dir = codegen.addOutputDirectoryArg("sentencepiece_proto_raw");
    const fixup = b.addExecutable(.{
        .name = "patch_sentencepiece_proto",
        .root_module = b.createModule(.{
            .root_source_file = root.path(b, "tools/patch_sentencepiece_proto.zig"),
            .target = b.graph.host,
            .optimize = .ReleaseSafe,
        }),
    });
    const run = b.addRunArtifact(fixup);
    run.addFileArg(raw_dir.path(b, "root.zig"));
    run.addFileArg(raw_dir.path(b, "sentencepiece.zig"));
    const output = run.addOutputDirectoryArg("sentencepiece_proto");
    return b.createModule(.{
        .root_source_file = output.path(b, "root.zig"),
        .imports = &.{.{ .name = "protobuf", .module = protobuf.module("protobuf") }},
    });
}
