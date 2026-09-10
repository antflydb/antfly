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

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const protobuf_dep = b.dependency("protobuf", .{
        .target = target,
        .optimize = optimize,
    });
    const protobuf_mod = protobuf_dep.module("protobuf");

    // NOTE: onnx_proto generation via `@import("protobuf").addProtoModule` was
    // removed when the pinned protobuf library dropped its build-time codegen
    // helper. None of the files under src/ currently import `onnx_proto` — the
    // ONNX wire code hand-rolls its own messages via `protobuf.message` /
    // `protobuf.wire`, so the generated module is no longer needed. If/when
    // consumers need the auto-generated bindings again, either restore the
    // upstream codegen helper or check in a generated `onnx_proto.zig`.

    const data_mod = b.addModule("onnx_data", .{
        .root_source_file = b.path("src/data.zig"),
        .target = target,
        .optimize = optimize,
    });
    data_mod.addImport("protobuf", protobuf_mod);

    const onnx_mod = b.addModule("onnx", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    onnx_mod.addImport("protobuf", protobuf_mod);
    onnx_mod.addImport("onnx_data", data_mod);

    // File-format tests run with protobuf alone. The parent build supplies ML
    // to the separate graph conversion suite.
    const test_step = b.step("test", "Run unit tests");
    const data_tests = b.addTest(.{ .root_module = data_mod });
    test_step.dependOn(&b.addRunArtifact(data_tests).step);

    // Attribute decoding uses the same proto types as the data module.
    const standalone_tests = [_][]const u8{
        "src/attrs.zig",
    };

    for (standalone_tests) |file| {
        const t = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path(file),
                .target = target,
                .optimize = optimize,
            }),
        });
        t.root_module.addImport("onnx_data", data_mod);
        test_step.dependOn(&b.addRunArtifact(t).step);
    }
}
