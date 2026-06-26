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

    const protobuf_build = @import("protobuf");
    const onnx_proto_mod = protobuf_build.addProtoModule(
        b,
        protobuf_dep,
        b.path("proto/onnx.desc"),
        "onnx_proto",
        &.{},
    );

    const onnx_mod = b.addModule("onnx", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    onnx_mod.addImport("protobuf", protobuf_mod);

    const test_step = b.step("test", "Run unit tests");

    // Standalone-testable files (no external imports)
    const standalone_tests = [_][]const u8{
        "src/attrs.zig",
    };

    for (standalone_tests) |file| {
        const t = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path(file),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "protobuf", .module = protobuf_mod },
                },
            }),
        });
        test_step.dependOn(&b.addRunArtifact(t).step);
    }

    const generated_proto_smoke = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.addWriteFiles().add("onnx_generated_proto_smoke.zig",
                \\const std = @import("std");
                \\const onnx = @import("onnx_proto").onnx;
                \\
                \\test "generated ONNX proto bindings encode and decode a model" {
                \\    const alloc = std.testing.allocator;
                \\    var opsets = [_]onnx.OperatorSetIdProto{.{ .domain = "", .version = 17 }};
                \\    var model = onnx.ModelProto{
                \\        .ir_version = 8,
                \\        .graph = .{ .name = "generated-smoke" },
                \\        .opset_import = opsets[0..],
                \\    };
                \\    const bytes = try model.encode(alloc);
                \\    defer alloc.free(bytes);
                \\
                \\    var decoded = try onnx.ModelProto.decode(alloc, bytes);
                \\    defer decoded.deinit(alloc);
                \\    try std.testing.expectEqual(@as(i64, 8), decoded.ir_version);
                \\    try std.testing.expectEqualStrings("generated-smoke", decoded.graph.name);
                \\    try std.testing.expectEqual(@as(usize, 1), decoded.opset_import.len);
                \\    try std.testing.expectEqual(@as(i64, 17), decoded.opset_import[0].version);
                \\}
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "onnx_proto", .module = onnx_proto_mod },
            },
        }),
    });
    test_step.dependOn(&b.addRunArtifact(generated_proto_smoke).step);
}
