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

    // NOTE: The real `xla_proto` bindings used to be generated at build
    // time from `proto/hlo.desc` via
    // `@import("protobuf").addProtoModule(...)`. That helper was removed
    // when the pinned protobuf library dropped its codegen support. Until
    // a replacement is wired up we fall back to a hand-written stub that
    // provides just the struct surface used by `src/hlo.zig` so the
    // module compiles; runtime serialization via this module is a no-op
    // and should only be reached when pjrt support is genuinely enabled.
    const xla_proto_mod = b.createModule(.{
        .root_source_file = b.path("proto/xla_proto_stub.zig"),
        .target = target,
        .optimize = optimize,
    });
    xla_proto_mod.addImport("protobuf", protobuf_mod);

    const pjrt_mod = b.addModule("pjrt", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    pjrt_mod.addImport("protobuf", protobuf_mod);
    pjrt_mod.addImport("xla_proto", xla_proto_mod);

    // Tests
    const test_step = b.step("test", "Run unit tests");

    const test_files = [_][]const u8{
        "src/hlo.zig",
        "src/pjrt.zig",
    };

    for (test_files) |file| {
        const mod = b.createModule(.{
            .root_source_file = b.path(file),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "protobuf", .module = protobuf_mod },
                .{ .name = "xla_proto", .module = xla_proto_mod },
            },
        });
        const t = b.addTest(.{ .root_module = mod });
        t.root_module.link_libc = true;
        test_step.dependOn(&b.addRunArtifact(t).step);
    }
}
