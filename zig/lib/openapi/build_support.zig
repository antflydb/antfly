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

pub const GenerateOptions = struct {
    compiler: *std.Build.Step.Compile,
    scripts_root: std.Build.LazyPath,
    spec: std.Build.LazyPath,
    package_name: []const u8,
    generate: []const u8,
    import_mappings: []const [2][]const u8 = &.{},
    zig_type_mappings: []const [2][]const u8 = &.{},
};

/// All callers share the same declared conversion inputs and generator protocol.
/// The returned directory is a build output; configuring it performs no I/O.
pub fn addGeneratedDirectory(b: *std.Build, options: GenerateOptions) std.Build.LazyPath {
    const convert = b.addSystemCommand(&.{ "uv", "run", "--project" });
    convert.addDirectoryArg(options.scripts_root);
    convert.addArgs(&.{ "--locked", "python" });
    convert.addFileArg(options.scripts_root.path(b, "yaml_to_json.py"));
    convert.addFileInput(options.scripts_root.path(b, "pyproject.toml"));
    convert.addFileInput(options.scripts_root.path(b, "uv.lock"));
    convert.addFileArg(options.spec);
    const json_spec = convert.addOutputFileArg(b.fmt("{s}.json", .{options.package_name}));

    const codegen = b.addRunArtifact(options.compiler);
    codegen.addArg("--spec");
    codegen.addFileArg(json_spec);
    codegen.addArgs(&.{ "--package", options.package_name, "--generate", options.generate });
    for (options.import_mappings) |mapping| {
        codegen.addArgs(&.{ "--import-mapping", b.fmt("{s}={s}", .{ mapping[0], mapping[1] }) });
    }
    for (options.zig_type_mappings) |mapping| {
        codegen.addArgs(&.{ "--zig-type-mapping", b.fmt("{s}={s}", .{ mapping[0], mapping[1] }) });
    }
    codegen.addArg("--output");
    return codegen.addOutputDirectoryArg(options.package_name);
}
