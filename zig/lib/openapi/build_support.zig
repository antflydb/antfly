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

pub const GenerateOptions = struct {
    compiler: *std.Build.Step.Compile,
    scripts_root: std.Build.LazyPath,
    spec: std.Build.LazyPath,
    package_name: []const u8,
    generate: []const u8,
    external_types_module: ?[]const u8 = null,
    import_mappings: []const [2][]const u8 = &.{},
    zig_type_mappings: []const [2][]const u8 = &.{},
    /// Named component -> JSON pointer into the original document. Keeps
    /// vendored specs intact when their operation schemas are inline.
    schema_aliases: []const [2][]const u8 = &.{},
};

/// All callers share the same declared conversion inputs and generator protocol.
/// The returned directory is a build output; configuring it performs no I/O.
pub fn addGeneratedDirectory(b: *std.Build, options: GenerateOptions) std.Build.LazyPath {
    const convert = b.addSystemCommand(&.{ "uv", "run", "--project" });
    convert.addDirectoryArg2(options.scripts_root, .{ .make_absolute = true });
    convert.addArgs(&.{ "--locked", "python" });
    convert.addFileArg2(options.scripts_root.path(b, "yaml_to_json.py"), .{ .make_absolute = true });
    convert.addFileInput(options.scripts_root.path(b, "pyproject.toml"));
    convert.addFileInput(options.scripts_root.path(b, "uv.lock"));
    convert.addFileArg2(options.spec, .{ .make_absolute = true });
    const json_spec = convert.addOutputFileArg2(b.fmt("{s}.json", .{options.package_name}), .{ .make_absolute = true });
    for (options.schema_aliases) |alias| {
        convert.addArgs(&.{ "--schema-alias", b.fmt("{s}={s}", .{ alias[0], alias[1] }) });
    }

    const codegen = b.addRunArtifact(options.compiler);
    codegen.addArg("--spec");
    codegen.addFileArg2(json_spec, .{ .make_absolute = true });
    codegen.addArgs(&.{ "--package", options.package_name, "--generate", options.generate });
    if (options.external_types_module) |module_name|
        codegen.addArgs(&.{ "--external-types-module", module_name });
    for (options.import_mappings) |mapping| {
        codegen.addArgs(&.{ "--import-mapping", b.fmt("{s}={s}", .{ mapping[0], mapping[1] }) });
    }
    for (options.zig_type_mappings) |mapping| {
        codegen.addArgs(&.{ "--zig-type-mapping", b.fmt("{s}={s}", .{ mapping[0], mapping[1] }) });
    }
    codegen.addArg("--output");
    return codegen.addOutputDirectoryArg2(options.package_name, .{ .make_absolute = true });
}
