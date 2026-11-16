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
const c_file = @import("../util/c_file.zig");

/// Use the real pull-time publication gate on a private, contained copy. Raw
/// upstream snapshots do not carry Antfly's model_manifest.json. Never handwrite
/// capabilities in a serving qualification fixture or modify the source files.
pub fn prepare(a: std.mem.Allocator, temporary: *std.testing.TmpDir, source: []const u8) ![]u8 {
    if (!@import("builtin").is_test) @compileError("decision qualification fixture is test-only");
    const io = std.testing.io;
    try temporary.dir.createDirPath(io, "model/encoder_config");
    const root = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const directory = try std.fs.path.join(a, &.{ root, "model" });
    errdefer a.free(directory);
    inline for (.{ "config.json", "encoder_config/config.json", "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json", "model.safetensors", "antfly_inference_bundle.json", "antfly_inference_variants.json", "gliner2-encoder.Q8_0.gguf", "gliner2-head.Q8_0.gguf" }) |name| {
        const from = try std.fs.path.join(a, &.{ source, name });
        defer a.free(from);
        if (try c_file.fileExistsChecked(a, from)) {
            const to = try std.fs.path.join(a, &.{ directory, name });
            defer a.free(to);
            const from_z = try a.dupeSentinel(u8, from, 0);
            defer a.free(from_z);
            const to_z = try a.dupeSentinel(u8, to, 0);
            defer a.free(to_z);
            if (c_file.c.link(from_z.ptr, to_z.ptr) != 0) return error.DecideQualificationFixtureLinkFailed;
        }
    }
    const manifest = try @import("../registry/registry.zig").synthesizePulledModelManifestJson(a, directory, null, null);
    defer a.free(manifest);
    try temporary.dir.writeFile(io, .{ .sub_path = "model/model_manifest.json", .data = manifest });
    // Optional evidence export contains the canonical publication JSON only.
    // It can be reused with the same immutable files for process-level checks.
    if (@import("antfly_platform").env.getenv("ANTFLY_GLINER25_CUDA_MANIFEST_OUTPUT_DIR")) |output| {
        const name = if (try c_file.fileExistsInDirChecked(a, directory, "model.safetensors")) "decide-fp32-manifest.json" else "decide-q8-manifest.json";
        const path = try std.fs.path.join(a, &.{ output, name });
        defer a.free(path);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = manifest });
    }
    return directory;
}
