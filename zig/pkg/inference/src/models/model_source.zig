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

// Model manifest and config loading.
//
// Auto-detects model layout from a directory: ONNX files, tokenizer type,
// config.json, tokenizer_config.json, and optional model_manifest.json.

const std = @import("std");
const manifest_mod = @import("manifest.zig");

/// Borrowed construction input. Physical identity, format selection and ONNX
/// dependency context are independent: a symlink target need not retain the
/// requested filename's extension or directory layout. Owners retain all
/// strings and the manifest until synchronous construction completes.
pub const ModelSource = struct {
    pub const Kind = enum { directory, gguf, onnx };
    path: []const u8,
    kind: Kind,
    logical_path: ?[]const u8 = null,
    manifest: ?*const manifest_mod.ModelManifest = null,
    external_base_dir: ?[]const u8 = null,

    pub fn fromPath(path: []const u8) ModelSource {
        return .{ .path = path, .kind = kindForPath(path) };
    }
    pub fn kindForPath(path: []const u8) Kind {
        if (std.mem.endsWith(u8, path, ".onnx")) return .onnx;
        if (std.mem.endsWith(u8, path, ".gguf")) return .gguf;
        return .directory;
    }
};
