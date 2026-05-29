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

const constants = @import("constants.zig");

pub const Target = enum {
    onnx,
    gguf,
    safetensors,
};

pub const Options = struct {
    model_dir: []const u8,
    output_path: ?[]const u8 = null,
    target: Target = .onnx,
    format: []const u8 = constants.default_format,
    min_elements: usize = constants.default_min_elements,
    quantize_include_prefixes: ?[]const u8 = null,
    quantize_exclude_prefixes: ?[]const u8 = null,
    projector_output_path: ?[]const u8 = null,
    projector_format: ?[]const u8 = null,
    dry_run: bool = false,
};
