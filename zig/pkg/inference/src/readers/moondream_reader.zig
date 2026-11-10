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

//! `/read` adapter for the native Moondream2 pipeline. It owns the question
//! that the reader sends to the model; `reader.zig` parses the answer.

const std = @import("std");
const pipeline_mod = @import("../pipelines/moondream.zig");
const generation = @import("../pipelines/generation.zig");
const reader_types = @import("types.zig");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

pub const ReadOptions = reader_types.ReadOptions;
pub const default_max_tokens: usize = 256;

pub fn probeModelDir(allocator: std.mem.Allocator, model_dir: []const u8) !bool {
    return pipeline_mod.probeModelDir(allocator, model_dir);
}

/// `/read` reads text, so absent or blank prompts ask Moondream to transcribe
/// the image, like every other reader's default task. Other prompts are
/// natural-language questions passed through as-is.
pub const default_read_question = "Transcribe all text in this image.";

pub fn question(prompt: ?[]const u8) []const u8 {
    const raw = prompt orelse return default_read_question;
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    return if (trimmed.len == 0) default_read_question else trimmed;
}

pub const LoadedMoondreamReader = struct {
    allocator: std.mem.Allocator,
    pipeline: pipeline_mod.Pipeline,

    pub fn loadFromDirWithControl(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        control: ?InferenceExecutionControl,
        io: ?std.Io,
    ) !LoadedMoondreamReader {
        return .{
            .allocator = allocator,
            .pipeline = try pipeline_mod.Pipeline.loadWithControl(allocator, model_path, control, io),
        };
    }

    pub fn deinit(self: *LoadedMoondreamReader) void {
        self.pipeline.deinit();
    }

    /// Generate the raw answer text for one image. Caller owns the result.
    pub fn readRaw(self: *LoadedMoondreamReader, image_data: []const u8, options: ReadOptions) !generation.GenerationResult {
        if (options.cache_dtype) |dtype| {
            if (!std.mem.eql(u8, dtype, "f32")) return error.UnsupportedCacheDtype;
        }
        const max_tokens = options.max_tokens orelse default_max_tokens;
        return self.pipeline.answer(image_data, question(options.prompt), .{
            .max_tokens = @intCast(@min(max_tokens, std.math.maxInt(i32))),
        }, options.execution_control);
    }

    pub fn inputTokenCount(self: *LoadedMoondreamReader, options: ReadOptions) !usize {
        return self.pipeline.inputTokenCount(question(options.prompt));
    }
};

test "read question defaults to transcription" {
    try std.testing.expectEqualStrings(default_read_question, question(null));
    try std.testing.expectEqualStrings(default_read_question, question(" \n"));
    try std.testing.expectEqualStrings("What color is it?", question(" What color is it? "));
}
