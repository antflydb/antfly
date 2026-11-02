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

// Speech2Seq audio transcription pipeline (Whisper via ONNX or native).
//
// Architecture: audio preprocessing → encoder → text decoder.
// Converts audio to text using Whisper-style models.
//
// Required model files:
//   - encoder_model.onnx (or via native architectures)
//   - decoder_model.onnx
//   - tokenizer.json
//   - config.json (model_type: whisper)
//   - preprocessor_config.json (mel spectrogram params)

const std = @import("std");

/// One audio item's generated-token budget, shared by every window and retry.
/// Forced prompt tokens are excluded; generated timestamps and EOS are included.
pub const OutputTokenBudget = struct {
    remaining: ?usize = null,

    pub fn check(self: @This()) !void {
        if (self.remaining == 0) return error.InferenceOutputTokensExceeded;
    }

    pub fn consume(self: *@This()) !void {
        try self.check();
        if (self.remaining) |*remaining| remaining.* -= 1;
    }

    /// Bound both the current pipelined step and speculative lookahead. The
    /// caller's decoder position includes its prompt and previous output.
    pub fn decodeEnd(self: @This(), position: usize, model_end: usize) !usize {
        try self.check();
        return if (self.remaining) |remaining| @min(model_end, position +| remaining) else model_end;
    }
};

test "transcription output budget persists across windows and retries" {
    var budget: OutputTokenBudget = .{ .remaining = 4 };
    // A first window consumes text and EOS; the next window inherits two slots.
    try budget.consume();
    try budget.consume();
    try std.testing.expectEqual(@as(usize, 102), try budget.decodeEnd(100, 448));
    try budget.consume();
    try budget.consume();
    try std.testing.expectError(error.InferenceOutputTokensExceeded, budget.check());
    try std.testing.expectError(error.InferenceOutputTokensExceeded, budget.consume());
    try std.testing.expectError(error.InferenceOutputTokensExceeded, budget.decodeEnd(104, 448));
}

test "transcription lookahead respects the last token and model context" {
    const budget: OutputTokenBudget = .{ .remaining = 1 };
    try std.testing.expectEqual(@as(usize, 41), try budget.decodeEnd(40, 448));
    try std.testing.expectEqual(@as(usize, 448), try budget.decodeEnd(448, 448));
    const automatic: OutputTokenBudget = .{};
    try std.testing.expectEqual(@as(usize, 448), try automatic.decodeEnd(40, 448));
}
