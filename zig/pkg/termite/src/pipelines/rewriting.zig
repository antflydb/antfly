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

// Seq2Seq text rewriting pipeline (T5/BART via encoder-decoder).
//
// Architecture: encode input text → decode output text autoregressively.
// Uses the EncoderDecoderPipeline for the generation loop — works with any
// backend that provides separate encode/decode sessions (ONNX, native, MLX).
//
// Required model files:
//   - encoder_model.onnx (or encoder.onnx)
//   - decoder_model_merged.onnx (or decoder_model.onnx)
//   - tokenizer.json
//   - config.json (model_type: t5, bart, etc.)

const std = @import("std");
const backends = @import("../backends/backends.zig");
const tokenizer_mod = @import("termite_tokenizer");
const enc_dec_mod = @import("encoder_decoder.zig");

pub const RewriteConfig = struct {
    max_length: usize = 512,
};

pub const RewriteResult = struct {
    text: []const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *RewriteResult) void {
        self.allocator.free(self.text);
    }
};

pub const RewritingPipeline = struct {
    allocator: std.mem.Allocator,
    enc_dec: enc_dec_mod.EncoderDecoderPipeline,
    tokenizer: tokenizer_mod.Tokenizer,
    config: RewriteConfig,

    pub fn rewrite(self: *RewritingPipeline, text: []const u8) !RewriteResult {
        const allocator = self.allocator;

        // 1. Tokenize input text (raw, no [CLS]/[SEP] — T5/BART have their own special tokens)
        const token_ids_i32 = try self.tokenizer.encode(allocator, text);
        defer allocator.free(token_ids_i32);

        // Convert i32 token IDs to i64 for the backend
        const seq_len = @min(token_ids_i32.len, self.config.max_length);
        const input_ids = try allocator.alloc(i64, seq_len);
        defer allocator.free(input_ids);
        for (0..seq_len) |i| {
            input_ids[i] = @intCast(token_ids_i32[i]);
        }

        // 2. Run encoder
        const encoder_outputs = try self.enc_dec.encode(allocator, input_ids, seq_len);
        defer {
            for (encoder_outputs) |*o| o.deinit();
            allocator.free(encoder_outputs);
        }

        // 3. Build encoder attention mask (all 1s for real tokens)
        const enc_mask = try allocator.alloc(i64, seq_len);
        defer allocator.free(enc_mask);
        @memset(enc_mask, 1);

        // 4. Greedy decode
        var gen_result = try self.enc_dec.greedyDecode(allocator, encoder_outputs, enc_mask, seq_len);
        defer gen_result.deinit();

        // 5. Decode output token IDs to text
        const output_text = try self.tokenizer.decode(allocator, gen_result.text_ids);

        return .{
            .text = output_text,
            .allocator = allocator,
        };
    }
};
