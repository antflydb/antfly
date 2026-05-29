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

// TokenizerBatch — thin wrapper around HfTokenizer that produces a token_fn
// compatible with fused_chunker_data.assembleTokenBatch.
//
// Usage:
//   var tb = try TokenizerBatch.loadFromDir(allocator, "/path/to/model", 512);
//   defer tb.deinit();
//   var ctx = tb.makeTokenFnCtx();
//   const batch = try fused_chunker_data.assembleTokenBatch(
//       allocator, samples, indices, 512, 64, &ctx, TokenFnCtx.call,
//   );

const std = @import("std");
const hf_tokenizer = @import("inference_hf_tokenizer");
const tokenizer_mod = @import("inference_tokenizer");
const compat = @import("../io/compat.zig");

const HfTokenizer = hf_tokenizer.HfTokenizer;
const Tokenizer = tokenizer_mod.Tokenizer;

pub const TokenizerBatch = struct {
    allocator: std.mem.Allocator,
    tok: *HfTokenizer,
    max_length: usize,

    /// Load a tokenizer from a directory containing tokenizer.json.
    pub fn loadFromDir(
        allocator: std.mem.Allocator,
        dir_path: []const u8,
        max_length: usize,
    ) !TokenizerBatch {
        const io = compat.io();
        const cwd = compat.cwd();
        const dir = try cwd.openDir(io, dir_path, .{});
        const tok = try HfTokenizer.loadFromDir(allocator, dir, io, "tokenizer.json");
        return .{
            .allocator = allocator,
            .tok = tok,
            .max_length = max_length,
        };
    }

    pub fn deinit(self: *TokenizerBatch) void {
        self.tok.tokenizer().deinitTokenizer();
        self.* = undefined;
    }

    /// Return a TokenFnCtx whose `call` method satisfies the token_fn contract
    /// expected by fused_chunker_data.assembleTokenBatch.
    pub fn makeTokenFnCtx(self: *TokenizerBatch) TokenFnCtx {
        return .{ .tb = self };
    }
};

pub const TokenFnCtx = struct {
    tb: *TokenizerBatch,

    /// token_fn contract:
    ///   fn(ctx: *TokenFnCtx, text: []const u8, out_ids: []i32, out_mask: []i32, out_offsets: ?[][2]u32) usize
    ///
    /// Fills pre-allocated out_ids and out_mask (length max_seq_len), optionally
    /// fills out_offsets with per-token byte ranges, and returns the actual number
    /// of tokens produced (<= out_ids.len).  Returns 0 on encoding error.
    pub fn call(
        ctx: *TokenFnCtx,
        text: []const u8,
        out_ids: []i32,
        out_mask: []i32,
        out_offsets: ?[][2]u32,
    ) usize {
        const allocator = ctx.tb.allocator;
        const tok: Tokenizer = ctx.tb.tok.tokenizer();

        var result = tok.encodeForModel(allocator, text, ctx.tb.max_length) catch return 0;
        defer result.deinit();

        const actual = result.ids.len;
        const n = @min(actual, out_ids.len);

        // Copy token ids and attention mask.
        @memcpy(out_ids[0..n], result.ids[0..n]);
        @memcpy(out_mask[0..n], result.attention_mask[0..n]);

        // Zero-pad any remaining slots beyond the actual token count.
        if (n < out_ids.len) {
            @memset(out_ids[n..], 0);
            @memset(out_mask[n..], 0);
        }

        // Copy offsets when both sides provide them.
        if (out_offsets) |dst| {
            if (result.offsets) |src| {
                const ncopy = @min(n, dst.len);
                @memcpy(dst[0..ncopy], src[0..ncopy]);
            }
        }

        return n;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "TokenizerBatch compile" {
    _ = TokenizerBatch;
    _ = TokenFnCtx;
}
