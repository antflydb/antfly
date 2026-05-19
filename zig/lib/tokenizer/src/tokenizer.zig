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

// Unified tokenizer interface.
//
// Abstracts over SentencePiece (.model) and HuggingFace (tokenizer.json) tokenizers
// so that pipelines can use either transparently.

const std = @import("std");

pub const sentencepiece = @import("sentencepiece.zig");
pub const hf = @import("hf_tokenizer.zig");

pub const Token = struct {
    id: i32,
    text: []const u8,
};

pub const EncodeResult = struct {
    ids: []i32,
    attention_mask: []i32,
    /// Character offsets [start, end) for each token position.
    /// null when the tokenizer doesn't support offset tracking.
    offsets: ?[]const [2]u32 = null,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *EncodeResult) void {
        if (self.offsets) |o| self.allocator.free(o);
        self.allocator.free(self.ids);
        self.allocator.free(self.attention_mask);
    }
};

/// Special token IDs for a tokenizer.
pub const SpecialTokens = struct {
    cls_id: i32 = 101,
    sep_id: i32 = 102,
    pad_id: i32 = 0,
    unk_id: i32 = 100,
    mask_id: i32 = 103,
};

/// Unified tokenizer interface over SentencePiece and HuggingFace tokenizers.
pub const Tokenizer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Encode text into token IDs (without special tokens or padding).
        encode: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8) anyerror![]i32,
        /// Encode into a caller-provided buffer, appending token IDs. Lets
        /// hot ingest paths reuse a single ArrayList across many encode
        /// calls instead of allocating and freeing per call.
        encodeInto: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) anyerror!void,
        /// Encode text with model wrapping such as [CLS]/[SEP], optionally including offsets.
        encodeForModel: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize) anyerror!EncodeResult,
        /// Encode text for causal generation, optionally with BOS-aware start-of-sequence semantics.
        encodeGeneration: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) anyerror!EncodeResult,
        /// Decode token IDs back to text.
        decode: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, ids: []const i32) anyerror![]u8,
        /// Get special token IDs.
        specialTokens: *const fn (ptr: *anyopaque) SpecialTokens,
        /// Get vocabulary size.
        vocabSize: *const fn (ptr: *anyopaque) usize,
        /// Free resources.
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn encode(self: Tokenizer, allocator: std.mem.Allocator, text: []const u8) ![]i32 {
        return self.vtable.encode(self.ptr, allocator, text);
    }

    /// Append `text`'s token IDs to `out`. Caller owns `out`. The buffer is
    /// not cleared on entry, so callers can either pre-clear (`clearRetainingCapacity`)
    /// to encode a fresh sequence or skip clearing to concatenate sequences.
    pub fn encodeInto(self: Tokenizer, allocator: std.mem.Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) !void {
        return self.vtable.encodeInto(self.ptr, allocator, text, out);
    }

    pub fn decode(self: Tokenizer, allocator: std.mem.Allocator, ids: []const i32) ![]u8 {
        return self.vtable.decode(self.ptr, allocator, ids);
    }

    pub fn specialTokens(self: Tokenizer) SpecialTokens {
        return self.vtable.specialTokens(self.ptr);
    }

    pub fn vocabSize(self: Tokenizer) usize {
        return self.vtable.vocabSize(self.ptr);
    }

    pub fn deinitTokenizer(self: Tokenizer) void {
        self.vtable.deinit(self.ptr);
    }

    /// Encode text with [CLS] and [SEP] special tokens, padded/truncated to max_length.
    /// Returns token IDs and attention mask.
    pub fn encodeForModel(self: Tokenizer, allocator: std.mem.Allocator, text: []const u8, max_length: usize) !EncodeResult {
        return self.vtable.encodeForModel(self.ptr, allocator, text, max_length);
    }

    /// Encode text for causal generation without implicitly appending an EOS/SEP token.
    /// This avoids classifier-style wrapping for decoder-only models.
    pub fn encodeForGeneration(self: Tokenizer, allocator: std.mem.Allocator, text: []const u8, max_length: usize) !EncodeResult {
        return self.encodeForGenerationConfigured(allocator, text, max_length, false);
    }

    pub fn encodeForGenerationConfigured(
        self: Tokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        max_length: usize,
        add_bos_token: bool,
    ) !EncodeResult {
        return self.vtable.encodeGeneration(self.ptr, allocator, text, max_length, add_bos_token);
    }

    /// Generic fallback for tokenizers that don't need BOS-aware pre-tokenization.
    pub fn encodeForGenerationFallback(self: Tokenizer, allocator: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) !EncodeResult {
        const raw_ids = try self.encode(allocator, text);
        defer allocator.free(raw_ids);

        const prepend_bos = add_bos_token and self.specialTokens().cls_id >= 0 and max_length > 0;
        const available = if (prepend_bos) max_length - 1 else max_length;
        const token_count = @min(raw_ids.len, available);
        const ids = try allocator.alloc(i32, max_length);
        const mask = try allocator.alloc(i32, max_length);

        var pos: usize = 0;
        if (prepend_bos) {
            ids[0] = self.specialTokens().cls_id;
            mask[0] = 1;
            pos = 1;
        }
        for (0..token_count) |i| {
            ids[pos + i] = raw_ids[i];
            mask[pos + i] = 1;
        }
        for (pos + token_count..max_length) |i| {
            ids[i] = self.specialTokens().pad_id;
            mask[i] = 0;
        }

        return .{
            .ids = ids,
            .attention_mask = mask,
            .allocator = allocator,
        };
    }

    /// Encode a pair of texts for cross-encoder: [CLS] text_a [SEP] text_b [SEP]
    pub fn encodeForPair(self: Tokenizer, allocator: std.mem.Allocator, text_a: []const u8, text_b: []const u8, max_length: usize) !EncodeResult {
        const ids_a = try self.encode(allocator, text_a);
        defer allocator.free(ids_a);
        const ids_b = try self.encode(allocator, text_b);
        defer allocator.free(ids_b);

        const special = self.specialTokens();

        // [CLS] a_tokens [SEP] b_tokens [SEP]
        const overhead = 3; // CLS + SEP + SEP
        const max_tokens = if (max_length >= overhead) max_length - overhead else 0;

        // Split available space: prioritize text_a, give rest to text_b
        const a_len = @min(ids_a.len, max_tokens);
        const remaining = max_tokens - a_len;
        const b_len = @min(ids_b.len, remaining);
        const total = a_len + b_len + overhead;

        const ids = try allocator.alloc(i32, max_length);
        const mask = try allocator.alloc(i32, max_length);

        var pos: usize = 0;

        // [CLS]
        ids[pos] = special.cls_id;
        mask[pos] = 1;
        pos += 1;

        // text_a tokens
        for (0..a_len) |i| {
            ids[pos] = ids_a[i];
            mask[pos] = 1;
            pos += 1;
        }

        // [SEP]
        ids[pos] = special.sep_id;
        mask[pos] = 1;
        pos += 1;

        // text_b tokens
        for (0..b_len) |i| {
            ids[pos] = ids_b[i];
            mask[pos] = 1;
            pos += 1;
        }

        // [SEP]
        ids[pos] = special.sep_id;
        mask[pos] = 1;
        pos += 1;

        _ = total;

        // Padding
        for (pos..max_length) |i| {
            ids[i] = special.pad_id;
            mask[i] = 0;
        }

        return .{
            .ids = ids,
            .attention_mask = mask,
            .allocator = allocator,
        };
    }
};
