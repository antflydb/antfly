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

// Grammar-constrained decoding for JSON mode and GBNF grammars.
//
// Implements two grammar engines:
// 1. JsonGrammar — hand-written FSM for JSON validation (fast, simple)
// 2. GbnfGrammar — general-purpose GBNF grammar engine (llama.cpp compatible)
//
// Both produce a token mask for each decode step. The mask sets disallowed tokens
// to -inf so the model can only generate text conforming to the grammar.
//
// The engines operate on raw bytes, not tokens. For each candidate token in the
// vocabulary we simulate advancing a copy of the state, and disallow tokens that
// would produce an invalid transition.

const std = @import("std");
const tokenizer_mod = @import("inference_tokenizer");

/// Pre-decoded token byte table. Decodes all vocab tokens once so that
/// `allowedTokenMask` can look up token bytes without per-call allocation.
pub const TokenByteTable = struct {
    /// Offset into `byte_data` for each token_id.
    token_offsets: []u32,
    /// Byte length of each token's decoded form.
    token_lengths: []u16,
    /// Concatenated decoded bytes for all tokens.
    byte_data: []u8,
    initial_token_offsets: ?[]u32 = null,
    initial_token_lengths: ?[]u16 = null,
    byte_fallback_tokens: ?[]bool = null,

    pub fn init(allocator: std.mem.Allocator, tok: tokenizer_mod.Tokenizer, vocab_size: usize) !TokenByteTable {
        const offsets = try allocator.alloc(u32, vocab_size);
        errdefer allocator.free(offsets);
        const lengths = try allocator.alloc(u16, vocab_size);
        errdefer allocator.free(lengths);

        const initial_offsets = try allocator.alloc(u32, vocab_size);
        errdefer allocator.free(initial_offsets);
        const initial_lengths = try allocator.alloc(u16, vocab_size);
        errdefer allocator.free(initial_lengths);
        const fallback = try allocator.alloc(bool, vocab_size);
        errdefer allocator.free(fallback);
        @memset(fallback, false);
        var byte_list = std.ArrayListUnmanaged(u8).empty;
        errdefer byte_list.deinit(allocator);
        for (0..vocab_size) |token_id| {
            for ([_]bool{ false, true }) |at_start| {
                const target_offsets = if (at_start) initial_offsets else offsets;
                const target_lengths = if (at_start) initial_lengths else lengths;
                target_offsets[token_id] = std.math.cast(u32, byte_list.items.len) orelse return error.VocabTooLarge;
                target_lengths[token_id] = 0;
                const token_bytes = if (at_start)
                    tok.decodeToken(allocator, @intCast(token_id), true) catch |err| {
                        if (err == error.OutOfMemory) return err;
                        continue;
                    }
                else blk: {
                    const piece = tok.tokenPiece(allocator, @intCast(token_id)) catch |err| {
                        if (err == error.OutOfMemory) return err;
                        continue;
                    };
                    fallback[token_id] = piece.is_byte_fallback;
                    break :blk piece.bytes;
                };
                defer allocator.free(token_bytes);
                const len = std.math.cast(u16, token_bytes.len) orelse return error.TokenTooLarge;
                if (at_start and std.mem.eql(u8, token_bytes, byte_list.items[offsets[token_id]..][0..lengths[token_id]])) {
                    target_offsets[token_id] = offsets[token_id];
                } else {
                    try byte_list.appendSlice(allocator, token_bytes);
                }
                target_lengths[token_id] = len;
            }
        }
        const byte_data = try byte_list.toOwnedSlice(allocator);

        return .{
            .token_offsets = offsets,
            .token_lengths = lengths,
            .byte_data = byte_data,
            .initial_token_offsets = initial_offsets,
            .initial_token_lengths = initial_lengths,
            .byte_fallback_tokens = fallback,
        };
    }

    pub fn getTokenBytes(self: *const TokenByteTable, token_id: usize) []const u8 {
        if (token_id >= self.token_lengths.len) return &[_]u8{};
        const len = self.token_lengths[token_id];
        if (len == 0) return &[_]u8{};
        const off = self.token_offsets[token_id];
        return self.byte_data[off..][0..len];
    }

    pub fn getTokenBytesAtPosition(self: *const TokenByteTable, token_id: usize, at_start: bool) []const u8 {
        if (at_start) {
            if (self.initial_token_lengths) |lengths| {
                if (token_id >= lengths.len) return &.{};
                return self.byte_data[self.initial_token_offsets.?[token_id]..][0..lengths[token_id]];
            }
        }
        return self.getTokenBytes(token_id);
    }

    pub fn isByteFallback(self: *const TokenByteTable, id: usize) bool {
        return if (self.byte_fallback_tokens) |flags| id < flags.len and flags[id] else false;
    }

    pub fn decodeTokens(self: *const TokenByteTable, allocator: std.mem.Allocator, ids: []const i32) ![]u8 {
        var size: usize = 0;
        for (ids, 0..) |id, index| {
            if (id < 0 or id >= self.token_lengths.len) return error.InvalidTokenEvidence;
            size = std.math.add(usize, size, self.getTokenBytesAtPosition(@intCast(id), index == 0).len) catch return error.VocabTooLarge;
        }
        const bytes = try allocator.alloc(u8, size);
        var offset: usize = 0;
        for (ids, 0..) |id, index| {
            const piece = self.getTokenBytesAtPosition(@intCast(id), index == 0);
            @memcpy(bytes[offset..][0..piece.len], piece);
            offset += piece.len;
        }
        return bytes;
    }

    pub fn deinit(self: *TokenByteTable, allocator: std.mem.Allocator) void {
        if (self.byte_fallback_tokens) |fallback| allocator.free(fallback);
        allocator.free(self.token_offsets);
        allocator.free(self.token_lengths);
        allocator.free(self.byte_data);
        if (self.initial_token_offsets) |offsets| allocator.free(offsets);
        if (self.initial_token_lengths) |lengths| allocator.free(lengths);
        self.* = undefined;
    }
};

/// JSON FSM states.
pub const State = enum {
    /// Initial state — expecting a JSON value (object, array, string, number, literal).
    start,
    /// Inside an object after '{' — expecting a key (string) or '}'.
    object_open,
    /// Just finished reading an object key — expecting ':'.
    colon,
    /// Just read ':' — expecting a value.
    object_value,
    /// After a complete value inside an object — expecting ',' or '}'.
    comma_or_close_object,
    /// Inside an array after '[' — expecting a value or ']'.
    array_open,
    /// After a complete value inside an array — expecting ',' or ']'.
    comma_or_close_array,
    /// Inside a string (after opening '"') — consuming characters until closing '"'.
    string,
    /// Inside a string after '\' — expecting escape character.
    string_escape,
    /// Inside a number — consuming digits, '.', 'e', 'E', '+', '-'.
    number,
    /// Reading a literal (true, false, null) — tracking expected remaining bytes.
    literal,
    /// Completed a valid top-level JSON value.
    done,
    /// Error state — invalid JSON detected.
    err,
};

/// Tracks which context we came from when finishing a value, so we know whether
/// to transition to comma_or_close_object vs comma_or_close_array.
const StackEntry = enum {
    object,
    array,
};

/// Maximum nesting depth for JSON structures.
const max_depth = 64;

/// Grammar-constrained JSON decoder.
///
/// Maintains a byte-level FSM over JSON syntax. Call `advance` to feed generated bytes,
/// `allowedTokenMask` to build a boolean mask over the vocabulary, and `isComplete` to
/// check whether valid JSON has been produced.
pub const JsonGrammar = struct {
    at_start: bool = true,
    state: State,
    /// Nesting stack for objects and arrays.
    stack: [max_depth]StackEntry,
    stack_len: usize,
    /// For literal state: remaining expected bytes (e.g. "rue" after 't').
    literal_remaining: [5]u8,
    literal_remaining_len: u8,
    /// Whether the string we are reading is an object key (affects post-string state).
    string_is_key: bool,
    /// Number parsing sub-state for validation.
    number_has_dot: bool,
    number_has_exp: bool,
    number_last_was_exp_sign: bool,

    pub fn init() JsonGrammar {
        return .{
            .state = .start,
            .stack = undefined,
            .stack_len = 0,
            .literal_remaining = undefined,
            .literal_remaining_len = 0,
            .string_is_key = false,
            .number_has_dot = false,
            .number_has_exp = false,
            .number_last_was_exp_sign = false,
        };
    }

    /// Feed a sequence of generated bytes through the FSM, advancing state.
    pub fn advance(self: *JsonGrammar, bytes: []const u8) void {
        for (bytes) |b| {
            self.advanceByte(b);
        }
    }

    /// Check whether we have produced a complete, valid top-level JSON value.
    pub fn isComplete(self: *const JsonGrammar) bool {
        return self.state == .done;
    }

    /// Build a boolean mask over the vocabulary. `mask[token_id]` is true if that token
    /// is allowed (i.e. appending its bytes keeps the JSON valid).
    ///
    /// The caller must supply a tokenizer for decoding token IDs to bytes and the
    /// vocabulary size. The returned slice is allocated with `allocator`.
    pub fn allowedTokenMask(
        self: *const JsonGrammar,
        allocator: std.mem.Allocator,
        tok: tokenizer_mod.Tokenizer,
        vocab_size: usize,
    ) ![]bool {
        const mask = try allocator.alloc(bool, vocab_size);
        errdefer allocator.free(mask);
        @memset(mask, false);

        var any_allowed = false;

        // For each token in the vocabulary, simulate advancing a copy of the FSM.
        for (0..vocab_size) |token_id| {
            const token_bytes = tok.decodeToken(allocator, @intCast(token_id), self.at_start) catch |err| {
                if (err == error.OutOfMemory) return err;
                continue;
            };
            defer allocator.free(token_bytes);

            if (token_bytes.len == 0) continue;

            // Simulate: copy current state, advance with token bytes, check validity.
            var sim = self.*;
            var valid = true;
            for (token_bytes) |b| {
                sim.advanceByte(b);
                if (sim.state == .err) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                mask[token_id] = true;
                any_allowed = true;
            }
        }

        if (!any_allowed and !self.isComplete()) return error.GrammarNoValidToken;

        return mask;
    }

    /// Fast variant that uses a pre-decoded TokenByteTable instead of
    /// calling tokenizer.decode() per token. Avoids per-token allocation.
    pub fn allowedTokenMaskFast(
        self: *const JsonGrammar,
        allocator: std.mem.Allocator,
        token_table: *const TokenByteTable,
        vocab_size: usize,
    ) ![]bool {
        const mask = try allocator.alloc(bool, vocab_size);
        errdefer allocator.free(mask);
        @memset(mask, false);

        var any_allowed = false;

        for (0..vocab_size) |token_id| {
            const token_bytes = token_table.getTokenBytesAtPosition(token_id, self.at_start);
            if (token_bytes.len == 0) continue;

            var sim = self.*;
            var valid = true;
            for (token_bytes) |b| {
                sim.advanceByte(b);
                if (sim.state == .err) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                mask[token_id] = true;
                any_allowed = true;
            }
        }

        if (!any_allowed and !self.isComplete()) return error.GrammarNoValidToken;

        return mask;
    }

    /// Apply the grammar mask to logits: set disallowed tokens to -inf.
    pub fn applyMask(mask: []const bool, logits: []f32) void {
        const len = @min(mask.len, logits.len);
        for (0..len) |i| {
            if (!mask[i]) {
                logits[i] = -std.math.inf(f32);
            }
        }
    }

    // --- Internal FSM logic ---

    fn advanceByte(self: *JsonGrammar, b: u8) void {
        self.at_start = false;
        switch (self.state) {
            .start => self.handleTopLevelValueStart(b),
            .object_open => self.handleObjectOpen(b),
            .colon => self.handleColon(b),
            .object_value => self.handleValueStart(b),
            .array_open => self.handleArrayOpen(b),
            .comma_or_close_object => self.handleCommaOrCloseObject(b),
            .comma_or_close_array => self.handleCommaOrCloseArray(b),
            .string => self.handleString(b),
            .string_escape => self.handleStringEscape(b),
            .number => self.handleNumber(b),
            .literal => self.handleLiteral(b),
            .done => {
                // After a complete value, only whitespace is allowed.
                if (isWhitespace(b)) return;
                self.state = .err;
            },
            .err => {},
        }
    }

    /// Handle the start of a JSON value (used for start, object_value, and array values).
    fn handleTopLevelValueStart(self: *JsonGrammar, b: u8) void {
        if (isWhitespace(b)) {
            self.state = .err;
            return;
        }
        self.handleValueStart(b);
    }

    /// Handle the start of a JSON value (used for object_value and array values).
    fn handleValueStart(self: *JsonGrammar, b: u8) void {
        if (isWhitespace(b)) return;

        switch (b) {
            '"' => {
                self.state = .string;
                self.string_is_key = false;
            },
            '{' => {
                if (self.stack_len >= max_depth) {
                    self.state = .err;
                    return;
                }
                self.stack[self.stack_len] = .object;
                self.stack_len += 1;
                self.state = .object_open;
            },
            '[' => {
                if (self.stack_len >= max_depth) {
                    self.state = .err;
                    return;
                }
                self.stack[self.stack_len] = .array;
                self.stack_len += 1;
                self.state = .array_open;
            },
            't' => {
                self.state = .literal;
                self.literal_remaining[0] = 'r';
                self.literal_remaining[1] = 'u';
                self.literal_remaining[2] = 'e';
                self.literal_remaining_len = 3;
            },
            'f' => {
                self.state = .literal;
                self.literal_remaining[0] = 'a';
                self.literal_remaining[1] = 'l';
                self.literal_remaining[2] = 's';
                self.literal_remaining[3] = 'e';
                self.literal_remaining_len = 4;
            },
            'n' => {
                self.state = .literal;
                self.literal_remaining[0] = 'u';
                self.literal_remaining[1] = 'l';
                self.literal_remaining[2] = 'l';
                self.literal_remaining_len = 3;
            },
            '-', '0'...'9' => {
                self.state = .number;
                self.number_has_dot = false;
                self.number_has_exp = false;
                self.number_last_was_exp_sign = false;
            },
            else => {
                self.state = .err;
            },
        }
    }

    fn handleObjectOpen(self: *JsonGrammar, b: u8) void {
        if (isWhitespace(b)) return;

        switch (b) {
            '"' => {
                self.state = .string;
                self.string_is_key = true;
            },
            '}' => {
                self.popStack();
            },
            else => {
                self.state = .err;
            },
        }
    }

    fn handleColon(self: *JsonGrammar, b: u8) void {
        if (isWhitespace(b)) return;

        if (b == ':') {
            self.state = .object_value;
        } else {
            self.state = .err;
        }
    }

    fn handleArrayOpen(self: *JsonGrammar, b: u8) void {
        if (isWhitespace(b)) return;

        if (b == ']') {
            self.popStack();
            return;
        }

        // Otherwise it's the start of a value.
        self.handleValueStart(b);
    }

    fn handleCommaOrCloseObject(self: *JsonGrammar, b: u8) void {
        if (isWhitespace(b)) return;

        switch (b) {
            ',' => {
                // After comma in object, expect another key.
                self.state = .object_open;
            },
            '}' => {
                self.popStack();
            },
            else => {
                self.state = .err;
            },
        }
    }

    fn handleCommaOrCloseArray(self: *JsonGrammar, b: u8) void {
        if (isWhitespace(b)) return;

        switch (b) {
            ',' => {
                self.state = .array_open;
            },
            ']' => {
                self.popStack();
            },
            else => {
                self.state = .err;
            },
        }
    }

    fn handleString(self: *JsonGrammar, b: u8) void {
        switch (b) {
            '"' => {
                // End of string.
                if (self.string_is_key) {
                    self.state = .colon;
                } else {
                    self.finishValue();
                }
            },
            '\\' => {
                self.state = .string_escape;
            },
            // Control characters (0x00-0x1F) are not allowed in JSON strings.
            0x00...0x1F => {
                self.state = .err;
            },
            else => {
                // Normal character, stay in string state.
            },
        }
    }

    fn handleStringEscape(self: *JsonGrammar, b: u8) void {
        // After '\' in a string, valid escapes: " \ / b f n r t u
        switch (b) {
            '"', '\\', '/', 'b', 'f', 'n', 'r', 't', 'u' => {
                // Valid escape. For \u we'd ideally track 4 hex digits, but
                // for practical token-level decoding we accept it and return to string.
                self.state = .string;
            },
            else => {
                self.state = .err;
            },
        }
    }

    fn handleNumber(self: *JsonGrammar, b: u8) void {
        switch (b) {
            '0'...'9' => {
                self.number_last_was_exp_sign = false;
            },
            '.' => {
                if (self.number_has_dot or self.number_has_exp) {
                    self.state = .err;
                    return;
                }
                self.number_has_dot = true;
            },
            'e', 'E' => {
                if (self.number_has_exp) {
                    self.state = .err;
                    return;
                }
                self.number_has_exp = true;
            },
            '+', '-' => {
                // Only valid immediately after 'e'/'E'.
                if (!self.number_has_exp) {
                    self.state = .err;
                    return;
                }
                self.number_last_was_exp_sign = true;
            },
            else => {
                // Number ended — this byte belongs to the next state.
                self.finishValue();
                // Re-process this byte in the new state.
                self.advanceByte(b);
            },
        }
    }

    fn handleLiteral(self: *JsonGrammar, b: u8) void {
        if (self.literal_remaining_len == 0) {
            // Literal is complete, this byte belongs to the next state.
            self.finishValue();
            self.advanceByte(b);
            return;
        }

        if (b == self.literal_remaining[0]) {
            // Shift remaining bytes left.
            var i: u8 = 0;
            while (i < self.literal_remaining_len - 1) : (i += 1) {
                self.literal_remaining[i] = self.literal_remaining[i + 1];
            }
            self.literal_remaining_len -= 1;

            // If we consumed the last byte of the literal, the literal is complete.
            if (self.literal_remaining_len == 0) {
                self.finishValue();
            }
        } else {
            self.state = .err;
        }
    }

    /// Called when a complete value (string, number, literal, object, array) is finished.
    /// Transitions to the appropriate state based on the nesting stack.
    fn finishValue(self: *JsonGrammar) void {
        if (self.stack_len == 0) {
            self.state = .done;
            return;
        }

        switch (self.stack[self.stack_len - 1]) {
            .object => self.state = .comma_or_close_object,
            .array => self.state = .comma_or_close_array,
        }
    }

    /// Pop a nesting level (closing '}' or ']') and transition appropriately.
    fn popStack(self: *JsonGrammar) void {
        if (self.stack_len == 0) {
            self.state = .err;
            return;
        }
        self.stack_len -= 1;
        self.finishValue();
    }

    fn isWhitespace(b: u8) bool {
        return b == ' ' or b == '\t' or b == '\n' or b == '\r';
    }
};

// ============================================================================
// GBNF Grammar Engine
// ============================================================================
//
// A general-purpose grammar engine compatible with llama.cpp's GBNF format.
//
// GBNF syntax:
//   rule-name  ::= alt1 | alt2 | alt3
//   "literal"           — literal string
//   [abc]               — character class
//   [a-z]               — character range
//   [^abc]              — negated character class
//   rule-name           — rule reference
//   element*            — zero or more
//   element+            — one or more
//   element?            — optional
//   (alt1 | alt2)       — grouped alternatives

/// A single element in a GBNF rule alternative.
const GbnfElement = union(enum) {
    /// Literal string that must match exactly.
    literal: []const u8,
    /// Character class: set of allowed byte ranges. `negated` inverts the set.
    char_class: CharClass,
    /// Reference to another rule by name.
    rule_ref: []const u8,
    /// Grouped sub-alternatives (from parenthesized expressions).
    group: []const GbnfAlternative,
};

const CharClass = struct {
    ranges: []const [2]u8,
    negated: bool,

    fn matches(self: CharClass, byte: u8) bool {
        var in_set = false;
        for (self.ranges) |range| {
            if (byte >= range[0] and byte <= range[1]) {
                in_set = true;
                break;
            }
        }
        return if (self.negated) !in_set else in_set;
    }
};

/// A quantified element: the element plus its repetition mode.
const QuantifiedElement = struct {
    element: GbnfElement,
    quantifier: Quantifier,
};

const Quantifier = enum {
    once, // exactly once
    zero_or_more, // *
    one_or_more, // +
    optional, // ?
};

/// One alternative in a rule: a sequence of quantified elements.
const GbnfAlternative = struct {
    elements: []const QuantifiedElement,
};

/// A named rule: one or more alternatives.
const GbnfRule = struct {
    name: []const u8,
    alternatives: []const GbnfAlternative,
};

/// A parse position tracks where we are in the grammar during matching.
/// It's a stack of frames — each frame says which rule/alternative/element
/// we're in, plus progress through literals and repetitions.
const ParsePosition = struct {
    frames: [max_gbnf_depth]Frame,
    frame_count: u8,

    const Frame = struct {
        rule_name: []const u8,
        alt_idx: u16,
        elem_idx: u16,
        /// For literals: how many bytes of the literal we've consumed.
        literal_offset: u16,
        /// For repetitions (+ and *): whether at least one match completed.
        /// Exact counts do not change these quantifiers' continuation language.
        rep_count: u16,
    };

    fn topFrame(self: *const ParsePosition) ?*const Frame {
        if (self.frame_count == 0) return null;
        return &self.frames[self.frame_count - 1];
    }

    fn topFrameMut(self: *ParsePosition) ?*Frame {
        if (self.frame_count == 0) return null;
        return &self.frames[self.frame_count - 1];
    }

    fn isComplete(self: *const ParsePosition) bool {
        return self.frame_count == 0;
    }

    fn clone(self: *const ParsePosition) ParsePosition {
        var copy: ParsePosition = undefined;
        copy.frame_count = self.frame_count;
        for (0..self.frame_count) |i| {
            copy.frames[i] = self.frames[i];
        }
        return copy;
    }
};

const max_gbnf_depth = 32;
const max_positions = 256;

const ArenaAllocation = struct {
    bytes: []u8,
    alignment: std.mem.Alignment,
};

/// GBNF grammar engine. Maintains a set of active parse positions and
/// advances them byte-by-byte as tokens are generated.
pub const GbnfGrammar = struct {
    const MatchError = error{ OutOfMemory, GrammarResourceLimit };

    at_start: bool = true,
    allocator: std.mem.Allocator,
    rules: std.StringArrayHashMapUnmanaged(GbnfRule),
    positions: std.ArrayListUnmanaged(ParsePosition),
    /// All allocations that need to be freed on deinit.
    arena_slices: std.ArrayListUnmanaged(ArenaAllocation),
    // A request owns one mask, bounded by vocabulary size. Repeated characters
    // inside a schema string often leave the continuation state unchanged.
    mask_cache: ?*MaskCache,
    evidence_tokens: std.StringHashMapUnmanaged([]usize) = .empty,
    const MaskCache = struct {
        key: ?[32]u8 = null,
        mask: ?[]bool = null,
    };

    pub fn parse(allocator: std.mem.Allocator, source: []const u8) !GbnfGrammar {
        var grammar = GbnfGrammar{
            .allocator = allocator,
            .rules = .empty,
            .positions = .empty,
            .arena_slices = .empty,
            .mask_cache = allocator.create(MaskCache) catch null,
        };
        if (grammar.mask_cache) |cache| cache.* = .{};
        errdefer grammar.deinit();

        if (source.len > 64 * 1024) return error.GrammarResourceLimit;
        try parseGbnfSource(&grammar, source);

        // Initialize with a single position at the start of the "root" rule.
        if (grammar.rules.get("root")) |_| {
            var start_pos: ParsePosition = undefined;
            start_pos.frame_count = 1;
            start_pos.frames[0] = .{
                .rule_name = "root",
                .alt_idx = 0,
                .elem_idx = 0,
                .literal_offset = 0,
                .rep_count = 0,
            };
            // Fork for each alternative of the root rule.
            const root_rule = grammar.rules.get("root").?;
            if (root_rule.alternatives.len == 1) {
                try grammar.positions.append(allocator, start_pos);
            } else {
                for (0..root_rule.alternatives.len) |alt_i| {
                    var pos = start_pos;
                    pos.frames[0].alt_idx = @intCast(alt_i);
                    try grammar.positions.append(allocator, pos);
                }
            }
        } else {
            return error.NoRootRule;
        }

        try dedupPositions(&grammar.positions);
        return grammar;
    }

    pub fn deinit(self: *GbnfGrammar) void {
        var evidence_it = self.evidence_tokens.valueIterator();
        while (evidence_it.next()) |ids| self.allocator.free(ids.*);
        self.evidence_tokens.deinit(self.allocator);
        if (self.mask_cache) |cache| {
            if (cache.mask) |mask| self.allocator.free(mask);
            self.allocator.destroy(cache);
        }
        for (self.arena_slices.items) |allocation| {
            self.allocator.rawFree(allocation.bytes, allocation.alignment, @returnAddress());
        }
        self.arena_slices.deinit(self.allocator);
        self.positions.deinit(self.allocator);
        // Rules and their contents are allocated from arena_slices, so no
        // per-rule cleanup needed — we just free all arena slices above.
        self.rules.deinit(self.allocator);
    }

    pub fn admitTokenEvidence(self: *GbnfGrammar, table: *const TokenByteTable) !void {
        if (self.mask_cache) |cache| cache.key = null;
        // Index only required values, then visit the vocabulary once. Repeated
        // question schemas in a batch share admission work and borrowed bytes.
        var required = std.StringHashMapUnmanaged(?usize).empty;
        defer required.deinit(self.allocator);
        for (self.rules.values()) |rule| {
            if (!std.mem.startsWith(u8, rule.name, "antfly_finite_token_v1_")) continue;
            for (rule.alternatives) |alternative| {
                if (alternative.elements.len != 1 or alternative.elements[0].element != .literal) return error.UnsupportedConstrainedProbabilities;
                try required.put(self.allocator, alternative.elements[0].element.literal, null);
            }
        }
        for (0..table.token_lengths.len) |candidate| {
            const bytes = table.getTokenBytes(candidate);
            if (bytes.len == 0 or bytes.len > 4096) continue;
            if (required.getPtr(bytes)) |id| {
                if (id.* == null or (table.isByteFallback(id.*.?) and !table.isByteFallback(candidate))) id.* = candidate;
            }
        }
        for (self.rules.values()) |rule| {
            if (!std.mem.startsWith(u8, rule.name, "antfly_finite_token_v1_")) continue;
            const ids = try self.allocator.alloc(usize, rule.alternatives.len);
            errdefer self.allocator.free(ids);
            for (rule.alternatives, ids) |alternative, *id| {
                const bytes = alternative.elements[0].element.literal;
                id.* = required.get(bytes).? orelse {
                    std.log.warn("token_logprobs requires a standalone vocabulary token for value {s}", .{bytes});
                    return error.UnsupportedConstrainedProbabilities;
                };
            }
            const entry = try self.evidence_tokens.getOrPut(self.allocator, rule.name);
            if (entry.found_existing) self.allocator.free(entry.value_ptr.*);
            entry.value_ptr.* = ids;
        }
    }

    fn advanceTokenPosition(self: *const GbnfGrammar, pos: *const ParsePosition, byte: u8, token_id: usize, token_len: usize, offset: usize, next: *std.ArrayListUnmanaged(ParsePosition), expanded: *std.ArrayListUnmanaged(ParsePosition)) MatchError!void {
        expanded.clearRetainingCapacity();
        try self.expandPosition(pos, expanded);
        for (expanded.items) |*terminal| {
            const frame = terminal.topFrame() orelse continue;
            if (self.evidence_tokens.get(frame.rule_name)) |ids| {
                const literal = self.rules.get(frame.rule_name).?.alternatives[frame.alt_idx].elements[0].element.literal;
                if (token_id != ids[frame.alt_idx] or literal.len != token_len or frame.literal_offset != offset) continue;
            }
            try self.tryConsumeByte(terminal, byte, next);
        }
    }

    /// Feed a sequence of bytes and advance all parse positions.
    pub fn advance(self: *GbnfGrammar, bytes: []const u8) MatchError!void {
        for (bytes) |b| {
            try self.advanceByte(b);
        }
    }

    /// Advance all active positions by one byte. Positions that can't consume
    /// the byte are dropped. Positions at the end of a quantified element may
    /// fork (one continues the repetition, one advances past it).
    pub fn advanceByte(self: *GbnfGrammar, byte: u8) MatchError!void {
        self.at_start = false;
        var next = std.ArrayListUnmanaged(ParsePosition).empty;
        errdefer next.deinit(self.allocator);

        for (self.positions.items) |*pos| {
            try self.advancePosition(pos, byte, &next);
        }

        try dedupPositions(&next);
        self.positions.deinit(self.allocator);
        self.positions = next;
    }

    /// Check whether any active position has completed the root rule.
    pub fn isComplete(self: *const GbnfGrammar) MatchError!bool {
        for (self.positions.items) |*pos| {
            var expanded = std.ArrayListUnmanaged(ParsePosition).empty;
            defer expanded.deinit(self.allocator);
            try self.expandPosition(pos, &expanded);

            if (expanded.items.len == 0 and self.positionAtEnd(pos)) return true;
            for (expanded.items) |*epos| {
                if (self.positionAtEnd(epos)) return true;
            }
        }

        return false;
    }

    /// Build a boolean mask over the vocabulary. Same interface as JsonGrammar.
    pub fn allowedTokenMask(
        self: *const GbnfGrammar,
        allocator: std.mem.Allocator,
        tok: tokenizer_mod.Tokenizer,
        vocab_size: usize,
    ) ![]bool {
        const mask = try allocator.alloc(bool, vocab_size);
        errdefer allocator.free(mask);
        @memset(mask, false);

        var any_allowed = false;

        for (0..vocab_size) |token_id| {
            const token_bytes = tok.decodeToken(allocator, @intCast(token_id), self.at_start) catch |err| {
                if (err == error.OutOfMemory) return err;
                continue;
            };
            defer allocator.free(token_bytes);

            if (token_bytes.len == 0) continue;

            // Simulate: clone positions, advance with token bytes, check survival.
            var sim_positions = std.ArrayListUnmanaged(ParsePosition).empty;
            defer sim_positions.deinit(self.allocator);
            try sim_positions.appendSlice(self.allocator, self.positions.items);

            var valid = true;
            for (token_bytes) |b| {
                var next = std.ArrayListUnmanaged(ParsePosition).empty;
                errdefer next.deinit(self.allocator);
                for (sim_positions.items) |*pos| {
                    try self.advancePosition(pos, b, &next);
                }
                sim_positions.deinit(self.allocator);
                sim_positions = next;
                next = .empty;
                try dedupPositions(&sim_positions);

                if (sim_positions.items.len == 0) {
                    valid = false;
                    break;
                }
            }

            if (valid and sim_positions.items.len > 0) {
                mask[token_id] = true;
                any_allowed = true;
            }
        }

        if (!any_allowed and !try self.isComplete()) return error.GrammarNoValidToken;

        return mask;
    }

    /// Fast variant using a pre-decoded TokenByteTable.
    pub fn allowedTokenMaskFast(
        self: *const GbnfGrammar,
        allocator: std.mem.Allocator,
        token_table: *const TokenByteTable,
        vocab_size: usize,
    ) ![]bool {
        // At an unambiguous finite-value boundary, skip optional whitespace
        // and mask directly to the admitted IDs. Whitespace vocabulary must
        // not turn a two-value predicate into incomplete top-k evidence.
        if (self.evidence_tokens.count() != 0) {
            var terminals = std.ArrayListUnmanaged(ParsePosition).empty;
            defer terminals.deinit(allocator);
            for (self.positions.items) |*position| try self.expandPosition(position, &terminals);
            var finite = false;
            var only_finite_or_space = true;
            for (terminals.items) |*terminal| {
                const frame = terminal.topFrame() orelse {
                    only_finite_or_space = false;
                    break;
                };
                if (self.evidence_tokens.contains(frame.rule_name) and frame.literal_offset == 0) {
                    finite = true;
                } else if (!std.mem.eql(u8, frame.rule_name, "ws")) {
                    only_finite_or_space = false;
                    break;
                }
            }
            if (finite and only_finite_or_space) {
                const mask = try allocator.alloc(bool, vocab_size);
                @memset(mask, false);
                for (terminals.items) |*terminal| {
                    const frame = terminal.topFrame() orelse continue;
                    if (self.evidence_tokens.get(frame.rule_name)) |ids| mask[ids[frame.alt_idx]] = true;
                }
                return mask;
            }
        }
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("gbnf-token-mask-v2");
        hash.update(std.mem.asBytes(&self.at_start));
        if (token_table.initial_token_offsets) |offsets| hash.update(std.mem.sliceAsBytes(offsets));
        if (token_table.initial_token_lengths) |lengths| hash.update(std.mem.sliceAsBytes(lengths));
        inline for (.{ token_table.token_offsets.len, token_table.token_lengths.len, token_table.byte_data.len, self.positions.items.len }) |length| hash.update(std.mem.asBytes(&length));
        hash.update(std.mem.sliceAsBytes(token_table.token_offsets));
        hash.update(std.mem.sliceAsBytes(token_table.token_lengths));
        hash.update(token_table.byte_data);
        hash.update(std.mem.asBytes(&vocab_size));
        for (self.positions.items) |position| {
            hash.update(std.mem.asBytes(&position.frame_count));
            for (position.frames[0..position.frame_count]) |frame| {
                const length = frame.rule_name.len;
                hash.update(std.mem.asBytes(&length));
                hash.update(frame.rule_name);
                inline for (.{ frame.alt_idx, frame.elem_idx, frame.literal_offset, frame.rep_count }) |value| hash.update(std.mem.asBytes(&value));
            }
        }
        var key: [32]u8 = undefined;
        hash.final(&key);
        if (self.mask_cache) |cache| {
            if (cache.key) |previous| {
                if (std.mem.eql(u8, &previous, &key)) return allocator.dupe(bool, cache.mask.?);
            }
        }
        const mask = try allocator.alloc(bool, vocab_size);
        errdefer allocator.free(mask);
        @memset(mask, false);

        var any_allowed = false;

        // Vocabulary-wide simulation must not allocate for every token byte.
        // These buffers belong to the grammar allocator, which also owns the
        // positions appended by expansion and consumption.
        var sim_positions = std.ArrayListUnmanaged(ParsePosition).empty;
        defer sim_positions.deinit(self.allocator);
        var next = std.ArrayListUnmanaged(ParsePosition).empty;
        defer next.deinit(self.allocator);
        var expanded = std.ArrayListUnmanaged(ParsePosition).empty;
        defer expanded.deinit(self.allocator);

        for (0..vocab_size) |token_id| {
            const token_bytes = token_table.getTokenBytesAtPosition(token_id, self.at_start);
            if (token_bytes.len == 0) continue;

            sim_positions.clearRetainingCapacity();
            try sim_positions.appendSlice(self.allocator, self.positions.items);

            var valid = true;
            for (token_bytes, 0..) |b, offset| {
                next.clearRetainingCapacity();
                for (sim_positions.items) |*pos| {
                    try self.advanceTokenPosition(pos, b, token_id, token_bytes.len, offset, &next, &expanded);
                }
                std.mem.swap(std.ArrayListUnmanaged(ParsePosition), &sim_positions, &next);
                try dedupPositions(&sim_positions);

                if (sim_positions.items.len == 0) {
                    valid = false;
                    break;
                }
            }

            if (valid and sim_positions.items.len > 0) {
                mask[token_id] = true;
                any_allowed = true;
            }
        }

        if (!any_allowed and !try self.isComplete()) {
            if (self.evidence_tokens.count() != 0) return error.UnsupportedConstrainedProbabilities;
            return error.GrammarNoValidToken;
        }

        // Cache admission is optional. An allocation failure must not discard
        // an otherwise valid mask or reuse an earlier continuation's result.
        if (self.mask_cache) |cache| {
            cache.key = null;
            if (cache.mask) |cached| {
                if (cached.len != mask.len) {
                    self.allocator.free(cached);
                    cache.mask = null;
                }
            }
            if (cache.mask == null) cache.mask = self.allocator.alloc(bool, mask.len) catch null;
            if (cache.mask) |cached| {
                @memcpy(cached, mask);
                cache.key = key;
            }
        }
        return mask;
    }

    /// Apply the grammar mask to logits: set disallowed tokens to -inf.
    pub fn applyMask(mask: []const bool, logits: []f32) void {
        JsonGrammar.applyMask(mask, logits);
    }

    // --- Internal position advancement ---

    fn appendPosition(self: *const GbnfGrammar, positions: *std.ArrayListUnmanaged(ParsePosition), position: ParsePosition) MatchError!void {
        if (positions.items.len >= max_positions) {
            try dedupPositions(positions);
            for (positions.items) |*existing| {
                if (positionsEqual(existing, &position)) return;
            }
            if (positions.items.len >= max_positions) return error.GrammarResourceLimit;
        }
        try positions.append(self.allocator, position);
    }

    fn advancePosition(
        self: *const GbnfGrammar,
        pos: *const ParsePosition,
        byte: u8,
        next: *std.ArrayListUnmanaged(ParsePosition),
    ) MatchError!void {
        var expanded = std.ArrayListUnmanaged(ParsePosition).empty;
        defer expanded.deinit(self.allocator);
        try self.advancePositionWithScratch(pos, byte, next, &expanded);
    }

    fn advancePositionWithScratch(
        self: *const GbnfGrammar,
        pos: *const ParsePosition,
        byte: u8,
        next: *std.ArrayListUnmanaged(ParsePosition),
        expanded: *std.ArrayListUnmanaged(ParsePosition),
    ) MatchError!void {
        // First, expand the position: if we're at the start of a rule_ref or
        // group, or past the end of an alternative, adjust the position.
        expanded.clearRetainingCapacity();
        try self.expandPosition(pos, expanded);

        for (expanded.items) |*epos| {
            try self.tryConsumeByte(epos, byte, next);
        }
    }

    /// Expand a position by resolving rule references and handling end-of-alternative.
    /// A single position may expand to multiple positions (e.g., at a rule_ref fork).
    fn expandPosition(
        self: *const GbnfGrammar,
        pos: *const ParsePosition,
        out: *std.ArrayListUnmanaged(ParsePosition),
    ) MatchError!void {
        if (pos.frame_count == 0) return;

        const frame = pos.topFrame().?;
        const rule = self.rules.get(frame.rule_name) orelse return;
        if (frame.alt_idx >= rule.alternatives.len) return;
        const alt = rule.alternatives[frame.alt_idx];

        // Past end of elements: pop frame and advance parent.
        if (frame.elem_idx >= alt.elements.len) {
            var popped = pos.clone();
            popped.frame_count -= 1;
            if (popped.frame_count > 0) {
                const parent = popped.topFrameMut().?;
                const parent_rule = self.rules.get(parent.rule_name) orelse return;
                if (parent.alt_idx >= parent_rule.alternatives.len) return;
                const parent_alt = parent_rule.alternatives[parent.alt_idx];
                if (parent.elem_idx >= parent_alt.elements.len) return;
                const parent_qe = parent_alt.elements[parent.elem_idx];

                // The parent frame was paused on a rule reference or group.
                // When the nested frame completes, update repetition state the
                // same way terminal elements do in tryConsumeByte().
                parent.literal_offset = 0;
                switch (parent_qe.quantifier) {
                    .once, .optional => {
                        parent.elem_idx += 1;
                        parent.rep_count = 0;
                    },
                    .zero_or_more, .one_or_more => {
                        parent.rep_count = 1;
                    },
                }
                // Recursively expand the popped position.
                try self.expandPosition(&popped, out);
            } else {
                try self.appendPosition(out, popped);
            }
            return;
        }

        const qe = alt.elements[frame.elem_idx];

        // For repetitions at rep_count > 0 (or optional/zero_or_more at any count),
        // fork: one position skips past the element, one stays to match more.
        switch (qe.quantifier) {
            .zero_or_more => {
                // Fork: skip past this element.
                var skip = pos.clone();
                const sf = skip.topFrameMut().?;
                sf.elem_idx += 1;
                sf.literal_offset = 0;
                sf.rep_count = 0;
                try self.expandPosition(&skip, out);
            },
            .optional => {
                if (frame.rep_count == 0) {
                    // Fork: skip past this element.
                    var skip = pos.clone();
                    const sf = skip.topFrameMut().?;
                    sf.elem_idx += 1;
                    sf.literal_offset = 0;
                    sf.rep_count = 0;
                    try self.expandPosition(&skip, out);
                }
            },
            .one_or_more => {
                if (frame.rep_count > 0) {
                    // Already matched at least once — can skip.
                    var skip = pos.clone();
                    const sf = skip.topFrameMut().?;
                    sf.elem_idx += 1;
                    sf.literal_offset = 0;
                    sf.rep_count = 0;
                    try self.expandPosition(&skip, out);
                }
            },
            .once => {},
        }

        // Now expand the current element.
        switch (qe.element) {
            .rule_ref => |ref_name| {
                if (frame.literal_offset > 0) {
                    // Already started matching this ref, don't re-enter.
                    try self.appendPosition(out, pos.clone());
                    return;
                }
                const ref_rule = self.rules.get(ref_name) orelse return;
                // Push a new frame for the referenced rule, fork for each alternative.
                for (0..ref_rule.alternatives.len) |alt_i| {
                    if (pos.frame_count >= max_gbnf_depth) return error.GrammarResourceLimit;
                    var new_pos = pos.clone();
                    // Mark that we've entered the ref (use literal_offset as a flag).
                    new_pos.topFrameMut().?.literal_offset = 1;
                    new_pos.frames[new_pos.frame_count] = .{
                        .rule_name = ref_name,
                        .alt_idx = @intCast(alt_i),
                        .elem_idx = 0,
                        .literal_offset = 0,
                        .rep_count = 0,
                    };
                    new_pos.frame_count += 1;
                    try self.expandPosition(&new_pos, out);
                }
            },
            .group => |alts| {
                if (frame.literal_offset > 0) {
                    try self.appendPosition(out, pos.clone());
                    return;
                }
                // Groups are inlined: create a synthetic rule name and treat like a ref.
                // For simplicity, just try each alternative inline.
                for (0..alts.len) |alt_i| {
                    var new_pos = pos.clone();
                    new_pos.topFrameMut().?.literal_offset = 1;
                    // Push a pseudo-frame. We store the group's alternatives using
                    // a rule we synthesize. For groups, we use a special convention:
                    // the rule_name points to a synthetic entry.
                    _ = alt_i;
                    // Actually, let's just handle groups by direct expansion.
                    try self.appendPosition(out, pos.clone());
                }
            },
            .literal, .char_class => {
                // These are terminal — add position as-is for byte consumption.
                try self.appendPosition(out, pos.clone());
            },
        }
    }

    /// Try to consume a byte at a (fully expanded) position.
    fn tryConsumeByte(
        self: *const GbnfGrammar,
        pos: *const ParsePosition,
        byte: u8,
        next: *std.ArrayListUnmanaged(ParsePosition),
    ) MatchError!void {
        if (pos.frame_count == 0) return;

        const frame = pos.topFrame().?;
        const rule = self.rules.get(frame.rule_name) orelse return;
        if (frame.alt_idx >= rule.alternatives.len) return;
        const alt = rule.alternatives[frame.alt_idx];
        if (frame.elem_idx >= alt.elements.len) return;

        const qe = alt.elements[frame.elem_idx];

        switch (qe.element) {
            .literal => |lit| {
                const offset = frame.literal_offset;
                // For rule_ref expansion, literal_offset is used as a flag (1).
                // But for actual literals, it tracks byte progress.
                if (offset >= lit.len) return;
                if (byte != lit[offset]) return;

                var new_pos = pos.clone();
                const nf = new_pos.topFrameMut().?;
                if (offset + 1 >= lit.len) {
                    // Literal fully consumed.
                    switch (qe.quantifier) {
                        .once, .optional => {
                            nf.elem_idx += 1;
                            nf.literal_offset = 0;
                            nf.rep_count = 0;
                        },
                        .zero_or_more, .one_or_more => {
                            // Reset to start of this element for another repetition.
                            nf.literal_offset = 0;
                            nf.rep_count = 1;
                        },
                    }
                } else {
                    nf.literal_offset += 1;
                }
                try self.appendPosition(next, new_pos);
            },
            .char_class => |cc| {
                if (!cc.matches(byte)) return;

                var new_pos = pos.clone();
                const nf = new_pos.topFrameMut().?;
                switch (qe.quantifier) {
                    .once, .optional => {
                        nf.elem_idx += 1;
                        nf.literal_offset = 0;
                        nf.rep_count = 0;
                    },
                    .zero_or_more, .one_or_more => {
                        nf.rep_count = 1;
                    },
                }
                try self.appendPosition(next, new_pos);
            },
            .rule_ref, .group => {
                // These should have been expanded away by expandPosition.
                // If we get here, it means the expansion didn't resolve — skip.
            },
        }
    }

    /// Check if a position has reached the end of the root rule.
    fn positionAtEnd(self: *const GbnfGrammar, pos: *const ParsePosition) bool {
        if (pos.frame_count == 0) return true;
        if (pos.frame_count > 1) return false;

        const frame = pos.topFrame().?;
        const rule = self.rules.get(frame.rule_name) orelse return false;
        if (frame.alt_idx >= rule.alternatives.len) return false;
        const alt = rule.alternatives[frame.alt_idx];

        // All elements consumed?
        if (frame.elem_idx >= alt.elements.len) return true;

        // Check if remaining elements are all skippable (zero_or_more, optional).
        var ei = frame.elem_idx;
        while (ei < alt.elements.len) : (ei += 1) {
            switch (alt.elements[ei].quantifier) {
                .zero_or_more, .optional => {},
                .once, .one_or_more => return false,
            }
        }
        return true;
    }

    /// Remove duplicate positions to prevent exponential blowup.
    fn dedupPositions(positions: *std.ArrayListUnmanaged(ParsePosition)) MatchError!void {
        if (positions.items.len <= 1) return;

        var keep: usize = 0;
        for (0..positions.items.len) |i| {
            var is_dup = false;
            for (0..keep) |j| {
                if (positionsEqual(&positions.items[i], &positions.items[j])) {
                    is_dup = true;
                    break;
                }
            }
            if (!is_dup) {
                positions.items[keep] = positions.items[i];
                keep += 1;
            }
        }
        positions.items.len = keep;

        if (positions.items.len > max_positions) return error.GrammarResourceLimit;
    }

    fn positionsEqual(a: *const ParsePosition, b: *const ParsePosition) bool {
        if (a.frame_count != b.frame_count) return false;
        for (0..a.frame_count) |i| {
            const fa = a.frames[i];
            const fb = b.frames[i];
            if (fa.alt_idx != fb.alt_idx or fa.elem_idx != fb.elem_idx or
                fa.literal_offset != fb.literal_offset or fa.rep_count != fb.rep_count)
                return false;
            if (!std.mem.eql(u8, fa.rule_name, fb.rule_name)) return false;
        }
        return true;
    }
};

fn trackArenaAllocation(grammar: *GbnfGrammar, comptime T: type, slice: []T) !void {
    try grammar.arena_slices.append(grammar.allocator, .{
        .bytes = std.mem.sliceAsBytes(slice),
        .alignment = .fromByteUnits(@alignOf(T)),
    });
}

// ============================================================================
// GBNF Parser — converts GBNF source text into rule structures
// ============================================================================

fn parseGbnfSource(grammar: *GbnfGrammar, source: []const u8) !void {
    var pos: usize = 0;

    while (pos < source.len) {
        // Skip whitespace and comments.
        pos = skipWsAndComments(source, pos);
        if (pos >= source.len) break;

        // Parse rule: name ::= alternatives
        const name_start = pos;
        while (pos < source.len and isIdentChar(source[pos])) : (pos += 1) {}
        if (pos == name_start) return error.ExpectedRuleName;

        const name = source[name_start..pos];
        const owned_name = try grammar.allocator.dupe(u8, name);
        try trackArenaAllocation(grammar, u8, owned_name);

        pos = skipWs(source, pos);

        // Expect "::="
        if (pos + 3 > source.len or !std.mem.eql(u8, source[pos..][0..3], "::="))
            return error.ExpectedDefinition;
        pos += 3;

        pos = skipWs(source, pos);

        // Parse alternatives.
        const alternatives = try parseAlternatives(grammar, source, &pos);

        try grammar.rules.put(grammar.allocator, owned_name, .{
            .name = owned_name,
            .alternatives = alternatives,
        });

        pos = skipWsAndComments(source, pos);
    }
}

fn parseAlternatives(grammar: *GbnfGrammar, source: []const u8, pos: *usize) anyerror![]const GbnfAlternative {
    var alts = std.ArrayListUnmanaged(GbnfAlternative).empty;

    const first_alt = try parseAlternative(grammar, source, pos);
    try alts.append(grammar.allocator, first_alt);

    while (pos.* < source.len) {
        const p = skipWs(source, pos.*);
        if (p >= source.len or source[p] != '|') break;
        pos.* = p + 1; // skip '|'
        pos.* = skipWs(source, pos.*);
        const alt = try parseAlternative(grammar, source, pos);
        try alts.append(grammar.allocator, alt);
    }

    const result = try grammar.allocator.dupe(GbnfAlternative, alts.items);
    try trackArenaAllocation(grammar, GbnfAlternative, result);
    alts.deinit(grammar.allocator);
    return result;
}

fn parseAlternative(grammar: *GbnfGrammar, source: []const u8, pos: *usize) anyerror!GbnfAlternative {
    var elements = std.ArrayListUnmanaged(QuantifiedElement).empty;

    while (pos.* < source.len) {
        pos.* = skipWs(source, pos.*);
        if (pos.* >= source.len) break;

        const ch = source[pos.*];
        // Stop at '|', ')', or newline (end of alternatives/rule).
        if (ch == '|' or ch == ')' or ch == '\n' or ch == '\r') break;
        // Also stop at start of a new rule (identifier followed by ::=).
        if (isIdentStartChar(ch) and looksLikeRuleDef(source, pos.*)) break;

        const elem = try parseElement(grammar, source, pos);

        // Check for quantifier.
        var quantifier: Quantifier = .once;
        if (pos.* < source.len) {
            switch (source[pos.*]) {
                '*' => {
                    quantifier = .zero_or_more;
                    pos.* += 1;
                },
                '+' => {
                    quantifier = .one_or_more;
                    pos.* += 1;
                },
                '?' => {
                    quantifier = .optional;
                    pos.* += 1;
                },
                else => {},
            }
        }

        try elements.append(grammar.allocator, .{
            .element = elem,
            .quantifier = quantifier,
        });
    }

    const result = try grammar.allocator.dupe(QuantifiedElement, elements.items);
    try trackArenaAllocation(grammar, QuantifiedElement, result);
    elements.deinit(grammar.allocator);
    return .{ .elements = result };
}

fn parseElement(grammar: *GbnfGrammar, source: []const u8, pos: *usize) anyerror!GbnfElement {
    if (pos.* >= source.len) return error.UnexpectedEnd;

    const ch = source[pos.*];

    if (ch == '"') {
        // Literal string.
        return parseLiteral(grammar, source, pos);
    } else if (ch == '[') {
        // Character class.
        return parseCharClass(grammar, source, pos);
    } else if (ch == '(') {
        // Grouped alternatives.
        pos.* += 1; // skip '('
        pos.* = skipWs(source, pos.*);
        const alts = try parseAlternatives(grammar, source, pos);
        pos.* = skipWs(source, pos.*);
        if (pos.* >= source.len or source[pos.*] != ')') return error.ExpectedCloseParen;
        pos.* += 1; // skip ')'
        return .{ .group = alts };
    } else if (isIdentStartChar(ch)) {
        // Rule reference.
        return parseRuleRef(grammar, source, pos);
    } else {
        return error.UnexpectedChar;
    }
}

fn parseLiteral(grammar: *GbnfGrammar, source: []const u8, pos: *usize) anyerror!GbnfElement {
    pos.* += 1; // skip opening '"'
    var bytes = std.ArrayListUnmanaged(u8).empty;

    while (pos.* < source.len and source[pos.*] != '"') {
        if (source[pos.*] == '\\') {
            pos.* += 1;
            if (pos.* >= source.len) return error.UnexpectedEnd;
            const escaped: u8 = switch (source[pos.*]) {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '\\' => '\\',
                '"' => '"',
                else => source[pos.*],
            };
            try bytes.append(grammar.allocator, escaped);
        } else {
            try bytes.append(grammar.allocator, source[pos.*]);
        }
        pos.* += 1;
    }

    if (pos.* >= source.len) return error.UnterminatedString;
    pos.* += 1; // skip closing '"'

    const result = try grammar.allocator.dupe(u8, bytes.items);
    try trackArenaAllocation(grammar, u8, result);
    bytes.deinit(grammar.allocator);
    return .{ .literal = result };
}

fn parseCharClass(grammar: *GbnfGrammar, source: []const u8, pos: *usize) anyerror!GbnfElement {
    pos.* += 1; // skip '['
    var negated = false;
    if (pos.* < source.len and source[pos.*] == '^') {
        negated = true;
        pos.* += 1;
    }

    var ranges = std.ArrayListUnmanaged([2]u8).empty;

    while (pos.* < source.len and source[pos.*] != ']') {
        var lo = source[pos.*];
        if (lo == '\\') {
            pos.* += 1;
            if (pos.* >= source.len) return error.UnexpectedEnd;
            lo = escapeChar(source[pos.*]);
        }
        pos.* += 1;

        // Check for range: a-z
        if (pos.* + 1 < source.len and source[pos.*] == '-' and source[pos.* + 1] != ']') {
            pos.* += 1; // skip '-'
            var hi = source[pos.*];
            if (hi == '\\') {
                pos.* += 1;
                if (pos.* >= source.len) return error.UnexpectedEnd;
                hi = escapeChar(source[pos.*]);
            }
            pos.* += 1;
            try ranges.append(grammar.allocator, .{ lo, hi });
        } else {
            try ranges.append(grammar.allocator, .{ lo, lo });
        }
    }

    if (pos.* >= source.len) return error.UnterminatedCharClass;
    pos.* += 1; // skip ']'

    const result = try grammar.allocator.dupe([2]u8, ranges.items);
    try trackArenaAllocation(grammar, [2]u8, result);
    ranges.deinit(grammar.allocator);
    return .{ .char_class = .{ .ranges = result, .negated = negated } };
}

fn parseRuleRef(grammar: *GbnfGrammar, source: []const u8, pos: *usize) anyerror!GbnfElement {
    const start = pos.*;
    while (pos.* < source.len and isIdentChar(source[pos.*])) : (pos.* += 1) {}
    const name = source[start..pos.*];
    const owned = try grammar.allocator.dupe(u8, name);
    try trackArenaAllocation(grammar, u8, owned);
    return .{ .rule_ref = owned };
}

fn escapeChar(ch: u8) u8 {
    return switch (ch) {
        'n' => '\n',
        'r' => '\r',
        't' => '\t',
        '\\' => '\\',
        ']' => ']',
        '[' => '[',
        '^' => '^',
        '-' => '-',
        else => ch,
    };
}

fn isIdentStartChar(ch: u8) bool {
    return (ch >= 'a' and ch <= 'z') or (ch >= 'A' and ch <= 'Z') or ch == '_';
}

fn isIdentChar(ch: u8) bool {
    return isIdentStartChar(ch) or (ch >= '0' and ch <= '9') or ch == '-';
}

fn skipWs(source: []const u8, start: usize) usize {
    var p = start;
    while (p < source.len and (source[p] == ' ' or source[p] == '\t')) : (p += 1) {}
    return p;
}

fn skipWsAndComments(source: []const u8, start: usize) usize {
    var p = start;
    while (p < source.len) {
        if (source[p] == ' ' or source[p] == '\t' or source[p] == '\n' or source[p] == '\r') {
            p += 1;
        } else if (source[p] == '#') {
            // Skip comment until end of line.
            while (p < source.len and source[p] != '\n') : (p += 1) {}
        } else {
            break;
        }
    }
    return p;
}

pub fn buildJsonSchemaGrammar(allocator: std.mem.Allocator, schema: std.json.Value) ![]u8 {
    return buildJsonSchemaGrammarWithOptions(allocator, schema, .{});
}

pub const JsonSchemaGrammarOptions = struct {
    /// Internal summaries must not spend their output budget on indentation.
    /// This bounds formatting whitespace only; string content is unaffected.
    bounded_whitespace: bool = false,
    canonical_evidence: bool = false,
};

pub fn buildJsonSchemaGrammarWithEvidence(allocator: std.mem.Allocator, schema: std.json.Value, canonical_evidence: bool) ![]u8 {
    return buildJsonSchemaGrammarWithOptions(allocator, schema, .{ .canonical_evidence = canonical_evidence });
}

pub fn buildJsonSchemaGrammarWithOptions(allocator: std.mem.Allocator, schema: std.json.Value, options: JsonSchemaGrammarOptions) ![]u8 {
    var builder = JsonSchemaGrammarBuilder{
        .canonical_evidence = options.canonical_evidence,
        .allocator = allocator,
        .options = options,
    };
    errdefer builder.deinit();
    const result = try builder.build(schema);
    builder.deinitAux();
    return result;
}

const JsonSchemaGrammarBuilder = struct {
    canonical_evidence: bool = false,
    allocator: std.mem.Allocator,
    options: JsonSchemaGrammarOptions,
    out: std.ArrayListUnmanaged(u8) = .empty,
    rule_names: std.ArrayListUnmanaged([]const u8) = .empty,
    next_rule_id: usize = 0,

    pub fn deinit(self: *JsonSchemaGrammarBuilder) void {
        self.deinitAux();
        self.out.deinit(self.allocator);
    }

    fn deinitAux(self: *JsonSchemaGrammarBuilder) void {
        for (self.rule_names.items) |name| self.allocator.free(name);
        self.rule_names.deinit(self.allocator);
    }

    fn build(self: *JsonSchemaGrammarBuilder, schema: std.json.Value) ![]u8 {
        try self.emitSharedRules();
        const root_rule = try self.emitSchemaRule(schema);
        try self.out.appendSlice(self.allocator, "root ::= ");
        try self.out.appendSlice(self.allocator, root_rule);
        try self.out.appendSlice(self.allocator, " ws\n");
        return self.out.toOwnedSlice(self.allocator);
    }

    fn nextRuleName(self: *JsonSchemaGrammarBuilder, comptime prefix: []const u8) ![]const u8 {
        var buf: [64]u8 = undefined;
        const text = try std.fmt.bufPrint(&buf, "{s}_{d}", .{ prefix, self.next_rule_id });
        self.next_rule_id += 1;
        const name = try self.allocator.dupe(u8, text);
        try self.rule_names.append(self.allocator, name);
        return name;
    }

    fn appendGrammarLiteral(self: *JsonSchemaGrammarBuilder, literal: []const u8) !void {
        try self.out.append(self.allocator, '"');
        for (literal) |ch| {
            switch (ch) {
                '"' => try self.out.appendSlice(self.allocator, "\\\""),
                '\\' => try self.out.appendSlice(self.allocator, "\\\\"),
                '\n' => try self.out.appendSlice(self.allocator, "\\n"),
                '\r' => try self.out.appendSlice(self.allocator, "\\r"),
                '\t' => try self.out.appendSlice(self.allocator, "\\t"),
                else => try self.out.append(self.allocator, ch),
            }
        }
        try self.out.append(self.allocator, '"');
    }

    fn emitSharedRules(self: *JsonSchemaGrammarBuilder) !void {
        try self.out.appendSlice(self.allocator, if (self.options.bounded_whitespace)
            "ws ::= [ \\t\\n\\r]?\n"
        else
            "ws ::= [ \\t\\n\\r]*\n");
        try self.out.appendSlice(self.allocator, "hex ::= [0-9a-fA-F]\n" ++
            "json_escape ::= [\"\\\\/bfnrt] | \"u\" hex hex hex hex\n" ++
            // The byte-oriented GBNF parser accepts a literal control-byte
            // range. JSON strings require these bytes to be escaped.
            "json_char ::= [^\"\\\\\x00-\x1f] | \"\\\\\" json_escape\n" ++
            \\json_string ::= "\"" json_char* "\""
            \\json_digit ::= [0-9]
            \\json_digits ::= json_digit+
            \\json_int_body ::= "0" | [1-9] json_digit*
            \\json_integer ::= "-"? json_int_body
            \\json_frac ::= "." json_digits
            \\json_exp_sign ::= [+-]
            \\json_exp ::= [eE] json_exp_sign? json_digits
            \\json_number ::= json_integer json_frac? json_exp?
            \\json_boolean ::= "true" | "false"
            \\json_null ::= "null"
            \\json_member_generic ::= json_string ws ":" ws json_value_generic
            \\json_member_generic_tail ::= ws "," ws json_member_generic
            \\json_object_generic ::= "{" ws "}" | "{" ws json_member_generic json_member_generic_tail* ws "}"
            \\json_array_generic_tail ::= ws "," ws json_value_generic
            \\json_array_generic ::= "[" ws "]" | "[" ws json_value_generic json_array_generic_tail* ws "]"
            \\json_value_generic ::= json_object_generic | json_array_generic | json_string | json_number | json_boolean | json_null
            \\
        );
    }

    fn emitSchemaRule(self: *JsonSchemaGrammarBuilder, schema: std.json.Value) anyerror![]const u8 {
        if (schema != .object) return "json_value_generic";
        const schema_obj = schema.object;

        if (schema_obj.get("const")) |const_value| {
            return self.emitLiteralValueRule(const_value);
        }
        if (schema_obj.get("enum")) |enum_values| {
            if (enum_values != .array) return error.InvalidSchema;
            // Integer enums include positional IDs in the batch envelope;
            // they are not confidence-bearing choice or predicate fields.
            if (self.canonical_evidence and enum_values.array.items.len > 0 and
                (enum_values.array.items[0] == .string or enum_values.array.items[0] == .bool))
                return self.emitEvidenceRule(enum_values.array.items);
            var rules = std.ArrayListUnmanaged([]const u8).empty;
            defer rules.deinit(self.allocator);
            for (enum_values.array.items) |item| {
                try rules.append(self.allocator, try self.emitLiteralValueRule(item));
            }
            return self.emitUnionRule(rules.items);
        }
        if (schema_obj.get("anyOf")) |schemas| {
            return self.emitSchemaUnion(schemas);
        }
        if (schema_obj.get("oneOf")) |schemas| {
            return self.emitSchemaUnion(schemas);
        }
        if (schema_obj.get("allOf")) |schemas| {
            if (schemas != .array) return error.InvalidSchema;
            if (schemas.array.items.len == 1) return self.emitSchemaRule(schemas.array.items[0]);
            return "json_value_generic";
        }

        const inferred_type = try schemaTypeName(schema_obj);
        if (inferred_type) |type_name| {
            if (std.mem.eql(u8, type_name, "object")) return self.emitObjectRule(schema_obj);
            if (std.mem.eql(u8, type_name, "array")) return self.emitArrayRule(schema_obj);
            if (std.mem.eql(u8, type_name, "string")) return "json_string";
            if (std.mem.eql(u8, type_name, "number")) return "json_number";
            if (std.mem.eql(u8, type_name, "integer")) return self.emitIntegerRule(schema_obj);
            if (std.mem.eql(u8, type_name, "boolean")) return if (self.canonical_evidence) self.emitEvidenceRule(&.{ .{ .bool = false }, .{ .bool = true } }) else "json_boolean";
            if (std.mem.eql(u8, type_name, "null")) return "json_null";
        }

        return "json_value_generic";
    }

    // Isolate finite values from JSON punctuation. These named rules are
    // admitted against the actual vocabulary and consumed in one decoder step.
    fn emitEvidenceRule(self: *JsonSchemaGrammarBuilder, values: []const std.json.Value) ![]const u8 {
        if (values.len == 0 or values.len > 64) return error.UnsupportedConstrainedProbabilities;
        const quoted = values[0] == .string;
        const finite = try self.nextRuleName("antfly_finite_token_v1");
        try self.out.appendSlice(self.allocator, finite);
        try self.out.appendSlice(self.allocator, " ::= ");
        for (values, 0..) |value, i| {
            if ((value == .string) != quoted) return error.UnsupportedConstrainedProbabilities;
            const encoded = try std.json.Stringify.valueAlloc(self.allocator, value, .{});
            defer self.allocator.free(encoded);
            if (i != 0) try self.out.appendSlice(self.allocator, " | ");
            try self.appendGrammarLiteral(if (quoted) encoded[1 .. encoded.len - 1] else encoded);
        }
        try self.out.append(self.allocator, '\n');
        if (!quoted) return finite;
        const wrapper = try self.nextRuleName("finite_string");
        try self.out.appendSlice(self.allocator, wrapper);
        try self.out.appendSlice(self.allocator, " ::= ");
        try self.appendGrammarLiteral("\"");
        try self.out.appendSlice(self.allocator, " ");
        try self.out.appendSlice(self.allocator, finite);
        try self.out.appendSlice(self.allocator, " ");
        try self.appendGrammarLiteral("\"");
        try self.out.append(self.allocator, '\n');
        return wrapper;
    }

    fn emitSchemaUnion(self: *JsonSchemaGrammarBuilder, schemas: std.json.Value) anyerror![]const u8 {
        if (schemas != .array) return error.InvalidSchema;
        var rules = std.ArrayListUnmanaged([]const u8).empty;
        defer rules.deinit(self.allocator);
        for (schemas.array.items) |item| {
            try rules.append(self.allocator, try self.emitSchemaRule(item));
        }
        return self.emitUnionRule(rules.items);
    }

    fn emitUnionRule(self: *JsonSchemaGrammarBuilder, rules: []const []const u8) anyerror![]const u8 {
        if (rules.len == 0) return error.InvalidSchema;
        if (rules.len == 1) return rules[0];

        const rule_name = try self.nextRuleName("schema_union");
        try self.out.appendSlice(self.allocator, rule_name);
        try self.out.appendSlice(self.allocator, " ::= ");
        for (rules, 0..) |rule, idx| {
            if (idx != 0) try self.out.appendSlice(self.allocator, " | ");
            try self.out.appendSlice(self.allocator, rule);
        }
        try self.out.append(self.allocator, '\n');
        return rule_name;
    }

    fn emitLiteralValueRule(self: *JsonSchemaGrammarBuilder, value: std.json.Value) anyerror![]const u8 {
        const rule_name = try self.nextRuleName("schema_literal");
        const literal = try std.json.Stringify.valueAlloc(self.allocator, value, .{});
        defer self.allocator.free(literal);

        try self.out.appendSlice(self.allocator, rule_name);
        try self.out.appendSlice(self.allocator, " ::= ");
        try self.appendGrammarLiteral(literal);
        try self.out.append(self.allocator, '\n');
        return rule_name;
    }

    fn emitIntegerRule(self: *JsonSchemaGrammarBuilder, schema_obj: std.json.ObjectMap) anyerror![]const u8 {
        const minimum = schemaIntegerBound(schema_obj, "minimum");
        const maximum = schemaIntegerBound(schema_obj, "maximum");
        if (minimum != null and maximum != null and maximum.? >= minimum.?) {
            const width = @as(i128, maximum.?) - @as(i128, minimum.?) + 1;
            if (width <= 256) {
                const rule_name = try self.nextRuleName("schema_integer");
                try self.out.appendSlice(self.allocator, rule_name);
                try self.out.appendSlice(self.allocator, " ::= ");
                var value = minimum.?;
                var first = true;
                while (true) {
                    if (!first) try self.out.appendSlice(self.allocator, " | ");
                    first = false;
                    var buf: [32]u8 = undefined;
                    const text = try std.fmt.bufPrint(&buf, "{d}", .{value});
                    try self.appendGrammarLiteral(text);
                    if (value == maximum.?) break;
                    value += 1;
                }
                try self.out.append(self.allocator, '\n');
                return rule_name;
            }
        }
        return "json_integer";
    }

    fn emitArrayRule(self: *JsonSchemaGrammarBuilder, schema_obj: std.json.ObjectMap) anyerror![]const u8 {
        const item_rule = if (schema_obj.get("items")) |item_schema|
            try self.emitSchemaRule(item_schema)
        else
            "json_value_generic";
        const min_items = schemaNonNegativeInt(schema_obj, "minItems") orelse 0;
        const max_items = schemaNonNegativeInt(schema_obj, "maxItems");
        if (max_items) |max_len| {
            if (max_len < min_items) return error.InvalidSchema;
        }

        const tail_rule = try self.nextRuleName("schema_array_tail");
        try self.out.appendSlice(self.allocator, tail_rule);
        try self.out.appendSlice(self.allocator, " ::= ws \",\" ws ");
        try self.out.appendSlice(self.allocator, item_rule);
        try self.out.append(self.allocator, '\n');

        const rule_name = try self.nextRuleName("schema_array");
        try self.out.appendSlice(self.allocator, rule_name);
        try self.out.appendSlice(self.allocator, " ::= ");

        if (max_items) |max_len| {
            var first_alt = true;
            var len = min_items;
            while (len <= max_len) : (len += 1) {
                if (!first_alt) try self.out.appendSlice(self.allocator, " | ");
                first_alt = false;
                try self.appendArrayExactAlternative(item_rule, tail_rule, len);
            }
        } else {
            if (min_items == 0) {
                try self.out.appendSlice(self.allocator, "\"[\" ws \"]\" | ");
            }
            try self.appendArrayAtLeastAlternative(item_rule, tail_rule, min_items);
        }

        try self.out.append(self.allocator, '\n');
        return rule_name;
    }

    fn appendArrayExactAlternative(self: *JsonSchemaGrammarBuilder, item_rule: []const u8, tail_rule: []const u8, len: usize) anyerror!void {
        try self.out.appendSlice(self.allocator, "\"[\" ws");
        if (len == 0) {
            try self.out.appendSlice(self.allocator, " \"]\"");
            return;
        }
        try self.out.append(self.allocator, ' ');
        try self.out.appendSlice(self.allocator, item_rule);
        for (1..len) |_| {
            try self.out.append(self.allocator, ' ');
            try self.out.appendSlice(self.allocator, tail_rule);
        }
        try self.out.appendSlice(self.allocator, " ws \"]\"");
    }

    fn appendArrayAtLeastAlternative(self: *JsonSchemaGrammarBuilder, item_rule: []const u8, tail_rule: []const u8, min_items: usize) anyerror!void {
        try self.out.appendSlice(self.allocator, "\"[\" ws ");
        if (min_items == 0) {
            try self.out.appendSlice(self.allocator, item_rule);
        } else {
            try self.out.appendSlice(self.allocator, item_rule);
            for (1..min_items) |_| {
                try self.out.append(self.allocator, ' ');
                try self.out.appendSlice(self.allocator, tail_rule);
            }
        }
        try self.out.append(self.allocator, ' ');
        try self.out.appendSlice(self.allocator, tail_rule);
        try self.out.appendSlice(self.allocator, "* ws \"]\"");
    }

    fn emitObjectRule(self: *JsonSchemaGrammarBuilder, schema_obj: std.json.ObjectMap) anyerror![]const u8 {
        var properties = std.ArrayListUnmanaged(PropertySpec).empty;
        defer properties.deinit(self.allocator);

        var required_names = std.ArrayListUnmanaged([]const u8).empty;
        defer required_names.deinit(self.allocator);
        if (schema_obj.get("required")) |required| {
            if (required != .array) return error.InvalidSchema;
            for (required.array.items) |entry| {
                if (entry != .string) return error.InvalidSchema;
                try required_names.append(self.allocator, entry.string);
            }
        }

        if (schema_obj.get("properties")) |prop_value| {
            if (prop_value != .object) return error.InvalidSchema;
            var it = prop_value.object.iterator();
            while (it.next()) |entry| {
                try properties.append(self.allocator, .{
                    .key = entry.key_ptr.*,
                    .rule_name = try self.emitSchemaRule(entry.value_ptr.*),
                    .required = stringSliceContains(required_names.items, entry.key_ptr.*),
                });
            }
        }

        // A generator may choose a subset of the schema's language, but it
        // must never drop the declared properties' constraints. Enumerating
        // permutations is useful for small objects; larger schemas use a
        // flat sequence in schema order instead of factorial expansion.
        for (required_names.items) |required| {
            var found = false;
            for (properties.items) |property| {
                if (std.mem.eql(u8, property.key, required)) found = true;
            }
            if (!found) return error.UnsupportedSchema;
        }
        if (properties.items.len <= 4) return self.emitPreciseObjectRule(properties.items);
        return self.emitOrderedObjectRule(properties.items);
    }

    fn emitOrderedObjectRule(self: *JsonSchemaGrammarBuilder, properties: []const PropertySpec) ![]const u8 {
        // Anchor on the first required field. Earlier optional fields own a
        // trailing comma; later fields own a leading comma. Individual member
        // rules return immediately, keeping the grammar stack flat for wide
        // schemas rather than recursing once per property.
        const members = try self.allocator.alloc([2][]const u8, properties.len);
        defer self.allocator.free(members);
        var anchor: ?usize = null;
        for (properties, 0..) |property, i| {
            if (anchor == null and property.required) anchor = i;
            members[i] = .{ try self.nextRuleName("member_before"), try self.nextRuleName("member_after") };
            try self.out.appendSlice(self.allocator, members[i][0]);
            try self.out.appendSlice(self.allocator, " ::= ");
            try self.appendOrderedProperty(property);
            try self.out.appendSlice(self.allocator, " ws \",\" ws\n");
            try self.out.appendSlice(self.allocator, members[i][1]);
            try self.out.appendSlice(self.allocator, " ::= ws \",\" ws ");
            try self.appendOrderedProperty(property);
            try self.out.append(self.allocator, '\n');
        }
        const rule = try self.nextRuleName("schema_object");
        try self.out.appendSlice(self.allocator, rule);
        try self.out.appendSlice(self.allocator, " ::= ");
        if (anchor) |required| {
            try self.out.appendSlice(self.allocator, "\"{\" ws ");
            for (members[0..required]) |member| {
                try self.out.appendSlice(self.allocator, member[0]);
                try self.out.appendSlice(self.allocator, "? ");
            }
            try self.appendOrderedProperty(properties[required]);
            try self.appendOrderedPropertyTail(properties[required + 1 ..], members[required + 1 ..]);
            try self.out.appendSlice(self.allocator, " ws \"}\"");
        } else {
            // All fields optional: select the first present field, then append
            // optional comma-prefixed fields. Bound the quadratic expansion.
            if (properties.len > 64) return error.UnsupportedSchema;
            try self.out.appendSlice(self.allocator, "\"{\" ws \"}\"");
            for (properties, 0..) |property, i| {
                try self.out.appendSlice(self.allocator, " | \"{\" ws ");
                try self.appendOrderedProperty(property);
                try self.appendOrderedPropertyTail(properties[i + 1 ..], members[i + 1 ..]);
                try self.out.appendSlice(self.allocator, " ws \"}\"");
            }
        }
        try self.out.append(self.allocator, '\n');
        return rule;
    }

    fn appendOrderedProperty(self: *JsonSchemaGrammarBuilder, property: PropertySpec) !void {
        try self.appendJsonStringToken(property.key);
        try self.out.appendSlice(self.allocator, " ws \":\" ws ");
        try self.out.appendSlice(self.allocator, property.rule_name);
    }

    fn appendOrderedPropertyTail(self: *JsonSchemaGrammarBuilder, properties: []const PropertySpec, members: []const [2][]const u8) !void {
        for (properties, members) |property, member| {
            try self.out.append(self.allocator, ' ');
            try self.out.appendSlice(self.allocator, member[1]);
            if (!property.required) try self.out.append(self.allocator, '?');
        }
    }

    fn emitPreciseObjectRule(self: *JsonSchemaGrammarBuilder, properties: []const PropertySpec) anyerror![]const u8 {
        const rule_name = try self.nextRuleName("schema_object");
        try self.out.appendSlice(self.allocator, rule_name);
        try self.out.appendSlice(self.allocator, " ::= ");

        var alternatives = std.ArrayListUnmanaged([]const usize).empty;
        defer {
            for (alternatives.items) |alt| self.allocator.free(alt);
            alternatives.deinit(self.allocator);
        }
        try collectPropertySubsets(self.allocator, properties, &alternatives);

        var first_alt = true;
        for (alternatives.items) |subset| {
            if (subset.len == 0) {
                if (!first_alt) try self.out.appendSlice(self.allocator, " | ");
                first_alt = false;
                try self.out.appendSlice(self.allocator, "\"{\" ws \"}\"");
                continue;
            }

            var permutations = std.ArrayListUnmanaged([]const usize).empty;
            defer {
                for (permutations.items) |perm| self.allocator.free(perm);
                permutations.deinit(self.allocator);
            }
            try collectPermutations(self.allocator, subset, &permutations);
            for (permutations.items) |perm| {
                if (!first_alt) try self.out.appendSlice(self.allocator, " | ");
                first_alt = false;
                try self.appendObjectAlternative(properties, perm);
            }
        }

        try self.out.append(self.allocator, '\n');
        return rule_name;
    }

    fn appendObjectAlternative(self: *JsonSchemaGrammarBuilder, properties: []const PropertySpec, indices: []const usize) anyerror!void {
        try self.out.appendSlice(self.allocator, "\"{\" ws");
        for (indices, 0..) |prop_idx, idx| {
            if (idx != 0) try self.out.appendSlice(self.allocator, " ws \",\" ws");
            try self.out.append(self.allocator, ' ');
            try self.appendJsonStringToken(properties[prop_idx].key);
            try self.out.appendSlice(self.allocator, " ws \":\" ws ");
            try self.out.appendSlice(self.allocator, properties[prop_idx].rule_name);
        }
        try self.out.appendSlice(self.allocator, " ws \"}\"");
    }

    fn appendJsonStringToken(self: *JsonSchemaGrammarBuilder, value: []const u8) !void {
        const literal = try std.json.Stringify.valueAlloc(self.allocator, value, .{});
        defer self.allocator.free(literal);
        try self.appendGrammarLiteral(literal);
    }
};

const PropertySpec = struct {
    key: []const u8,
    rule_name: []const u8,
    required: bool,
};

fn collectPropertySubsets(
    allocator: std.mem.Allocator,
    properties: []const PropertySpec,
    out: *std.ArrayListUnmanaged([]const usize),
) !void {
    const prop_count = properties.len;
    const total_masks: usize = @as(usize, 1) << @intCast(prop_count);
    for (0..total_masks) |mask| {
        var valid = true;
        var count: usize = 0;
        for (properties, 0..) |prop, idx| {
            const present = ((mask >> @intCast(idx)) & 1) == 1;
            if (prop.required and !present) {
                valid = false;
                break;
            }
            if (present) count += 1;
        }
        if (!valid) continue;

        const subset = try allocator.alloc(usize, count);
        var write_idx: usize = 0;
        for (0..prop_count) |idx| {
            if (((mask >> @intCast(idx)) & 1) == 1) {
                subset[write_idx] = idx;
                write_idx += 1;
            }
        }
        try out.append(allocator, subset);
    }
}

fn collectPermutations(
    allocator: std.mem.Allocator,
    indices: []const usize,
    out: *std.ArrayListUnmanaged([]const usize),
) !void {
    if (indices.len == 0) {
        const empty = try allocator.alloc(usize, 0);
        try out.append(allocator, empty);
        return;
    }

    const working = try allocator.dupe(usize, indices);
    defer allocator.free(working);
    try permuteIndices(allocator, working, 0, out);
}

fn permuteIndices(
    allocator: std.mem.Allocator,
    indices: []usize,
    start: usize,
    out: *std.ArrayListUnmanaged([]const usize),
) !void {
    if (start >= indices.len) {
        try out.append(allocator, try allocator.dupe(usize, indices));
        return;
    }

    var idx = start;
    while (idx < indices.len) : (idx += 1) {
        std.mem.swap(usize, &indices[start], &indices[idx]);
        try permuteIndices(allocator, indices, start + 1, out);
        std.mem.swap(usize, &indices[start], &indices[idx]);
    }
}

fn schemaTypeName(schema_obj: std.json.ObjectMap) !?[]const u8 {
    if (schema_obj.get("type")) |type_value| {
        if (type_value != .string) return error.InvalidSchema;
        return type_value.string;
    }
    if (schema_obj.get("properties") != null or schema_obj.get("required") != null) return "object";
    return null;
}

fn schemaNonNegativeInt(schema_obj: std.json.ObjectMap, key: []const u8) ?usize {
    const value = schema_obj.get(key) orelse return null;
    return switch (value) {
        .integer => |num| if (num >= 0) @intCast(num) else null,
        .number_string => |text| std.fmt.parseInt(usize, text, 10) catch null,
        else => null,
    };
}

fn schemaIntegerBound(schema_obj: std.json.ObjectMap, key: []const u8) ?i64 {
    const value = schema_obj.get(key) orelse return null;
    return switch (value) {
        .integer => |num| num,
        .number_string => |text| std.fmt.parseInt(i64, text, 10) catch null,
        else => null,
    };
}

fn stringSliceContains(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

/// Check if source[pos..] looks like a rule definition (identifier followed by ::=).
fn looksLikeRuleDef(source: []const u8, pos: usize) bool {
    var p = pos;
    while (p < source.len and isIdentChar(source[p])) : (p += 1) {}
    p = skipWs(source, p);
    return p + 3 <= source.len and std.mem.eql(u8, source[p..][0..3], "::=");
}

// ============================================================================
// Tests — JSON Schema Grammar Compiler
// ============================================================================

test "json schema grammar: required object property" {
    const allocator = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{
        \\  "type": "object",
        \\  "properties": { "answer": { "type": "string" } },
        \\  "required": ["answer"],
        \\  "additionalProperties": false
        \\}
    , .{});
    defer parsed.deinit();

    const source = try buildJsonSchemaGrammar(allocator, parsed.value);
    defer allocator.free(source);

    var grammar = try GbnfGrammar.parse(allocator, source);
    defer grammar.deinit();
    try grammar.advance("{\"answer\":\"ok\"}");
    try std.testing.expect(try grammar.isComplete());
}

test "json schema grammar: optional property arbitrary order" {
    const allocator = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{
        \\  "type": "object",
        \\  "properties": {
        \\    "answer": { "type": "string" },
        \\    "count": { "type": "integer" }
        \\  },
        \\  "required": ["answer"],
        \\  "additionalProperties": false
        \\}
    , .{});
    defer parsed.deinit();

    const source = try buildJsonSchemaGrammar(allocator, parsed.value);
    defer allocator.free(source);

    var grammar = try GbnfGrammar.parse(allocator, source);
    defer grammar.deinit();
    try grammar.advance("{\"count\":2,\"answer\":\"ok\"}");
    try std.testing.expect(try grammar.isComplete());
}

test "json schema grammar: bounded array and integer range" {
    const allocator = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{
        \\  "type": "array",
        \\  "items": { "type": "integer", "minimum": 1, "maximum": 2 },
        \\  "minItems": 1,
        \\  "maxItems": 2
        \\}
    , .{});
    defer parsed.deinit();

    const source = try buildJsonSchemaGrammar(allocator, parsed.value);
    defer allocator.free(source);

    var grammar = try GbnfGrammar.parse(allocator, source);
    defer grammar.deinit();
    try grammar.advance("[1,2]");
    try std.testing.expect(try grammar.isComplete());
}

test "json schema grammar: bounded formatting forces progress without changing strings" {
    const a = std.testing.allocator;
    const schema = try std.json.parseFromSlice(std.json.Value, a,
        \\{"type":"object","properties":{"conversation":{"type":"string"},"tool_evidence":{"type":"array","maxItems":0,"items":{"type":"string"}}},"required":["conversation","tool_evidence"],"additionalProperties":false}
    , .{});
    defer schema.deinit();
    const source = try buildJsonSchemaGrammarWithOptions(a, schema.value, .{ .bounded_whitespace = true });
    defer a.free(source);
    var grammar = try GbnfGrammar.parse(a, source);
    defer grammar.deinit();
    // A repeated-whitespace token must be rejected before sampling it, while
    // spaces inside a completed string remain ordinary content.
    try grammar.advance("{\"conversation\":\"a  b\"");
    var offsets = [_]u32{ 0, 1, 0, 2 };
    var lengths = [_]u16{ 1, 1, 2, 1 };
    const table = TokenByteTable{ .token_offsets = &offsets, .token_lengths = &lengths, .byte_data = @constCast(" \n,") };
    const before = try grammar.allowedTokenMaskFast(a, &table, offsets.len);
    defer a.free(before);
    try std.testing.expectEqualSlices(bool, &.{ true, true, false, true }, before);
    try grammar.advance(" ");
    const after = try grammar.allowedTokenMaskFast(a, &table, offsets.len);
    defer a.free(after);
    try std.testing.expectEqualSlices(bool, &.{ false, false, false, true }, after);
    try grammar.advance(", \"tool_evidence\": []}");
    try std.testing.expect(try grammar.isComplete());

    const default_source = try buildJsonSchemaGrammar(a, schema.value);
    defer a.free(default_source);
    var default_grammar = try GbnfGrammar.parse(a, default_source);
    defer default_grammar.deinit();
    try default_grammar.advance("{\n  \"conversation\":\"a  b\",\n  \"tool_evidence\": []}");
    try std.testing.expect(try default_grammar.isComplete());
}

test "json schema grammar: string masks reject every raw control byte and permit escapes" {
    const a = std.testing.allocator;
    const schema = try std.json.parseFromSlice(std.json.Value, a, "{\"type\":\"string\"}", .{});
    defer schema.deinit();
    const source = try buildJsonSchemaGrammar(a, schema.value);
    defer a.free(source);
    var grammar = try GbnfGrammar.parse(a, source);
    defer grammar.deinit();
    try grammar.advance("\"");
    var bytes: [35]u8 = undefined;
    var offsets: [34]u32 = undefined;
    var lengths: [34]u16 = @splat(1);
    for (0..32) |i| {
        bytes[i] = @intCast(i);
        offsets[i] = @intCast(i);
    }
    @memcpy(bytes[32..], "a\\n");
    offsets[32] = 32;
    offsets[33] = 33;
    lengths[33] = 2;
    const table = TokenByteTable{ .token_offsets = &offsets, .token_lengths = &lengths, .byte_data = &bytes };
    const mask = try grammar.allowedTokenMaskFast(a, &table, offsets.len);
    defer a.free(mask);
    for (mask[0..32]) |allowed| try std.testing.expect(!allowed);
    try std.testing.expect(mask[32] and mask[33]);
    try grammar.advance("a\\n\\u0000\"");
    try std.testing.expect(try grammar.isComplete());
}

test "json schema grammar: large objects retain required fields and enum constraints" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"type":"object","properties":{"a":{"enum":["yes"]},"b":{"type":"boolean"},"c":{"type":"string"},"d":{"type":"string"},"e":{"type":"string"}},"required":["a","e"],"additionalProperties":false}
    , .{});
    defer parsed.deinit();
    const source = try buildJsonSchemaGrammar(a, parsed.value);
    defer a.free(source);
    for ([_][]const u8{ "{\"a\":\"yes\",\"e\":\"ok\"}", "{\"a\":\"yes\",\"b\":true,\"c\":\"c\",\"d\":\"d\",\"e\":\"e\"}" }) |valid| {
        var grammar = try GbnfGrammar.parse(a, source);
        defer grammar.deinit();
        try grammar.advance(valid);
        try std.testing.expect(try grammar.isComplete());
    }
    for ([_][]const u8{ "{}", "{\"a\":\"no\",\"e\":\"ok\"}", "{\"a\":\"yes\"}", "{,\"a\":\"yes\",\"e\":\"ok\"}", "{\"a\":\"yes\",\"e\":\"ok\",}" }) |invalid| {
        var grammar = try GbnfGrammar.parse(a, source);
        defer grammar.deinit();
        try grammar.advance(invalid);
        try std.testing.expect(!try grammar.isComplete());
    }
}

test "json schema grammar: optional fields before the anchor and entirely optional objects" {
    const a = std.testing.allocator;
    for (0..2) |all_optional| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a,
            \\{"type":"object","properties":{"a":{"type":"boolean"},"b":{"type":"boolean"},"c":{"type":"boolean"},"d":{"type":"boolean"},"e":{"type":"boolean"}},"required":["e"],"additionalProperties":false}
        , .{});
        defer parsed.deinit();
        if (all_optional == 1) _ = parsed.value.object.swapRemove("required");
        const source = try buildJsonSchemaGrammar(a, parsed.value);
        defer a.free(source);
        for ([_][]const u8{ "{\"e\":true}", "{\"a\":false,\"e\":true}", "{\"b\":true,\"d\":false,\"e\":true}" }) |valid| {
            var grammar = try GbnfGrammar.parse(a, source);
            defer grammar.deinit();
            try grammar.advance(valid);
            try std.testing.expect(try grammar.isComplete());
        }
        var empty = try GbnfGrammar.parse(a, source);
        defer empty.deinit();
        try empty.advance("{}");
        try std.testing.expectEqual(all_optional == 1, try empty.isComplete());
        var trailing = try GbnfGrammar.parse(a, source);
        defer trailing.deinit();
        try trailing.advance("{\"e\":true,}");
        try std.testing.expect(!try trailing.isComplete());
    }
}

test "json schema grammar: bounded integers include i64 maximum without overflow" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"type":"integer","minimum":9223372036854775806,"maximum":9223372036854775807}
    , .{});
    defer parsed.deinit();
    const source = try buildJsonSchemaGrammar(a, parsed.value);
    defer a.free(source);
    var grammar = try GbnfGrammar.parse(a, source);
    defer grammar.deinit();
    try grammar.advance("9223372036854775807");
    try std.testing.expect(try grammar.isComplete());
}

// ============================================================================
// Tests — JSON Grammar
// ============================================================================

test "json grammar: simple object" {
    var g = JsonGrammar.init();
    const json = "{\"key\": \"value\"}";
    g.advance(json);
    try std.testing.expect(g.isComplete());
}

test "json grammar: nested object" {
    var g = JsonGrammar.init();
    g.advance("{\"a\": {\"b\": 123}}");
    try std.testing.expect(g.isComplete());
}

test "json grammar: array" {
    var g = JsonGrammar.init();
    g.advance("[1, 2, 3]");
    try std.testing.expect(g.isComplete());
}

test "json grammar: mixed nested" {
    var g = JsonGrammar.init();
    g.advance("{\"arr\": [1, \"two\", true, null, {\"n\": false}]}");
    try std.testing.expect(g.isComplete());
}

test "json grammar: string value" {
    var g = JsonGrammar.init();
    g.advance("\"hello world\"");
    try std.testing.expect(g.isComplete());
}

test "json grammar: number value" {
    var g = JsonGrammar.init();
    g.advance("42");
    // Number needs a trailing byte to confirm it ended. At the raw level,
    // after the last digit we're still in .number state. To handle this,
    // when checking isComplete we also accept number state at stack depth 0.
    // Actually let's test with trailing whitespace:
    g.advance(" ");
    try std.testing.expect(g.isComplete());
}

test "json grammar: literals" {
    for ([_][]const u8{ "true", "false", "null" }) |lit| {
        var g = JsonGrammar.init();
        g.advance(lit);
        try std.testing.expect(g.isComplete());
    }
}

test "json grammar: invalid start" {
    var g = JsonGrammar.init();
    g.advance("xyz");
    try std.testing.expectEqual(State.err, g.state);
}

test "json grammar: escape in string" {
    var g = JsonGrammar.init();
    g.advance("{\"key\": \"val\\\"ue\"}");
    try std.testing.expect(g.isComplete());
}

test "json grammar: empty object" {
    var g = JsonGrammar.init();
    g.advance("{}");
    try std.testing.expect(g.isComplete());
}

test "json grammar: empty array" {
    var g = JsonGrammar.init();
    g.advance("[]");
    try std.testing.expect(g.isComplete());
}

test "json grammar: negative number" {
    var g = JsonGrammar.init();
    g.advance("-3.14e10 ");
    try std.testing.expect(g.isComplete());
}

test "json grammar: incremental advance" {
    var g = JsonGrammar.init();
    g.advance("{");
    try std.testing.expect(!g.isComplete());
    try std.testing.expectEqual(State.object_open, g.state);

    g.advance("\"k\"");
    try std.testing.expectEqual(State.colon, g.state);

    g.advance(": ");
    try std.testing.expectEqual(State.object_value, g.state);

    g.advance("1");
    try std.testing.expectEqual(State.number, g.state);

    g.advance("}");
    try std.testing.expect(g.isComplete());
}

// ============================================================================
// Tests — GBNF Grammar
// ============================================================================

test "gbnf: simple literal" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"hello\"");
    defer g.deinit();

    try g.advance("hello");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: literal rejects wrong input" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"hello\"");
    defer g.deinit();

    try g.advance("hell");
    try std.testing.expect(!try g.isComplete());
    try g.advance("x"); // wrong byte
    try std.testing.expectEqual(@as(usize, 0), g.positions.items.len);
}

test "gbnf: character class" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [abc]");
    defer g.deinit();

    try g.advance("a");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: character range" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [a-z]");
    defer g.deinit();

    try g.advance("m");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: negated character class" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [^0-9]");
    defer g.deinit();

    try g.advance("a");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: negated class rejects matching char" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [^0-9]");
    defer g.deinit();

    try g.advance("5");
    try std.testing.expectEqual(@as(usize, 0), g.positions.items.len);
}

test "gbnf: zero or more repetition" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [a-z]*");
    defer g.deinit();

    // Zero repetitions should be complete.
    try std.testing.expect(try g.isComplete());

    try g.advance("abc");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: one or more repetition" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [0-9]+");
    defer g.deinit();

    // Zero repetitions: not complete.
    try std.testing.expect(!try g.isComplete());

    try g.advance("1");
    try std.testing.expect(try g.isComplete());

    try g.advance("23");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: optional element" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= "a" "b"?
    );
    defer g.deinit();

    try g.advance("a");
    // "b" is optional, so "a" alone is complete.
    try std.testing.expect(try g.isComplete());
}

test "gbnf: optional element with match" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= "a" "b"?
    );
    defer g.deinit();

    try g.advance("ab");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: alternatives" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"yes\" | \"no\"");
    defer g.deinit();

    try g.advance("no");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: rule reference" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= greeting
        \\greeting ::= "hi"
    );
    defer g.deinit();

    try g.advance("hi");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: sequence of elements" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"a\" \"b\" \"c\"");
    defer g.deinit();

    try g.advance("abc");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: no root rule" {
    const allocator = std.testing.allocator;
    const result = GbnfGrammar.parse(allocator, "foo ::= \"bar\"");
    try std.testing.expectError(error.NoRootRule, result);
}

test "gbnf: comment and whitespace" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\# This is a comment
        \\root ::= "ok"
    );
    defer g.deinit();

    try g.advance("ok");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: literal with escape" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"a\\nb\"");
    defer g.deinit();

    try g.advance("a\nb");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: digits grammar" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= digit+
        \\digit ::= [0-9]
    );
    defer g.deinit();

    try g.advance("42");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: simple word grammar" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= letter+
        \\letter ::= [a-zA-Z]
    );
    defer g.deinit();

    try g.advance("Hello");
    try std.testing.expect(try g.isComplete());
}

test "gbnf: incremental byte advance" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"abc\"");
    defer g.deinit();

    try g.advanceByte('a');
    try std.testing.expect(!try g.isComplete());
    try std.testing.expect(g.positions.items.len > 0);

    try g.advanceByte('b');
    try std.testing.expect(!try g.isComplete());

    try g.advanceByte('c');
    try std.testing.expect(try g.isComplete());
}

test "gbnf: repeated rule reference" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= item+
        \\item ::= [a-z]
    );
    defer g.deinit();

    try g.advance("abc");
    try std.testing.expect(try g.isComplete());
}

test "gbnf token masks bound ambiguous repetition and preserve rejection" {
    const a = std.testing.allocator;
    var grammar = try GbnfGrammar.parse(a, "root ::= item+ \"b\"\nitem ::= \"a\" | \"a\"");
    defer grammar.deinit();
    var bytes: [302]u8 = undefined;
    @memset(bytes[0..300], 'a');
    bytes[300] = 'c';
    bytes[301] = 'b';
    const offsets = [_]u32{ 0, 0, 301, 0 };
    const lengths = [_]u16{ 300, 301, 1, 0 };
    const table = TokenByteTable{ .token_offsets = @constCast(&offsets), .token_lengths = @constCast(&lengths), .byte_data = @constCast(&bytes) };
    const first = try grammar.allowedTokenMaskFast(a, &table, 4);
    defer a.free(first);
    try std.testing.expectEqualSlices(bool, &.{ true, false, false, false }, first);
    const repeated = try grammar.allowedTokenMaskFast(a, &table, 4);
    defer a.free(repeated);
    try std.testing.expectEqualSlices(bool, first, repeated);
    // Cache identity includes decoded bytes, not only table pointers or size.
    bytes[300] = 'a';
    const changed = try grammar.allowedTokenMaskFast(a, &table, 4);
    defer a.free(changed);
    try std.testing.expectEqualSlices(bool, &.{ true, true, false, false }, changed);
    try std.testing.expectEqualSlices(bool, &.{ true, false, false, false }, first);
    bytes[300] = 'c';
    try grammar.advance(bytes[0..300]);
    try std.testing.expect(grammar.positions.items.len < 8);
    const after = try grammar.allowedTokenMaskFast(a, &table, 4);
    defer a.free(after);
    try std.testing.expectEqualSlices(bool, &.{ true, false, true, false }, after);
    try grammar.advance("a");
    const continued = try grammar.allowedTokenMaskFast(a, &table, 4);
    defer a.free(continued);
    try std.testing.expectEqualSlices(bool, after, continued);
    {
        const cache = grammar.mask_cache;
        grammar.mask_cache = null;
        defer grammar.mask_cache = cache;
        const uncached = try grammar.allowedTokenMaskFast(a, &table, 4);
        defer a.free(uncached);
        try std.testing.expectEqualSlices(bool, continued, uncached);
    }
    try grammar.advance("b");
    try std.testing.expect(try grammar.isComplete());
}

test "gbnf vocabulary masking bounds scratch allocations and isolates token candidates" {
    const a = std.testing.allocator;
    var counted = std.testing.FailingAllocator.init(a, .{});
    var grammar = try GbnfGrammar.parse(counted.allocator(), "root ::= item+ \"b\"\nitem ::= \"a\" | \"a\"");
    defer grammar.deinit();
    const vocab_size = 4096;
    const offsets = try a.alloc(u32, vocab_size);
    defer a.free(offsets);
    const lengths = try a.alloc(u16, vocab_size);
    defer a.free(lengths);
    const token_offsets = [_]u32{ 0, 4, 5, 6, 0 };
    const token_lengths = [_]u16{ 4, 1, 1, 1, 0 };
    for (offsets, lengths, 0..) |*offset, *length, i| {
        offset.* = token_offsets[i % token_offsets.len];
        length.* = token_lengths[i % token_lengths.len];
    }
    const table = TokenByteTable{
        .token_offsets = offsets,
        .token_lengths = lengths,
        .byte_data = @constCast("aaababc"),
    };
    for (0..2) |step| {
        const before = counted.allocations;
        // Returning the mask through a different allocator must not change
        // ownership of the grammar's scratch buffers.
        const mask = try grammar.allowedTokenMaskFast(a, &table, vocab_size);
        defer a.free(mask);
        try std.testing.expect(counted.allocations - before <= 32);
        for (mask, 0..) |allowed, i| {
            const kind = i % token_offsets.len;
            const expected = kind == 0 or kind == 1 or (step == 1 and kind == 2);
            try std.testing.expectEqual(expected, allowed);
        }
        try grammar.advance("a");
    }
}

test "grammar empty language fails closed but completion leaves EOS to the pipeline" {
    const a = std.testing.allocator;
    var grammar = try GbnfGrammar.parse(a, "root ::= \"a\"");
    defer grammar.deinit();
    var offsets = [_]u32{0};
    var lengths = [_]u16{1};
    var bytes = [_]u8{'b'};
    const table: TokenByteTable = .{ .token_offsets = &offsets, .token_lengths = &lengths, .byte_data = &bytes };
    try std.testing.expectError(error.GrammarNoValidToken, grammar.allowedTokenMaskFast(a, &table, 1));
    try grammar.advance("a");
    const completed = try grammar.allowedTokenMaskFast(a, &table, 1);
    defer a.free(completed);
    try std.testing.expectEqualSlices(bool, &.{false}, completed);
}

test "grammar recursive expansion returns an explicit resource limit" {
    const a = std.testing.allocator;
    var grammar = try GbnfGrammar.parse(a, "root ::= root | \"a\"");
    defer grammar.deinit();
    try std.testing.expectError(error.GrammarResourceLimit, grammar.advanceByte('a'));
}

test "grammar failed byte allocation preserves parser state" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var grammar = try GbnfGrammar.parse(failing.allocator(), "root ::= \"ab\"");
    defer grammar.deinit();
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, grammar.advanceByte('a'));
    failing.fail_index = std.math.maxInt(usize);
    try grammar.advance("ab");
    try std.testing.expect(try grammar.isComplete());
}

test "grammar masks preserve metaspace continuation bytes in schema keys" {
    const allocator = std.testing.allocator;
    const HfTokenizer = @import("inference_hf_tokenizer").HfTokenizer;
    var hf = try HfTokenizer.loadFromBytes(allocator,
        \\{"model":{"type":"Unigram","unk_id":0,"vocab":[["<unk>",0],["▁{",-1],["\"",-1],["▁tool_evidence",-1],["tool_evidence",-1],["\":[]}",-1]]},"pre_tokenizer":{"type":"Metaspace","replacement":"▁","prepend_scheme":"always"}}
    );
    defer hf.deinitSelf();
    const tok = hf.tokenizer();
    var table = try TokenByteTable.init(allocator, tok, 6);
    defer table.deinit(allocator);
    try std.testing.expectEqualStrings("{", table.getTokenBytesAtPosition(1, true));
    try std.testing.expectEqualStrings(" {", table.getTokenBytesAtPosition(1, false));
    try std.testing.expectEqualStrings(" tool_evidence", table.getTokenBytesAtPosition(3, false));
    var grammar = try GbnfGrammar.parse(allocator,
        \\root ::= "{\"tool_evidence\":[]}"
    );
    defer grammar.deinit();
    const initial_mask = try grammar.allowedTokenMaskFast(allocator, &table, 6);
    defer allocator.free(initial_mask);
    try std.testing.expect(initial_mask[1]);
    try grammar.advance(table.getTokenBytesAtPosition(1, grammar.at_start));
    try grammar.advance(table.getTokenBytesAtPosition(2, grammar.at_start));
    const fast = try grammar.allowedTokenMaskFast(allocator, &table, 6);
    defer allocator.free(fast);
    const slow = try grammar.allowedTokenMask(allocator, tok, 6);
    defer allocator.free(slow);
    try std.testing.expectEqualSlices(bool, slow, fast);
    try std.testing.expect(!fast[3]);
    try std.testing.expect(fast[4]);
    try grammar.advance(table.getTokenBytesAtPosition(4, grammar.at_start));
    try grammar.advance(table.getTokenBytesAtPosition(5, grammar.at_start));
    try std.testing.expect(try grammar.isComplete());
    const decoded = try tok.decode(allocator, &.{ 1, 2, 4, 5 });
    defer allocator.free(decoded);
    try std.testing.expectEqualStrings("{\"tool_evidence\":[]}", decoded);
}

test "grammar token byte table agrees with whole WordPiece and byte-level decoding" {
    const allocator = std.testing.allocator;
    const HfTokenizer = @import("inference_hf_tokenizer").HfTokenizer;
    const fixtures = [_][]const u8{
        \\{"model":{"type":"WordPiece","unk_token":"[UNK]","continuing_subword_prefix":"##","vocab":{"[UNK]":0,"test":1,"##ing":2,"again":3}}}
        ,
        \\{"model":{"type":"BPE","vocab":{"x":0,"Ġtest":1,"ing":2,"Ġagain":3},"merges":[]},"pre_tokenizer":{"type":"ByteLevel"}}
        ,
    };
    for (fixtures) |fixture| {
        var hf = try HfTokenizer.loadFromBytes(allocator, fixture);
        defer hf.deinitSelf();
        const tok = hf.tokenizer();
        var table = try TokenByteTable.init(allocator, tok, 4);
        defer table.deinit(allocator);
        var joined = std.ArrayListUnmanaged(u8).empty;
        defer joined.deinit(allocator);
        for ([_]usize{ 1, 2, 3 }, 0..) |id, index| {
            try joined.appendSlice(allocator, table.getTokenBytesAtPosition(id, index == 0));
        }
        const decoded = try tok.decode(allocator, &.{ 1, 2, 3 });
        defer allocator.free(decoded);
        try std.testing.expectEqualStrings(decoded, joined.items);
        const table_decoded = try table.decodeTokens(allocator, &.{ 1, 2, 3 });
        defer allocator.free(table_decoded);
        try std.testing.expectEqualStrings(decoded, table_decoded);
    }
}

test "canonical evidence isolates asymmetric vocabulary tokens and rejects unsupported values" {
    const a = std.testing.allocator;
    var schema = try std.json.parseFromSlice(std.json.Value, a, "{\"enum\":[\"1\",\"2\"]}", .{});
    defer schema.deinit();
    const source = try buildJsonSchemaGrammarWithEvidence(a, schema.value, true);
    defer a.free(source);
    var grammar = try GbnfGrammar.parse(a, source);
    defer grammar.deinit();
    const offsets = [_]u32{ 0, 1, 2, 3, 5, 7 };
    const lengths = [_]u16{ 1, 1, 1, 2, 2, 1 };
    const fallback = [_]bool{ false, true, false, false, false, false };
    const table = TokenByteTable{ .byte_data = @constCast("\"121\"\"21"), .token_offsets = @constCast(&offsets), .token_lengths = @constCast(&lengths), .byte_fallback_tokens = @constCast(&fallback) };
    // Admission must invalidate a permissive cached mask, and repeated
    // admission must replace owned ID arrays without leaking them.
    const unrestricted = try grammar.allowedTokenMaskFast(a, &table, lengths.len);
    defer a.free(unrestricted);
    try std.testing.expect(unrestricted[4]);
    try grammar.admitTokenEvidence(&table);
    try grammar.admitTokenEvidence(&table);
    const first = try grammar.allowedTokenMaskFast(a, &table, lengths.len);
    defer a.free(first);
    try std.testing.expect(first[0]);
    try std.testing.expect(!first[4]); // opening quote fused with 2
    try grammar.advance("\"");
    const values = try grammar.allowedTokenMaskFast(a, &table, lengths.len);
    defer a.free(values);
    try std.testing.expect(values[5] and values[2]);
    try std.testing.expect(!values[3]); // 1 fused with closing quote
    try std.testing.expect(!values[1]); // Prefer a regular piece over its byte fallback alias.
    try grammar.advance("1");
    const closing = try grammar.allowedTokenMaskFast(a, &table, lengths.len);
    defer a.free(closing);
    try std.testing.expect(closing[0]);

    var unsupported = try GbnfGrammar.parse(a, "root ::= antfly_finite_token_v1_0\nantfly_finite_token_v1_0 ::= \"a_is_b\" | \"none\"\n");
    defer unsupported.deinit();
    try std.testing.expectError(error.UnsupportedConstrainedProbabilities, unsupported.admitTokenEvidence(&table));
}

test "canonical evidence predicates exclude optional whitespace from the finite distribution" {
    const a = std.testing.allocator;
    var grammar = try GbnfGrammar.parse(a, "root ::= ws antfly_finite_token_v1_0 ws\nws ::= [ \\t\\n]*\nantfly_finite_token_v1_0 ::= \"true\" | \"false\"\n");
    defer grammar.deinit();
    const offsets = [_]u32{ 0, 4, 9, 10 };
    const lengths = [_]u16{ 4, 5, 1, 1 };
    const table = TokenByteTable{ .byte_data = @constCast("truefalse \n"), .token_offsets = @constCast(&offsets), .token_lengths = @constCast(&lengths) };
    try grammar.admitTokenEvidence(&table);
    const mask = try grammar.allowedTokenMaskFast(a, &table, lengths.len);
    defer a.free(mask);
    try std.testing.expect(mask[0] and mask[1]);
    try std.testing.expect(!mask[2] and !mask[3]);
}

test "canonical evidence preserves large positional batch ID enums" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var schema: std.json.Value = .{ .object = .empty };
    var ids: std.json.Value = .{ .array = std.json.Array.init(a) };
    for (0..128) |id| try ids.array.append(.{ .integer = @intCast(id) });
    try schema.object.put(a, "enum", ids);
    const source = try buildJsonSchemaGrammarWithEvidence(a, schema, true);
    try std.testing.expect(std.mem.indexOf(u8, source, "antfly_finite_token_v1_") == null);
    var grammar = try GbnfGrammar.parse(a, source);
    defer grammar.deinit();
    try grammar.advance("127");
    try std.testing.expect(try grammar.isComplete());
}
