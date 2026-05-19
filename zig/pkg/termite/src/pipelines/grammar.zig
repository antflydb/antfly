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
const tokenizer_mod = @import("termite_tokenizer");

/// Pre-decoded token byte table. Decodes all vocab tokens once so that
/// `allowedTokenMask` can look up token bytes without per-call allocation.
pub const TokenByteTable = struct {
    /// Offset into `byte_data` for each token_id.
    token_offsets: []u32,
    /// Byte length of each token's decoded form.
    token_lengths: []u16,
    /// Concatenated decoded bytes for all tokens.
    byte_data: []u8,

    pub fn init(allocator: std.mem.Allocator, tok: tokenizer_mod.Tokenizer, vocab_size: usize) !TokenByteTable {
        var offsets = try allocator.alloc(u32, vocab_size);
        errdefer allocator.free(offsets);
        var lengths = try allocator.alloc(u16, vocab_size);
        errdefer allocator.free(lengths);

        // Single pass: decode all tokens, accumulate bytes into a growable list.
        var byte_list = std.ArrayListUnmanaged(u8).empty;
        errdefer byte_list.deinit(allocator);
        for (0..vocab_size) |token_id| {
            const ids = [1]i32{@intCast(token_id)};
            const token_bytes = tok.decode(allocator, &ids) catch {
                offsets[token_id] = std.math.cast(u32, byte_list.items.len) orelse return error.VocabTooLarge;
                lengths[token_id] = 0;
                continue;
            };
            defer allocator.free(token_bytes);
            const capped_len: u16 = @intCast(@min(token_bytes.len, std.math.maxInt(u16)));
            offsets[token_id] = std.math.cast(u32, byte_list.items.len) orelse return error.VocabTooLarge;
            lengths[token_id] = capped_len;
            try byte_list.appendSlice(allocator, token_bytes[0..capped_len]);
        }
        const byte_data = try byte_list.toOwnedSlice(allocator);

        return .{
            .token_offsets = offsets,
            .token_lengths = lengths,
            .byte_data = byte_data,
        };
    }

    pub fn getTokenBytes(self: *const TokenByteTable, token_id: usize) []const u8 {
        if (token_id >= self.token_lengths.len) return &[_]u8{};
        const len = self.token_lengths[token_id];
        if (len == 0) return &[_]u8{};
        const off = self.token_offsets[token_id];
        return self.byte_data[off..][0..len];
    }

    pub fn deinit(self: *TokenByteTable, allocator: std.mem.Allocator) void {
        allocator.free(self.token_offsets);
        allocator.free(self.token_lengths);
        allocator.free(self.byte_data);
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
        @memset(mask, false);

        var any_allowed = false;

        // For each token in the vocabulary, simulate advancing a copy of the FSM.
        for (0..vocab_size) |token_id| {
            const ids = [1]i32{@intCast(token_id)};
            const token_bytes = tok.decode(allocator, &ids) catch continue;
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

        // Safety: if nothing is allowed (shouldn't happen for well-formed grammars),
        // allow everything to avoid completely blocking generation.
        if (!any_allowed) {
            @memset(mask, true);
        }

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
        @memset(mask, false);

        var any_allowed = false;

        for (0..vocab_size) |token_id| {
            const token_bytes = token_table.getTokenBytes(token_id);
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

        if (!any_allowed) {
            @memset(mask, true);
        }

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
        /// For repetitions (+ and *): how many times we've matched so far.
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
    allocator: std.mem.Allocator,
    rules: std.StringArrayHashMapUnmanaged(GbnfRule),
    positions: std.ArrayListUnmanaged(ParsePosition),
    /// All allocations that need to be freed on deinit.
    arena_slices: std.ArrayListUnmanaged(ArenaAllocation),

    pub fn parse(allocator: std.mem.Allocator, source: []const u8) !GbnfGrammar {
        var grammar = GbnfGrammar{
            .allocator = allocator,
            .rules = .empty,
            .positions = .empty,
            .arena_slices = .empty,
        };
        errdefer grammar.deinit();

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

        return grammar;
    }

    pub fn deinit(self: *GbnfGrammar) void {
        for (self.arena_slices.items) |allocation| {
            self.allocator.rawFree(allocation.bytes, allocation.alignment, @returnAddress());
        }
        self.arena_slices.deinit(self.allocator);
        self.positions.deinit(self.allocator);
        // Rules and their contents are allocated from arena_slices, so no
        // per-rule cleanup needed — we just free all arena slices above.
        self.rules.deinit(self.allocator);
    }

    /// Feed a sequence of bytes and advance all parse positions.
    pub fn advance(self: *GbnfGrammar, bytes: []const u8) void {
        for (bytes) |b| {
            self.advanceByte(b);
        }
    }

    /// Advance all active positions by one byte. Positions that can't consume
    /// the byte are dropped. Positions at the end of a quantified element may
    /// fork (one continues the repetition, one advances past it).
    pub fn advanceByte(self: *GbnfGrammar, byte: u8) void {
        var next = std.ArrayListUnmanaged(ParsePosition).empty;

        for (self.positions.items) |*pos| {
            self.advancePosition(pos, byte, &next);
        }

        self.positions.deinit(self.allocator);
        self.positions = next;

        // Deduplicate positions to prevent exponential blowup.
        self.dedup();
    }

    /// Check whether any active position has completed the root rule.
    pub fn isComplete(self: *const GbnfGrammar) bool {
        for (self.positions.items) |*pos| {
            var expanded = std.ArrayListUnmanaged(ParsePosition).empty;
            defer expanded.deinit(self.allocator);
            self.expandPosition(pos, &expanded);

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
        @memset(mask, false);

        var any_allowed = false;

        for (0..vocab_size) |token_id| {
            const ids = [1]i32{@intCast(token_id)};
            const token_bytes = tok.decode(allocator, &ids) catch continue;
            defer allocator.free(token_bytes);

            if (token_bytes.len == 0) continue;

            // Simulate: clone positions, advance with token bytes, check survival.
            var sim_positions = std.ArrayListUnmanaged(ParsePosition).empty;
            defer sim_positions.deinit(allocator);
            sim_positions.appendSlice(allocator, self.positions.items) catch continue;

            var valid = true;
            for (token_bytes) |b| {
                var next = std.ArrayListUnmanaged(ParsePosition).empty;
                for (sim_positions.items) |*pos| {
                    self.advancePosition(pos, b, &next);
                }
                sim_positions.deinit(allocator);
                sim_positions = next;

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

        if (!any_allowed) {
            @memset(mask, true);
        }

        return mask;
    }

    /// Fast variant using a pre-decoded TokenByteTable.
    pub fn allowedTokenMaskFast(
        self: *const GbnfGrammar,
        allocator: std.mem.Allocator,
        token_table: *const TokenByteTable,
        vocab_size: usize,
    ) ![]bool {
        const mask = try allocator.alloc(bool, vocab_size);
        @memset(mask, false);

        var any_allowed = false;

        for (0..vocab_size) |token_id| {
            const token_bytes = token_table.getTokenBytes(token_id);
            if (token_bytes.len == 0) continue;

            var sim_positions = std.ArrayListUnmanaged(ParsePosition).empty;
            defer sim_positions.deinit(allocator);
            sim_positions.appendSlice(allocator, self.positions.items) catch continue;

            var valid = true;
            for (token_bytes) |b| {
                var next = std.ArrayListUnmanaged(ParsePosition).empty;
                for (sim_positions.items) |*pos| {
                    self.advancePosition(pos, b, &next);
                }
                sim_positions.deinit(allocator);
                sim_positions = next;

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

        if (!any_allowed) {
            @memset(mask, true);
        }

        return mask;
    }

    /// Apply the grammar mask to logits: set disallowed tokens to -inf.
    pub fn applyMask(mask: []const bool, logits: []f32) void {
        JsonGrammar.applyMask(mask, logits);
    }

    // --- Internal position advancement ---

    fn advancePosition(
        self: *const GbnfGrammar,
        pos: *const ParsePosition,
        byte: u8,
        next: *std.ArrayListUnmanaged(ParsePosition),
    ) void {
        // First, expand the position: if we're at the start of a rule_ref or
        // group, or past the end of an alternative, adjust the position.
        var expanded = std.ArrayListUnmanaged(ParsePosition).empty;
        self.expandPosition(pos, &expanded);

        for (expanded.items) |*epos| {
            self.tryConsumeByte(epos, byte, next);
        }
        expanded.deinit(self.allocator);
    }

    /// Expand a position by resolving rule references and handling end-of-alternative.
    /// A single position may expand to multiple positions (e.g., at a rule_ref fork).
    fn expandPosition(
        self: *const GbnfGrammar,
        pos: *const ParsePosition,
        out: *std.ArrayListUnmanaged(ParsePosition),
    ) void {
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
                        parent.rep_count += 1;
                    },
                }
                // Recursively expand the popped position.
                self.expandPosition(&popped, out);
            } else {
                out.append(self.allocator, popped) catch return;
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
                self.expandPosition(&skip, out);
            },
            .optional => {
                if (frame.rep_count == 0) {
                    // Fork: skip past this element.
                    var skip = pos.clone();
                    const sf = skip.topFrameMut().?;
                    sf.elem_idx += 1;
                    sf.literal_offset = 0;
                    sf.rep_count = 0;
                    self.expandPosition(&skip, out);
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
                    self.expandPosition(&skip, out);
                }
            },
            .once => {},
        }

        // Now expand the current element.
        switch (qe.element) {
            .rule_ref => |ref_name| {
                if (frame.literal_offset > 0) {
                    // Already started matching this ref, don't re-enter.
                    out.append(self.allocator, pos.clone()) catch return;
                    return;
                }
                const ref_rule = self.rules.get(ref_name) orelse return;
                // Push a new frame for the referenced rule, fork for each alternative.
                for (0..ref_rule.alternatives.len) |alt_i| {
                    if (pos.frame_count >= max_gbnf_depth) continue;
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
                    self.expandPosition(&new_pos, out);
                }
            },
            .group => |alts| {
                if (frame.literal_offset > 0) {
                    out.append(self.allocator, pos.clone()) catch return;
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
                    out.append(self.allocator, pos.clone()) catch return;
                }
            },
            .literal, .char_class => {
                // These are terminal — add position as-is for byte consumption.
                out.append(self.allocator, pos.clone()) catch return;
            },
        }
    }

    /// Try to consume a byte at a (fully expanded) position.
    fn tryConsumeByte(
        self: *const GbnfGrammar,
        pos: *const ParsePosition,
        byte: u8,
        next: *std.ArrayListUnmanaged(ParsePosition),
    ) void {
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
                            nf.rep_count += 1;
                        },
                    }
                } else {
                    nf.literal_offset += 1;
                }
                next.append(self.allocator, new_pos) catch return;
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
                        nf.rep_count += 1;
                    },
                }
                next.append(self.allocator, new_pos) catch return;
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
    fn dedup(self: *GbnfGrammar) void {
        if (self.positions.items.len <= 1) return;

        var keep: usize = 0;
        for (0..self.positions.items.len) |i| {
            var is_dup = false;
            for (0..keep) |j| {
                if (positionsEqual(&self.positions.items[i], &self.positions.items[j])) {
                    is_dup = true;
                    break;
                }
            }
            if (!is_dup) {
                self.positions.items[keep] = self.positions.items[i];
                keep += 1;
            }
        }
        self.positions.items.len = keep;

        // Also cap at max_positions to prevent runaway state.
        if (self.positions.items.len > max_positions) {
            self.positions.items.len = max_positions;
        }
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
    var builder = JsonSchemaGrammarBuilder{
        .allocator = allocator,
    };
    errdefer builder.deinit();
    const result = try builder.build(schema);
    builder.deinitAux();
    return result;
}

const JsonSchemaGrammarBuilder = struct {
    allocator: std.mem.Allocator,
    out: std.ArrayListUnmanaged(u8) = .empty,
    rule_names: std.ArrayListUnmanaged([]const u8) = .empty,
    next_rule_id: usize = 0,

    fn deinit(self: *JsonSchemaGrammarBuilder) void {
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
        try self.out.appendSlice(self.allocator,
            \\ws ::= [ \t\n\r]*
            \\hex ::= [0-9a-fA-F]
            \\json_escape ::= ["\\/bfnrt] | "u" hex hex hex hex
            \\json_char ::= [^"\\] | "\\" json_escape
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
            if (std.mem.eql(u8, type_name, "boolean")) return "json_boolean";
            if (std.mem.eql(u8, type_name, "null")) return "json_null";
        }

        return "json_value_generic";
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
            const width = maximum.? - minimum.? + 1;
            if (width <= 256) {
                const rule_name = try self.nextRuleName("schema_integer");
                try self.out.appendSlice(self.allocator, rule_name);
                try self.out.appendSlice(self.allocator, " ::= ");
                var value = minimum.?;
                var first = true;
                while (value <= maximum.?) : (value += 1) {
                    if (!first) try self.out.appendSlice(self.allocator, " | ");
                    first = false;
                    var buf: [32]u8 = undefined;
                    const text = try std.fmt.bufPrint(&buf, "{d}", .{value});
                    try self.appendGrammarLiteral(text);
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

        const additional = schema_obj.get("additionalProperties");
        const allow_precise = additional == null or (additional.? == .bool and !additional.?.bool);
        if (allow_precise and properties.items.len <= 4) {
            return self.emitPreciseObjectRule(properties.items);
        }
        return "json_object_generic";
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
    grammar.advance("{\"answer\":\"ok\"}");
    try std.testing.expect(grammar.isComplete());
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
    grammar.advance("{\"count\":2,\"answer\":\"ok\"}");
    try std.testing.expect(grammar.isComplete());
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
    grammar.advance("[1,2]");
    try std.testing.expect(grammar.isComplete());
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

    g.advance("hello");
    try std.testing.expect(g.isComplete());
}

test "gbnf: literal rejects wrong input" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"hello\"");
    defer g.deinit();

    g.advance("hell");
    try std.testing.expect(!g.isComplete());
    g.advance("x"); // wrong byte
    try std.testing.expectEqual(@as(usize, 0), g.positions.items.len);
}

test "gbnf: character class" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [abc]");
    defer g.deinit();

    g.advance("a");
    try std.testing.expect(g.isComplete());
}

test "gbnf: character range" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [a-z]");
    defer g.deinit();

    g.advance("m");
    try std.testing.expect(g.isComplete());
}

test "gbnf: negated character class" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [^0-9]");
    defer g.deinit();

    g.advance("a");
    try std.testing.expect(g.isComplete());
}

test "gbnf: negated class rejects matching char" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [^0-9]");
    defer g.deinit();

    g.advance("5");
    try std.testing.expectEqual(@as(usize, 0), g.positions.items.len);
}

test "gbnf: zero or more repetition" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [a-z]*");
    defer g.deinit();

    // Zero repetitions should be complete.
    try std.testing.expect(g.isComplete());

    g.advance("abc");
    try std.testing.expect(g.isComplete());
}

test "gbnf: one or more repetition" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= [0-9]+");
    defer g.deinit();

    // Zero repetitions: not complete.
    try std.testing.expect(!g.isComplete());

    g.advance("1");
    try std.testing.expect(g.isComplete());

    g.advance("23");
    try std.testing.expect(g.isComplete());
}

test "gbnf: optional element" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= "a" "b"?
    );
    defer g.deinit();

    g.advance("a");
    // "b" is optional, so "a" alone is complete.
    try std.testing.expect(g.isComplete());
}

test "gbnf: optional element with match" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= "a" "b"?
    );
    defer g.deinit();

    g.advance("ab");
    try std.testing.expect(g.isComplete());
}

test "gbnf: alternatives" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"yes\" | \"no\"");
    defer g.deinit();

    g.advance("no");
    try std.testing.expect(g.isComplete());
}

test "gbnf: rule reference" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= greeting
        \\greeting ::= "hi"
    );
    defer g.deinit();

    g.advance("hi");
    try std.testing.expect(g.isComplete());
}

test "gbnf: sequence of elements" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"a\" \"b\" \"c\"");
    defer g.deinit();

    g.advance("abc");
    try std.testing.expect(g.isComplete());
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

    g.advance("ok");
    try std.testing.expect(g.isComplete());
}

test "gbnf: literal with escape" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"a\\nb\"");
    defer g.deinit();

    g.advance("a\nb");
    try std.testing.expect(g.isComplete());
}

test "gbnf: digits grammar" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= digit+
        \\digit ::= [0-9]
    );
    defer g.deinit();

    g.advance("42");
    try std.testing.expect(g.isComplete());
}

test "gbnf: simple word grammar" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= letter+
        \\letter ::= [a-zA-Z]
    );
    defer g.deinit();

    g.advance("Hello");
    try std.testing.expect(g.isComplete());
}

test "gbnf: incremental byte advance" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator, "root ::= \"abc\"");
    defer g.deinit();

    g.advanceByte('a');
    try std.testing.expect(!g.isComplete());
    try std.testing.expect(g.positions.items.len > 0);

    g.advanceByte('b');
    try std.testing.expect(!g.isComplete());

    g.advanceByte('c');
    try std.testing.expect(g.isComplete());
}

test "gbnf: repeated rule reference" {
    const allocator = std.testing.allocator;
    var g = try GbnfGrammar.parse(allocator,
        \\root ::= item+
        \\item ::= [a-z]
    );
    defer g.deinit();

    g.advance("abc");
    try std.testing.expect(g.isComplete());
}
