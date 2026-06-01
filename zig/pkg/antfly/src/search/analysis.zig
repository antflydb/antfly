// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Text analysis pipeline: character filters → tokenizer → token filters → terms.
//!
//! Provides the building blocks for full-text search:
//!   - Character filters: transform raw text before tokenization (HTML strip, ASCII fold)
//!   - Tokenizers: split text into tokens (unicode_words, whitespace, keyword, ngram, edge_ngram, character)
//!   - Token filters: transform tokens (lowercase, stop words, Porter2 stemmer, ngram, edge_ngram,
//!     shingle, length, truncate, unique, reverse, camel_case, elision, apostrophe)
//!   - Analyzer: composes char filters + tokenizer + filter chain
//!
//! Default analyzer: unicode_words → lowercase → English stop words → Porter2 stemmer

const std = @import("std");
const Allocator = std.mem.Allocator;
const stopwords_mod = @import("stopwords.zig");
const stemmers_mod = @import("stemmers.zig");
pub const Language = stopwords_mod.Language;

// ============================================================================
// Token
// ============================================================================

pub const Token = struct {
    term: []const u8,
    position: u32,
    start_byte: u32,
    end_byte: u32,
};

// ============================================================================
// Character Filters
// ============================================================================

pub const CharFilter = enum {
    html_strip,
    ascii_fold,
    zero_width_non_joiner,

    /// Apply character filter, returning a new allocation. Caller owns result.
    pub fn apply(self: CharFilter, alloc: Allocator, text: []const u8) ![]u8 {
        return switch (self) {
            .html_strip => applyHtmlStrip(alloc, text),
            .ascii_fold => applyAsciiFold(alloc, text),
            .zero_width_non_joiner => applyZwnj(alloc, text),
        };
    }
};

fn applyHtmlStrip(alloc: Allocator, text: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    var i: usize = 0;
    while (i < text.len) {
        // SIMD scan for '<' or '&' in 16-byte chunks
        const remaining = text.len - i;
        if (remaining >= 16) {
            const chunk: @Vector(16, u8) = text[i..][0..16].*;
            const lt: @Vector(16, u8) = @splat('<');
            const amp: @Vector(16, u8) = @splat('&');
            const lt_mask: u16 = @bitCast(chunk == lt);
            const amp_mask: u16 = @bitCast(chunk == amp);
            const mask_int = lt_mask | amp_mask;
            if (mask_int == 0) {
                // No special chars in this chunk, copy all 16 bytes
                try out.appendSlice(alloc, text[i..][0..16]);
                i += 16;
                continue;
            }
            // Copy up to first special char
            const first_special = @ctz(mask_int);
            if (first_special > 0) {
                try out.appendSlice(alloc, text[i..][0..first_special]);
                i += first_special;
            }
            // Fall through to scalar handling
        }

        if (i >= text.len) break;

        if (text[i] == '<') {
            // Skip tag content
            i += 1;
            while (i < text.len and text[i] != '>') : (i += 1) {}
            if (i < text.len) i += 1; // skip '>'
            // Replace tag with space to avoid merging words
            if (out.items.len > 0 and out.items[out.items.len - 1] != ' ') {
                try out.append(alloc, ' ');
            }
        } else if (text[i] == '&') {
            // Decode HTML entity
            const entity_start = i;
            i += 1;
            if (i < text.len and text[i] == '#') {
                // Numeric entity: &#NNN; or &#xHH;
                i += 1;
                if (i < text.len and (text[i] == 'x' or text[i] == 'X')) {
                    // Hex
                    i += 1;
                    var val: u32 = 0;
                    while (i < text.len and text[i] != ';') : (i += 1) {
                        const c = text[i];
                        if (c >= '0' and c <= '9') {
                            val = val * 16 + (c - '0');
                        } else if (c >= 'a' and c <= 'f') {
                            val = val * 16 + (c - 'a' + 10);
                        } else if (c >= 'A' and c <= 'F') {
                            val = val * 16 + (c - 'A' + 10);
                        } else break;
                    }
                    if (i < text.len and text[i] == ';') i += 1;
                    if (val < 128) {
                        try out.append(alloc, @intCast(val));
                    } else {
                        // Encode as UTF-8
                        try appendUtf8(alloc, &out, val);
                    }
                } else {
                    // Decimal
                    var val: u32 = 0;
                    while (i < text.len and text[i] != ';') : (i += 1) {
                        const c = text[i];
                        if (c >= '0' and c <= '9') {
                            val = val * 10 + (c - '0');
                        } else break;
                    }
                    if (i < text.len and text[i] == ';') i += 1;
                    if (val < 128) {
                        try out.append(alloc, @intCast(val));
                    } else {
                        try appendUtf8(alloc, &out, val);
                    }
                }
            } else {
                // Named entity
                const name_start = i;
                while (i < text.len and text[i] != ';' and i - name_start < 10) : (i += 1) {}
                if (i < text.len and text[i] == ';') {
                    const name = text[name_start..i];
                    i += 1;
                    if (std.mem.eql(u8, name, "amp")) {
                        try out.append(alloc, '&');
                    } else if (std.mem.eql(u8, name, "lt")) {
                        try out.append(alloc, '<');
                    } else if (std.mem.eql(u8, name, "gt")) {
                        try out.append(alloc, '>');
                    } else if (std.mem.eql(u8, name, "quot")) {
                        try out.append(alloc, '"');
                    } else if (std.mem.eql(u8, name, "apos")) {
                        try out.append(alloc, '\'');
                    } else if (std.mem.eql(u8, name, "nbsp")) {
                        try out.append(alloc, ' ');
                    } else {
                        // Unknown entity, pass through
                        try out.appendSlice(alloc, text[entity_start..i]);
                    }
                } else {
                    // No semicolon found, pass through
                    try out.appendSlice(alloc, text[entity_start..i]);
                }
            }
        } else {
            try out.append(alloc, text[i]);
            i += 1;
        }
    }

    return try alloc.dupe(u8, out.items);
}

fn appendUtf8(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), codepoint: u32) !void {
    if (codepoint < 0x80) {
        try out.append(alloc, @intCast(codepoint));
    } else if (codepoint < 0x800) {
        try out.append(alloc, @intCast(0xC0 | (codepoint >> 6)));
        try out.append(alloc, @intCast(0x80 | (codepoint & 0x3F)));
    } else if (codepoint < 0x10000) {
        try out.append(alloc, @intCast(0xE0 | (codepoint >> 12)));
        try out.append(alloc, @intCast(0x80 | ((codepoint >> 6) & 0x3F)));
        try out.append(alloc, @intCast(0x80 | (codepoint & 0x3F)));
    } else {
        try out.append(alloc, @intCast(0xF0 | (codepoint >> 18)));
        try out.append(alloc, @intCast(0x80 | ((codepoint >> 12) & 0x3F)));
        try out.append(alloc, @intCast(0x80 | ((codepoint >> 6) & 0x3F)));
        try out.append(alloc, @intCast(0x80 | (codepoint & 0x3F)));
    }
}

/// Comptime-generated ASCII folding table for Latin-1 Supplement (0xC0-0xFF second byte).
/// Maps 2-byte UTF-8 sequences (0xC3 0x80..0xBF, 0xC2 0x80..0xBF) to ASCII equivalents.
const ascii_fold_c3: [64]u8 = blk: {
    @setEvalBranchQuota(5000);
    var table: [64]u8 = undefined;
    // U+00C0..U+00FF mapped via second byte (0x80..0xBF) = index 0..63
    // Default: keep original (0 = no mapping)
    for (&table, 0..) |*entry, idx| {
        entry.* = switch (idx) {
            // U+00C0 À, U+00C1 Á, U+00C2 Â, U+00C3 Ã, U+00C4 Ä, U+00C5 Å
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05 => 'A',
            0x06 => 'A', // U+00C6 Æ → A (bleve maps to AE but we do single char)
            0x07 => 'C', // U+00C7 Ç
            // U+00C8 È, U+00C9 É, U+00CA Ê, U+00CB Ë
            0x08, 0x09, 0x0A, 0x0B => 'E',
            // U+00CC Ì, U+00CD Í, U+00CE Î, U+00CF Ï
            0x0C, 0x0D, 0x0E, 0x0F => 'I',
            0x10 => 'D', // U+00D0 Ð
            0x11 => 'N', // U+00D1 Ñ
            // U+00D2 Ò, U+00D3 Ó, U+00D4 Ô, U+00D5 Õ, U+00D6 Ö, U+00D8 Ø
            0x12, 0x13, 0x14, 0x15, 0x16, 0x18 => 'O',
            0x19 => 'U', // U+00D9 Ù
            0x1A => 'U', // U+00DA Ú
            0x1B => 'U', // U+00DB Û
            0x1C => 'U', // U+00DC Ü
            0x1D => 'Y', // U+00DD Ý
            // U+00E0 à, U+00E1 á, U+00E2 â, U+00E3 ã, U+00E4 ä, U+00E5 å
            0x20, 0x21, 0x22, 0x23, 0x24, 0x25 => 'a',
            0x26 => 'a', // U+00E6 æ
            0x27 => 'c', // U+00E7 ç
            // U+00E8 è, U+00E9 é, U+00EA ê, U+00EB ë
            0x28, 0x29, 0x2A, 0x2B => 'e',
            // U+00EC ì, U+00ED í, U+00EE î, U+00EF ï
            0x2C, 0x2D, 0x2E, 0x2F => 'i',
            0x30 => 'd', // U+00F0 ð
            0x31 => 'n', // U+00F1 ñ
            // U+00F2 ò, U+00F3 ó, U+00F4 ô, U+00F5 õ, U+00F6 ö, U+00F8 ø
            0x32, 0x33, 0x34, 0x35, 0x36, 0x38 => 'o',
            0x39 => 'u', // U+00F9 ù
            0x3A => 'u', // U+00FA ú
            0x3B => 'u', // U+00FB û
            0x3C => 'u', // U+00FC ü
            0x3D => 'y', // U+00FD ý
            0x3F => 'y', // U+00FF ÿ
            else => 0, // no mapping
        };
    }
    break :blk table;
};

fn applyAsciiFold(alloc: Allocator, text: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.ensureTotalCapacity(alloc, text.len);

    var i: usize = 0;
    while (i < text.len) {
        const b = text[i];
        if (b < 0x80) {
            try out.append(alloc, b);
            i += 1;
        } else if (b == 0xC3 and i + 1 < text.len) {
            // Latin-1 Supplement: U+00C0..U+00FF
            const second = text[i + 1];
            if (second >= 0x80 and second <= 0xBF) {
                const mapped = ascii_fold_c3[second - 0x80];
                if (mapped != 0) {
                    try out.append(alloc, mapped);
                    i += 2;
                    continue;
                }
            }
            // No mapping, pass through
            try out.append(alloc, b);
            i += 1;
        } else if ((b == 0xC4 or b == 0xC5) and i + 1 < text.len) {
            // Latin Extended-A subset (U+0100..U+017F)
            const second = text[i + 1];
            const mapped: u8 = if (b == 0xC4) switch (second) {
                0x84 => @as(u8, 'A'), // U+0104 Ą
                0x85 => 'a', // U+0105 ą
                0x86 => 'C', // U+0106 Ć
                0x87 => 'c', // U+0107 ć
                0x98 => 'E', // U+0118 Ę
                0x99 => 'e', // U+0119 ę
                else => 0,
            } else switch (second) {
                0x81 => @as(u8, 'L'), // U+0141 Ł
                0x82 => 'l', // U+0142 ł
                0x83 => 'N', // U+0143 Ń
                0x84 => 'n', // U+0144 ń
                0x9A => 'S', // U+015A Ś
                0x9B => 's', // U+015B ś
                0xB9 => 'Z', // U+0179 Ź
                0xBA => 'z', // U+017A ź
                0xBB => 'Z', // U+017B Ż
                0xBC => 'z', // U+017C ż
                else => 0,
            };
            if (mapped != 0) {
                try out.append(alloc, mapped);
                i += 2;
            } else {
                try out.append(alloc, b);
                i += 1;
            }
        } else {
            // Multi-byte UTF-8, pass through byte-by-byte
            try out.append(alloc, b);
            i += 1;
        }
    }

    return try alloc.dupe(u8, out.items);
}

fn applyZwnj(alloc: Allocator, text: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.ensureTotalCapacity(alloc, text.len);

    var i: usize = 0;
    while (i < text.len) {
        if (i + 2 < text.len and text[i] == 0xE2 and text[i + 1] == 0x80) {
            const third = text[i + 2];
            if (third == 0x8C or // U+200C ZWNJ
                third == 0x8B or // U+200B ZWS
                third == 0x8D) // U+200D ZWJ
            {
                i += 3;
                continue;
            }
        }
        if (i + 2 < text.len and text[i] == 0xEF and text[i + 1] == 0xBB and text[i + 2] == 0xBF) {
            // U+FEFF BOM
            i += 3;
            continue;
        }
        try out.append(alloc, text[i]);
        i += 1;
    }

    return try alloc.dupe(u8, out.items);
}

// ============================================================================
// Shared config types
// ============================================================================

pub const NgramConfig = struct {
    min: u8 = 2,
    max: u8 = 3,
};

pub const EdgeNgramConfig = struct {
    min: u8 = 1,
    max: u8 = 3,
    side: enum { front, back } = .front,
};

// ============================================================================
// Tokenizer
// ============================================================================

pub const Tokenizer = union(enum) {
    unicode_words,
    whitespace,
    keyword,
    character,
    ngram: NgramConfig,
    edge_ngram: EdgeNgramConfig,

    /// Tokenize text into tokens. Caller owns the returned slice and token terms.
    pub fn tokenize(self: Tokenizer, alloc: Allocator, text: []const u8) ![]Token {
        return switch (self) {
            .unicode_words => tokenizeUnicodeWords(alloc, text),
            .whitespace => tokenizeWhitespace(alloc, text),
            .keyword => tokenizeKeyword(alloc, text),
            .character => tokenizeCharacter(alloc, text),
            .ngram => |cfg| tokenizeNgram(alloc, text, cfg),
            .edge_ngram => |cfg| tokenizeEdgeNgram(alloc, text, cfg),
        };
    }
};

fn tokenizeUnicodeWords(alloc: Allocator, text: []const u8) ![]Token {
    var tokens = std.ArrayListUnmanaged(Token).empty;
    defer tokens.deinit(alloc);

    var pos: u32 = 0;
    var position: u32 = 0;
    const len: u32 = @intCast(text.len);

    while (pos < len) {
        // Skip non-alphanumeric
        while (pos < len and !isAlphanumeric(text[pos])) {
            pos += utf8ByteLen(text[pos]);
        }
        if (pos >= len) break;

        const start = pos;
        // Consume alphanumeric run
        while (pos < len and isAlphanumeric(text[pos])) {
            pos += utf8ByteLen(text[pos]);
        }

        const term = try alloc.dupe(u8, text[start..pos]);
        try tokens.append(alloc, .{
            .term = term,
            .position = position,
            .start_byte = start,
            .end_byte = pos,
        });
        position += 1;
    }

    return try tokens.toOwnedSlice(alloc);
}

fn tokenizeWhitespace(alloc: Allocator, text: []const u8) ![]Token {
    var tokens = std.ArrayListUnmanaged(Token).empty;
    defer tokens.deinit(alloc);

    var pos: u32 = 0;
    var position: u32 = 0;
    const len: u32 = @intCast(text.len);

    while (pos < len) {
        while (pos < len and isWhitespace(text[pos])) : (pos += 1) {}
        if (pos >= len) break;

        const start = pos;
        while (pos < len and !isWhitespace(text[pos])) : (pos += 1) {}

        const term = try alloc.dupe(u8, text[start..pos]);
        try tokens.append(alloc, .{
            .term = term,
            .position = position,
            .start_byte = start,
            .end_byte = pos,
        });
        position += 1;
    }

    return try tokens.toOwnedSlice(alloc);
}

fn tokenizeKeyword(alloc: Allocator, text: []const u8) ![]Token {
    const tokens = try alloc.alloc(Token, 1);
    tokens[0] = .{
        .term = try alloc.dupe(u8, text),
        .position = 0,
        .start_byte = 0,
        .end_byte = @intCast(text.len),
    };
    return tokens;
}

fn tokenizeCharacter(alloc: Allocator, text: []const u8) ![]Token {
    var tokens = std.ArrayListUnmanaged(Token).empty;
    defer tokens.deinit(alloc);

    var pos: u32 = 0;
    var position: u32 = 0;
    const len: u32 = @intCast(text.len);

    while (pos < len) {
        const byte_len = utf8ByteLen(text[pos]);
        const end = @min(pos + byte_len, len);
        const term = try alloc.dupe(u8, text[pos..end]);
        try tokens.append(alloc, .{
            .term = term,
            .position = position,
            .start_byte = pos,
            .end_byte = end,
        });
        position += 1;
        pos = end;
    }

    return try tokens.toOwnedSlice(alloc);
}

fn tokenizeNgram(alloc: Allocator, text: []const u8, cfg: NgramConfig) ![]Token {
    var tokens = std.ArrayListUnmanaged(Token).empty;
    defer tokens.deinit(alloc);

    var position: u32 = 0;
    const len = text.len;

    var n: u8 = cfg.min;
    while (n <= cfg.max) : (n += 1) {
        var start: usize = 0;
        while (start + n <= len) : (start += 1) {
            const end = start + n;
            const term = try alloc.dupe(u8, text[start..end]);
            try tokens.append(alloc, .{
                .term = term,
                .position = position,
                .start_byte = @intCast(start),
                .end_byte = @intCast(end),
            });
            position += 1;
        }
    }

    return try tokens.toOwnedSlice(alloc);
}

fn tokenizeEdgeNgram(alloc: Allocator, text: []const u8, cfg: EdgeNgramConfig) ![]Token {
    // First tokenize into words, then generate edge n-grams per word
    const words = try tokenizeUnicodeWords(alloc, text);
    defer {
        for (words) |w| alloc.free(@constCast(w.term));
        alloc.free(words);
    }

    var tokens = std.ArrayListUnmanaged(Token).empty;
    defer tokens.deinit(alloc);

    var position: u32 = 0;
    for (words) |word| {
        const word_len = word.term.len;
        var n: u8 = cfg.min;
        while (n <= cfg.max and n <= word_len) : (n += 1) {
            const term = if (cfg.side == .front)
                try alloc.dupe(u8, word.term[0..n])
            else
                try alloc.dupe(u8, word.term[word_len - n ..]);
            try tokens.append(alloc, .{
                .term = term,
                .position = position,
                .start_byte = word.start_byte,
                .end_byte = word.end_byte,
            });
            position += 1;
        }
    }

    return try tokens.toOwnedSlice(alloc);
}

fn isAlphanumeric(c: u8) bool {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or (c >= '0' and c <= '9') or c >= 0x80;
}

fn isWhitespace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r';
}

fn utf8ByteLen(first: u8) u32 {
    if (first < 0x80) return 1;
    if (first < 0xE0) return 2;
    if (first < 0xF0) return 3;
    return 4;
}

// ============================================================================
// Token filters
// ============================================================================

pub const TokenFilter = union(enum) {
    lowercase,
    stop_words,
    stemmer,
    ngram: NgramConfig,
    edge_ngram: EdgeNgramConfig,
    shingle: ShingleConfig,
    length: LengthConfig,
    truncate: TruncateConfig,
    unique,
    reverse,
    camel_case,
    elision,
    apostrophe,
    stop_words_lang: Language,
    stemmer_lang: Language,

    pub const ShingleConfig = struct { min: u8 = 2, max: u8 = 2 };
    pub const LengthConfig = struct { min: u8 = 0, max: u8 = 255 };
    pub const TruncateConfig = struct { max_len: u8 = 255 };

    /// Apply this filter to tokens, potentially removing or modifying them.
    /// Caller owns all memory. Returned slice may differ from input.
    pub fn apply(self: TokenFilter, alloc: Allocator, tokens: []Token) ![]Token {
        return switch (self) {
            .lowercase => applyLowercase(alloc, tokens),
            .stop_words => applyStopWords(alloc, tokens),
            .stemmer => applyStemmer(alloc, tokens),
            .ngram => |cfg| applyNgramFilter(alloc, tokens, cfg),
            .edge_ngram => |cfg| applyEdgeNgramFilter(alloc, tokens, cfg),
            .shingle => |cfg| applyShingle(alloc, tokens, cfg),
            .length => |cfg| applyLength(alloc, tokens, cfg),
            .truncate => |cfg| applyTruncate(alloc, tokens, cfg),
            .unique => applyUnique(alloc, tokens),
            .reverse => applyReverse(alloc, tokens),
            .camel_case => applyCamelCase(alloc, tokens),
            .elision => applyElision(alloc, tokens),
            .apostrophe => applyApostrophe(alloc, tokens),
            .stop_words_lang => |lang| applyStopWordsLang(alloc, tokens, lang),
            .stemmer_lang => |lang| applyStemmerLang(alloc, tokens, lang),
        };
    }
};

fn applyLowercase(alloc: Allocator, tokens: []Token) ![]Token {
    for (tokens) |*tok| {
        if (!containsAsciiUpper(tok.term)) continue;
        const lowered = try alloc.alloc(u8, tok.term.len);
        const len = tok.term.len;
        var i: usize = 0;

        // SIMD: process 16 bytes at a time
        const simd_len = len - (len % 16);
        while (i < simd_len) : (i += 16) {
            const V = @Vector(16, u8);
            const v: V = tok.term[i..][0..16].*;
            const a_vec: V = @splat('A');
            const z_vec: V = @splat('Z');
            const diff: V = @splat(32);
            const ge_a: u16 = @bitCast(v >= a_vec);
            const le_z: u16 = @bitCast(v <= z_vec);
            const mask: @Vector(16, bool) = @bitCast(ge_a & le_z);
            const result = @select(u8, mask, v +| diff, v);
            lowered[i..][0..16].* = result;
        }

        // Scalar remainder
        while (i < len) : (i += 1) {
            lowered[i] = if (tok.term[i] >= 'A' and tok.term[i] <= 'Z') tok.term[i] + 32 else tok.term[i];
        }

        alloc.free(@constCast(tok.term));
        tok.term = lowered;
    }
    return tokens;
}

fn containsAsciiUpper(s: []const u8) bool {
    var i: usize = 0;
    const simd_len = s.len - (s.len % 16);
    while (i < simd_len) : (i += 16) {
        const V = @Vector(16, u8);
        const v: V = s[i..][0..16].*;
        const a_vec: V = @splat('A');
        const z_vec: V = @splat('Z');
        const ge_a: u16 = @bitCast(v >= a_vec);
        const le_z: u16 = @bitCast(v <= z_vec);
        if ((ge_a & le_z) != 0) return true;
    }

    while (i < s.len) : (i += 1) {
        if (s[i] >= 'A' and s[i] <= 'Z') return true;
    }
    return false;
}

fn applyStopWords(alloc: Allocator, tokens: []Token) ![]Token {
    return applyStopWordsLang(alloc, tokens, .english);
}

fn applyStemmer(alloc: Allocator, tokens: []Token) ![]Token {
    return applyStemmerLang(alloc, tokens, .english);
}

fn applyNgramFilter(alloc: Allocator, tokens: []Token, cfg: NgramConfig) ![]Token {
    var result = std.ArrayListUnmanaged(Token).empty;
    defer result.deinit(alloc);

    for (tokens) |tok| {
        const word = tok.term;
        var n: u8 = cfg.min;
        while (n <= cfg.max) : (n += 1) {
            var start: usize = 0;
            while (start + n <= word.len) : (start += 1) {
                const term = try alloc.dupe(u8, word[start..][0..n]);
                try result.append(alloc, .{
                    .term = term,
                    .position = tok.position,
                    .start_byte = tok.start_byte + @as(u32, @intCast(start)),
                    .end_byte = tok.start_byte + @as(u32, @intCast(start + n)),
                });
            }
        }
        alloc.free(@constCast(tok.term));
    }

    alloc.free(tokens);
    return try result.toOwnedSlice(alloc);
}

fn applyEdgeNgramFilter(alloc: Allocator, tokens: []Token, cfg: EdgeNgramConfig) ![]Token {
    var result = std.ArrayListUnmanaged(Token).empty;
    defer result.deinit(alloc);

    for (tokens) |tok| {
        const word = tok.term;
        var n: u8 = cfg.min;
        while (n <= cfg.max and n <= word.len) : (n += 1) {
            const term = if (cfg.side == .front)
                try alloc.dupe(u8, word[0..n])
            else
                try alloc.dupe(u8, word[word.len - n ..]);
            try result.append(alloc, .{
                .term = term,
                .position = tok.position,
                .start_byte = tok.start_byte,
                .end_byte = tok.end_byte,
            });
        }
        alloc.free(@constCast(tok.term));
    }

    alloc.free(tokens);
    return try result.toOwnedSlice(alloc);
}

fn applyShingle(alloc: Allocator, tokens: []Token, cfg: TokenFilter.ShingleConfig) ![]Token {
    var result = std.ArrayListUnmanaged(Token).empty;
    defer result.deinit(alloc);

    const count = tokens.len;
    var n: u8 = cfg.min;
    while (n <= cfg.max) : (n += 1) {
        if (n > count) continue;
        var i: usize = 0;
        while (i + n <= count) : (i += 1) {
            // Build shingle: join tokens[i..i+n] with space
            var total_len: usize = 0;
            for (0..n) |j| {
                if (j > 0) total_len += 1; // space
                total_len += tokens[i + j].term.len;
            }
            const term = try alloc.alloc(u8, total_len);
            var pos: usize = 0;
            for (0..n) |j| {
                if (j > 0) {
                    term[pos] = ' ';
                    pos += 1;
                }
                @memcpy(term[pos..][0..tokens[i + j].term.len], tokens[i + j].term);
                pos += tokens[i + j].term.len;
            }
            try result.append(alloc, .{
                .term = term,
                .position = tokens[i].position,
                .start_byte = tokens[i].start_byte,
                .end_byte = tokens[i + n - 1].end_byte,
            });
        }
    }

    // Free original tokens
    for (tokens) |tok| alloc.free(@constCast(tok.term));
    alloc.free(tokens);

    return try result.toOwnedSlice(alloc);
}

fn applyLength(alloc: Allocator, tokens: []Token, cfg: TokenFilter.LengthConfig) ![]Token {
    var result = std.ArrayListUnmanaged(Token).empty;
    defer result.deinit(alloc);

    var position: u32 = 0;
    for (tokens) |tok| {
        const l = tok.term.len;
        if (l >= cfg.min and l <= cfg.max) {
            var t = tok;
            t.position = position;
            try result.append(alloc, t);
            position += 1;
        } else {
            alloc.free(@constCast(tok.term));
        }
    }

    alloc.free(tokens);
    return try result.toOwnedSlice(alloc);
}

fn applyTruncate(alloc: Allocator, tokens: []Token, cfg: TokenFilter.TruncateConfig) ![]Token {
    for (tokens) |*tok| {
        if (tok.term.len > cfg.max_len) {
            const truncated = try alloc.dupe(u8, tok.term[0..cfg.max_len]);
            alloc.free(@constCast(tok.term));
            tok.term = truncated;
        }
    }
    return tokens;
}

fn applyUnique(alloc: Allocator, tokens: []Token) ![]Token {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);

    var result = std.ArrayListUnmanaged(Token).empty;
    defer result.deinit(alloc);

    var position: u32 = 0;
    for (tokens) |tok| {
        const gop = try seen.getOrPut(alloc, tok.term);
        if (gop.found_existing) {
            alloc.free(@constCast(tok.term));
        } else {
            var t = tok;
            t.position = position;
            try result.append(alloc, t);
            position += 1;
        }
    }

    alloc.free(tokens);
    return try result.toOwnedSlice(alloc);
}

fn applyReverse(alloc: Allocator, tokens: []Token) ![]Token {
    for (tokens) |*tok| {
        const reversed = try alloc.alloc(u8, tok.term.len);
        for (tok.term, 0..) |c, idx| {
            reversed[tok.term.len - 1 - idx] = c;
        }
        alloc.free(@constCast(tok.term));
        tok.term = reversed;
    }
    return tokens;
}

fn applyCamelCase(alloc: Allocator, tokens: []Token) ![]Token {
    var result = std.ArrayListUnmanaged(Token).empty;
    defer result.deinit(alloc);

    for (tokens) |tok| {
        const word = tok.term;
        if (word.len == 0) {
            alloc.free(@constCast(tok.term));
            continue;
        }

        var start: usize = 0;
        var i: usize = 1;
        while (i < word.len) : (i += 1) {
            if (word[i] >= 'A' and word[i] <= 'Z' and i > start) {
                // Split here
                const part = try toLowerDupe(alloc, word[start..i]);
                try result.append(alloc, .{
                    .term = part,
                    .position = tok.position,
                    .start_byte = tok.start_byte + @as(u32, @intCast(start)),
                    .end_byte = tok.start_byte + @as(u32, @intCast(i)),
                });
                start = i;
            }
        }
        // Last part
        if (start < word.len) {
            const part = try toLowerDupe(alloc, word[start..]);
            try result.append(alloc, .{
                .term = part,
                .position = tok.position,
                .start_byte = tok.start_byte + @as(u32, @intCast(start)),
                .end_byte = tok.end_byte,
            });
        }
        alloc.free(@constCast(tok.term));
    }

    alloc.free(tokens);
    return try result.toOwnedSlice(alloc);
}

fn toLowerDupe(alloc: Allocator, s: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, s.len);
    for (s, 0..) |c, i| {
        out[i] = if (c >= 'A' and c <= 'Z') c + 32 else c;
    }
    return out;
}

const elision_prefixes = [_][]const u8{ "l'", "L'", "d'", "D'", "qu'", "Qu'", "j'", "J'", "c'", "C'", "n'", "N'", "m'", "M'", "s'", "S'", "t'", "T'" };

fn applyElision(alloc: Allocator, tokens: []Token) ![]Token {
    for (tokens) |*tok| {
        for (&elision_prefixes) |prefix| {
            if (tok.term.len > prefix.len and std.mem.startsWith(u8, tok.term, prefix)) {
                const new_term = try alloc.dupe(u8, tok.term[prefix.len..]);
                alloc.free(@constCast(tok.term));
                tok.term = new_term;
                break;
            }
        }
    }
    return tokens;
}

fn applyApostrophe(alloc: Allocator, tokens: []Token) ![]Token {
    for (tokens) |*tok| {
        var i: usize = 0;
        while (i < tok.term.len) : (i += 1) {
            const c = tok.term[i];
            const is_apostrophe = c == '\'' or
                // U+2019 RIGHT SINGLE QUOTATION MARK = E2 80 99
                (c == 0xE2 and i + 2 < tok.term.len and tok.term[i + 1] == 0x80 and tok.term[i + 2] == 0x99);
            if (is_apostrophe) {
                if (i > 0) {
                    const new_term = try alloc.dupe(u8, tok.term[0..i]);
                    alloc.free(@constCast(tok.term));
                    tok.term = new_term;
                }
                break;
            }
        }
    }
    return tokens;
}

fn applyStopWordsLang(alloc: Allocator, tokens: []Token, lang: Language) ![]Token {
    const stops = stopwords_mod.getStopWords(lang);
    var has_stop = false;
    for (tokens) |tok| {
        if (stops.has(tok.term)) {
            has_stop = true;
            break;
        }
    }
    if (!has_stop) return tokens;

    var result = std.ArrayListUnmanaged(Token).empty;
    defer result.deinit(alloc);

    var position: u32 = 0;
    for (tokens) |tok| {
        if (stops.has(tok.term)) {
            alloc.free(@constCast(tok.term));
        } else {
            var t = tok;
            t.position = position;
            try result.append(alloc, t);
            position += 1;
        }
    }

    alloc.free(tokens);
    return try result.toOwnedSlice(alloc);
}

fn applyStemmerLang(alloc: Allocator, tokens: []Token, lang: Language) ![]Token {
    for (tokens) |*tok| {
        const stemmed = try stemmers_mod.stem(alloc, tok.term, lang);
        if (stemmed.ptr != tok.term.ptr) {
            alloc.free(@constCast(tok.term));
            tok.term = stemmed;
        }
    }
    return tokens;
}

// ============================================================================
// Porter2 stemmer (English)
// ============================================================================

/// Stem a word using the Porter2 algorithm. Returns a new allocation if
/// the word was modified, otherwise returns the original slice.
pub fn porter2Stem(alloc: Allocator, word: []const u8) ![]const u8 {
    if (word.len <= 2) return word;
    if (!porter2MayChange(word)) return word;

    // Work on a mutable copy. Most indexed terms are short and many do not
    // stem; keep that common path off the allocator and only allocate the final
    // returned term when the stem actually changes.
    var stack_buf: [256]u8 = undefined;
    var heap_buf: ?[]u8 = null;
    var buf: []u8 = if (word.len <= stack_buf.len) stack_buf[0..word.len] else blk: {
        const owned = try alloc.alloc(u8, word.len);
        heap_buf = owned;
        break :blk owned;
    };
    defer if (heap_buf) |owned| alloc.free(owned);
    @memcpy(buf, word);
    var len = buf.len;

    // Step 0: Remove 's, 's, '
    len = step0(buf, len);

    // Step 1a: sses→ss, ied/ies→i/ie, ss→ss, us→us, s→(remove if preceded by vowel)
    len = step1a(buf, len);

    // Step 1b: eed/eedly→ee (if in R1), ed/edly/ing/ingly→(remove if contains vowel)
    len = step1b(buf, len);

    // Step 1c: y/Y → i (if preceded by non-vowel and len > 2)
    len = step1c(buf, len);

    // Step 2: suffix replacements in R1
    len = step2(buf, len);

    // Step 3: suffix replacements in R1
    len = step3(buf, len);

    // Step 4: suffix deletions in R2
    len = step4(buf, len);

    // Step 5: final e/l cleanup
    len = step5(buf, len);

    if (len == word.len and std.mem.eql(u8, buf[0..len], word)) {
        return word;
    }

    return try alloc.dupe(u8, buf[0..len]);
}

fn porter2MayChange(word: []const u8) bool {
    const last = word[word.len - 1];
    // Conservative final-byte filter for the Porter2 suffix table. If the word
    // does not end in one of these bytes, no step can remove or replace a
    // suffix. This keeps generated identifier-like terms off the stemmer's
    // region scans without changing stemming semantics.
    return switch (last) {
        's', 'd', 'g', 'y', 'Y', 'l', 'i', 'n', 'r', 'm', 'e', 't', 'c' => true,
        else => false,
    };
}

pub fn isVowel(c: u8) bool {
    return c == 'a' or c == 'e' or c == 'i' or c == 'o' or c == 'u';
}

pub fn containsVowel(word: []const u8) bool {
    for (word) |c| {
        if (isVowel(c)) return true;
    }
    return false;
}

/// R1 is the region after the first non-vowel after a vowel.
pub fn r1(word: []const u8) usize {
    var i: usize = 0;
    // Find first vowel
    while (i < word.len and !isVowel(word[i])) : (i += 1) {}
    // Find non-vowel after vowel
    while (i < word.len and isVowel(word[i])) : (i += 1) {}
    if (i < word.len) return i + 1;
    return word.len;
}

/// R2 is the region after the first non-vowel after a vowel in R1.
pub fn r2(word: []const u8) usize {
    const r1_start = r1(word);
    if (r1_start >= word.len) return word.len;
    const r2_in_r1 = r1(word[r1_start..]);
    return r1_start + r2_in_r1;
}

pub fn endsWith(word: []const u8, len: usize, suffix: []const u8) bool {
    if (len < suffix.len) return false;
    return std.mem.eql(u8, word[len - suffix.len .. len], suffix);
}

fn step0(buf: []u8, len: usize) usize {
    if (endsWith(buf, len, "'s'")) return len - 3;
    if (endsWith(buf, len, "'s")) return len - 2;
    if (endsWith(buf, len, "'")) return len - 1;
    return len;
}

fn step1a(buf: []u8, len: usize) usize {
    if (endsWith(buf, len, "sses")) return len - 2;
    if (endsWith(buf, len, "ied") or endsWith(buf, len, "ies")) {
        return if (len > 4) len - 2 else len - 1;
    }
    if (endsWith(buf, len, "ss") or endsWith(buf, len, "us")) return len;
    if (len > 1 and buf[len - 1] == 's') {
        // Remove s if preceded by a vowel (not immediately before)
        if (containsVowel(buf[0 .. len - 2])) return len - 1;
    }
    return len;
}

fn step1b(buf: []u8, len: usize) usize {
    const r1_pos = r1(buf[0..len]);
    if (endsWith(buf, len, "eedly")) {
        if (len - 5 >= r1_pos) return len - 3; // → ee
        return len;
    }
    if (endsWith(buf, len, "eed")) {
        if (len - 3 >= r1_pos) return len - 1; // → ee
        return len;
    }

    var new_len = len;
    var modified = false;
    if (endsWith(buf, len, "ingly")) {
        new_len = len - 5;
        modified = containsVowel(buf[0..new_len]);
    } else if (endsWith(buf, len, "edly")) {
        new_len = len - 4;
        modified = containsVowel(buf[0..new_len]);
    } else if (endsWith(buf, len, "ing")) {
        new_len = len - 3;
        modified = containsVowel(buf[0..new_len]);
    } else if (endsWith(buf, len, "ed")) {
        new_len = len - 2;
        modified = containsVowel(buf[0..new_len]);
    }

    if (!modified) return len;

    // Post-step1b: at/bl/iz → +e, double letter → remove, short → +e
    if (endsWith(buf, new_len, "at") or endsWith(buf, new_len, "bl") or endsWith(buf, new_len, "iz")) {
        buf[new_len] = 'e';
        return new_len + 1;
    }
    if (new_len >= 2 and buf[new_len - 1] == buf[new_len - 2]) {
        const c = buf[new_len - 1];
        if (c != 'l' and c != 's' and c != 'z') return new_len - 1;
    }
    if (isShortWord(buf[0..new_len])) {
        buf[new_len] = 'e';
        return new_len + 1;
    }
    return new_len;
}

fn step1c(buf: []u8, len: usize) usize {
    if (len <= 2) return len;
    if ((buf[len - 1] == 'y' or buf[len - 1] == 'Y') and !isVowel(buf[len - 2])) {
        buf[len - 1] = 'i';
    }
    return len;
}

fn isShortSyllable(word: []const u8, i: usize) bool {
    if (i == 0) {
        return word.len >= 2 and isVowel(word[0]) and !isVowel(word[1]);
    }
    if (i >= word.len) return false;
    return i >= 1 and i + 1 < word.len and
        !isVowel(word[i - 1]) and isVowel(word[i]) and !isVowel(word[i + 1]) and
        word[i + 1] != 'w' and word[i + 1] != 'x' and word[i + 1] != 'Y';
}

fn isShortWord(word: []const u8) bool {
    const r1_pos = r1(word);
    if (r1_pos < word.len) return false;
    if (word.len >= 3) return isShortSyllable(word, word.len - 3);
    if (word.len == 2) return isShortSyllable(word, 0);
    return false;
}

pub const SuffixRule = struct { suffix: []const u8, replacement: []const u8 };
const step2_table = [_]SuffixRule{
    .{ .suffix = "ational", .replacement = "ate" },
    .{ .suffix = "tional", .replacement = "tion" },
    .{ .suffix = "enci", .replacement = "ence" },
    .{ .suffix = "anci", .replacement = "ance" },
    .{ .suffix = "abli", .replacement = "able" },
    .{ .suffix = "entli", .replacement = "ent" },
    .{ .suffix = "izer", .replacement = "ize" },
    .{ .suffix = "ization", .replacement = "ize" },
    .{ .suffix = "ation", .replacement = "ate" },
    .{ .suffix = "ator", .replacement = "ate" },
    .{ .suffix = "alism", .replacement = "al" },
    .{ .suffix = "aliti", .replacement = "al" },
    .{ .suffix = "alli", .replacement = "al" },
    .{ .suffix = "fulness", .replacement = "ful" },
    .{ .suffix = "ousli", .replacement = "ous" },
    .{ .suffix = "ousness", .replacement = "ous" },
    .{ .suffix = "iveness", .replacement = "ive" },
    .{ .suffix = "iviti", .replacement = "ive" },
    .{ .suffix = "biliti", .replacement = "ble" },
    .{ .suffix = "bli", .replacement = "ble" },
    .{ .suffix = "fulli", .replacement = "ful" },
    .{ .suffix = "lessli", .replacement = "less" },
    .{ .suffix = "logi", .replacement = "log" },
};

fn step2(buf: []u8, len: usize) usize {
    const r1_pos = r1(buf[0..len]);
    // Special case: li preceded by valid li-ending
    if (endsWith(buf, len, "li") and len >= 3) {
        const c = buf[len - 3];
        if (len - 2 >= r1_pos and (c == 'c' or c == 'd' or c == 'e' or c == 'g' or c == 'h' or
            c == 'k' or c == 'm' or c == 'n' or c == 'r' or c == 't'))
        {
            return len - 2;
        }
    }
    for (&step2_table) |pair| {
        if (endsWith(buf, len, pair.suffix)) {
            if (len - pair.suffix.len >= r1_pos) {
                const new_len = len - pair.suffix.len;
                @memcpy(buf[new_len..][0..pair.replacement.len], pair.replacement);
                return new_len + pair.replacement.len;
            }
            return len;
        }
    }
    return len;
}

const step3_table = [_]SuffixRule{
    .{ .suffix = "ational", .replacement = "ate" },
    .{ .suffix = "tional", .replacement = "tion" },
    .{ .suffix = "alize", .replacement = "al" },
    .{ .suffix = "icate", .replacement = "ic" },
    .{ .suffix = "iciti", .replacement = "ic" },
    .{ .suffix = "ful", .replacement = "" },
    .{ .suffix = "ness", .replacement = "" },
};

fn step3(buf: []u8, len: usize) usize {
    const r1_pos = r1(buf[0..len]);
    // Special: ative → (delete if in R2)
    if (endsWith(buf, len, "ative")) {
        const r2_pos = r2(buf[0..len]);
        if (len - 5 >= r2_pos) return len - 5;
        return len;
    }
    for (&step3_table) |pair| {
        if (endsWith(buf, len, pair.suffix)) {
            if (len - pair.suffix.len >= r1_pos) {
                const new_len = len - pair.suffix.len;
                @memcpy(buf[new_len..][0..pair.replacement.len], pair.replacement);
                return new_len + pair.replacement.len;
            }
            return len;
        }
    }
    return len;
}

const step4_suffixes = [_][]const u8{
    "ement", "ment", "ence", "ance", "able", "ible", "ant", "ent",
    "ism",   "ate",  "iti",  "ous",  "ive",  "ize",  "ion", "al",
    "er",    "ic",
};

fn step4(buf: []u8, len: usize) usize {
    const r2_pos = r2(buf[0..len]);
    for (&step4_suffixes) |suffix| {
        if (endsWith(buf, len, suffix)) {
            const new_len = len - suffix.len;
            if (new_len >= r2_pos) {
                // Special: ion → must be preceded by s or t
                if (std.mem.eql(u8, suffix, "ion")) {
                    if (new_len > 0 and (buf[new_len - 1] == 's' or buf[new_len - 1] == 't')) {
                        return new_len;
                    }
                    return len;
                }
                return new_len;
            }
            return len;
        }
    }
    return len;
}

fn step5(buf: []u8, len: usize) usize {
    if (len == 0) return len;
    if (buf[len - 1] == 'e') {
        const r2_pos = r2(buf[0..len]);
        if (len - 1 >= r2_pos) return len - 1;
        const r1_pos = r1(buf[0..len]);
        if (len - 1 >= r1_pos and (len < 3 or !isShortSyllable(buf[0..len], len - 3))) return len - 1;
    }
    if (buf[len - 1] == 'l' and len >= 2 and buf[len - 2] == 'l') {
        const r2_pos = r2(buf[0..len]);
        if (len - 1 >= r2_pos) return len - 1;
    }
    return len;
}

// ============================================================================
// Analyzer
// ============================================================================

pub const Analyzer = struct {
    char_filters: []const CharFilter = &.{},
    tokenizer: Tokenizer,
    filters: []const TokenFilter,

    /// Analyze text into tokens. Caller owns the returned slice and all token terms.
    pub fn analyze(self: *const Analyzer, alloc: Allocator, text: []const u8) ![]Token {
        if (self.isDefaultEnglishNoCharFilters()) return analyzeDefaultEnglish(alloc, text);

        // Apply character filters
        var processed: []const u8 = text;
        var owned = false;
        for (self.char_filters) |cf| {
            const result = try cf.apply(alloc, processed);
            if (owned) alloc.free(@constCast(processed));
            processed = result;
            owned = true;
        }
        defer if (owned) alloc.free(@constCast(processed));

        var tokens = try self.tokenizer.tokenize(alloc, processed);
        for (self.filters) |filter| {
            tokens = try filter.apply(alloc, tokens);
        }
        return tokens;
    }

    fn isDefaultEnglishNoCharFilters(self: *const Analyzer) bool {
        if (self.char_filters.len != 0) return false;
        if (self.tokenizer != .unicode_words) return false;
        if (self.filters.len != 3) return false;
        return self.filters[0] == .lowercase and
            self.filters[1] == .stop_words and
            self.filters[2] == .stemmer;
    }

    /// Free tokens returned by analyze().
    pub fn freeTokens(alloc: Allocator, tokens: []Token) void {
        for (tokens) |tok| {
            alloc.free(@constCast(tok.term));
        }
        alloc.free(tokens);
    }
};

fn analyzeDefaultEnglish(alloc: Allocator, text: []const u8) ![]Token {
    const stops = stopwords_mod.getStopWords(.english);
    var tokens = std.ArrayListUnmanaged(Token).empty;
    defer tokens.deinit(alloc);

    var pos: u32 = 0;
    var position: u32 = 0;
    const len: u32 = @intCast(text.len);

    while (pos < len) {
        while (pos < len and !isAlphanumeric(text[pos])) {
            pos += utf8ByteLen(text[pos]);
        }
        if (pos >= len) break;

        const start = pos;
        while (pos < len and isAlphanumeric(text[pos])) {
            pos += utf8ByteLen(text[pos]);
        }

        const raw = text[start..pos];
        var stack_lower: [256]u8 = undefined;
        var heap_lower: ?[]u8 = null;
        const normalized = try lowercaseScratch(alloc, raw, &stack_lower, &heap_lower);
        defer if (heap_lower) |owned| alloc.free(owned);

        if (stops.has(normalized)) continue;

        var owned_term = try alloc.dupe(u8, normalized);
        errdefer alloc.free(owned_term);
        const stemmed = try porter2Stem(alloc, owned_term);
        if (stemmed.ptr != owned_term.ptr) {
            alloc.free(owned_term);
            owned_term = @constCast(stemmed);
        }

        try tokens.append(alloc, .{
            .term = owned_term,
            .position = position,
            .start_byte = start,
            .end_byte = pos,
        });
        position += 1;
    }

    return try tokens.toOwnedSlice(alloc);
}

fn lowercaseScratch(
    alloc: Allocator,
    text: []const u8,
    stack: *[256]u8,
    heap: *?[]u8,
) ![]const u8 {
    if (!containsAsciiUpper(text)) return text;
    const out: []u8 = if (text.len <= stack.len) stack[0..text.len] else blk: {
        const owned = try alloc.alloc(u8, text.len);
        heap.* = owned;
        break :blk owned;
    };
    for (text, 0..) |c, i| {
        out[i] = if (c >= 'A' and c <= 'Z') c + 32 else c;
    }
    return out;
}

/// Default English analyzer: unicode_words → lowercase → stop_words → stemmer
pub const default_analyzer = Analyzer{
    .tokenizer = .unicode_words,
    .filters = &.{ .lowercase, .stop_words, .stemmer },
};

/// Simple analyzer: unicode_words → lowercase (no stemming or stop words)
pub const simple_analyzer = Analyzer{
    .tokenizer = .unicode_words,
    .filters = &.{.lowercase},
};

/// Keyword analyzer: entire input as single token, no filters
pub const keyword_analyzer = Analyzer{
    .tokenizer = .keyword,
    .filters = &.{},
};

/// HTML analyzer: strip HTML tags → unicode_words → lowercase → stop_words → stemmer
pub const html_analyzer = Analyzer{
    .char_filters = &.{.html_strip},
    .tokenizer = .unicode_words,
    .filters = &.{ .lowercase, .stop_words, .stemmer },
};

/// Search-as-you-type 2-gram shingle subfield: unicode_words → lowercase → shingle(2)
pub const search_as_you_type_2gram_analyzer = Analyzer{
    .tokenizer = .unicode_words,
    .filters = &.{ .lowercase, .{ .shingle = .{ .min = 2, .max = 2 } } },
};

/// Search-as-you-type 3-gram shingle subfield: unicode_words → lowercase → shingle(3)
pub const search_as_you_type_3gram_analyzer = Analyzer{
    .tokenizer = .unicode_words,
    .filters = &.{ .lowercase, .{ .shingle = .{ .min = 3, .max = 3 } } },
};

/// Search-as-you-type prefix subfield: unicode_words → lowercase → shingle(1..3) → edge_ngram(min=2, max=20)
pub const search_as_you_type_index_prefix_analyzer = Analyzer{
    .tokenizer = .unicode_words,
    .filters = &.{ .lowercase, .{ .shingle = .{ .min = 1, .max = 3 } }, .{ .edge_ngram = .{ .min = 2, .max = 20 } } },
};

pub const search_as_you_type_analyzer = search_as_you_type_index_prefix_analyzer;

/// Language-specific analyzer: unicode_words → lowercase → language stop words → language stemmer
pub fn languageAnalyzer(comptime lang: Language) Analyzer {
    return .{
        .tokenizer = .unicode_words,
        .filters = &.{
            .lowercase,
            .{ .stop_words_lang = lang },
            .{ .stemmer_lang = lang },
        },
    };
}

pub const german_analyzer = languageAnalyzer(.german);
pub const french_analyzer = languageAnalyzer(.french);
pub const spanish_analyzer = languageAnalyzer(.spanish);
pub const italian_analyzer = languageAnalyzer(.italian);
pub const portuguese_analyzer = languageAnalyzer(.portuguese);
pub const dutch_analyzer = languageAnalyzer(.dutch);
pub const swedish_analyzer = languageAnalyzer(.swedish);
pub const norwegian_analyzer = languageAnalyzer(.norwegian);
pub const danish_analyzer = languageAnalyzer(.danish);
pub const finnish_analyzer = languageAnalyzer(.finnish);

pub fn builtinAnalyzerByName(name: []const u8) ?*const Analyzer {
    if (std.mem.eql(u8, name, "standard") or std.mem.eql(u8, name, "default")) return &default_analyzer;
    if (std.mem.eql(u8, name, "simple")) return &simple_analyzer;
    if (std.mem.eql(u8, name, "keyword")) return &keyword_analyzer;
    if (std.mem.eql(u8, name, "html") or std.mem.eql(u8, name, "html_analyzer")) return &html_analyzer;
    if (std.mem.eql(u8, name, "search_as_you_type") or std.mem.eql(u8, name, "search_as_you_type_analyzer")) return &search_as_you_type_analyzer;
    if (std.mem.eql(u8, name, "search_as_you_type_2gram")) return &search_as_you_type_2gram_analyzer;
    if (std.mem.eql(u8, name, "search_as_you_type_3gram")) return &search_as_you_type_3gram_analyzer;
    if (std.mem.eql(u8, name, "search_as_you_type_index_prefix")) return &search_as_you_type_index_prefix_analyzer;
    if (std.mem.eql(u8, name, "german")) return &german_analyzer;
    if (std.mem.eql(u8, name, "french")) return &french_analyzer;
    if (std.mem.eql(u8, name, "spanish")) return &spanish_analyzer;
    if (std.mem.eql(u8, name, "italian")) return &italian_analyzer;
    if (std.mem.eql(u8, name, "portuguese")) return &portuguese_analyzer;
    if (std.mem.eql(u8, name, "dutch")) return &dutch_analyzer;
    if (std.mem.eql(u8, name, "swedish")) return &swedish_analyzer;
    if (std.mem.eql(u8, name, "norwegian")) return &norwegian_analyzer;
    if (std.mem.eql(u8, name, "danish")) return &danish_analyzer;
    if (std.mem.eql(u8, name, "finnish")) return &finnish_analyzer;
    return null;
}

// ============================================================================
// Tests
// ============================================================================

test "unicode word tokenizer" {
    const alloc = std.testing.allocator;
    const tokens = try (Tokenizer{ .unicode_words = {} }).tokenize(alloc, "Hello, World! 123");
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("Hello", tokens[0].term);
    try std.testing.expectEqualStrings("World", tokens[1].term);
    try std.testing.expectEqualStrings("123", tokens[2].term);
    try std.testing.expectEqual(@as(u32, 0), tokens[0].start_byte);
    try std.testing.expectEqual(@as(u32, 5), tokens[0].end_byte);
}

test "whitespace tokenizer" {
    const alloc = std.testing.allocator;
    const tokens = try (Tokenizer{ .whitespace = {} }).tokenize(alloc, "hello world\tfoo");
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("hello", tokens[0].term);
    try std.testing.expectEqualStrings("world", tokens[1].term);
    try std.testing.expectEqualStrings("foo", tokens[2].term);
}

test "keyword tokenizer" {
    const alloc = std.testing.allocator;
    const tokens = try (Tokenizer{ .keyword = {} }).tokenize(alloc, "exact match");
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 1), tokens.len);
    try std.testing.expectEqualStrings("exact match", tokens[0].term);
}

test "lowercase filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .unicode_words = {} }).tokenize(alloc, "Hello WORLD");
    tokens = try (TokenFilter{ .lowercase = {} }).apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqualStrings("hello", tokens[0].term);
    try std.testing.expectEqualStrings("world", tokens[1].term);
}

test "stop words filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .unicode_words = {} }).tokenize(alloc, "the quick brown fox");
    tokens = try (TokenFilter{ .lowercase = {} }).apply(alloc, tokens);
    tokens = try (TokenFilter{ .stop_words = {} }).apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    // "the" is a stop word, should be removed
    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("quick", tokens[0].term);
    try std.testing.expectEqualStrings("brown", tokens[1].term);
    try std.testing.expectEqualStrings("fox", tokens[2].term);
}

test "porter2 stemmer" {
    const alloc = std.testing.allocator;

    // Test common stemming cases
    const cases = [_][2][]const u8{
        .{ "running", "run" },
        .{ "dogs", "dog" },
        .{ "caresses", "caress" },
        .{ "generalization", "gener" },
        .{ "relational", "relat" },
    };

    for (&cases) |pair| {
        const stemmed = try porter2Stem(alloc, pair[0]);
        defer if (stemmed.ptr != pair[0].ptr) alloc.free(@constCast(stemmed));
        try std.testing.expectEqualStrings(pair[1], stemmed);
    }
}

test "porter2 short words unchanged" {
    const alloc = std.testing.allocator;

    // Short words should not be modified
    const short = try porter2Stem(alloc, "hi");
    try std.testing.expectEqualStrings("hi", short);
    // Should return original pointer (no allocation)
}

test "default analyzer end-to-end" {
    const alloc = std.testing.allocator;
    const tokens = try default_analyzer.analyze(alloc, "The dogs are running quickly");
    defer Analyzer.freeTokens(alloc, tokens);

    // "The" → "the" → stop word removed
    // "dogs" → "dog"
    // "are" → stop word removed
    // "running" → "run"
    // "quickly" → "quick" (ly removal)
    try std.testing.expect(tokens.len >= 2);

    // Check that "the" and "are" were removed
    for (tokens) |tok| {
        try std.testing.expect(!std.mem.eql(u8, tok.term, "the"));
        try std.testing.expect(!std.mem.eql(u8, tok.term, "are"));
    }
}

test "analyzer preserves positions for phrase queries" {
    const alloc = std.testing.allocator;
    const tokens = try simple_analyzer.analyze(alloc, "hello beautiful world");
    defer Analyzer.freeTokens(alloc, tokens);

    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqual(@as(u32, 0), tokens[0].position);
    try std.testing.expectEqual(@as(u32, 1), tokens[1].position);
    try std.testing.expectEqual(@as(u32, 2), tokens[2].position);
}

test "analyzer preserves byte offsets for highlighting" {
    const alloc = std.testing.allocator;
    const text = "Hello World";
    const tokens = try simple_analyzer.analyze(alloc, text);
    defer Analyzer.freeTokens(alloc, tokens);

    try std.testing.expectEqual(@as(usize, 2), tokens.len);
    // "Hello" at bytes 0..5
    try std.testing.expectEqual(@as(u32, 0), tokens[0].start_byte);
    try std.testing.expectEqual(@as(u32, 5), tokens[0].end_byte);
    // "World" at bytes 6..11
    try std.testing.expectEqual(@as(u32, 6), tokens[1].start_byte);
    try std.testing.expectEqual(@as(u32, 11), tokens[1].end_byte);
}

// ---- Phase 17 tests ----

test "html_strip basic" {
    const alloc = std.testing.allocator;
    const result = try CharFilter.html_strip.apply(alloc, "<p>Hello <b>World</b></p>");
    defer alloc.free(result);
    // Tags replaced with spaces, trimmed/normalized
    try std.testing.expect(std.mem.indexOf(u8, result, "Hello") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "World") != null);
    // No angle brackets remain
    try std.testing.expect(std.mem.indexOf(u8, result, "<") == null);
    try std.testing.expect(std.mem.indexOf(u8, result, ">") == null);
}

test "html_strip entities" {
    const alloc = std.testing.allocator;
    const result = try CharFilter.html_strip.apply(alloc, "&amp; &lt; &#65; &#x41;");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("& < A A", result);
}

test "ascii_fold" {
    const alloc = std.testing.allocator;
    const result = try CharFilter.ascii_fold.apply(alloc, "caf\xC3\xA9 r\xC3\xA9sum\xC3\xA9 na\xC3\xAFve");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("cafe resume naive", result);
}

test "zero_width_non_joiner" {
    const alloc = std.testing.allocator;
    // Insert ZWNJ (0xE2 0x80 0x8C) between "hel" and "lo"
    const input = "hel\xE2\x80\x8Clo";
    const result = try CharFilter.zero_width_non_joiner.apply(alloc, input);
    defer alloc.free(result);
    try std.testing.expectEqualStrings("hello", result);
}

test "analyzer with char_filters" {
    const alloc = std.testing.allocator;
    const analyzer = Analyzer{
        .char_filters = &.{.html_strip},
        .tokenizer = .unicode_words,
        .filters = &.{.lowercase},
    };
    const tokens = try analyzer.analyze(alloc, "<b>Hello</b> <i>World</i>");
    defer Analyzer.freeTokens(alloc, tokens);

    try std.testing.expectEqual(@as(usize, 2), tokens.len);
    try std.testing.expectEqualStrings("hello", tokens[0].term);
    try std.testing.expectEqualStrings("world", tokens[1].term);
}

test "ngram tokenizer" {
    const alloc = std.testing.allocator;
    const tokenizer = Tokenizer{ .ngram = .{ .min = 2, .max = 3 } };
    const tokens = try tokenizer.tokenize(alloc, "abcd");
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    // min=2: "ab","bc","cd" (3)  min=3: "abc","bcd" (2) = 5 total
    try std.testing.expectEqual(@as(usize, 5), tokens.len);
    try std.testing.expectEqualStrings("ab", tokens[0].term);
    try std.testing.expectEqualStrings("bc", tokens[1].term);
    try std.testing.expectEqualStrings("cd", tokens[2].term);
    try std.testing.expectEqualStrings("abc", tokens[3].term);
    try std.testing.expectEqualStrings("bcd", tokens[4].term);
}

test "ngram short text" {
    const alloc = std.testing.allocator;
    const tokenizer = Tokenizer{ .ngram = .{ .min = 3, .max = 4 } };
    const tokens = try tokenizer.tokenize(alloc, "ab");
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    // "ab" is shorter than min=3, no tokens
    try std.testing.expectEqual(@as(usize, 0), tokens.len);
}

test "edge_ngram front" {
    const alloc = std.testing.allocator;
    const tokenizer = Tokenizer{ .edge_ngram = .{ .min = 1, .max = 3, .side = .front } };
    const tokens = try tokenizer.tokenize(alloc, "hello");
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("h", tokens[0].term);
    try std.testing.expectEqualStrings("he", tokens[1].term);
    try std.testing.expectEqualStrings("hel", tokens[2].term);
}

test "edge_ngram back" {
    const alloc = std.testing.allocator;
    const tokenizer = Tokenizer{ .edge_ngram = .{ .min = 1, .max = 3, .side = .back } };
    const tokens = try tokenizer.tokenize(alloc, "hello");
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("o", tokens[0].term);
    try std.testing.expectEqualStrings("lo", tokens[1].term);
    try std.testing.expectEqualStrings("llo", tokens[2].term);
}

test "character tokenizer" {
    const alloc = std.testing.allocator;
    const tokens = try (Tokenizer{ .character = {} }).tokenize(alloc, "hi!");
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("h", tokens[0].term);
    try std.testing.expectEqualStrings("i", tokens[1].term);
    try std.testing.expectEqualStrings("!", tokens[2].term);
}

test "ngram filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .keyword = {} }).tokenize(alloc, "hello");
    const filter = TokenFilter{ .ngram = .{ .min = 2, .max = 3 } };
    tokens = try filter.apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    // min=2: "he","el","ll","lo" (4)  min=3: "hel","ell","llo" (3) = 7 total
    try std.testing.expectEqual(@as(usize, 7), tokens.len);
}

test "edge_ngram filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .keyword = {} }).tokenize(alloc, "hello");
    const filter = TokenFilter{ .edge_ngram = .{ .min = 1, .max = 3, .side = .front } };
    tokens = try filter.apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("h", tokens[0].term);
    try std.testing.expectEqualStrings("he", tokens[1].term);
    try std.testing.expectEqualStrings("hel", tokens[2].term);
}

test "shingle min=2 max=2" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .unicode_words = {} }).tokenize(alloc, "the quick brown");
    tokens = try (TokenFilter{ .lowercase = {} }).apply(alloc, tokens);
    const filter = TokenFilter{ .shingle = .{ .min = 2, .max = 2 } };
    tokens = try filter.apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 2), tokens.len);
    try std.testing.expectEqualStrings("the quick", tokens[0].term);
    try std.testing.expectEqualStrings("quick brown", tokens[1].term);
}

test "shingle min=2 max=3" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .unicode_words = {} }).tokenize(alloc, "a b c");
    const filter = TokenFilter{ .shingle = .{ .min = 2, .max = 3 } };
    tokens = try filter.apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    // bigrams: "a b", "b c" (2) + trigrams: "a b c" (1) = 3
    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("a b", tokens[0].term);
    try std.testing.expectEqualStrings("b c", tokens[1].term);
    try std.testing.expectEqualStrings("a b c", tokens[2].term);
}

test "length filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .unicode_words = {} }).tokenize(alloc, "a bb ccc dddd");
    const filter = TokenFilter{ .length = .{ .min = 2, .max = 3 } };
    tokens = try filter.apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 2), tokens.len);
    try std.testing.expectEqualStrings("bb", tokens[0].term);
    try std.testing.expectEqualStrings("ccc", tokens[1].term);
}

test "truncate filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .keyword = {} }).tokenize(alloc, "longword");
    const filter = TokenFilter{ .truncate = .{ .max_len = 4 } };
    tokens = try filter.apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 1), tokens.len);
    try std.testing.expectEqualStrings("long", tokens[0].term);
}

test "unique filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .unicode_words = {} }).tokenize(alloc, "hello world hello");
    tokens = try (TokenFilter{ .unique = {} }).apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 2), tokens.len);
    try std.testing.expectEqualStrings("hello", tokens[0].term);
    try std.testing.expectEqualStrings("world", tokens[1].term);
}

test "reverse filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .keyword = {} }).tokenize(alloc, "hello");
    tokens = try (TokenFilter{ .reverse = {} }).apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqualStrings("olleh", tokens[0].term);
}

test "camel_case filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .keyword = {} }).tokenize(alloc, "firstName");
    tokens = try (TokenFilter{ .camel_case = {} }).apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqual(@as(usize, 2), tokens.len);
    try std.testing.expectEqualStrings("first", tokens[0].term);
    try std.testing.expectEqualStrings("name", tokens[1].term);
}

test "elision filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .keyword = {} }).tokenize(alloc, "l'amour");
    tokens = try (TokenFilter{ .elision = {} }).apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqualStrings("amour", tokens[0].term);
}

test "apostrophe filter" {
    const alloc = std.testing.allocator;
    var tokens = try (Tokenizer{ .keyword = {} }).tokenize(alloc, "it's");
    tokens = try (TokenFilter{ .apostrophe = {} }).apply(alloc, tokens);
    defer {
        for (tokens) |t| alloc.free(@constCast(t.term));
        alloc.free(tokens);
    }

    try std.testing.expectEqualStrings("it", tokens[0].term);
}

test "html_analyzer end-to-end" {
    const alloc = std.testing.allocator;
    const tokens = try html_analyzer.analyze(alloc, "<h1>The Dogs</h1><p>are running quickly</p>");
    defer Analyzer.freeTokens(alloc, tokens);

    // "The" → stop word removed, "Dogs" → "dog", "are" → stop word removed,
    // "running" → "run", "quickly" → "quick"
    try std.testing.expect(tokens.len >= 2);
    for (tokens) |tok| {
        try std.testing.expect(!std.mem.eql(u8, tok.term, "the"));
        try std.testing.expect(!std.mem.eql(u8, tok.term, "are"));
        try std.testing.expect(std.mem.indexOf(u8, tok.term, "<") == null);
    }
}

fn expectTokenTerm(tokens: []const Token, expected: []const u8) !void {
    for (tokens) |token| {
        if (std.mem.eql(u8, token.term, expected)) return;
    }
    return error.TestUnexpectedResult;
}

test "search_as_you_type analyzers" {
    const alloc = std.testing.allocator;
    const grams2 = try search_as_you_type_2gram_analyzer.analyze(alloc, "quick brown fox");
    defer Analyzer.freeTokens(alloc, grams2);
    try std.testing.expectEqual(@as(usize, 2), grams2.len);
    try std.testing.expectEqualStrings("quick brown", grams2[0].term);
    try std.testing.expectEqualStrings("brown fox", grams2[1].term);

    const grams3 = try search_as_you_type_3gram_analyzer.analyze(alloc, "quick brown fox");
    defer Analyzer.freeTokens(alloc, grams3);
    try std.testing.expectEqual(@as(usize, 1), grams3.len);
    try std.testing.expectEqualStrings("quick brown fox", grams3[0].term);

    // "hello" → lowercase → shingle(1,3) → "hello" → edge_ngram(2,20)
    const prefixes = try search_as_you_type_index_prefix_analyzer.analyze(alloc, "hello");
    defer Analyzer.freeTokens(alloc, prefixes);
    try std.testing.expectEqual(@as(usize, 4), prefixes.len);
    try std.testing.expectEqualStrings("he", prefixes[0].term);
    try std.testing.expectEqualStrings("hel", prefixes[1].term);
    try std.testing.expectEqualStrings("hell", prefixes[2].term);
    try std.testing.expectEqualStrings("hello", prefixes[3].term);

    const phrase_prefixes = try search_as_you_type_index_prefix_analyzer.analyze(alloc, "quick brown fox");
    defer Analyzer.freeTokens(alloc, phrase_prefixes);
    try expectTokenTerm(phrase_prefixes, "brown f");
    try expectTokenTerm(phrase_prefixes, "quick brown f");
}

test "german analyzer end-to-end" {
    const alloc = std.testing.allocator;
    // "Die" (stop), "Häuser" → stem, "sind" (stop), "groß" → stem
    const tokens = try german_analyzer.analyze(alloc, "die hauser sind gross");
    defer Analyzer.freeTokens(alloc, tokens);

    // "die" and "sind" are German stop words → removed
    // Remaining tokens are stemmed
    for (tokens) |tok| {
        try std.testing.expect(!std.mem.eql(u8, tok.term, "die"));
        try std.testing.expect(!std.mem.eql(u8, tok.term, "sind"));
    }
    try std.testing.expect(tokens.len >= 1);
}

test "french analyzer end-to-end" {
    const alloc = std.testing.allocator;
    const tokens = try french_analyzer.analyze(alloc, "les maisons sont grandes");
    defer Analyzer.freeTokens(alloc, tokens);

    // "les" and "sont" are French stop words → removed
    for (tokens) |tok| {
        try std.testing.expect(!std.mem.eql(u8, tok.term, "les"));
        try std.testing.expect(!std.mem.eql(u8, tok.term, "sont"));
    }
    try std.testing.expect(tokens.len >= 1);
}

test "spanish analyzer end-to-end" {
    const alloc = std.testing.allocator;
    const tokens = try spanish_analyzer.analyze(alloc, "las casas son grandes");
    defer Analyzer.freeTokens(alloc, tokens);

    // "las" and "son" are Spanish stop words → removed
    for (tokens) |tok| {
        try std.testing.expect(!std.mem.eql(u8, tok.term, "las"));
        try std.testing.expect(!std.mem.eql(u8, tok.term, "son"));
    }
    try std.testing.expect(tokens.len >= 1);
}

test "languageAnalyzer works for all languages" {
    const alloc = std.testing.allocator;
    const langs = [_]Language{ .english, .german, .french, .spanish, .italian, .portuguese, .dutch, .swedish, .norwegian, .danish, .finnish };
    inline for (langs) |lang| {
        const analyzer = comptime languageAnalyzer(lang);
        const tokens = try analyzer.analyze(alloc, "hello world");
        defer Analyzer.freeTokens(alloc, tokens);
        // Should produce at least one token for any language
        try std.testing.expect(tokens.len >= 1);
    }
}
