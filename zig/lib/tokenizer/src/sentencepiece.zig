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

// SentencePiece BPE tokenizer — ported from github.com/ajroetker/go-sentencepiece.
//
// Supports BPE (Byte Pair Encoding) tokenization as used by Gemma, Gemini, etc.
// Loads vocabulary from SentencePiece protobuf model files.

const std = @import("std");
const proto = @import("proto.zig");
const PrefixMatcher = @import("prefix_matcher.zig").PrefixMatcher;
const PriorityQueue = @import("priority_queue.zig").PriorityQueue;

pub const Token = struct {
    id: i32,
    text: []const u8,
};

pub const TokenSpan = struct {
    start: usize, // byte offset inclusive
    end: usize, // byte offset exclusive
};

pub const TokenWithSpan = struct {
    token: Token,
    span: TokenSpan,
};

pub const ModelInfo = struct {
    vocabulary_size: usize,
    bos_id: i32,
    eos_id: i32,
    unk_id: i32,
    pad_id: i32,
};

const PieceType = enum(u8) {
    normal = 1,
    unknown = 2,
    control = 3,
    user_defined = 4,
    unused = 5,
    byte = 6,
};

const Piece = struct {
    text: []const u8,
    score: f32,
    piece_type: PieceType,
};

fn looksLikeSpecialPiece(text: []const u8) bool {
    return text.len >= 3 and text[0] == '<' and text[text.len - 1] == '>';
}

/// The SentencePiece separator character (U+2581, "▁", 3 bytes in UTF-8).
const separator = "\xe2\x96\x81";

const SymListElem = struct {
    prev: i32,
    next: i32,
    no_merge: bool,
    symbol: []const u8,
    norm_start: usize,
    norm_end: usize,
};

const MergeCandidate = struct {
    left: i32,
    right: i32,
    length: usize,
    score: f32,
};

fn mergeCandidateCmp(a: MergeCandidate, b: MergeCandidate) std.math.Order {
    // Higher score first; tie-break by position (earlier first).
    if (a.score > b.score) return .gt;
    if (a.score < b.score) return .lt;
    if (a.left < b.left) return .gt;
    if (a.left > b.left) return .lt;
    return .eq;
}

pub const PieceInit = struct {
    text: []const u8,
    score: f32,
    piece_type: u8,
};

pub const InitOptions = struct {
    byte_fallback: bool = false,
    unk_surface: []const u8 = " \xe2\x81\x87 ",
    add_dummy_prefix: bool = true,
    remove_extra_whitespaces: bool = true,
};

pub const Processor = struct {
    allocator: std.mem.Allocator,
    pieces: []const Piece,
    piece_map: std.StringHashMap(i32),
    reserved_map: std.StringHashMap(i32),
    extra_reserved_map: std.StringHashMap(i32),
    unknown_id: i32,
    byte_fallback: bool,
    unk_surface: []const u8,
    special_matcher: PrefixMatcher,
    byte_to_token: [256]?Token,
    id_to_byte: std.AutoHashMap(i32, u8),
    extra_id_to_text: std.AutoHashMap(i32, []const u8),
    max_vocab_id: i32,
    max_piece_length: usize,
    add_dummy_prefix: bool,
    remove_extra_whitespaces: bool,
    preserve_inline_specials_after_literal_bos: bool = false,

    pub fn initFromPath(allocator: std.mem.Allocator, path: []const u8) !Processor {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const data = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, allocator, .limited(256 * 1024 * 1024));
        defer allocator.free(data);
        return try init(allocator, data);
    }

    pub fn init(allocator: std.mem.Allocator, model_data: []const u8) !Processor {
        const model = try proto.parseModelProto(allocator, model_data);
        defer model.deinit(allocator);

        if (model.model_type != .bpe) return error.UnsupportedModelType;

        const pieces = try allocator.alloc(PieceInit, model.pieces.len);
        defer allocator.free(pieces);
        for (model.pieces, 0..) |p, i| {
            pieces[i] = .{
                .text = p.text,
                .score = p.score,
                .piece_type = p.piece_type,
            };
        }

        return initFromPieces(allocator, pieces, .{
            .byte_fallback = model.byte_fallback,
            .unk_surface = model.unk_surface,
            .add_dummy_prefix = model.add_dummy_prefix,
            .remove_extra_whitespaces = model.remove_extra_whitespaces,
        });
    }

    pub fn initFromPieces(allocator: std.mem.Allocator, pieces_init: []const PieceInit, options: InitOptions) !Processor {
        return initFromPieceSpecs(allocator, pieces_init, options);
    }

    pub fn deinit(self: *Processor) void {
        for (self.pieces) |p| self.allocator.free(p.text);
        self.allocator.free(self.pieces);
        self.piece_map.deinit();
        self.reserved_map.deinit();
        var extra_it = self.extra_id_to_text.iterator();
        while (extra_it.next()) |entry| self.allocator.free(entry.value_ptr.*);
        self.extra_reserved_map.deinit();
        self.id_to_byte.deinit();
        self.extra_id_to_text.deinit();
        self.special_matcher.deinit();
        self.allocator.free(self.unk_surface);
    }

    pub fn addExternalSpecialToken(self: *Processor, token: []const u8, id: i32) !void {
        if (self.reserved_map.contains(token) or self.piece_map.contains(token) or self.extra_reserved_map.contains(token)) {
            return;
        }
        const owned = try self.allocator.dupe(u8, token);
        errdefer self.allocator.free(owned);
        try self.extra_reserved_map.put(owned, id);
        errdefer _ = self.extra_reserved_map.remove(owned);
        try self.extra_id_to_text.put(id, owned);
        self.max_vocab_id = @max(self.max_vocab_id, id);
        try self.rebuildSpecialMatcher();
    }

    pub fn setPreserveInlineSpecialsAfterLiteralBos(self: *Processor, enabled: bool) void {
        self.preserve_inline_specials_after_literal_bos = enabled;
    }

    fn rebuildSpecialMatcher(self: *Processor) !void {
        self.special_matcher.deinit();
        var special_tokens = std.StringHashMap(void).init(self.allocator);
        defer special_tokens.deinit();

        var piece_it = self.piece_map.iterator();
        while (piece_it.next()) |entry| {
            if (looksLikeSpecialPiece(entry.key_ptr.*)) {
                try special_tokens.put(entry.key_ptr.*, {});
            }
        }
        var reserved_it = self.reserved_map.iterator();
        while (reserved_it.next()) |entry| {
            try special_tokens.put(entry.key_ptr.*, {});
        }
        var extra_it = self.extra_reserved_map.iterator();
        while (extra_it.next()) |entry| {
            try special_tokens.put(entry.key_ptr.*, {});
        }
        self.special_matcher = try PrefixMatcher.initFromMap(self.allocator, &special_tokens);
    }

    pub fn encode(self: *const Processor, allocator: std.mem.Allocator, text: []const u8) ![]Token {
        return self.encodeWithOptions(allocator, text, self.add_dummy_prefix, true);
    }

    fn encodeWithOptions(
        self: *const Processor,
        allocator: std.mem.Allocator,
        text: []const u8,
        add_dummy_prefix: bool,
        allow_inline_special: bool,
    ) ![]Token {
        const normalized = try normalize(allocator, text, add_dummy_prefix, self.remove_extra_whitespaces);
        defer allocator.free(normalized);

        const sym_list = try self.encodeToSymList(allocator, normalized, allow_inline_special);
        defer allocator.free(sym_list);
        if (sym_list.len == 0) return try allocator.alloc(Token, 0);

        // Count tokens
        var n_tokens: usize = 0;
        var idx: i32 = 0;
        while (idx >= 0) : (idx = sym_list[@intCast(idx)].next) {
            const sym = sym_list[@intCast(idx)].symbol;
            const id = self.symbolToID(sym);
            if (id == self.unknown_id and self.byte_fallback) {
                n_tokens += sym.len;
            } else {
                n_tokens += 1;
            }
        }

        var tokens = try allocator.alloc(Token, n_tokens);
        var ti: usize = 0;
        idx = 0;
        while (idx >= 0) : (idx = sym_list[@intCast(idx)].next) {
            const sym = sym_list[@intCast(idx)].symbol;
            const id = self.symbolToID(sym);

            if (id == self.unknown_id and self.byte_fallback) {
                for (sym) |byte_val| {
                    tokens[ti] = self.byte_to_token[byte_val].?;
                    ti += 1;
                }
            } else {
                tokens[ti] = .{ .id = id, .text = sym };
                ti += 1;
            }
        }
        return tokens[0..ti];
    }

    pub fn decode(self: *const Processor, allocator: std.mem.Allocator, ids: []const i32) ![]u8 {
        var result: std.ArrayListUnmanaged(u8) = .empty;

        var i: usize = 0;
        while (i < ids.len) {
            if (self.extra_id_to_text.get(ids[i])) |extra_text| {
                try result.appendSlice(allocator, extra_text);
                i += 1;
                continue;
            }
            // Find run of byte IDs
            var next_non_byte = i;
            while (next_non_byte < ids.len and self.isByteID(ids[next_non_byte])) {
                next_non_byte += 1;
            }

            if (next_non_byte > i) {
                // Decode byte run as UTF-8
                var buf = try allocator.alloc(u8, next_non_byte - i);
                defer allocator.free(buf);
                for (i..next_non_byte, 0..) |bi, j| {
                    buf[j] = self.id_to_byte.get(ids[bi]).?;
                }
                try appendByteRunWithReplacement(allocator, &result, buf);
            }

            if (next_non_byte >= ids.len) break;

            const id = ids[next_non_byte];
            if (self.isControlID(id)) {
                // Skip control tokens
            } else if (id == self.unknown_id) {
                try result.appendSlice(allocator, self.unk_surface);
            } else {
                const piece = self.pieces[@intCast(id)].text;
                // Replace separator back to space
                var j: usize = 0;
                while (j < piece.len) {
                    if (j + 3 <= piece.len and std.mem.eql(u8, piece[j .. j + 3], separator)) {
                        try result.append(allocator, ' ');
                        j += 3;
                    } else {
                        try result.append(allocator, piece[j]);
                        j += 1;
                    }
                }
            }
            i = next_non_byte + 1;
        }

        return result.toOwnedSlice(allocator);
    }

    pub fn modelInfo(self: *const Processor) ModelInfo {
        const getControlID = struct {
            fn f(proc: *const Processor, symbol: []const u8) i32 {
                const id = proc.symbolToID(symbol);
                if (id != proc.unknown_id and proc.isControlID(id)) return id;
                return -1;
            }
        }.f;
        const getFirstControlID = struct {
            fn f(proc: *const Processor, symbols: []const []const u8) i32 {
                for (symbols) |symbol| {
                    const id = getControlID(proc, symbol);
                    if (id >= 0) return id;
                }
                return -1;
            }
        }.f;

        return .{
            .vocabulary_size = self.pieces.len,
            .bos_id = getFirstControlID(self, &.{ "<bos>", "<s>" }),
            .eos_id = getFirstControlID(self, &.{ "<eos>", "</s>" }),
            .pad_id = getControlID(self, "<pad>"),
            .unk_id = self.unknown_id,
        };
    }

    // --- Internal ---

    fn encodeToSymList(
        self: *const Processor,
        allocator: std.mem.Allocator,
        normalized: []const u8,
        allow_inline_special: bool,
    ) ![]SymListElem {
        if (normalized.len == 0) return try allocator.alloc(SymListElem, 0);

        // Build initial symbol list: each symbol is a user-defined match or a single rune.
        var sym_list: std.ArrayListUnmanaged(SymListElem) = .empty;

        var pos: usize = 0;
        while (pos < normalized.len) {
            const result = self.symbolMatch(normalized[pos..], allow_inline_special);
            const slen = result.len;

            try sym_list.append(allocator, .{
                .prev = @as(i32, @intCast(sym_list.items.len)) - 1,
                .next = @as(i32, @intCast(sym_list.items.len)) + 1,
                .no_merge = result.is_user_defined,
                .symbol = normalized[pos .. pos + slen],
                .norm_start = pos,
                .norm_end = pos + slen,
            });

            pos += slen;
        }

        var syms = try sym_list.toOwnedSlice(allocator);
        if (syms.len == 0) return syms;
        syms[syms.len - 1].next = -1;

        // Priority queue of merge candidates
        var merge_queue = try PriorityQueue(MergeCandidate).init(allocator, mergeCandidateCmp);
        defer merge_queue.deinit();

        // Merge buffer
        const buf = try allocator.alloc(u8, self.max_piece_length);
        defer allocator.free(buf);

        // Seed queue with all adjacent pairs
        for (1..syms.len) |i| {
            try self.suggestMerge(&merge_queue, syms, @as(i32, @intCast(i)) - 1, @intCast(i), buf);
        }

        // Main BPE merge loop
        var dead_count: usize = 0;
        while (merge_queue.len() > 0) {
            const candidate = merge_queue.popMax();

            // Check if candidate is stale
            const left_sym = syms[@intCast(candidate.left)].symbol;
            const right_sym = syms[@intCast(candidate.right)].symbol;
            if (left_sym.len == 0 or right_sym.len == 0 or
                left_sym.len + right_sym.len != candidate.length)
            {
                if (dead_count > 0) dead_count -= 1;
                continue;
            }

            // Periodic dead candidate cleanup
            if (dead_count * 3 > merge_queue.len()) {
                merge_queue.removeMatching(struct {
                    syms: []const SymListElem,
                    pub fn isDead(ctx: @This(), mc: MergeCandidate) bool {
                        const ls = ctx.syms[@intCast(mc.left)].symbol;
                        const rs = ctx.syms[@intCast(mc.right)].symbol;
                        return ls.len == 0 or rs.len == 0 or ls.len + rs.len != mc.length;
                    }
                }{ .syms = syms });
                dead_count = 0;
            }

            // Perform merge
            const merged = self.findMerged(syms[@intCast(candidate.left)], syms[@intCast(candidate.right)], buf);
            if (merged == null) continue; // shouldn't happen

            syms[@intCast(candidate.left)].symbol = merged.?.text;
            syms[@intCast(candidate.left)].norm_end = syms[@intCast(candidate.right)].norm_end;
            syms[@intCast(candidate.left)].next = syms[@intCast(candidate.right)].next;

            if (syms[@intCast(candidate.right)].next >= 0) {
                syms[@intCast(syms[@intCast(candidate.right)].next)].prev = candidate.left;
            }

            syms[@intCast(candidate.right)].symbol = "";
            dead_count += 1;

            // Suggest new merges with neighbors
            try self.suggestMerge(&merge_queue, syms, syms[@intCast(candidate.left)].prev, candidate.left, buf);
            try self.suggestMerge(&merge_queue, syms, candidate.left, syms[@intCast(candidate.left)].next, buf);
        }

        return syms;
    }

    const FindMergedResult = struct {
        text: []const u8,
        id: i32,
    };

    fn findMerged(self: *const Processor, x: SymListElem, y: SymListElem, buf: []u8) ?FindMergedResult {
        const combined_len = x.symbol.len + y.symbol.len;
        if (combined_len > buf.len) return null;

        @memcpy(buf[0..x.symbol.len], x.symbol);
        @memcpy(buf[x.symbol.len..combined_len], y.symbol);
        const key = buf[0..combined_len];

        if (self.piece_map.get(key)) |id| {
            return .{ .text = self.pieces[@intCast(id)].text, .id = id };
        }
        return null;
    }

    fn suggestMerge(
        self: *const Processor,
        queue: *PriorityQueue(MergeCandidate),
        syms: []const SymListElem,
        left: i32,
        right: i32,
        buf: []u8,
    ) !void {
        if (left < 0 or right < 0) return;
        if (syms[@intCast(left)].no_merge or syms[@intCast(right)].no_merge) return;

        if (self.findMerged(syms[@intCast(left)], syms[@intCast(right)], buf)) |merged| {
            try queue.insert(.{
                .left = left,
                .right = right,
                .length = merged.text.len,
                .score = self.pieces[@intCast(merged.id)].score,
            });
        }
    }

    const SymbolMatchResult = struct {
        len: usize,
        is_user_defined: bool,
    };

    fn symbolMatch(self: *const Processor, text: []const u8, allow_inline_special: bool) SymbolMatchResult {
        if (allow_inline_special) {
            const prefix_len = self.special_matcher.findPrefixLen(text);
            if (prefix_len > 0) return .{ .len = prefix_len, .is_user_defined = true };
        }

        // Single UTF-8 codepoint
        const rune_len = std.unicode.utf8ByteSequenceLength(text[0]) catch 1;
        return .{ .len = @min(rune_len, text.len), .is_user_defined = false };
    }

    fn symbolToID(self: *const Processor, symbol: []const u8) i32 {
        if (self.reserved_map.get(symbol)) |id| return id;
        if (self.extra_reserved_map.get(symbol)) |id| return id;
        if (self.piece_map.get(symbol)) |id| return id;
        return self.unknown_id;
    }

    fn isByteID(self: *const Processor, id: i32) bool {
        if (id < 0 or id >= @as(i32, @intCast(self.pieces.len))) return false;
        return self.pieces[@intCast(id)].piece_type == .byte;
    }

    fn isControlID(self: *const Processor, id: i32) bool {
        if (id < 0 or id >= @as(i32, @intCast(self.pieces.len))) return false;
        return self.pieces[@intCast(id)].piece_type == .control;
    }

    // --- Tokenizer vtable adapter ---

    const Tokenizer = @import("tokenizer.zig").Tokenizer;
    const SpecialTokens = @import("tokenizer.zig").SpecialTokens;

    pub fn tokenizer(self: *Processor) Tokenizer {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Tokenizer.VTable{
        .encode = &vtableEncode,
        .encodeInto = &vtableEncodeInto,
        .encodeForModel = &vtableEncodeForModel,
        .encodeGeneration = &vtableEncodeGeneration,
        .decode = &vtableDecode,
        .specialTokens = &vtableSpecialTokens,
        .vocabSize = &vtableVocabSize,
        .deinit = &vtableDeinit,
    };

    fn vtableEncode(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8) anyerror![]i32 {
        const self: *const Processor = @ptrCast(@alignCast(ptr));
        const tokens = try self.encode(allocator, text);
        defer allocator.free(tokens);
        const ids = try allocator.alloc(i32, tokens.len);
        for (tokens, 0..) |tok, i| ids[i] = tok.id;
        return ids;
    }

    fn vtableEncodeInto(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        text: []const u8,
        out: *std.ArrayListUnmanaged(i32),
    ) anyerror!void {
        const self: *const Processor = @ptrCast(@alignCast(ptr));
        const tokens = try self.encode(allocator, text);
        defer allocator.free(tokens);
        try out.ensureUnusedCapacity(allocator, tokens.len);
        for (tokens) |tok| out.appendAssumeCapacity(tok.id);
    }

    fn vtableEncodeForModel(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize) anyerror!@import("tokenizer.zig").EncodeResult {
        const self: *const Processor = @ptrCast(@alignCast(ptr));
        const tokens = try self.encode(allocator, text);
        defer allocator.free(tokens);

        const info = self.modelInfo();
        const max_tokens = if (max_length >= 2) max_length - 2 else 0;
        const token_count = @min(tokens.len, max_tokens);
        const total = token_count + 2;

        const ids = try allocator.alloc(i32, max_length);
        const mask = try allocator.alloc(i32, max_length);

        ids[0] = if (info.bos_id >= 0) info.bos_id else 101;
        mask[0] = 1;

        for (0..token_count) |i| {
            ids[i + 1] = tokens[i].id;
            mask[i + 1] = 1;
        }

        ids[total - 1] = if (info.eos_id >= 0) info.eos_id else 102;
        mask[total - 1] = 1;

        const pad_id: i32 = if (info.pad_id >= 0) info.pad_id else 0;
        for (total..max_length) |i| {
            ids[i] = pad_id;
            mask[i] = 0;
        }

        return .{
            .ids = ids,
            .attention_mask = mask,
            .allocator = allocator,
        };
    }

    fn vtableEncodeGeneration(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) anyerror!@import("tokenizer.zig").EncodeResult {
        const self: *const Processor = @ptrCast(@alignCast(ptr));
        const info = self.modelInfo();
        const bos_piece = if (add_bos_token and info.bos_id >= 0) self.pieces[@intCast(info.bos_id)].text else "";
        const literal_bos_prefix = bos_piece.len > 0 and std.mem.startsWith(u8, text, bos_piece);
        const tokens = if (literal_bos_prefix and self.preserve_inline_specials_after_literal_bos) blk: {
            // Gemma-style multimodal/chat prompts need later inline specials
            // like <start_of_turn> and <image_soft_token> to remain active even
            // when the raw prompt literally starts with BOS.
            const token_text = text[bos_piece.len..];
            const add_dummy_prefix = self.add_dummy_prefix and !literal_bos_prefix;
            break :blk try self.encodeWithOptions(allocator, token_text, add_dummy_prefix, true);
        } else blk: {
            // Llama.cpp-style BOS handling: when BOS is added from generation
            // config, keep a literal leading BOS piece as ordinary text by
            // disabling inline-special matching for that raw prompt.
            break :blk try self.encodeWithOptions(allocator, text, self.add_dummy_prefix, !literal_bos_prefix);
        };
        defer allocator.free(tokens);

        const prepend_bos = add_bos_token and info.bos_id >= 0 and max_length > 0;
        const available = if (prepend_bos) max_length - 1 else max_length;
        const token_count = @min(tokens.len, available);
        const ids = try allocator.alloc(i32, max_length);
        const mask = try allocator.alloc(i32, max_length);

        var pos: usize = 0;
        if (prepend_bos) {
            ids[0] = info.bos_id;
            mask[0] = 1;
            pos = 1;
        }
        for (0..token_count) |i| {
            ids[pos + i] = tokens[i].id;
            mask[pos + i] = 1;
        }
        const pad_id: i32 = if (info.pad_id >= 0) info.pad_id else 0;
        for (pos + token_count..max_length) |i| {
            ids[i] = pad_id;
            mask[i] = 0;
        }

        return .{
            .ids = ids,
            .attention_mask = mask,
            .allocator = allocator,
        };
    }

    fn vtableDecode(ptr: *anyopaque, allocator: std.mem.Allocator, ids: []const i32) anyerror![]u8 {
        const self: *const Processor = @ptrCast(@alignCast(ptr));
        return self.decode(allocator, ids);
    }

    fn vtableSpecialTokens(ptr: *anyopaque) SpecialTokens {
        const self: *const Processor = @ptrCast(@alignCast(ptr));
        const info = self.modelInfo();
        return .{
            .cls_id = info.bos_id, // SentencePiece uses BOS as CLS equivalent
            .sep_id = info.eos_id, // SentencePiece uses EOS as SEP equivalent
            .pad_id = if (info.pad_id >= 0) info.pad_id else 0,
            .unk_id = info.unk_id,
            .mask_id = -1,
        };
    }

    fn vtableVocabSize(ptr: *anyopaque) usize {
        const self: *const Processor = @ptrCast(@alignCast(ptr));
        return @as(usize, @intCast(self.max_vocab_id + 1));
    }

    fn vtableDeinit(ptr: *anyopaque) void {
        const self: *Processor = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

fn appendByteRunWithReplacement(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    buf: []const u8,
) !void {
    // Decode as much valid UTF-8 as possible and replace malformed or
    // truncated sequences one byte at a time so decode never panics.
    var pos: usize = 0;
    while (pos < buf.len) {
        const seq_len = std.unicode.utf8ByteSequenceLength(buf[pos]) catch {
            try out.appendSlice(allocator, "\xef\xbf\xbd");
            pos += 1;
            continue;
        };
        if (pos + seq_len > buf.len) {
            try out.appendSlice(allocator, "\xef\xbf\xbd");
            break;
        }

        const chunk = buf[pos .. pos + seq_len];
        if (std.unicode.utf8ValidateSlice(chunk)) {
            try out.appendSlice(allocator, chunk);
            pos += seq_len;
            continue;
        }

        try out.appendSlice(allocator, "\xef\xbf\xbd");
        pos += 1;
    }
}

fn initFromPieceSpecs(allocator: std.mem.Allocator, pieces_init: []const PieceInit, options: InitOptions) !Processor {
    var piece_map = std.StringHashMap(i32).init(allocator);
    var reserved_map = std.StringHashMap(i32).init(allocator);
    var special_tokens = std.StringHashMap(void).init(allocator);
    defer special_tokens.deinit();
    var id_to_byte = std.AutoHashMap(i32, u8).init(allocator);
    var byte_to_token: [256]?Token = @splat(null);
    var unk_id: i32 = -1;
    var max_piece_length: usize = 0;

    var owned_pieces = try allocator.alloc(Piece, pieces_init.len);
    errdefer allocator.free(owned_pieces);
    var owned_count: usize = 0;
    errdefer {
        for (owned_pieces[0..owned_count]) |piece| allocator.free(piece.text);
    }

    for (pieces_init, 0..) |p, i| {
        const idx: i32 = @intCast(i);
        const piece_type: PieceType = switch (p.piece_type) {
            1 => .normal,
            2 => .unknown,
            3 => .control,
            4 => .user_defined,
            5 => .unused,
            6 => .byte,
            else => return error.UnsupportedPieceType,
        };
        const text = try allocator.dupe(u8, p.text);

        owned_pieces[i] = .{
            .text = text,
            .score = p.score,
            .piece_type = piece_type,
        };
        owned_count += 1;

        const is_normal = piece_type == .normal or
            piece_type == .user_defined or
            piece_type == .unused;

        if (is_normal) {
            try piece_map.put(text, idx);
            max_piece_length = @max(max_piece_length, text.len);
        } else {
            try reserved_map.put(text, idx);
        }

        switch (piece_type) {
            .user_defined, .control, .unused => try special_tokens.put(text, {}),
            .unknown => {
                if (unk_id >= 0) return error.DuplicateUnknownSymbol;
                unk_id = idx;
            },
            .byte => {
                if (!options.byte_fallback) return error.BytePieceWithoutFallback;
                const bv = convertHexValue(text);
                if (bv) |v| {
                    byte_to_token[v] = .{ .id = idx, .text = text };
                    try id_to_byte.put(idx, v);
                }
            },
            else => {},
        }
        if (piece_type == .normal and looksLikeSpecialPiece(text)) {
            try special_tokens.put(text, {});
        }
    }

    if (unk_id < 0) return error.MissingUnknownSymbol;

    if (options.byte_fallback) {
        for (0..256) |i| {
            if (byte_to_token[i] == null) return error.IncompleteByteFallback;
        }
    }

    const matcher = try PrefixMatcher.initFromMap(allocator, &special_tokens);

    return .{
        .allocator = allocator,
        .pieces = owned_pieces,
        .piece_map = piece_map,
        .reserved_map = reserved_map,
        .extra_reserved_map = std.StringHashMap(i32).init(allocator),
        .unknown_id = unk_id,
        .byte_fallback = options.byte_fallback,
        .unk_surface = try allocator.dupe(u8, options.unk_surface),
        .special_matcher = matcher,
        .byte_to_token = byte_to_token,
        .id_to_byte = id_to_byte,
        .extra_id_to_text = std.AutoHashMap(i32, []const u8).init(allocator),
        .max_vocab_id = @as(i32, @intCast(pieces_init.len)) - 1,
        .max_piece_length = max_piece_length,
        .add_dummy_prefix = options.add_dummy_prefix,
        .remove_extra_whitespaces = options.remove_extra_whitespaces,
        .preserve_inline_specials_after_literal_bos = false,
    };
}

/// Replace spaces with the SentencePiece separator "▁" (U+2581).
fn normalize(
    allocator: std.mem.Allocator,
    text: []const u8,
    add_dummy_prefix: bool,
    remove_extra_whitespaces: bool,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);

    if (add_dummy_prefix and text.len > 0) {
        try out.appendSlice(allocator, separator);
    }

    var pending_space = false;
    for (text) |ch| {
        if (ch == ' ') {
            if (remove_extra_whitespaces) {
                pending_space = true;
            } else {
                try out.appendSlice(allocator, separator);
            }
            continue;
        }
        if (pending_space) {
            try out.appendSlice(allocator, separator);
            pending_space = false;
        }
        try out.append(allocator, ch);
    }

    return try out.toOwnedSlice(allocator);
}

/// Convert "<0xAB>" hex strings to byte values.
fn convertHexValue(text: []const u8) ?u8 {
    if (text.len < 5) return null; // minimum: "<0xN>"
    if (!std.mem.startsWith(u8, text, "<0x")) return null;
    if (!std.mem.endsWith(u8, text, ">")) return null;
    const hex = text[3 .. text.len - 1];
    return std.fmt.parseInt(u8, hex, 16) catch null;
}

// Tests

test "normalize" {
    const allocator = std.testing.allocator;
    const result = try normalize(allocator, "hello world", false, false);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("hello\xe2\x96\x81world", result);
}

test "normalize no spaces" {
    const allocator = std.testing.allocator;
    const result = try normalize(allocator, "hello", false, false);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("hello", result);
}

test "normalize with sentencepiece dummy prefix" {
    const allocator = std.testing.allocator;
    const result = try normalize(allocator, "hello world", true, true);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("\xe2\x96\x81hello\xe2\x96\x81world", result);
}

test "convert hex value" {
    try std.testing.expectEqual(@as(?u8, 0xFF), convertHexValue("<0xFF>"));
    try std.testing.expectEqual(@as(?u8, 0x00), convertHexValue("<0x00>"));
    try std.testing.expectEqual(@as(?u8, 0x1A), convertHexValue("<0x1A>"));
    try std.testing.expectEqual(@as(?u8, null), convertHexValue("bad"));
    try std.testing.expectEqual(@as(?u8, null), convertHexValue("<0x>"));
}

test "decode byte fallback tolerates truncated utf8 tail" {
    const allocator = std.testing.allocator;
    var piece_inits = try allocator.alloc(PieceInit, 257);
    defer allocator.free(piece_inits);
    piece_inits[0] = .{ .text = "<unk>", .score = 0, .piece_type = @intFromEnum(PieceType.unknown) };
    for (0..256) |byte_val| {
        piece_inits[byte_val + 1] = .{
            .text = try std.fmt.allocPrint(allocator, "<0x{X:0>2}>", .{byte_val}),
            .score = 0,
            .piece_type = @intFromEnum(PieceType.byte),
        };
    }
    defer for (piece_inits[1..]) |piece| allocator.free(piece.text);

    var processor = try initFromPieceSpecs(allocator, piece_inits, .{ .byte_fallback = true });
    defer processor.deinit();

    const decoded = try processor.decode(allocator, &.{ 0xE2 + 1, 0x82 + 1, 0x41 + 1 });
    defer allocator.free(decoded);
    try std.testing.expectEqualStrings("\xef\xbf\xbd\xef\xbf\xbdA", decoded);
}

test "modelInfo discovers gemma bos eos control tokens" {
    const allocator = std.testing.allocator;

    const pieces = try allocator.alloc(Piece, 4);
    errdefer allocator.free(pieces);
    pieces[0] = .{ .text = try allocator.dupe(u8, "<unk>"), .score = 0, .piece_type = .unknown };
    pieces[1] = .{ .text = try allocator.dupe(u8, "<bos>"), .score = 0, .piece_type = .control };
    pieces[2] = .{ .text = try allocator.dupe(u8, "<eos>"), .score = 0, .piece_type = .control };
    pieces[3] = .{ .text = try allocator.dupe(u8, "<pad>"), .score = 0, .piece_type = .control };
    errdefer {
        for (pieces) |piece| allocator.free(piece.text);
    }

    const piece_map = std.StringHashMap(i32).init(allocator);
    var reserved_map = std.StringHashMap(i32).init(allocator);
    var special_tokens = std.StringHashMap(void).init(allocator);
    defer special_tokens.deinit();
    try reserved_map.put(pieces[0].text, 0);
    try reserved_map.put(pieces[1].text, 1);
    try reserved_map.put(pieces[2].text, 2);
    try reserved_map.put(pieces[3].text, 3);
    try special_tokens.put(pieces[0].text, {});
    try special_tokens.put(pieces[1].text, {});
    try special_tokens.put(pieces[2].text, {});
    try special_tokens.put(pieces[3].text, {});

    var processor = Processor{
        .allocator = allocator,
        .pieces = pieces,
        .piece_map = piece_map,
        .reserved_map = reserved_map,
        .extra_reserved_map = std.StringHashMap(i32).init(allocator),
        .unknown_id = 0,
        .byte_fallback = false,
        .unk_surface = try allocator.dupe(u8, "<unk>"),
        .special_matcher = try PrefixMatcher.initFromMap(allocator, &special_tokens),
        .byte_to_token = @splat(null),
        .id_to_byte = std.AutoHashMap(i32, u8).init(allocator),
        .extra_id_to_text = std.AutoHashMap(i32, []const u8).init(allocator),
        .max_vocab_id = 3,
        .max_piece_length = 0,
        .add_dummy_prefix = false,
        .remove_extra_whitespaces = false,
    };
    defer processor.deinit();

    const info = processor.modelInfo();
    try std.testing.expectEqual(@as(i32, 1), info.bos_id);
    try std.testing.expectEqual(@as(i32, 2), info.eos_id);
    try std.testing.expectEqual(@as(i32, 3), info.pad_id);
    try std.testing.expectEqual(@as(i32, 0), info.unk_id);
}

test "external special token is encoded decoded and expands vocab" {
    const allocator = std.testing.allocator;

    const pieces = try allocator.alloc(Piece, 4);
    errdefer allocator.free(pieces);
    pieces[0] = .{ .text = try allocator.dupe(u8, "<unk>"), .score = 0, .piece_type = .unknown };
    pieces[1] = .{ .text = try allocator.dupe(u8, "<bos>"), .score = 0, .piece_type = .control };
    pieces[2] = .{ .text = try allocator.dupe(u8, "<eos>"), .score = 0, .piece_type = .control };
    pieces[3] = .{ .text = try allocator.dupe(u8, "hello"), .score = 1, .piece_type = .normal };
    errdefer {
        for (pieces) |piece| allocator.free(piece.text);
    }

    var piece_map = std.StringHashMap(i32).init(allocator);
    errdefer piece_map.deinit();
    try piece_map.put(pieces[3].text, 3);

    var reserved_map = std.StringHashMap(i32).init(allocator);
    errdefer reserved_map.deinit();
    var special_tokens = std.StringHashMap(void).init(allocator);
    defer special_tokens.deinit();
    try reserved_map.put(pieces[0].text, 0);
    try reserved_map.put(pieces[1].text, 1);
    try reserved_map.put(pieces[2].text, 2);
    try special_tokens.put(pieces[0].text, {});
    try special_tokens.put(pieces[1].text, {});
    try special_tokens.put(pieces[2].text, {});

    var processor = Processor{
        .allocator = allocator,
        .pieces = pieces,
        .piece_map = piece_map,
        .reserved_map = reserved_map,
        .extra_reserved_map = std.StringHashMap(i32).init(allocator),
        .unknown_id = 0,
        .byte_fallback = false,
        .unk_surface = try allocator.dupe(u8, "<unk>"),
        .special_matcher = try PrefixMatcher.initFromMap(allocator, &special_tokens),
        .byte_to_token = @splat(null),
        .id_to_byte = std.AutoHashMap(i32, u8).init(allocator),
        .extra_id_to_text = std.AutoHashMap(i32, []const u8).init(allocator),
        .max_vocab_id = 3,
        .max_piece_length = 5,
        .add_dummy_prefix = false,
        .remove_extra_whitespaces = false,
    };
    defer processor.deinit();

    try processor.addExternalSpecialToken("<image_soft_token>", 42);
    try std.testing.expectEqual(@as(usize, 43), processor.tokenizer().vocabSize());

    const encoded = try processor.encodeWithOptions(allocator, "<image_soft_token>", false, true);
    defer allocator.free(encoded);
    try std.testing.expectEqual(@as(usize, 1), encoded.len);
    try std.testing.expectEqual(@as(i32, 42), encoded[0].id);

    const decoded = try processor.decode(allocator, &.{42});
    defer allocator.free(decoded);
    try std.testing.expectEqualStrings("<image_soft_token>", decoded);
}

test "external special token matches inline before trailing text" {
    const allocator = std.testing.allocator;

    const pieces = try allocator.alloc(Piece, 5);
    errdefer allocator.free(pieces);
    pieces[0] = .{ .text = try allocator.dupe(u8, "<unk>"), .score = 0, .piece_type = .unknown };
    pieces[1] = .{ .text = try allocator.dupe(u8, "<bos>"), .score = 0, .piece_type = .control };
    pieces[2] = .{ .text = try allocator.dupe(u8, "<eos>"), .score = 0, .piece_type = .control };
    pieces[3] = .{ .text = try allocator.dupe(u8, "user"), .score = 1, .piece_type = .normal };
    pieces[4] = .{ .text = try allocator.dupe(u8, separator), .score = 1, .piece_type = .normal };
    errdefer {
        for (pieces) |piece| allocator.free(piece.text);
    }

    var piece_map = std.StringHashMap(i32).init(allocator);
    errdefer piece_map.deinit();
    try piece_map.put(pieces[3].text, 3);
    try piece_map.put(pieces[4].text, 4);

    var reserved_map = std.StringHashMap(i32).init(allocator);
    errdefer reserved_map.deinit();
    var special_tokens = std.StringHashMap(void).init(allocator);
    defer special_tokens.deinit();
    try reserved_map.put(pieces[0].text, 0);
    try reserved_map.put(pieces[1].text, 1);
    try reserved_map.put(pieces[2].text, 2);
    try special_tokens.put(pieces[0].text, {});
    try special_tokens.put(pieces[1].text, {});
    try special_tokens.put(pieces[2].text, {});

    var processor = Processor{
        .allocator = allocator,
        .pieces = pieces,
        .piece_map = piece_map,
        .reserved_map = reserved_map,
        .extra_reserved_map = std.StringHashMap(i32).init(allocator),
        .unknown_id = 0,
        .byte_fallback = false,
        .unk_surface = try allocator.dupe(u8, "<unk>"),
        .special_matcher = try PrefixMatcher.initFromMap(allocator, &special_tokens),
        .byte_to_token = @splat(null),
        .id_to_byte = std.AutoHashMap(i32, u8).init(allocator),
        .extra_id_to_text = std.AutoHashMap(i32, []const u8).init(allocator),
        .max_vocab_id = 4,
        .max_piece_length = 4,
        .add_dummy_prefix = true,
        .remove_extra_whitespaces = false,
    };
    defer processor.deinit();

    try processor.addExternalSpecialToken("<start_of_turn>", 105);
    const encoded = try processor.encodeWithOptions(allocator, "<start_of_turn>user", true, true);
    defer allocator.free(encoded);
    try std.testing.expect(encoded.len >= 2);
    try std.testing.expectEqual(@as(i32, 4), encoded[0].id);
    try std.testing.expectEqual(@as(i32, 105), encoded[1].id);
}

test "normal angle-bracket piece matches inline as special" {
    const allocator = std.testing.allocator;

    const pieces = try allocator.alloc(Piece, 3);
    errdefer allocator.free(pieces);
    pieces[0] = .{ .text = try allocator.dupe(u8, "<unk>"), .score = 0, .piece_type = .unknown };
    pieces[1] = .{ .text = try allocator.dupe(u8, "<start_of_turn>"), .score = 1, .piece_type = .normal };
    pieces[2] = .{ .text = try allocator.dupe(u8, "user"), .score = 1, .piece_type = .normal };
    errdefer {
        for (pieces) |piece| allocator.free(piece.text);
    }

    var piece_map = std.StringHashMap(i32).init(allocator);
    errdefer piece_map.deinit();
    try piece_map.put(pieces[1].text, 105);
    try piece_map.put(pieces[2].text, 3);

    var reserved_map = std.StringHashMap(i32).init(allocator);
    errdefer reserved_map.deinit();
    var special_tokens = std.StringHashMap(void).init(allocator);
    defer special_tokens.deinit();
    try special_tokens.put(pieces[1].text, {});

    var processor = Processor{
        .allocator = allocator,
        .pieces = pieces,
        .piece_map = piece_map,
        .reserved_map = reserved_map,
        .extra_reserved_map = std.StringHashMap(i32).init(allocator),
        .unknown_id = 0,
        .byte_fallback = false,
        .unk_surface = try allocator.dupe(u8, "<unk>"),
        .special_matcher = try PrefixMatcher.initFromMap(allocator, &special_tokens),
        .byte_to_token = @splat(null),
        .id_to_byte = std.AutoHashMap(i32, u8).init(allocator),
        .extra_id_to_text = std.AutoHashMap(i32, []const u8).init(allocator),
        .max_vocab_id = 105,
        .max_piece_length = pieces[1].text.len,
        .add_dummy_prefix = false,
        .remove_extra_whitespaces = false,
    };
    defer processor.deinit();

    const encoded = try processor.encodeWithOptions(allocator, "<start_of_turn>user", false, true);
    defer allocator.free(encoded);
    try std.testing.expectEqual(@as(i32, 105), encoded[0].id);
}

test "encodeGeneration preserves inline specials after literal bos prefix" {
    const allocator = std.testing.allocator;

    const pieces = try allocator.alloc(Piece, 5);
    errdefer allocator.free(pieces);
    pieces[0] = .{ .text = try allocator.dupe(u8, "<unk>"), .score = 0, .piece_type = .unknown };
    pieces[1] = .{ .text = try allocator.dupe(u8, "<bos>"), .score = 0, .piece_type = .control };
    pieces[2] = .{ .text = try allocator.dupe(u8, "<eos>"), .score = 0, .piece_type = .control };
    pieces[3] = .{ .text = try allocator.dupe(u8, "<start_of_turn>"), .score = 1, .piece_type = .normal };
    pieces[4] = .{ .text = try allocator.dupe(u8, "user"), .score = 1, .piece_type = .normal };
    errdefer {
        for (pieces) |piece| allocator.free(piece.text);
    }

    var piece_map = std.StringHashMap(i32).init(allocator);
    try piece_map.put(pieces[3].text, 105);
    try piece_map.put(pieces[4].text, 7);

    var reserved_map = std.StringHashMap(i32).init(allocator);
    try reserved_map.put(pieces[1].text, 1);
    try reserved_map.put(pieces[2].text, 2);

    var special_tokens = std.StringHashMap(void).init(allocator);
    defer special_tokens.deinit();
    try special_tokens.put(pieces[1].text, {});
    try special_tokens.put(pieces[2].text, {});
    try special_tokens.put(pieces[3].text, {});

    var processor = Processor{
        .allocator = allocator,
        .pieces = pieces,
        .piece_map = piece_map,
        .reserved_map = reserved_map,
        .extra_reserved_map = std.StringHashMap(i32).init(allocator),
        .unknown_id = 0,
        .byte_fallback = false,
        .unk_surface = try allocator.dupe(u8, "<unk>"),
        .special_matcher = try PrefixMatcher.initFromMap(allocator, &special_tokens),
        .byte_to_token = @splat(null),
        .id_to_byte = std.AutoHashMap(i32, u8).init(allocator),
        .extra_id_to_text = std.AutoHashMap(i32, []const u8).init(allocator),
        .max_vocab_id = 105,
        .max_piece_length = pieces[3].text.len,
        .add_dummy_prefix = true,
        .remove_extra_whitespaces = false,
        .preserve_inline_specials_after_literal_bos = true,
    };
    defer processor.deinit();

    var encoded = try processor.tokenizer().encodeForGenerationConfigured(
        allocator,
        "<bos><start_of_turn>user",
        16,
        true,
    );
    defer encoded.deinit();

    try std.testing.expectEqual(@as(i32, 1), encoded.ids[0]);
    try std.testing.expectEqual(@as(i32, 105), encoded.ids[1]);
}

test "encodeGeneration default mode does not preserve external specials after literal bos prefix" {
    const allocator = std.testing.allocator;

    const pieces = try allocator.alloc(Piece, 4);
    errdefer allocator.free(pieces);
    pieces[0] = .{ .text = try allocator.dupe(u8, "<unk>"), .score = 0, .piece_type = .unknown };
    pieces[1] = .{ .text = try allocator.dupe(u8, "<bos>"), .score = 0, .piece_type = .control };
    pieces[2] = .{ .text = try allocator.dupe(u8, "<eos>"), .score = 0, .piece_type = .control };
    pieces[3] = .{ .text = try allocator.dupe(u8, "user"), .score = 1, .piece_type = .normal };
    errdefer {
        for (pieces) |piece| allocator.free(piece.text);
    }

    var piece_map = std.StringHashMap(i32).init(allocator);
    try piece_map.put(pieces[3].text, 7);

    var reserved_map = std.StringHashMap(i32).init(allocator);
    try reserved_map.put(pieces[1].text, 1);
    try reserved_map.put(pieces[2].text, 2);

    var special_tokens = std.StringHashMap(void).init(allocator);
    defer special_tokens.deinit();
    try special_tokens.put(pieces[1].text, {});
    try special_tokens.put(pieces[2].text, {});

    var processor = Processor{
        .allocator = allocator,
        .pieces = pieces,
        .piece_map = piece_map,
        .reserved_map = reserved_map,
        .extra_reserved_map = std.StringHashMap(i32).init(allocator),
        .unknown_id = 0,
        .byte_fallback = false,
        .unk_surface = try allocator.dupe(u8, "<unk>"),
        .special_matcher = try PrefixMatcher.initFromMap(allocator, &special_tokens),
        .byte_to_token = @splat(null),
        .id_to_byte = std.AutoHashMap(i32, u8).init(allocator),
        .extra_id_to_text = std.AutoHashMap(i32, []const u8).init(allocator),
        .max_vocab_id = 7,
        .max_piece_length = pieces[3].text.len,
        .add_dummy_prefix = true,
        .remove_extra_whitespaces = false,
        .preserve_inline_specials_after_literal_bos = false,
    };
    defer processor.deinit();

    try processor.addExternalSpecialToken("<start_of_turn>", 105);

    var encoded = try processor.tokenizer().encodeForGenerationConfigured(
        allocator,
        "<bos><start_of_turn>user",
        16,
        true,
    );
    defer encoded.deinit();

    try std.testing.expectEqual(@as(i32, 1), encoded.ids[0]);
    var found_special = false;
    for (encoded.ids[1..], encoded.attention_mask[1..]) |id, mask| {
        if (mask == 0) break;
        if (id == 105) {
            found_special = true;
            break;
        }
    }
    try std.testing.expect(!found_special);
}

test {
    // Force test discovery for proto.zig so its decode/runtime tests run
    // alongside the sentencepiece tokenizer tests.
    _ = @import("proto.zig");
}
