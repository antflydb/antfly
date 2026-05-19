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

// SentencePiece model adapter backed by generated protobuf types.
//
// The checked-in descriptor set in lib/tokenizer/proto/sentencepiece_model.desc
// is compiled into the `sentencepiece_proto` module at build time. This file
// keeps the tokenizer-facing flattened view stable while delegating wire-format
// details to the generated `sentencepiece.ModelProto` types.

const std = @import("std");
const sentencepiece = @import("sentencepiece_proto").sentencepiece;

pub const ModelType = enum(u8) {
    unigram = 1,
    bpe = 2,
    word = 3,
    char = 4,
};

pub const PieceInfo = struct {
    text: []const u8,
    score: f32,
    piece_type: u8, // maps to ModelProto.SentencePiece.Type
};

/// Flattened view of the SentencePiece model, exposing only the fields the
/// tokenizer consumer needs. Strings borrow directly from the decoded proto.
pub const ModelProto = struct {
    pieces: []PieceInfo,
    model_type: ModelType,
    byte_fallback: bool,
    unk_surface: []const u8,
    add_dummy_prefix: bool,
    remove_extra_whitespaces: bool,

    pub fn deinit(self: *const ModelProto, allocator: std.mem.Allocator) void {
        allocator.free(self.pieces);
    }
};

/// Decode a SentencePiece model and return the flattened view. The caller
/// must ensure `data` outlives the returned `ModelProto` because strings inside
/// `PieceInfo.text` and `unk_surface` borrow from the decoded proto.
pub fn parseModelProto(allocator: std.mem.Allocator, data: []const u8) !ModelProto {
    var wire = try sentencepiece.ModelProto.decode(allocator, data);
    defer wire.deinit(allocator);

    const pieces = try allocator.alloc(PieceInfo, wire.pieces.len);
    errdefer allocator.free(pieces);

    for (wire.pieces, 0..) |p, i| {
        pieces[i] = .{
            .text = p.piece,
            .score = p.score,
            .piece_type = @intCast(@intFromEnum(p.type)),
        };
    }

    const model_type: ModelType = switch (wire.trainer_spec.model_type) {
        .UNIGRAM => .unigram,
        .BPE => .bpe,
        .WORD => .word,
        .CHAR => .char,
        else => .unigram,
    };

    return .{
        .pieces = pieces,
        .model_type = model_type,
        .byte_fallback = wire.trainer_spec.byte_fallback,
        .unk_surface = wire.trainer_spec.unk_surface,
        .add_dummy_prefix = wire.normalizer_spec.add_dummy_prefix,
        .remove_extra_whitespaces = wire.normalizer_spec.remove_extra_whitespaces,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "parseModelProto flattens nested trainer/normalizer specs" {
    const alloc = testing.allocator;

    var pieces = [_]sentencepiece.ModelProto.SentencePiece{
        .{ .piece = "hello", .score = -0.5, .type = .NORMAL },
        .{ .piece = "<unk>", .score = 0.0, .type = .UNKNOWN },
        .{ .piece = "<0xAB>", .score = -1.0, .type = .BYTE },
    };
    const original = sentencepiece.ModelProto{
        .pieces = pieces[0..],
        .trainer_spec = .{
            .model_type = .BPE,
            .byte_fallback = true,
            .unk_surface = "[UNK]",
        },
        .normalizer_spec = .{
            .add_dummy_prefix = false,
            .remove_extra_whitespaces = true,
        },
    };

    const bytes = try original.encode(alloc);
    defer alloc.free(bytes);

    const parsed = try parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);

    try testing.expectEqual(ModelType.bpe, parsed.model_type);
    try testing.expect(parsed.byte_fallback);
    try testing.expectEqualStrings("[UNK]", parsed.unk_surface);
    try testing.expectEqual(false, parsed.add_dummy_prefix);
    try testing.expectEqual(true, parsed.remove_extra_whitespaces);

    try testing.expectEqual(@as(usize, 3), parsed.pieces.len);
    try testing.expectEqualStrings("hello", parsed.pieces[0].text);
    try testing.expectEqual(@as(f32, -0.5), parsed.pieces[0].score);
    try testing.expectEqual(@as(u8, 1), parsed.pieces[0].piece_type);
    try testing.expectEqualStrings("<unk>", parsed.pieces[1].text);
    try testing.expectEqual(@as(u8, 2), parsed.pieces[1].piece_type);
    try testing.expectEqualStrings("<0xAB>", parsed.pieces[2].text);
    try testing.expectEqual(@as(u8, 6), parsed.pieces[2].piece_type);
}

test "parseModelProto applies proto2 defaults when specs are absent" {
    const alloc = testing.allocator;

    var pieces = [_]sentencepiece.ModelProto.SentencePiece{
        .{ .piece = "a", .score = 0.0 },
    };
    const original = sentencepiece.ModelProto{
        .pieces = pieces[0..],
    };

    const bytes = try original.encode(alloc);
    defer alloc.free(bytes);

    const parsed = try parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);

    try testing.expectEqual(ModelType.unigram, parsed.model_type);
    try testing.expect(!parsed.byte_fallback);
    try testing.expectEqualStrings(" \xe2\x81\x87 ", parsed.unk_surface);
    try testing.expectEqual(true, parsed.add_dummy_prefix);
    try testing.expectEqual(true, parsed.remove_extra_whitespaces);

    try testing.expectEqual(@as(usize, 1), parsed.pieces.len);
    try testing.expectEqual(@as(u8, 1), parsed.pieces[0].piece_type);
}
