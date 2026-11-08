// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const grounding = @import("../pipelines/memorilla_grounding.zig");

pub const ContextMode = enum { latent, source_verified };

// Ordinary Zig numeric parsing permits quoted numbers. The API wire contract
// requires actual numeric tokens, including inside each feature vector.
fn JsonNumber(comptime T: type) type {
    return struct {
        value: T,
        pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
            if (try source.peekNextTokenType() != .number) return error.UnexpectedToken;
            return .{ .value = try std.json.innerParse(T, a, source, options) };
        }
    };
}

pub const SourceTexts = struct {
    value: ?[]const []const u8 = null,

    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
        if (try source.peekNextTokenType() != .array_begin) return error.UnexpectedToken;
        return .{ .value = try std.json.innerParse([]const []const u8, a, source, options) };
    }
};

pub const Request = struct {
    model: []const u8,
    question: []const u8,
    document_embeddings: []const []const JsonNumber(f32),
    encoder_execution_identity: []const u8,
    max_new_tokens: JsonNumber(i64) = .{ .value = 64 },
    context_mode: ContextMode = .latent,
    document_texts: SourceTexts = .{},

    pub fn validateSourceContext(self: @This()) !void {
        switch (self.context_mode) {
            .latent => if (self.document_texts.value != null) return error.UnexpectedMemorillaSourceContext,
            .source_verified => try grounding.validateSources(self.document_texts.value orelse return error.MemorillaSourceContextRequired, self.document_embeddings.len),
        }
    }
};

test "Memorilla request legacy defaults preserve latent-only source semantics" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(Request, a,
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id"}
    , .{});
    defer parsed.deinit();
    try parsed.value.validateSourceContext();
    try std.testing.expect(parsed.value.context_mode == .latent);
    try std.testing.expect(parsed.value.document_texts.value == null);
    try std.testing.expectEqual(@as(i64, 64), parsed.value.max_new_tokens.value);
}

test "Memorilla request requires explicit mode and aligned nonblank sources" {
    const a = std.testing.allocator;
    const cases = [_]struct { body: []const u8, failure: anyerror }{
        .{ .body =
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","document_texts":["fact"]}
        , .failure = error.UnexpectedMemorillaSourceContext },
        .{ .body =
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","context_mode":"source_verified"}
        , .failure = error.MemorillaSourceContextRequired },
        .{ .body =
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","context_mode":"source_verified","document_texts":[]}
        , .failure = error.InvalidMemorillaSourceCount },
        .{ .body =
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","context_mode":"source_verified","document_texts":["a","b"]}
        , .failure = error.InvalidMemorillaSourceCount },
        .{ .body =
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","context_mode":"source_verified","document_texts":[" \n\t"]}
        , .failure = error.InvalidMemorillaSourceText },
    };
    for (cases) |case| {
        var parsed = try std.json.parseFromSlice(Request, a, case.body, .{});
        defer parsed.deinit();
        try std.testing.expectError(case.failure, parsed.value.validateSourceContext());
    }
    var valid = try std.json.parseFromSlice(Request, a,
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","context_mode":"source_verified","document_texts":["A fact."]}
    , .{});
    defer valid.deinit();
    try valid.value.validateSourceContext();
}

test "Memorilla request wire rejects ambiguous modes null sources and number coercion" {
    const a = std.testing.allocator;
    const cases = [_][]const u8{
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","context_mode":null}
        ,
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","context_mode":"automatic"}
        ,
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","context_mode":"latent","context_mode":"source_verified"}
        ,
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","document_texts":null}
        ,
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","document_texts":[1]}
        ,
        \\{"model":"m","question":"q","document_embeddings":[["1"]],"encoder_execution_identity":"id"}
        ,
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","max_new_tokens":"3"}
        ,
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","max_new_tokens":null}
        ,
        \\{"model":"m","question":"q","document_embeddings":[[1]],"encoder_execution_identity":"id","stream":false}
        ,
    };
    for (cases) |body| {
        if (std.json.parseFromSlice(Request, a, body, .{})) |parsed_value| {
            var parsed = parsed_value;
            parsed.deinit();
            return error.TestUnexpectedResult;
        } else |_| {}
    }
}
