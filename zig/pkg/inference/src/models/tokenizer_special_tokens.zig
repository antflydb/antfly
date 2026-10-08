// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Listing needs marker metadata, not an allocated vocabulary/merge tree.
//! Unknown subtrees are syntax-checked by the JSON scanner. Full tokenizer
//! semantics and consumed artifact identity remain the model loader's job.
const std = @import("std");

pub const Fields = struct {
    added_tokens: ?std.json.Value = null,
    added_tokens_decoder: ?std.json.Value = null,
};

pub fn parse(allocator: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Fields) {
    const trimmed = std.mem.trimStart(u8, bytes, " \t\r\n");
    if (trimmed.len == 0 or trimmed[0] != '{') {
        // Preserve listing's historical behavior for valid non-object JSON;
        // malformed input must still fail instead of becoming empty metadata.
        var ignored = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
        defer ignored.deinit();
        return std.json.parseFromSlice(Fields, allocator, "{}", .{});
    }
    return std.json.parseFromSlice(Fields, allocator, bytes, .{ .ignore_unknown_fields = true });
}

test "tokenizer marker projection handles escaped fields and syntax errors" {
    const a = std.testing.allocator;
    var fields = try parse(a,
        \\{"model":{"vocab":{"ignored":42}},"added\u005ftokens":[{"content":"[L]","id":17}],"added_tokens_decoder":{"18":{"content":"[SEP_STRUCT]"}}}
    );
    defer fields.deinit();
    try std.testing.expectEqual(@as(i64, 17), fields.value.added_tokens.?.array.items[0].object.get("id").?.integer);
    try std.testing.expectEqualStrings("[SEP_STRUCT]", fields.value.added_tokens_decoder.?.object.get("18").?.object.get("content").?.string);
    try std.testing.expectError(error.DuplicateField, parse(a, "{\"added_tokens\":[],\"added_tokens\":[]}"));
    try std.testing.expectError(error.SyntaxError, parse(a, "{\"model\":{\"vocab\":[1,]}}"));
    var nonobject = try parse(a, "[1,2]");
    defer nonobject.deinit();
    try std.testing.expect(nonobject.value.added_tokens == null);
}

test "tokenizer marker projection does not allocate the vocabulary" {
    const a = std.testing.allocator;
    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, "{\"model\":{\"vocab\":[");
    for (0..10000) |i| {
        if (i != 0) try input.append(a, ',');
        try input.appendSlice(a, "\"token\"");
    }
    try input.appendSlice(a, "]},\"added_tokens\":[{\"id\":17,\"content\":\"[L]\"}]}");
    var storage: [4096]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&storage);
    var fields = try parse(bounded.allocator(), input.items);
    defer fields.deinit();
    try std.testing.expectEqual(@as(i64, 17), fields.value.added_tokens.?.array.items[0].object.get("id").?.integer);
}

test "tokenizer marker projection unwinds every allocation failure" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            var fields = try parse(a, "{\"added_tokens\":[{\"id\":17,\"content\":\"[L]\"}],\"added_tokens_decoder\":{\"18\":{\"content\":\"[SEP_STRUCT]\"}}}");
            defer fields.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
