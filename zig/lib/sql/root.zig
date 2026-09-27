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

const std = @import("std");

pub const token = @import("token.zig");
pub const lexer = @import("lexer.zig");
pub const parser = @import("parser.zig");

// The generated parser is intentionally test-only while the imported grammar
// still has unresolved conflicts. Keeping it private prevents an experimental
// conflict-resolution policy from becoming a production API by accident.
const generated_parser = @import("generated_parser.zig");
const generated = @import("grammar/generated/root.zig");

test {
    _ = token;
    _ = lexer;
    _ = parser;
    _ = generated_parser;
    _ = generated;
}

test "generated SQL parser accepts a representative statement corpus" {
    const cases = [_][]const generated.Token{
        &.{ .SELECT, .NUMBER },
        &.{ .SELECT, .IDENT, .FROM, .IDENT, .WHERE, .IDENT, .EQ, .PLACEHOLDER },
        &.{ .INSERT, .INTO, .IDENT, .VALUES, .LPAREN, .NUMBER, .RPAREN },
        &.{ .UPDATE, .IDENT, .SET, .IDENT, .EQ, .NUMBER },
        &.{ .DELETE, .FROM, .IDENT },
    };

    for (cases) |case| {
        var token_ids: [16]u16 = undefined;
        for (case, 0..) |item, index| token_ids[index] = generated.tokenId(item);
        try generated.parse(std.testing.allocator, token_ids[0..case.len]);
        try std.testing.expectEqual(@as(?generated.ParseDiagnostic, null), try generated.parseDiagnostic(std.testing.allocator, token_ids[0..case.len]));
    }
}

test "generated SQL parser rejects malformed statements with diagnostics" {
    const malformed = [_]generated.Token{ .SELECT, .FROM };
    var token_ids: [malformed.len]u16 = undefined;
    for (malformed, 0..) |item, index| token_ids[index] = generated.tokenId(item);

    try std.testing.expectError(error.UnexpectedToken, generated.parse(std.testing.allocator, &token_ids));
    const diagnostic = (try generated.parseDiagnostic(std.testing.allocator, &token_ids)).?;
    defer diagnostic.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), diagnostic.token_index);
    try std.testing.expect(diagnostic.expected.len > 0);
}
