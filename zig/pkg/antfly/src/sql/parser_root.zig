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

const std = @import("std");

pub const token = @import("token.zig");
pub const lexer = @import("lexer.zig");
pub const parser = @import("parser.zig");
pub const generated = @import("grammar/generated/root.zig");

test {
    _ = token;
    _ = lexer;
    _ = parser;
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
