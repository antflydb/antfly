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

//! Extract complete top-level CPU function declarations with the Zig parser.
//! Anonymous struct return types must not be mistaken for function bodies.
const std = @import("std");
const Function = struct {
    name: []const u8,
    source: []const u8,
    token_count: usize,
    token_sha256: [64]u8,
};

fn functions(allocator: std.mem.Allocator, source: [:0]const u8) ![]Function {
    var tree = try std.zig.Ast.parse(allocator, source, .{});
    defer tree.deinit(allocator);
    if (tree.errors.len != 0) return error.InvalidSource;
    var result: std.ArrayList(Function) = .empty;
    errdefer result.deinit(allocator);
    for (tree.rootDecls()) |node| {
        if (tree.nodeTag(node) != .fn_decl) continue;
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const prototype = tree.fullFnProto(&buffer, node).?;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        var token_count: usize = 0;
        for (tree.firstToken(node)..tree.lastToken(node) + 1) |index| {
            const token: std.zig.Ast.TokenIndex = @intCast(index);
            const tag = tree.tokens.items(.tag)[index];
            if (tag == .doc_comment or tag == .container_doc_comment) continue;
            hash.update(@tagName(tag));
            hash.update("\x00");
            hash.update(tree.tokenSlice(token));
            hash.update("\x00");
            token_count += 1;
        }
        try result.append(allocator, .{
            .name = tree.tokenSlice(prototype.name_token.?),
            .source = tree.getNodeSource(node),
            .token_count = token_count,
            .token_sha256 = std.fmt.bytesToHex(hash.finalResult(), .lower),
        });
    }
    return result.toOwnedSlice(allocator);
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const input_path = args.next() orelse return error.MissingInput;
    const output_path = args.next() orelse return error.MissingOutput;
    if (args.next() != null) return error.UnexpectedArgument;
    const input = try std.Io.Dir.cwd().readFileAlloc(init.io, input_path, allocator, .limited(8 * 1024 * 1024));
    defer allocator.free(input);
    const source = try allocator.dupeSentinel(u8, input, 0);
    defer allocator.free(source);
    const inventory = try functions(allocator, source);
    defer allocator.free(inventory);
    const json = try std.json.Stringify.valueAlloc(allocator, inventory, .{});
    defer allocator.free(json);
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output_path, .data = json });
    std.debug.print("Audited {d} complete CPU function declarations\n", .{inventory.len});
}

test "anonymous return types and nested declarations retain complete bodies" {
    const source =
        \\fn tuple() struct { x: i32, y: i32 } {
        \\    const Inner = struct { fn value() i32 { return 7; } };
        \\    return .{ .x = Inner.value(), .y = 9 };
        \\}
        \\fn following() void {}
    ;
    const result = try functions(std.testing.allocator, source);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqual(@as(usize, 2), result.len);
    try std.testing.expectEqualStrings("tuple", result[0].name);
    try std.testing.expect(std.mem.endsWith(u8, result[0].source, "return .{ .x = Inner.value(), .y = 9 };\n}"));
    try std.testing.expectEqualStrings("fn following() void {}", result[1].source);
}

test "token identity ignores formatting and retains operators and string contents" {
    const variants = [_][:0]const u8{
        "fn f() u32 { return 1 + 2; }",
        "fn f( ) u32 { // same arithmetic\nreturn 1+2 ;\n}",
        "fn f() u32 { return 1 - 2; }",
        "fn f() []const u8 { return \"a//b\"; }",
        "fn f() []const u8 { return \"a//c\"; }",
    };
    var hashes: [variants.len][64]u8 = undefined;
    for (variants, 0..) |source, index| {
        const result = try functions(std.testing.allocator, source);
        defer std.testing.allocator.free(result);
        hashes[index] = result[0].token_sha256;
    }
    try std.testing.expectEqualStrings(&hashes[0], &hashes[1]);
    try std.testing.expect(!std.mem.eql(u8, &hashes[0], &hashes[2]));
    try std.testing.expect(!std.mem.eql(u8, &hashes[3], &hashes[4]));
}
