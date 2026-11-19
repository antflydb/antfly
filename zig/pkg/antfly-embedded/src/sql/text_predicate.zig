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

//! Native, unranked document predicates. Only positive WHERE conjuncts may
//! become exact index selection; all other expressions keep scalar binding.
const std = @import("std");
const ast = @import("ast.zig");
const A = std.mem.Allocator;
pub fn contains(predicate: ?*const ast.Predicate) bool {
    const p = predicate orelse return false;
    return switch (p.*) {
        .scalar => |s| scalarContains(s),
        .conjunction, .disjunction => |b| contains(b.left) or contains(b.right),
        .negation => |n| contains(n),
        else => false,
    };
}
fn scalarContains(s: *const ast.Scalar) bool {
    return switch (s.*) {
        .call => |c| std.ascii.eqlIgnoreCase(c.name, "full_text_search"),
        .binary => |b| scalarContains(b.left) or scalarContains(b.right),
        else => false,
    };
}
pub const Extracted = struct { residual: ?*const ast.Predicate, queries: []const ast.Value };
pub fn extract(a: A, predicate: ?*const ast.Predicate) !Extracted {
    if (!contains(predicate)) return .{ .residual = predicate, .queries = &.{} };
    var queries: std.ArrayList(ast.Value) = .empty;
    const residual = if (predicate) |p| try walk(a, p, &queries) else null;
    return .{ .residual = residual, .queries = try queries.toOwnedSlice(a) };
}
fn walk(a: A, p: *const ast.Predicate, out: *std.ArrayList(ast.Value)) anyerror!?*const ast.Predicate {
    if (p.* == .conjunction) {
        const l = try walk(a, p.conjunction.left, out);
        const r = try walk(a, p.conjunction.right, out);
        if (l == null) return r;
        if (r == null) return l;
        const result = try a.create(ast.Predicate);
        result.* = .{ .conjunction = .{ .left = l.?, .right = r.? } };
        return result;
    }
    if (p.* != .scalar) return p;
    const residual = try walkScalar(a, p.scalar, out);
    if (residual == null) return null;
    const result = try a.create(ast.Predicate);
    result.* = .{ .scalar = residual.? };
    return result;
}
fn walkScalar(a: A, s: *const ast.Scalar, out: *std.ArrayList(ast.Value)) anyerror!?*const ast.Scalar {
    if (s.* == .binary and s.binary.op == .@"and") {
        const l = try walkScalar(a, s.binary.left, out);
        const r = try walkScalar(a, s.binary.right, out);
        if (l == null) return r;
        if (r == null) return l;
        const result = try a.create(ast.Scalar);
        result.* = .{ .binary = .{ .op = .@"and", .left = l.?, .right = r.? } };
        return result;
    }
    if (s.* != .call or !std.ascii.eqlIgnoreCase(s.call.name, "full_text_search")) return s;
    const c = s.call;
    if (c.args.len != 1 or c.star or c.distinct or c.filter != null or c.window != null or c.subquery != null or c.within_group != null) return error.InvalidSqlParameters;
    if (c.args[0].* != .literal) return error.UnsupportedSqlShape;
    const value = c.args[0].literal;
    if (value != .string and value != .parameter) return error.InvalidSqlParameters;
    if (out.items.len >= 32) return error.SqlProgramLimitExceeded;
    try out.append(a, value);
    return null;
}

test "SQL text predicate leaves ordinary WHERE allocation free and preserves compiled inputs" {
    const ordinary: ast.Predicate = .{ .comparison = .{ .field = "status", .op = .eq, .value = .{ .string = "active" } } };
    const unchanged = try extract(std.testing.failing_allocator, &ordinary);
    try std.testing.expect(unchanged.residual == &ordinary);
    try std.testing.expectEqual(@as(usize, 0), unchanged.queries.len);
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "SELECT count(*) FROM docs WHERE full_text_search($1) AND status = 'active'", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const predicate = compiled.statement.select.predicate;
    const first = try extract(arena.allocator(), predicate);
    const second = try extract(arena.allocator(), predicate);
    try std.testing.expectEqual(@as(usize, 1), first.queries.len);
    try std.testing.expectEqual(@as(u32, 1), first.queries[0].parameter);
    try std.testing.expectEqual(first.queries[0].parameter, second.queries[0].parameter);
    try std.testing.expect(first.residual != null);
    try std.testing.expect(!contains(first.residual));
    try std.testing.expect(contains(compiled.statement.select.predicate));
}
