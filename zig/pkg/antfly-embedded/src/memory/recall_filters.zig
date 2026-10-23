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

const std = @import("std");
const A = std.mem.Allocator;
pub const Filters = struct {
    subjects: ?[]const []const u8 = null,
    overlapping_scope: ?[]const u8 = null,
    predicate: ?[]const u8 = null,
    scope: ?[]const u8 = null,
    kind: ?[]const u8 = null,
    environment: ?[]const u8 = null,
    task: ?[]const u8 = null,
    pub fn empty(self: Filters) bool {
        inline for (@typeInfo(Filters).@"struct".field_names) |name| if (@field(self, name) != null) return false;
        return true;
    }
};
fn token(a: A, field: []const u8, value: []const u8) ![]const u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(value, &hash, .{});
    // Alphanumeric with a numeric suffix: one standard-analyzer term, without
    // stopword/stemming ambiguity or a user-controlled query expression.
    return std.fmt.allocPrint(a, "mm{s}{s}0", .{ field, std.fmt.bytesToHex(hash, .lower) });
}
pub fn terms(a: A, claim: anytype) ![]const u8 {
    return std.fmt.allocPrint(a, "{s} {s} {s} {s} {s} {s}", .{
        try token(a, "p", claim.predicate),                                                 try token(a, "s", claim.scope),                                           try token(a, "k", @tagName(claim.kind)),
        try token(a, "u", if (@hasField(@TypeOf(claim), "subject")) claim.subject else ""), if (claim.environment) |v| try token(a, "e", v) else "mmenvironmentany0", if (claim.task) |v| try token(a, "t", v) else "mmtaskany0",
    });
}
// Use analyzed-field match rather than scalar equality: the lane field holds
// the statement and several tokens, so a residual exact-value check is false.
fn term(a: A, field: []const u8, value: []const u8) !std.json.Value {
    var fields: std.json.ObjectMap = .empty;
    try fields.put(a, "field", .{ .string = field });
    try fields.put(a, "text", .{ .string = value });
    var root: std.json.ObjectMap = .empty;
    try root.put(a, "match", .{ .object = fields });
    return .{ .object = root };
}
pub fn query(a: A, index_field: []const u8, filters: Filters) ![]const u8 {
    if (filters.empty()) return "";
    var clauses = std.json.Array.init(a);
    inline for (.{ .{ "predicate", "p" }, .{ "scope", "s" }, .{ "kind", "k" }, .{ "environment", "e" }, .{ "task", "t" } }) |entry| {
        if (@field(filters, entry[0])) |value| {
            const exact = try term(a, index_field, try token(a, entry[1], value));
            if (comptime std.mem.eql(u8, entry[0], "environment") or std.mem.eql(u8, entry[0], "task")) {
                var alternatives = std.json.Array.init(a);
                try alternatives.append(exact);
                try alternatives.append(try term(a, index_field, if (comptime std.mem.eql(u8, entry[0], "environment")) "mmenvironmentany0" else "mmtaskany0"));
                var disjunction: std.json.ObjectMap = .empty;
                try disjunction.put(a, "disjuncts", .{ .array = alternatives });
                try clauses.append(.{ .object = disjunction });
            } else try clauses.append(exact);
        }
    }
    if (filters.subjects) |subjects| {
        if (subjects.len == 0 or subjects.len > 4096) return error.InvalidMemoryRequest;
        var alternatives = std.json.Array.init(a);
        for (subjects) |subject| try alternatives.append(try term(a, index_field, try token(a, "u", subject)));
        var disjunction: std.json.ObjectMap = .empty;
        try disjunction.put(a, "disjuncts", .{ .array = alternatives });
        try clauses.append(.{ .object = disjunction });
    }
    if (filters.overlapping_scope) |scope| {
        var alternatives = std.json.Array.init(a);
        try alternatives.append(try term(a, index_field, try token(a, "s", scope)));
        try alternatives.append(try term(a, index_field, try token(a, "s", "")));
        var disjunction: std.json.ObjectMap = .empty;
        try disjunction.put(a, "disjuncts", .{ .array = alternatives });
        try clauses.append(.{ .object = disjunction });
    }
    var root: std.json.ObjectMap = .empty;
    try root.put(a, "conjuncts", .{ .array = clauses });
    return std.json.Stringify.valueAlloc(a, std.json.Value{ .object = root }, .{});
}

test "recall immutable predicates use exact indexed tokens with environment wildcards" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const rendered = try terms(a, .{ .predicate = "uses", .scope = "team", .kind = enum { fact }.fact, .environment = @as(?[]const u8, null), .task = @as(?[]const u8, "deploy") });
    try std.testing.expect(std.mem.indexOf(u8, rendered, "mmenvironmentany0") != null);
    const encoded = try query(a, "lane", .{ .predicate = "uses", .environment = "prod" });
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{});
    const clauses = parsed.object.get("conjuncts").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), clauses.len);
    const alternatives = clauses[1].object.get("disjuncts").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), alternatives.len);
    try std.testing.expectEqualStrings("mmenvironmentany0", alternatives[1].object.get("match").?.object.get("text").?.string);
    try std.testing.expectEqualStrings("", try query(a, "lane", .{}));
    try std.testing.expect(std.mem.indexOf(u8, encoded, "prod") == null);
}
