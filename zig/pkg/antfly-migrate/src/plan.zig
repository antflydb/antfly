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
pub const Kind = enum { text, integer, real, boolean, json };
pub const Column = struct {
    name: []const u8,
    from: []const u8,
    type: Kind,
    nullable: bool = true,
    transform: enum { identity, base64 } = .identity,
};
pub const Source = struct { name: []const u8, path: []const u8 };
pub const Table = struct {
    source: []const u8,
    query: []const u8,
    target: []const u8,
    key: []const []const u8,
    columns: []const Column,
    write_mode: enum { insert, upsert } = .insert,
};
pub const Plan = struct {
    version: u32,
    sources: []const Source,
    destination: []const u8,
    schema: struct { seed: ?[]const u8 = null, statements: []const []const u8 = &.{} },
    tables: []const Table,
    batch_rows: usize = 100,
    verification_rows: usize = 100,
    batch_bytes: usize = 1024 * 1024,

    pub fn validate(self: Plan) !void {
        if (self.version != 1) return error.UnsupportedPlanVersion;
        if (self.sources.len == 0 or self.tables.len == 0) return error.EmptyPlan;
        if (!std.mem.endsWith(u8, self.destination, ".aflite")) return error.InvalidDestination;
        if (self.batch_rows == 0 or self.batch_rows > 1000 or self.batch_bytes < 1024 or self.batch_bytes > 8 * 1024 * 1024) return error.InvalidBatchLimit;
        if (self.verification_rows == 0 or self.verification_rows > 1000) return error.InvalidVerificationLimit;
        if ((self.schema.seed != null) == (self.schema.statements.len != 0)) return error.ChooseSeedOrStatements;
        for (self.sources, 0..) |s, i| {
            if (s.name.len == 0 or s.path.len == 0) return error.InvalidSource;
            for (self.sources[0..i]) |previous| if (std.mem.eql(u8, previous.name, s.name)) return error.DuplicateSource;
        }
        for (self.tables, 0..) |t, i| {
            try identifier(t.target);
            for (self.tables[0..i]) |previous| if (std.mem.eql(u8, previous.target, t.target)) return error.DuplicateTarget;
            if (t.key.len == 0 or t.columns.len == 0 or t.columns.len > 256) return error.InvalidColumns;
            var found = false;
            for (self.sources) |s| if (std.mem.eql(u8, s.name, t.source)) {
                found = true;
            };
            if (!found) return error.UnknownSource;
            for (t.columns, 0..) |col, ci| {
                try identifier(col.name);
                try identifier(col.from);
                if (col.transform == .base64 and col.type != .text) return error.InvalidTransform;
                for (t.columns[0..ci]) |previous| if (std.mem.eql(u8, previous.name, col.name)) return error.DuplicateColumn;
            }
            for (t.key, 0..) |key, ki| {
                var exists = false;
                for (t.columns) |col| if (std.mem.eql(u8, col.name, key)) {
                    if (col.type == .json or col.type == .real or col.transform != .identity) return error.InvalidKey;
                    exists = true;
                };
                if (!exists) return error.UnknownKey;
                for (t.key[0..ki]) |previous| if (std.mem.eql(u8, previous, key)) return error.DuplicateKey;
            }
        }
    }
};

// Keep quoting unambiguous across the SQLite and Antfly dialects. Broader
// quoted identifiers can be added without changing the runner contract.
pub fn identifier(name: []const u8) !void {
    if (name.len == 0 or name.len > 128) return error.InvalidIdentifier;
    for (name, 0..) |ch, i| if (!(std.ascii.isAlphabetic(ch) or ch == '_' or (i != 0 and std.ascii.isDigit(ch)))) return error.InvalidIdentifier;
}

test "plan rejects unsupported versions and ambiguous schema ownership" {
    var p: Plan = .{ .version = 2, .sources = &.{}, .destination = "out.aflite", .schema = .{}, .tables = &.{} };
    try std.testing.expectError(error.UnsupportedPlanVersion, p.validate());
    p.version = 1;
    try std.testing.expectError(error.EmptyPlan, p.validate());
    try std.testing.expectError(error.InvalidIdentifier, identifier("a\";DROP TABLE x"));
}
