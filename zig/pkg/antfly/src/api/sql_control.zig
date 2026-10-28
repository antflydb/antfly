// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const compiler = @import("antfly_local_sources").sql_compiler;
const cache = @import("../sql/plan_cache.zig");

// Classification is bounded, schema independent, and uses the same grammar as
// execution. Untrusted hints and statement prefixes never claim control slots.
pub const max_statement_bytes = 4096;
pub const max_body_bytes = 8192;
pub fn isControl(statement: compiler.ast.Statement) bool {
    return switch (statement) {
        .commit, .rollback, .rollback_to_savepoint, .release_savepoint => true,
        else => false,
    };
}
pub fn classify(statement: []const u8) bool {
    if (statement.len > max_statement_bytes) return false;
    var buffer: [65536]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buffer);
    var compiled = compiler.compile(fba.allocator(), statement, .{ .max_bytes = max_statement_bytes, .max_tokens = 128, .max_nodes = 64, .max_depth = 8 }) catch return false;
    defer compiled.deinit();
    return compiled.parameter_count == 0 and isControl(compiled.statement);
}
pub fn classifyBody(body: []const u8) bool {
    if (body.len > max_body_bytes) return false;
    var buffer: [32768]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buffer);
    var parsed = std.json.parseFromSlice(struct { statement: ?[]const u8 = null }, fba.allocator(), body, .{ .ignore_unknown_fields = true }) catch return false;
    defer parsed.deinit();
    return classify(parsed.value.statement orelse return false);
}

// Control compilation cannot contend with fresh plan-cache fills. Its memory
// remains bounded by the separate control preparation owner and compiler limits.
pub const Plan = union(enum) {
    cached: cache.Lease,
    control: compiler.Compiled,
    pub fn acquire(alloc: std.mem.Allocator, owner: *cache.Cache, io: std.Io, key: cache.Key, diagnostic: *compiler.Diagnostic, protected: bool) !Plan {
        return acquireControlled(alloc, owner, io, key, diagnostic, protected, null);
    }
    pub fn acquireControlled(alloc: std.mem.Allocator, owner: *cache.Cache, io: std.Io, key: cache.Key, diagnostic: *compiler.Diagnostic, protected: bool, control: ?cache.Control) !Plan {
        if (!protected) return .{ .cached = try owner.acquireControlled(io, key, diagnostic, control) };
        var program = try compiler.compileDiagnosticControlled(alloc, key.statement, .{ .max_bytes = max_statement_bytes, .max_tokens = 128, .max_nodes = 64, .max_depth = 8 }, diagnostic, if (control) |value| value.work else null);
        errdefer program.deinit();
        if (!isControl(program.statement) or program.parameter_count != 0) return error.UnsupportedSqlExecution;
        return .{ .control = program };
    }
    pub fn compiled(self: *const Plan) *const compiler.Compiled {
        return switch (self.*) {
            .cached => |*lease| lease.compiled(),
            .control => |*value| value,
        };
    }
    pub fn release(self: *Plan, io: std.Io) void {
        switch (self.*) {
            .cached => |*lease| lease.release(io),
            .control => |*value| value.deinit(),
        }
    }
};

test "SQL protected control classification rejects fresh work and multiple statements" {
    for ([_][]const u8{ "COMMIT", "COMMIT TRANSACTION;", "ROLLBACK", "ROLLBACK TO SAVEPOINT s", "RELEASE SAVEPOINT s", "/* x */ COMMIT;" }) |s| try std.testing.expect(classify(s));
    for ([_][]const u8{ "BEGIN", "SELECT 1", "COMMIT; SELECT 1", "COMMIT bogus", "ROLLBACK TO $1", "INSERT INTO t VALUES (1)", "-- COMMIT\nSELECT 1" }) |s| try std.testing.expect(!classify(s));
    try std.testing.expect(classifyBody("{\"statement\":\"ROLLBACK\",\"session_id\":\"x\"}"));
    try std.testing.expect(!classifyBody("{\"statement\":\"SELECT 1\",\"priority\":\"control\"}"));
}
