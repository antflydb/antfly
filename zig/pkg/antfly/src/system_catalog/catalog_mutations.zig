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

//! Exact identities for prepared ordinary catalog DDL. Preparation resolves
//! physical identities once; retries submit or observe that same command.
const std = @import("std");
const A = std.mem.Allocator;
pub const outcomes = @import("sequence_outcomes.zig");
pub const Outcome = outcomes.Outcome;
pub const Preparation = struct {
    request: @import("antfly_local_sources").system_catalog_domain.Request,
    nonce: [16]u8,
    command_fence: ?@import("command_fence.zig").Fence = null,
};
pub const Admission = struct {
    request: outcomes.Admission,
    incarnation: [16]u8,
    pub fn validate(self: @This()) !void {
        try self.request.validate();
        if (std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidCatalogMutation;
    }
};
pub const Observation = struct {
    admission: Admission,
    digest: [32]u8,
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        if (std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidCatalogMutation;
    }
};
const Command = @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand;
fn hash(_: A, tag: []const u8, value: anytype) ![32]u8 {
    var state = std.crypto.hash.sha2.Sha256.init(.{});
    state.update(tag);
    var buffer: [4096]u8 = undefined;
    var count = std.Io.Writer.Discarding.init(&.{});
    var hashing = std.Io.Writer.Hashed(std.crypto.hash.sha2.Sha256).initHasher(&count.writer, state, &buffer);
    try std.json.Stringify.value(value, .{}, &hashing.writer);
    try hashing.writer.flush();
    if (count.fullCount() > @import("antfly_local_sources").system_catalog_domain.max_command_bytes) return error.CatalogCommandTooLarge;
    return hashing.hasher.finalResult();
}

pub fn validate(command: Command) !void {
    switch (command.mutation.kind) {
        .table, .database, .namespace, .tablespace, .query_source => {},
        // These have their own typed native journals and preparation proofs.
        else => return error.InvalidCatalogMutation,
    }
    try (command.sql_catalog_admission orelse return error.InvalidCatalogMutation).validate();
    if (command.sql_native_index != null or command.sql_authorization != null or command.sql_type != null or command.sql_cast != null or command.sql_routine != null or command.sql_sequence != null or command.sql_view != null) return error.InvalidCatalogMutation;
    if (command.version != 1 and command.version != 2 and command.version != 3 and command.version != Command.creation_sources_version) return error.InvalidCatalogMutation;
    try command.validateCreationSources();
}
pub fn observation(a: A, command: Command) !Observation {
    try validate(command);
    return .{ .admission = command.sql_catalog_admission.?, .digest = try hash(a, "antfly:sql-catalog-command:v1", command) };
}
pub fn boundDigest(a: A, request: Observation) ![32]u8 {
    try request.validate();
    return hash(a, "antfly:sql-catalog-outcome:v1", request);
}

test "SQL command fence catalog identity binds the whole prepared command without duplicating intent" {
    const a = std.testing.allocator;
    const command: Command = .{ .expected_revision = 9, .mutation = .{ .kind = .namespace, .action = .create, .name = "scoped" }, .sql_catalog_admission = .{ .request = .{ .nonce = @splat(1), .head = 4 }, .incarnation = @splat(2) } };
    const Probe = struct {
        fn run(alloc: A, value: Command) !void {
            const original = try observation(alloc, value);
            const digest = try boundDigest(alloc, original);
            const bytes = try std.json.Stringify.valueAlloc(alloc, value, .{});
            defer alloc.free(bytes);
            var decoded = try std.json.parseFromSlice(Command, alloc, bytes, .{});
            defer decoded.deinit();
            try std.testing.expectEqualDeep(original, try observation(alloc, decoded.value));
            for (0..6) |field| {
                var different = value;
                switch (field) {
                    0 => different.mutation.name = "other",
                    1 => different.expected_revision += 1,
                    2 => different.sql_catalog_admission.?.request.nonce = @splat(3),
                    3 => different.sql_catalog_admission.?.request.head += 1,
                    4 => different.sql_catalog_admission.?.incarnation = @splat(4),
                    5 => different.mutation.description = "changed",
                    else => unreachable,
                }
                try std.testing.expect(!std.mem.eql(u8, &digest, &try boundDigest(alloc, try observation(alloc, different))));
            }
            var invalid = value;
            invalid.sql_catalog_admission = null;
            try std.testing.expectError(error.InvalidCatalogMutation, observation(alloc, invalid));
            invalid = value;
            invalid.sql_catalog_admission.?.incarnation = @splat(0);
            try std.testing.expectError(error.InvalidCatalogMutation, observation(alloc, invalid));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{command});
}

test "SQL command fence catalog hashing streams large canonical definitions with zero heap allocation" {
    const a = std.testing.allocator;
    const payload = try a.alloc(u8, 1024 * 1024);
    defer a.free(payload);
    @memset(payload, 'a');
    const command: Command = .{ .expected_revision = 1, .mutation = .{ .kind = .query_source, .action = .create, .name = "source", .source_json = payload }, .sql_catalog_admission = .{ .request = .{ .nonce = @splat(1), .head = 0 }, .incarnation = @splat(2) } };
    const observed = try observation(std.testing.failing_allocator, command);
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    defer a.free(bytes);
    var reference = std.crypto.hash.sha2.Sha256.init(.{});
    reference.update("antfly:sql-catalog-command:v1");
    reference.update(bytes);
    try std.testing.expectEqualDeep(reference.finalResult(), observed.digest);
    _ = try boundDigest(std.testing.failing_allocator, observed);
    const too_large = try a.alloc(u8, @import("antfly_local_sources").system_catalog_domain.max_command_bytes);
    defer a.free(too_large);
    @memset(too_large, 'a');
    var invalid = command;
    invalid.mutation.source_json = too_large;
    try std.testing.expectError(error.CatalogCommandTooLarge, observation(std.testing.failing_allocator, invalid));
}
