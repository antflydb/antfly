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

//! Database-scoped cast publication on the existing catalog transaction and
//! bounded outcome journal. Search path applies only to the support function.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const records = local.sql_catalog.casts;
pub const outcomes = @import("sequence_outcomes.zig");
pub const Identity = outcomes.Identity;
pub const Result = outcomes.Result;
pub const Outcome = outcomes.Outcome;
pub const SnapshotRequest = struct {
    database: []const u8,
    database_id: ?u64 = null,
    pair: records.Pair,
    expected_revision: ?u64 = null,
    expected: ?Identity = null,
    pub fn validate(self: @This()) !void {
        try local.system_catalog_domain.validateName(self.database);
        try self.pair.validate();
        if (self.database_id == 0) return error.InvalidCatalogMutation;
        if (self.expected) |identity| try identity.validate();
    }
};
pub const Snapshot = struct {
    revision: u64,
    next_id: u64,
    head: u64,
    incarnation: [16]u8,
    database_id: ?u64,
    database: ?[]const u8,
    record: ?records.Record,
    pub fn validate(self: @This(), request: SnapshotRequest) !void {
        try request.validate();
        if ((self.database_id == null) != (self.database == null)) return error.InvalidSqlBackendResponse;
        if (self.database) |name| {
            try local.system_catalog_domain.validateName(name);
            if (request.database_id == null and !std.mem.eql(u8, name, request.database)) return error.InvalidSqlBackendResponse;
        }
        if (request.database_id) |id| if (self.database_id != id) return error.CatalogGenerationChanged;
        if (self.next_id < 3 or self.database_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSqlBackendResponse;
        if (request.expected_revision) |revision| if (revision != self.revision) return error.CatalogGenerationChanged;
        if (self.record) |record| {
            try record.validate();
            if (record.id >= self.next_id or record.database_id != self.database_id or !std.meta.eql(record.definition.pair, request.pair)) return error.InvalidSqlBackendResponse;
            if (request.expected) |expected| if (expected.id != record.id or expected.generation != record.generation) return error.CatalogGenerationChanged;
        } else if (request.expected != null) return error.CatalogGenerationChanged;
    }
};
pub const Mutation = struct {
    admission: outcomes.Admission,
    database: []const u8,
    database_id: u64,
    incarnation: [16]u8,
    operation: union(enum) {
        create: records.Definition,
        drop: struct { pair: records.Pair, expected: ?Identity, if_exists: bool = false },
    },
    pub fn pair(self: @This()) records.Pair {
        return switch (self.operation) {
            .create => |definition| definition.pair,
            .drop => |drop| drop.pair,
        };
    }
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try local.system_catalog_domain.validateName(self.database);
        if (self.database_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidCatalogMutation;
        switch (self.operation) {
            .create => |definition| try definition.validate(),
            .drop => |drop| {
                try drop.pair.validate();
                if (drop.expected) |expected| try expected.validate() else if (!drop.if_exists) return error.InvalidCatalogMutation;
            },
        }
    }
};
pub const OutcomeRequest = struct {
    admission: outcomes.Admission,
    database: []const u8,
    database_id: u64,
    pair: records.Pair,
    incarnation: [16]u8,
    digest: [32]u8,
    operation: enum { create, drop },
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try local.system_catalog_domain.validateName(self.database);
        try self.pair.validate();
        if (self.database_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidCatalogMutation;
    }
};
fn hash(a: std.mem.Allocator, value: anytype, domain: []const u8) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var digest = std.crypto.hash.sha2.Sha256.init(.{});
    digest.update(domain);
    digest.update(bytes);
    return digest.finalResult();
}
pub fn observation(a: std.mem.Allocator, request: Mutation) !OutcomeRequest {
    try request.validate();
    return .{ .admission = request.admission, .database = request.database, .database_id = request.database_id, .pair = request.pair(), .incarnation = request.incarnation, .digest = try hash(a, request, "antfly:sql-cast-mutation:v1\x00"), .operation = if (request.operation == .create) .create else .drop };
}
pub fn boundDigest(a: std.mem.Allocator, request: OutcomeRequest) ![32]u8 {
    try request.validate();
    return hash(a, request, "antfly:sql-cast-outcome:v1\x00");
}
pub fn command(a: std.mem.Allocator, request: Mutation) !@import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand {
    return .{ .version = 5, .expected_revision = 0, .mutation = .{ .kind = .cast, .action = if (request.operation == .create) .create else .drop, .database = request.database, .name = try request.pair().keyAlloc(a) }, .sql_cast = request };
}
