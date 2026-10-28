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

//! Namespace-owned type publication on the shared atomic catalog and outcome
//! journal. Requests own intent; snapshots retain immutable type identities.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
pub const records = local.sql_catalog.named_types;
pub const outcomes = @import("sequence_outcomes.zig");
pub const Identity = outcomes.Identity;
pub const Result = outcomes.Result;
pub const Outcome = outcomes.Outcome;
const A = std.mem.Allocator;
fn validateInputText(text: []const u8) !void {
    if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.InvalidCatalogMutation;
}
pub const Reference = struct {
    id: u64,
    namespace_id: u64,
    pub fn validate(self: @This()) !void {
        if (self.id == 0 or self.namespace_id == 0) return error.InvalidCatalogMutation;
    }
};
pub const SnapshotRequest = struct {
    targets: []const domain.Target,
    expected_revision: ?u64 = null,
    reference: ?Reference = null,
    namespace_id: ?u64 = null,
    scope_only: bool = false,
    pub fn validate(self: @This()) !void {
        if (self.reference) |reference| {
            try reference.validate();
            if (self.targets.len != 0 or self.namespace_id != null or self.scope_only) return error.InvalidCatalogMutation;
        } else if (self.targets.len == 0 or self.targets.len > 32) return error.InvalidCatalogMutation;
        if (self.namespace_id) |id| if (id == 0 or self.targets.len != 1) return error.InvalidCatalogMutation;
        for (self.targets) |target| try target.validate();
    }
};
pub const Snapshot = struct {
    pub const Scope = struct { target: domain.Target, namespace_id: ?u64, database_exists: bool };
    pub const Found = struct { scope: usize, record: records.Record };
    revision: u64,
    next_id: u64,
    head: u64,
    incarnation: [16]u8,
    scopes: []const Scope,
    found: ?Found,
    pub fn validate(self: @This(), a: A, request: SnapshotRequest) !void {
        try request.validate();
        if (self.next_id < 3 or self.scopes.len != (if (request.reference != null) @as(usize, 1) else request.targets.len) or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSqlBackendResponse;
        if (request.expected_revision) |revision| if (revision != self.revision) return error.CatalogGenerationChanged;
        if (request.reference == null) for (self.scopes, request.targets) |scope, target| {
            if (!std.mem.eql(u8, scope.target.table, target.table)) return error.InvalidSqlBackendResponse;
            if (request.namespace_id) |id| {
                if (scope.namespace_id != id) return error.CatalogGenerationChanged;
            } else if (!std.mem.eql(u8, scope.target.database, target.database) or !std.mem.eql(u8, scope.target.namespace, target.namespace)) return error.InvalidSqlBackendResponse;
        };
        for (self.scopes) |scope| {
            try scope.target.validate();
            if (scope.namespace_id == 0 or (!scope.database_exists and scope.namespace_id != null)) return error.InvalidSqlBackendResponse;
        }
        if (request.scope_only and self.found != null) return error.InvalidSqlBackendResponse;
        if (self.found) |found| {
            if (found.scope >= self.scopes.len or found.record.identity.id >= self.next_id or self.scopes[found.scope].namespace_id != found.record.identity.namespace_id) return error.InvalidSqlBackendResponse;
            try found.record.validate(a);
            if (request.reference) |reference| if (found.record.identity.id != reference.id or found.record.identity.namespace_id != reference.namespace_id) return error.InvalidSqlBackendResponse;
        } else if (request.reference != null) return error.CatalogGenerationChanged;
    }
};
pub const Mutation = struct {
    admission: outcomes.Admission,
    target: domain.Target,
    namespace_id: u64,
    incarnation: [16]u8,
    operation: union(enum) {
        create_enum: struct { labels: []const []const u8, owner: @FieldType(records.Record, "owner") },
        add_enum: struct { expected: Identity, label: []const u8, placement: records.Placement = .end, if_not_exists: bool = false },
        drop: struct { expected: ?Identity, if_exists: bool = false, cascade: bool = false },
    },
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try self.target.validate();
        if (self.namespace_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidCatalogMutation;
        switch (self.operation) {
            .create_enum => |create| {
                if (create.labels.len > records.max_labels) return error.SqlProgramLimitExceeded;
                for (create.labels) |label| try validateInputText(label);
            },
            .add_enum => |add| {
                try add.expected.validate();
                try validateInputText(add.label);
                switch (add.placement) {
                    .end => {},
                    .before, .after => |label| try validateInputText(label),
                }
            },
            .drop => |drop| {
                if (drop.expected) |expected| try expected.validate() else if (!drop.if_exists) return error.InvalidCatalogMutation;
            },
        }
    }
};
pub const OutcomeRequest = struct {
    admission: outcomes.Admission,
    target: domain.Target,
    namespace_id: u64,
    incarnation: [16]u8,
    digest: [32]u8,
    operation: enum { create, alter, drop },
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try self.target.validate();
        if (self.namespace_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidCatalogMutation;
    }
};
fn hash(a: A, value: anytype, label: []const u8) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var digest = std.crypto.hash.sha2.Sha256.init(.{});
    digest.update(label);
    digest.update(bytes);
    return digest.finalResult();
}
pub fn observation(a: A, request: Mutation) !OutcomeRequest {
    try request.validate();
    return .{ .admission = request.admission, .target = request.target, .namespace_id = request.namespace_id, .incarnation = request.incarnation, .digest = try hash(a, request, "antfly:sql-named-type-mutation:v1\x00"), .operation = switch (request.operation) {
        .create_enum => .create,
        .add_enum => .alter,
        .drop => .drop,
    } };
}
pub fn originId(request: Mutation) [16]u8 {
    var digest = std.crypto.hash.sha2.Sha256.init(.{});
    digest.update("antfly:sql-type-origin:v1\x00");
    digest.update(&request.incarnation);
    digest.update(&request.admission.nonce);
    return digest.finalResult()[0..16].*;
}
pub fn boundDigest(a: A, request: OutcomeRequest) ![32]u8 {
    try request.validate();
    return hash(a, request, "antfly:sql-named-type-outcome:v1\x00");
}

pub fn command(request: Mutation) @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand {
    return .{ .version = 6, .expected_revision = 0, .mutation = .{ .kind = .named_type, .action = if (request.operation == .drop) .drop else .create, .database = request.target.database, .namespace = request.target.namespace, .name = request.target.table }, .sql_type = request };
}
