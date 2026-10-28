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

//! Typed callable publication on the shared native catalog/outcome lane.
const std = @import("std");
const sources = @import("antfly_local_sources");
const domain = sources.system_catalog_domain;
pub const native = sources.sql_catalog.routines;
pub const records = native.records;
pub const outcomes = @import("sequence_outcomes.zig");
pub const Identity = outcomes.Identity;
pub const Result = outcomes.Result;
pub const Outcome = outcomes.Outcome;
pub const SnapshotRequest = struct {
    targets: []const domain.Target,
    expected_revision: ?u64 = null,
    /// Presentation metadata is excluded from hot-path execution capture.
    include_description: bool = false,
    kind: ?records.Kind = null,
    reference: ?records.RoutineReference = null,
    signature: ?[]const u8 = null,
    arguments: ?[]const records.Input = null,
    pub fn validate(self: @This()) !void {
        if ((self.targets.len == 0 and self.reference == null) or self.targets.len > 64 or (self.signature != null and self.arguments != null)) return error.InvalidCatalogMutation;
        if (self.reference) |reference| {
            try reference.validate();
            if (self.targets.len != 0 or self.signature != null or self.arguments != null) return error.InvalidCatalogMutation;
        }
        for (self.targets) |target| try target.validate();
        if (self.signature) |signature| if (signature.len > 4096 or std.mem.indexOfScalar(u8, signature, 0) != null) return error.InvalidCatalogMutation;
        if (self.arguments) |arguments| {
            if (arguments.len > records.max_arguments) return error.InvalidCatalogMutation;
            for (arguments) |argument| if (argument.type) |kind| try kind.validate();
        }
    }
};
pub const Snapshot = struct {
    pub const Scope = struct { target: domain.Target, namespace_id: ?u64, database_exists: bool };
    pub const Found = struct { scope: usize, record: records.Record, description: ?[]const u8 = null };
    revision: u64,
    next_id: u64,
    head: u64,
    incarnation: [16]u8,
    scopes: []const Scope,
    found: ?Found,
    pub fn validate(self: @This(), request: SnapshotRequest) !void {
        try request.validate();
        if (self.next_id < 3 or self.scopes.len != (if (request.reference != null) @as(usize, 1) else request.targets.len) or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSqlBackendResponse;
        if (request.expected_revision) |revision| if (revision != self.revision) return error.CatalogGenerationChanged;
        for (self.scopes[0..request.targets.len], request.targets) |scope, target| {
            if (!std.mem.eql(u8, scope.target.database, target.database) or !std.mem.eql(u8, scope.target.namespace, target.namespace) or !std.mem.eql(u8, scope.target.table, target.table) or
                scope.namespace_id == 0 or (!scope.database_exists and scope.namespace_id != null)) return error.InvalidSqlBackendResponse;
        }
        if (self.found) |found| {
            if (found.scope >= self.scopes.len or self.scopes[found.scope].namespace_id == null or found.record.id >= self.next_id) return error.InvalidSqlBackendResponse;
            try found.record.validate();
            if (request.reference) |reference| if (found.record.id != reference.id or found.record.kind != (request.kind orelse .function) or self.scopes[found.scope].namespace_id != reference.namespace_id) return error.InvalidSqlBackendResponse;
        }
    }
};
pub const Mutation = struct {
    admission: outcomes.Admission,
    target: domain.Target,
    namespace_id: u64,
    incarnation: [16]u8,
    operation: union(enum) {
        publish: struct { kind: records.Kind = .function, definition: records.Definition, replace: bool = false, expected: ?Identity = null },
        drop: struct { kind: records.Kind = .function, signature: []const u8, expected: ?Identity, if_exists: bool = false, cascade: bool = false },
        comment: struct { kind: records.Kind, signature: []const u8, expected: Identity, description: ?[]const u8 },
    },
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try self.target.validate();
        if (self.namespace_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidCatalogMutation;
        switch (self.operation) {
            .publish => |value| {
                try (records.Record{ .kind = value.kind, .id = 1, .generation = 1, .definition = value.definition }).validate();
                if (value.expected) |expected| try expected.validate();
                if (!value.replace and value.expected != null) return error.InvalidCatalogMutation;
            },
            .comment => |value| {
                try value.expected.validate();
                if (value.signature.len > 4096 or std.mem.indexOfScalar(u8, value.signature, 0) != null) return error.InvalidCatalogMutation;
                if (value.description) |text| {
                    if (text.len > 64 * 1024) return error.CatalogCommandTooLarge;
                    if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.InvalidCatalogMutation;
                }
            },
            .drop => |value| {
                if (value.signature.len > 4096 or std.mem.indexOfScalar(u8, value.signature, 0) != null or (!value.if_exists and value.expected == null)) return error.InvalidCatalogMutation;
                if (value.expected) |expected| try expected.validate();
            },
        }
    }
};
pub const OutcomeRequest = struct {
    kind: records.Kind = .function,
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
pub fn digest(a: std.mem.Allocator, request: Mutation) ![32]u8 {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:sql-routine-mutation:v1\x00");
    hash.update(bytes);
    return hash.finalResult();
}
/// The compact observation cannot change target authority while retaining an
/// opaque body digest. The journal key commits both independently.
pub fn boundDigest(a: std.mem.Allocator, request: OutcomeRequest) ![32]u8 {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:sql-routine-bound-outcome:v1\x00");
    hash.update(bytes);
    return hash.finalResult();
}
pub fn observation(a: std.mem.Allocator, request: Mutation) !OutcomeRequest {
    return .{ .kind = switch (request.operation) {
        .publish => |value| value.kind,
        .drop => |value| value.kind,
        .comment => |value| value.kind,
    }, .admission = request.admission, .target = request.target, .namespace_id = request.namespace_id, .incarnation = request.incarnation, .digest = try digest(a, request), .operation = switch (request.operation) {
        .publish => |value| if (value.replace) .alter else .create,
        .drop => .drop,
        .comment => .alter,
    } };
}
pub fn command(request: Mutation) @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand {
    return .{ .version = 4, .expected_revision = 0, .mutation = .{ .kind = .routine, .action = commandAction(request), .database = request.target.database, .namespace = request.target.namespace, .name = request.target.table }, .sql_routine = request };
}

/// One authoritative envelope mapping shared by producers and native apply.
pub fn commandAction(request: Mutation) domain.Action {
    return switch (request.operation) {
        .publish => .create,
        .drop => .drop,
        .comment => .comment,
    };
}

pub fn storageSignature(a: std.mem.Allocator, kind: ?records.Kind, signature: []const u8) ![]const u8 {
    return if (kind == .operator) try std.mem.concat(a, u8, &.{ "operator:", signature }) else signature;
}
pub fn inputSignature(kind: ?records.Kind, signature: []const u8) ?[]const u8 {
    const operator = std.mem.startsWith(u8, signature, "operator:");
    if (operator != (kind == .operator)) return null;
    return if (operator) signature[9..] else signature;
}
