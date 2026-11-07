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
    /// Complete bounded dependent cut for type evolution preparation. Ordinary
    /// cast/name reads never load table schemas or walk dependency edges.
    include_dependents: bool = false,
    prepare_activation: bool = false,
    prepare_retirement: bool = false,
    check_dependents: bool = false,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (.{ "targets", "expected_revision", "reference", "namespace_id", "scope_only" }) |field| {
            try writer.objectField(field);
            try writer.write(@field(self, field));
        }
        inline for (.{ "include_dependents", "prepare_activation", "prepare_retirement", "check_dependents" }) |field| if (@field(self, field)) {
            try writer.objectField(field);
            try writer.write(true);
        };
        try writer.endObject();
    }
    pub fn validate(self: @This()) !void {
        if (self.scope_only and self.include_dependents) return error.InvalidCatalogMutation;
        if ((self.prepare_activation or self.prepare_retirement) and !self.include_dependents) return error.InvalidCatalogMutation;
        if (self.prepare_activation and self.prepare_retirement) return error.InvalidCatalogMutation;
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
    pub const Found = struct {
        scope: usize,
        record: records.Record,
        description: ?[]const u8 = null,
        pub fn jsonStringify(self: @This(), writer: anytype) !void {
            if (self.description) |text| return writer.write(.{ .scope = self.scope, .record = self.record, .description = text });
            return writer.write(.{ .scope = self.scope, .record = self.record });
        }
    };
    pub const Activation = @import("../metadata/schema_validation_publication.zig").OwnerCut;
    activation: ?Activation = null,
    has_dependents: bool = false,
    revision: u64,
    next_id: u64,
    head: u64,
    incarnation: [16]u8,
    scopes: []const Scope,
    found: ?Found,
    dependents: []const @import("antfly_local_sources").common_topology_records.TableRecord = &.{},
    pub const max_dependency_bytes = 16 * 1024 * 1024;
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (.{ "revision", "next_id", "head", "incarnation", "scopes", "found" }) |field| {
            try writer.objectField(field);
            try writer.write(@field(self, field));
        }
        if (self.dependents.len != 0) {
            try writer.objectField("dependents");
            try writer.write(self.dependents);
        }
        if (self.activation) |activation| {
            try writer.objectField("activation");
            try writer.write(activation);
        }
        if (self.has_dependents) {
            try writer.objectField("has_dependents");
            try writer.write(true);
        }
        try writer.endObject();
    }
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
            try domain.validateDescription(found.description);
            if (request.reference) |reference| if (found.record.identity.id != reference.id or found.record.identity.namespace_id != reference.namespace_id) return error.InvalidSqlBackendResponse;
        } else if (request.reference != null) return error.CatalogGenerationChanged;
        if (self.has_dependents and (self.found == null or (!request.check_dependents and !request.include_dependents))) return error.InvalidSqlBackendResponse;
        if (self.dependents.len > @import("named_type_dependencies.zig").max_dependents or
            (self.dependents.len != 0 and (!request.include_dependents or self.found == null))) return error.InvalidSqlBackendResponse;
        if (self.activation) |activation| {
            if (!request.prepare_activation or self.dependents.len == 0 or activation.metadata_group_id == 0 or
                !std.mem.eql(u8, &activation.metadata_incarnation, &std.fmt.bytesToHex(self.incarnation, .lower)) or
                activation.ranges.len == 0 or activation.ranges.len > @import("../metadata/schema_validation_publication.zig").max_owners) return error.InvalidSqlBackendResponse;
            var ordinal: usize = 0;
            for (self.dependents) |table| {
                const start = ordinal;
                while (ordinal < activation.ranges.len and activation.ranges[ordinal].table_id == table.table_id) : (ordinal += 1) {}
                if (ordinal == start) return error.InvalidSqlBackendResponse;
                try @import("../metadata/table_manager.zig").validateCompleteKeyspaceRanges(activation.ranges[start..ordinal]);
            }
            if (ordinal != activation.ranges.len) return error.InvalidSqlBackendResponse;
        } else if (request.prepare_activation and self.dependents.len != 0) return error.InvalidSqlBackendResponse;
        const layouts = @import("antfly_local_sources").sql_catalog.named_layouts;
        const expected_digest = if (self.dependents.len != 0) try layouts.digest(a, self.found.?.record) else @as([32]u8, @splat(0));
        var previous: u64 = 0;
        var total: usize = 0;
        for (self.dependents) |table| {
            if (table.table_id <= previous) return error.InvalidSqlBackendResponse;
            previous = table.table_id;
            total +|= table.schema_json.len;
            total +|= table.read_schema_json.len;
            total +|= table.indexes_json.len;
            if (total > max_dependency_bytes) return error.SqlProgramLimitExceeded;
            const guards = try layouts.guardsForDefinitionsAlloc(a, table.schema_json, table.read_schema_json);
            defer a.free(guards);
            const record = self.found.?.record;
            const matches = for (guards) |guard| {
                if (guard.id != record.identity.id) continue;
                if (guard.namespace_id != record.identity.namespace_id or guard.generation != record.identity.generation or
                    !std.mem.eql(u8, &guard.digest, &expected_digest)) return error.InvalidSqlBackendResponse;
                break true;
            } else false;
            if (!matches) return error.InvalidSqlBackendResponse;
        }
    }
};
pub const BindingsRequest = struct {
    ids: []const u64 = &.{},
    /// Each lookup is an ordered, bounded search path. Definitions are returned
    /// once in the shared directory, not once per referencing column.
    names: []const []const domain.Target = &.{},
    expected_revision: ?u64 = null,
    pub fn validate(self: @This()) !void {
        if (self.ids.len > local.sql_catalog.named_layouts.max_types) return error.InvalidCatalogMutation;
        if (self.names.len > 256 or (self.names.len != 0 and self.ids.len != 0)) return error.InvalidCatalogMutation;
        for (self.names) |path| {
            if (path.len == 0 or path.len > 32) return error.InvalidCatalogMutation;
            for (path) |target| try target.validate();
        }
        var previous: u64 = 0;
        for (self.ids) |id| {
            if (id <= previous) return error.InvalidCatalogMutation;
            previous = id;
        }
    }
};
/// All entries come from one immutable metadata read cut. Scope names are
/// resolved by namespace ID so rename/reuse cannot redirect authorization.
pub const BindingsSnapshot = struct {
    pub const Entry = struct { target: domain.Target, namespace_id: u64, record: records.Record };
    revision: u64,
    next_id: u64,
    incarnation: [16]u8,
    bindings: []const Entry,
    named: []const ?u32 = &.{},
    directory: []const Entry = &.{},
    pub fn validate(self: @This(), a: A, request: BindingsRequest) !void {
        try request.validate();
        if (self.next_id < 3 or self.bindings.len != request.ids.len or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSqlBackendResponse;
        if (request.expected_revision) |revision| if (revision != self.revision) return error.CatalogGenerationChanged;
        if (self.named.len != request.names.len or self.directory.len > self.named.len) return error.InvalidSqlBackendResponse;
        var directory_origins: std.AutoHashMapUnmanaged([16]u8, void) = .empty;
        defer directory_origins.deinit(a);
        var ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer ids.deinit(a);
        const used = try a.alloc(bool, self.directory.len);
        defer a.free(used);
        @memset(used, false);
        for (self.directory) |entry| {
            try entry.target.validate();
            try entry.record.validate(a);
            if (entry.record.identity.id >= self.next_id or entry.namespace_id != entry.record.identity.namespace_id or
                (try ids.getOrPut(a, entry.record.identity.id)).found_existing or
                (try directory_origins.getOrPut(a, entry.record.identity.origin_id)).found_existing) return error.InvalidSqlBackendResponse;
        }
        for (request.names, self.named) |path, position| if (position) |index| {
            if (index >= self.directory.len) return error.InvalidSqlBackendResponse;
            const entry = self.directory[index];
            const matches = for (path) |target| {
                if (std.mem.eql(u8, target.database, entry.target.database) and std.mem.eql(u8, target.namespace, entry.target.namespace) and std.mem.eql(u8, target.table, entry.target.table)) break true;
            } else false;
            if (!matches) return error.InvalidSqlBackendResponse;
            used[index] = true;
        };
        for (used) |present| if (!present) return error.InvalidSqlBackendResponse;
        var origins = std.AutoHashMapUnmanaged([16]u8, void).empty;
        defer origins.deinit(a);
        for (request.ids, self.bindings) |id, entry| {
            try entry.target.validate();
            try entry.record.validate(a);
            if (entry.record.identity.id != id or id >= self.next_id or entry.namespace_id != entry.record.identity.namespace_id) return error.InvalidSqlBackendResponse;
            if ((try origins.getOrPut(a, entry.record.identity.origin_id)).found_existing) return error.InvalidSqlBackendResponse;
        }
    }
};

pub const Mutation = struct {
    admission: outcomes.Admission,
    target: domain.Target,
    namespace_id: u64,
    incarnation: [16]u8,
    operation: union(enum) {
        create_domain: struct { definition: local.sql_catalog.domain_types.Definition, owner: @FieldType(records.Record, "owner") },
        create_enum: struct { labels: []const []const u8, owner: @FieldType(records.Record, "owner") },
        set_domain_not_null: struct { expected: Identity },
        add_enum: struct { expected: Identity, label: []const u8, placement: records.Placement = .end, if_not_exists: bool = false },
        drop: struct { expected: ?Identity, if_exists: bool = false, cascade: bool = false },
        comment: struct { expected: Identity, description: ?[]const u8 },
    },
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try self.target.validate();
        if (self.namespace_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidCatalogMutation;
        switch (self.operation) {
            .create_domain => |create| {
                if (create.definition.base != .builtin) return error.UnsupportedSqlExecution;
                if (create.definition.checks.len > local.sql_catalog.domain_types.max_checks) return error.SqlProgramLimitExceeded;
                if (create.definition.default_expression) |expression| if (expression.len > local.sql_catalog.domain_types.max_expression_bytes) return error.SqlProgramLimitExceeded;
                for (create.definition.checks) |check| {
                    if (check.expression.len > local.sql_catalog.domain_types.max_expression_bytes) return error.SqlProgramLimitExceeded;
                    try validateInputText(check.name);
                }
            },
            .create_enum => |create| {
                if (create.labels.len > records.max_labels) return error.SqlProgramLimitExceeded;
                for (create.labels) |label| try validateInputText(label);
            },
            .set_domain_not_null => |change| try change.expected.validate(),
            .add_enum => |add| {
                try add.expected.validate();
                try validateInputText(add.label);
                switch (add.placement) {
                    .end => {},
                    .before, .after => |label| try validateInputText(label),
                }
            },
            .comment => |comment| {
                try comment.expected.validate();
                try domain.validateDescription(comment.description);
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
    retirement: ?outcomes.RestoreWork = null,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try local.storage_db_relational_integrity_json.write(self, writer);
    }
    pub fn nativeJsonSkipField(self: @This(), comptime name: []const u8) bool {
        return std.mem.eql(u8, name, "retirement") and self.retirement == null;
    }
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try self.target.validate();
        if (self.retirement) |lookup| {
            try lookup.validate();
            if (self.operation != .drop or !std.mem.eql(u8, &lookup.incarnation, &self.incarnation)) return error.InvalidCatalogMutation;
        }
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
        .create_enum, .create_domain => .create,
        .add_enum, .set_domain_not_null, .comment => .alter,
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
    var immutable = request;
    // A work lookup is discovery, not another mutation identity. Its durable
    // progress proof must independently join this original request digest.
    immutable.retirement = null;
    return hash(a, immutable, "antfly:sql-named-type-outcome:v1\x00");
}

pub fn command(request: Mutation) @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand {
    return .{ .version = 6, .expected_revision = 0, .mutation = .{ .kind = .named_type, .action = if (request.operation == .drop) .drop else .create, .database = request.target.database, .namespace = request.target.namespace, .name = request.target.table }, .sql_type = request };
}

/// Immutable type retirement intent carried by the shared staged rewrite.
/// This does not authorize arbitrary schemas or independent type deletion.
pub const Retirement = struct {
    request: Mutation,
    before: records.Record,
    pub fn validate(self: @This(), a: A) !void {
        try self.request.validate();
        try self.before.validate(a);
        if (self.request.operation != .drop or !self.request.operation.drop.cascade) return error.InvalidCatalogMutation;
        const expected = self.request.operation.drop.expected orelse return error.InvalidCatalogMutation;
        if (self.before.identity.id != expected.id or self.before.identity.generation != expected.generation or self.before.identity.namespace_id != self.request.namespace_id) return error.InvalidCatalogMutation;
    }
};
