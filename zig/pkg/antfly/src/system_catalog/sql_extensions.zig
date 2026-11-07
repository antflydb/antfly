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

//! Database-scoped SQL installation of owned, immutable Antfly WASM packages.
//! The planner emits one ordinary catalog Delta: package identity, routines,
//! membership, dependency edges and comments commit together. There is no
//! separately committed global REST install or executable SQL migration text.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const records = local.sql_catalog.routines.records;
const package_domain = @import("../extensions/mod.zig");
const sql_package = package_domain.sql_package;
const storage = @import("storage.zig");
pub const outcomes = @import("sequence_outcomes.zig");
pub const Outcome = outcomes.Outcome;
pub const Result = outcomes.Result;
pub const Identity = outcomes.Identity;
const A = std.mem.Allocator;
pub const max_installed = 128;
pub const max_members = 256;
pub const max_cascade_objects = 1024;

pub const Member = domain.extensions.Member;
pub const Dependency = domain.extensions.Dependency;
pub const Record = domain.extensions.Record;

pub const SnapshotRequest = struct {
    database: []const u8,
    name: []const u8,
    namespace: []const u8,
    pub fn validate(self: @This()) !void {
        try domain.validateName(self.database);
        try domain.validateName(self.name);
        try domain.validateName(self.namespace);
    }
};
pub const Snapshot = struct {
    revision: u64,
    head: u64,
    incarnation: [16]u8,
    database_id: u64,
    namespace_id: u64,
    installed: ?struct { record: Record, description: ?[]const u8 },
    /// Exact package registry from the same native read transaction. Explicit
    /// size limits apply before decoding; request metadata is not availability.
    packages: []const package_domain.PackageManifest,
    controls: []const sql_package.PackageControl,
    installed_extensions: []const Dependency,
    pub fn validate(self: @This(), request: SnapshotRequest) !void {
        try request.validate();
        if (self.database_id == 0 or self.namespace_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSqlBackendResponse;
        try (sql_package.Registry{ .packages = self.packages, .controls = self.controls, .control_receipts = false }).validateControls();
        if (self.installed_extensions.len > max_installed) return error.InvalidSqlBackendResponse;
        for (self.installed_extensions, 0..) |installed, i| {
            if (installed.id == 0) return error.InvalidSqlBackendResponse;
            try domain.validateName(installed.name);
            for (self.installed_extensions[0..i]) |previous| if (previous.id == installed.id or std.mem.eql(u8, previous.name, installed.name)) return error.InvalidSqlBackendResponse;
        }
        if (self.installed) |installed| {
            try installed.record.validate();
            if (installed.record.database_id != self.database_id or !std.mem.eql(u8, installed.record.package_name, request.name)) return error.InvalidSqlBackendResponse;
        }
    }
};
pub const Mutation = struct {
    admission: outcomes.Admission,
    incarnation: [16]u8,
    database: []const u8,
    database_id: u64,
    registry_digest: [32]u8,
    name: []const u8,
    /// Only existing dependencies skipped by artifact preflight are pinned.
    /// Unrelated extension DDL does not invalidate an installation.
    dependency_proofs: []const Dependency = &.{},
    operation: union(enum) {
        install: struct { namespace: []const u8, schema_explicit: bool, version: ?[]const u8, if_not_exists: bool, cascade: bool, owner: @FieldType(records.Definition, "owner") },
        update: struct { version: ?[]const u8, expected: Identity },
        drop: struct { expected: ?Identity, if_exists: bool, cascade: bool },
        comment: struct { expected: Identity, description: ?[]const u8 },
    },
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try domain.validateName(self.database);
        try domain.validateName(self.name);
        if (self.database_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.registry_digest, 0)) return error.InvalidCatalogMutation;
        if (self.dependency_proofs.len > max_installed or (self.operation != .install and self.dependency_proofs.len != 0)) return error.InvalidCatalogMutation;
        for (self.dependency_proofs, 0..) |proof, i| {
            if (proof.id == 0) return error.InvalidCatalogMutation;
            try domain.validateName(proof.name);
            for (self.dependency_proofs[0..i]) |previous| if (previous.id == proof.id or std.mem.eql(u8, previous.name, proof.name)) return error.InvalidCatalogMutation;
        }
        switch (self.operation) {
            .install => |value| {
                try domain.validateName(value.namespace);
                if (value.version) |version| try validateVersion(version);
                try (records.Definition{ .arguments = &.{}, .result = .{ .scalar = .int64 }, .body = "SELECT 1", .owner = value.owner }).validate();
            },
            .update => |value| {
                try value.expected.validate();
                if (value.version) |version| try validateVersion(version);
            },
            .drop => |value| {
                if (!value.if_exists and value.expected == null) return error.InvalidCatalogMutation;
                if (value.expected) |expected| try expected.validate();
            },
            .comment => |value| {
                try value.expected.validate();
                if (value.description) |description| try domain.object_annotations.validateDescription(description);
            },
        }
    }
};
fn validateVersion(version: []const u8) !void {
    if (version.len == 0 or version.len > 128 or !std.unicode.utf8ValidateSlice(version) or std.mem.indexOfScalar(u8, version, 0) != null) return error.InvalidCatalogMutation;
}
pub const OutcomeRequest = struct {
    admission: outcomes.Admission,
    incarnation: [16]u8,
    database: []const u8,
    database_id: u64,
    name: []const u8,
    digest: [32]u8,
    operation: local.sql_catalog.CatalogOperationReceipt.Operation,
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try domain.validateName(self.database);
        try domain.validateName(self.name);
        if (self.database_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidCatalogMutation;
    }
};
pub fn observation(a: A, request: Mutation) !OutcomeRequest {
    try request.validate();
    return .{ .admission = request.admission, .incarnation = request.incarnation, .database = request.database, .database_id = request.database_id, .name = request.name, .digest = try hash(a, "antfly:sql-extension-mutation:v1", request), .operation = switch (request.operation) {
        .install => .create,
        .drop => .drop,
        .update, .comment => .alter,
    } };
}
pub fn boundDigest(a: A, request: OutcomeRequest) ![32]u8 {
    try request.validate();
    return hash(a, "antfly:sql-extension-outcome:v1", request);
}
pub fn packageRegistryDigest(a: A, packages: []const package_domain.PackageManifest, controls: []const sql_package.PackageControl) ![32]u8 {
    const compact = try a.alloc(sql_package.PackageControl, controls.len);
    defer a.free(compact);
    for (controls, 0..) |control, i| {
        compact[i] = control;
        compact[i].promotions = &.{};
    }
    return hash(a, "antfly:sql-extension-package-registry:v2", .{ .packages = packages, .controls = compact });
}
fn hash(a: A, prefix: []const u8, value: anytype) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var sha = std.crypto.hash.sha2.Sha256.init(.{});
    sha.update(prefix);
    sha.update(&.{0});
    sha.update(bytes);
    return sha.finalResult();
}
pub fn commandAction(request: Mutation) domain.Action {
    return switch (request.operation) {
        .install => .create,
        .update => .alter,
        .drop => .drop,
        .comment => .comment,
    };
}
pub fn command(request: Mutation) @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand {
    return .{ .version = 9, .expected_revision = 0, .mutation = .{ .kind = .extension, .action = commandAction(request), .database = request.database, .name = request.name }, .sql_extension = request };
}

pub const Plan = struct { delta: domain.Delta, result: Result };
/// One bounded preparation before any native write. The caller applies the
/// complete delta and appends the exact outcome in the same store transaction.
pub fn plan(a: A, view: storage.View, registry: sql_package.Registry, request: Mutation) !Plan {
    try request.validate();
    try registry.validateControls();
    if (!std.mem.eql(u8, &try packageRegistryDigest(a, registry.packages, registry.controls), &request.registry_digest)) return error.CatalogGenerationChanged;
    const database = (try view.lookup(.database, 0, request.database)) orelse return error.DatabaseNotFound;
    if (database.id != request.database_id) return error.CatalogGenerationChanged;
    for (request.dependency_proofs) |proof| {
        const installed = (try view.lookup(.extension, database.id, proof.name)) orelse return error.CatalogGenerationChanged;
        if (installed.id != proof.id) return error.CatalogGenerationChanged;
    }
    var builder: Builder = .{ .a = a, .view = view, .registry = registry, .database = database, .next_id = view.meta.next_id, .installed_count = (try view.childrenBounded(.extension, database.id, max_installed, 2 * 1024 * 1024)).len };
    const previous = try view.lookup(.extension, database.id, request.name);
    var identity: ?Identity = null;
    var removed: usize = 0;
    switch (request.operation) {
        .install => |intent| {
            if (previous) |resource| {
                if (!intent.if_not_exists) return error.SqlDuplicateExtension;
                const old = try Record.decode(a, resource);
                return .{ .delta = .{ .upserts = &.{}, .removes = &.{}, .next_id = view.meta.next_id }, .result = .{ .revision = view.meta.revision, .identity = .{ .id = old.id, .generation = old.generation }, .removed = 0, .no_op = true } };
            }
            const record = try builder.install(request.name, intent.version, intent.namespace, intent.schema_explicit, intent.cascade, intent.owner, 0);
            identity = .{ .id = record.id, .generation = record.generation };
        },
        .update => |intent| {
            const resource = previous orelse return error.SqlUndefinedExtension;
            const old = try Record.decode(a, resource);
            try requireExpected(old, intent.expected);
            const path = try registry.updatePath(a, request.name, old.package_version, intent.version);
            if (path.len == 0) return .{ .delta = .{ .upserts = &.{}, .removes = &.{}, .next_id = view.meta.next_id }, .result = .{ .revision = view.meta.revision, .identity = intent.expected, .removed = 0, .no_op = true } };
            var current = old;
            for (path) |target| current = try builder.publish(target, old.namespace_id, old.owner, current, resource.description);
            identity = .{ .id = current.id, .generation = current.generation };
        },
        .comment => |intent| {
            var resource = previous orelse return error.SqlUndefinedExtension;
            const old = try Record.decode(a, resource);
            try requireExpected(old, intent.expected);
            resource.description = domain.object_annotations.normalized(intent.description);
            try builder.upserts.append(a, resource);
            identity = intent.expected;
        },
        .drop => |intent| {
            if (previous) |resource| {
                const old = try Record.decode(a, resource);
                try requireExpected(old, intent.expected orelse return error.CatalogGenerationChanged);
                try builder.retire(resource, intent.cascade, 0);
                removed = 1;
            } else {
                if (intent.expected != null) return error.CatalogGenerationChanged;
                if (!intent.if_exists) return error.SqlUndefinedExtension;
                return .{ .delta = .{ .upserts = &.{}, .removes = &.{}, .next_id = view.meta.next_id }, .result = .{ .revision = view.meta.revision, .identity = null, .removed = 0, .no_op = true } };
            }
        },
    }
    const delta: domain.Delta = .{ .upserts = builder.upserts.items, .removes = builder.removes.items, .next_id = builder.next_id };
    return .{ .delta = delta, .result = .{ .revision = std.math.add(u64, view.meta.revision, 1) catch return error.CatalogRevisionExhausted, .identity = identity, .removed = removed } };
}
fn requireExpected(record: Record, identity: Identity) !void {
    if (record.id != identity.id or record.generation != identity.generation) return error.CatalogGenerationChanged;
}

const Builder = struct {
    a: A,
    view: storage.View,
    registry: sql_package.Registry,
    database: domain.Resource,
    next_id: u64,
    installed_count: usize,
    upserts: std.ArrayList(domain.Resource) = .empty,
    removes: std.ArrayList(domain.Resource) = .empty,
    visiting: std.StringHashMapUnmanaged(void) = .empty,

    fn allocate(self: *@This()) !u64 {
        const id = self.next_id;
        self.next_id = std.math.add(u64, id, 1) catch return error.CatalogRevisionExhausted;
        return id;
    }
    fn lookup(self: *@This(), name: []const u8) !?domain.Resource {
        for (self.upserts.items) |resource| if (resource.kind == .extension and std.mem.eql(u8, name, resource.name)) return resource;
        return self.view.lookup(.extension, self.database.id, name);
    }
    fn install(self: *@This(), name: []const u8, version: ?[]const u8, namespace: []const u8, schema_explicit: bool, cascade: bool, owner: @FieldType(records.Definition, "owner"), depth: usize) anyerror!Record {
        if (depth >= max_installed or self.upserts.items.len >= max_cascade_objects) return error.SqlProgramLimitExceeded;
        if (self.visiting.contains(name)) return error.SqlExtensionDependencyCycle;
        if (try self.lookup(name)) |resource| return Record.decode(self.a, resource);
        try self.visiting.put(self.a, name, {});
        defer _ = self.visiting.remove(name);
        const package = try self.registry.select(name, version);
        const control = package.sql.?;
        if (schema_explicit and control.schema != null and !cascade and !std.mem.eql(u8, control.schema.?, namespace)) return error.SqlExtensionSchemaMismatch;
        const schema = if (control.schema) |fixed| fixed else namespace;
        const target_namespace = try self.view.namespaceFor(self.database.name, schema);
        for (package.dependencies) |dependency| {
            if (dependency.optional) continue;
            if (try self.lookup(dependency.name) != null) continue;
            if (!cascade) return error.SqlRequiredExtensionNotInstalled;
            _ = try self.install(dependency.name, null, namespace, schema_explicit, true, owner, depth + 1);
        }
        if (self.installed_count == max_installed) return error.SqlProgramLimitExceeded;
        self.installed_count += 1;
        return self.publish(package, target_namespace.id, owner, null, if (package.description.len == 0) null else package.description);
    }
    fn objectById(self: *@This(), kind: domain.Kind, id: u64) !?domain.Resource {
        for (self.removes.items) |resource| if (resource.kind == kind and resource.id == id) return null;
        for (self.upserts.items) |resource| if (resource.kind == kind and resource.id == id) return resource;
        return self.view.byId(kind, id);
    }
    fn routineBySignature(self: *@This(), namespace: u64, name: []const u8, signature: []const u8) !?struct { value: domain.Resource } {
        for (self.upserts.items) |resource| if (resource.kind == .routine and resource.parent_id == namespace and std.mem.eql(u8, resource.name, name) and std.mem.eql(u8, resource.signature orelse "", signature)) return .{ .value = resource };
        const original = (try storage.findFunction(self.a, self.view.txn, self.view.group_id, namespace, name, signature)) orelse return null;
        for (self.removes.items) |resource| if (resource.kind == .routine and resource.id == original.value.id) return null;
        return .{ .value = original.value };
    }
    const Visit = enum { visiting, done };
    fn requireAcyclic(self: *@This(), record: Record, visited: *std.AutoHashMapUnmanaged(u64, Visit)) anyerror!void {
        const entry = try visited.getOrPut(self.a, record.id);
        if (entry.found_existing) {
            if (entry.value_ptr.* == .visiting) return error.SqlExtensionDependencyCycle;
            return;
        }
        if (visited.count() > max_installed) return error.SqlProgramLimitExceeded;
        entry.value_ptr.* = .visiting;
        for (record.dependencies) |dependency| {
            const resource = (try self.lookup(dependency.name)) orelse return error.InvalidSqlExtensionRecord;
            if (resource.id != dependency.id) return error.InvalidSqlExtensionRecord;
            try self.requireAcyclic(try Record.decode(self.a, resource), visited);
        }
        // Recursive insertion can relocate map entries; reacquire by identity.
        visited.getPtr(record.id).?.* = .done;
    }
    fn publish(self: *@This(), package: package_domain.PackageManifest, namespace_id: u64, owner: @FieldType(records.Definition, "owner"), previous: ?Record, description: ?[]const u8) !Record {
        const control = package.sql.?;
        const dependencies = try self.a.alloc(Dependency, package.dependencies.len);
        var dependency_count: usize = 0;
        for (package.dependencies) |dependency| {
            const resource = (try self.lookup(dependency.name)) orelse {
                if (dependency.optional) continue;
                return error.SqlRequiredExtensionNotInstalled;
            };
            dependencies[dependency_count] = .{ .id = resource.id, .name = resource.name };
            dependency_count += 1;
        }
        const id = if (previous) |old| old.id else try self.allocate();
        const members = try self.a.alloc(Member, control.exports.len);
        for (control.exports, members) |declaration, *member| {
            const signature = try declaration.signatureAlloc(self.a);
            const prior: ?Member = if (previous) |old| for (old.members) |candidate| {
                if (std.mem.eql(u8, candidate.name, declaration.name) and std.mem.eql(u8, candidate.signature, signature)) break candidate;
            } else null else null;
            const collision = try self.routineBySignature(namespace_id, declaration.name, signature);
            if (collision) |resource| if (prior == null or prior.?.id != resource.value.id) return error.SqlDuplicateFunction;
            const runtime = try sql_package.wasmRuntime(package, declaration.runtime);
            const artifact = try sql_package.wasmArtifact(package, runtime.artifact);
            const routine_id = if (prior) |old| old.id else try self.allocate();
            const generation = if (prior) |old| std.math.add(u32, old.generation, 1) catch return error.CatalogGenerationExhausted else 1;
            const routine: records.Record = .{ .id = routine_id, .generation = generation, .definition = .{
                .arguments = declaration.arguments,
                .result = declaration.result,
                .language = .wasm,
                .body = "",
                .strict = declaration.strict,
                .volatility = declaration.volatility,
                .owner = owner,
                .wasm = .{ .package_name = package.name, .package_version = package.version, .package_digest = try sql_package.digest(package.digest), .artifact = artifact.path, .artifact_digest = try sql_package.digest(artifact.digest), .runtime_name = runtime.name, .entrypoint = "antfly_extension_call_tool", .export_name = declaration.export_name },
            } };
            try routine.validate();
            if (collision) |resource| {
                const old = (try records.Record.decode(self.a, resource.value.source_json)).value;
                if (!std.meta.eql(old.definition.result, routine.definition.result)) return error.SqlFunctionReturnTypeChanged;
            }
            try self.put(.{ .kind = .routine, .id = routine_id, .parent_id = namespace_id, .name = declaration.name, .signature = signature, .source_json = try routine.encode(self.a), .description = if (collision) |resource| resource.value.description else null });
            member.* = .{ .id = routine_id, .generation = generation, .name = declaration.name, .signature = signature };
        }
        if (previous) |old| for (old.members) |member| {
            const retained = for (members) |current| {
                if (current.id == member.id) break true;
            } else false;
            if (retained) continue;
            const resource = (try self.objectById(.routine, member.id)) orelse return error.InvalidSqlExtensionRecord;
            const users = try storage.functionDependents(self.a, self.view.txn, self.view.group_id, member.id, max_cascade_objects);
            for (users) |user| if (!owns(old, user.id)) return error.SqlDependentExtensionObject;
            try self.remove(resource);
        };
        const record: Record = .{ .id = id, .generation = if (previous) |old| std.math.add(u64, old.generation, 1) catch return error.CatalogGenerationExhausted else 1, .database_id = self.database.id, .namespace_id = namespace_id, .package_name = package.name, .package_version = package.version, .package_digest = try sql_package.digest(package.digest), .relocatable = package.relocatable, .owner = owner, .members = members, .dependencies = dependencies[0..dependency_count] };
        try self.put(.{ .kind = .extension, .id = id, .parent_id = self.database.id, .name = package.name, .source_json = try record.encode(self.a), .description = description });
        var visited: std.AutoHashMapUnmanaged(u64, Visit) = .empty;
        defer visited.deinit(self.a);
        try self.requireAcyclic(record, &visited);
        return record;
    }
    fn put(self: *@This(), resource: domain.Resource) !void {
        for (self.upserts.items) |*prior| if (prior.kind == resource.kind and prior.id == resource.id) {
            prior.* = resource;
            return;
        };
        if (self.upserts.items.len >= max_cascade_objects) return error.SqlProgramLimitExceeded;
        try self.upserts.append(self.a, resource);
    }
    fn remove(self: *@This(), resource: domain.Resource) !void {
        for (self.removes.items) |prior| if (prior.kind == resource.kind and prior.id == resource.id) return;
        for (self.upserts.items, 0..) |prior, i| if (prior.kind == resource.kind and prior.id == resource.id) {
            _ = self.upserts.orderedRemove(i);
            break;
        };
        // A transient object created in an intermediate declarative version
        // never existed in the predecessor and must not enter the final delta.
        const original = (try self.view.byId(resource.kind, resource.id)) orelse return;
        if (self.removes.items.len >= max_cascade_objects) return error.SqlProgramLimitExceeded;
        try self.removes.append(self.a, original);
    }
    fn retireCallable(self: *@This(), resource: domain.Resource, depth: usize) anyerror!void {
        if (depth >= max_cascade_objects) return error.SqlProgramLimitExceeded;
        for (self.removes.items) |prior| if (prior.kind == resource.kind and prior.id == resource.id) return;
        if (resource.kind == .routine) {
            if (try storage.sqlExtensionMemberOwner(self.a, self.view.txn, self.view.group_id, resource.id)) |owner| {
                const extension = (try self.view.byId(.extension, owner)) orelse return error.InvalidSqlExtensionRecord;
                return self.retire(extension, true, depth + 1);
            }
            try self.remove(resource);
            const users = try storage.functionDependents(self.a, self.view.txn, self.view.group_id, resource.id, max_cascade_objects);
            for (users) |user| try self.retireCallable(user, depth + 1);
        } else try self.remove(resource);
    }
    fn retire(self: *@This(), resource: domain.Resource, cascade: bool, depth: usize) anyerror!void {
        if (depth >= max_installed) return error.SqlProgramLimitExceeded;
        for (self.removes.items) |prior| if (prior.kind == .extension and prior.id == resource.id) return;
        const record = try Record.decode(self.a, resource);
        try self.remove(resource);
        const extensions = try self.view.childrenBounded(.extension, self.database.id, max_installed, 2 * 1024 * 1024);
        for (extensions) |candidate| {
            if (candidate.id == resource.id) continue;
            const dependent = try Record.decode(self.a, candidate);
            for (dependent.dependencies) |dependency| if (dependency.id == resource.id) {
                if (!cascade) return error.SqlDependentExtensionObject;
                try self.retire(candidate, true, depth + 1);
                break;
            };
        }
        for (record.members) |member| {
            const object = (try self.view.byId(.routine, member.id)) orelse return error.InvalidSqlExtensionRecord;
            const routine = (try records.Record.decode(self.a, object.source_json)).value;
            if (routine.generation != member.generation or object.parent_id != record.namespace_id or !std.mem.eql(u8, object.name, member.name) or !std.mem.eql(u8, object.signature orelse return error.InvalidSqlExtensionRecord, member.signature)) return error.InvalidSqlExtensionRecord;
            const users = try storage.functionDependents(self.a, self.view.txn, self.view.group_id, member.id, max_cascade_objects);
            for (users) |user| {
                if (owns(record, user.id)) continue;
                if (!cascade) return error.SqlDependentExtensionObject;
                try self.retireCallable(user, depth + 1);
            }
            try self.remove(object);
        }
    }
};
fn owns(record: Record, id: u64) bool {
    for (record.members) |member| if (member.id == id) return true;
    return false;
}
