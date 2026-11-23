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

//! Provider-mediated relabeling on the existing native catalog journal. Labels
//! are attached to immutable resources, so native DROP/cascade, checkpoints,
//! portable catalog export and restore use the same identity/dependency cut.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
pub const labels = domain.security_labels;
const storage = @import("storage.zig");
const catalog = @import("catalog_mutations.zig");
const auth = @import("authorization.zig");
const Command = @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand;
const A = std.mem.Allocator;
pub const command_version: u16 = 13;
pub fn decoderVersion(kernel: bool) u16 {
    const protocol = @import("../metadata/topology_protocol.zig");
    return if (kernel) protocol.sql_selinux_approval_version else protocol.sql_security_labels_version;
}

pub fn applyAuthorityUpdate(a: A, txn: *local.storage_docstore.DocStore.Txn, group: u64, incarnation: [16]u8, bytes: []const u8) !void {
    const Update = storage.SelinuxAuthorityUpdate;
    if (bytes.len > Update.max_bytes) return error.InvalidMetadataTransitionEncoding;
    const parsed = try std.json.parseFromSlice(Update, a, bytes, .{});
    defer parsed.deinit();
    if (!std.meta.eql(parsed.value.incarnation, incarnation)) return;
    try storage.applySelinuxAuthority(a, txn, group, parsed.value);
}

pub const Intent = struct {
    target: domain.Target,
    class: labels.Class,
    signature: ?[]const u8 = null,
    read_revision: ?u64 = null,
    expected_table_id: ?u64 = null,
    provider: ?[]const u8 = null,
    label: ?[]const u8,
    actor: auth.Actor,
    /// Bound only by trusted SQL ingress after a current scoped native-admin
    /// check; the existing private setting_admin envelope protects this field.
    native_administrator: bool,

    pub fn validate(self: Intent) !void {
        try self.target.validate();
        try self.actor.validate();
        if (self.actor.current_role != null) return error.UnsupportedSqlExecution;
        if (self.provider) |name| try labels.validateProvider(name);
        try labels.validateLabel(self.label);
        if (self.expected_table_id) |id| if (self.class != .table or id == 0) return error.InvalidCatalogMutation;
        if (self.class != .procedure and self.signature != null) return error.InvalidCatalogMutation;
        if (self.signature) |value| if (value.len > 4096 or std.mem.indexOfScalar(u8, value, 0) != null) return error.InvalidCatalogMutation;
    }
    pub fn mutation(self: Intent) domain.Mutation {
        return .{ .action = .alter, .kind = switch (self.class) {
            .table => .table,
            .namespace => .namespace,
            .procedure => .routine,
        }, .database = self.target.database, .namespace = self.target.namespace, .name = self.target.table };
    }
};
pub const Prepared = struct {
    intent: Intent,
    object: labels.Object,
    /// Procedure definition changes and table/schema renames are also fenced
    /// by the enclosing exact catalog revision. The object ID defeats ABA.
    provider_name: []const u8,
    provider_digest: [32]u8,
    routine_generation: ?u64 = null,
    selinux_approval: ?labels.KernelRelabel.Approval = null,

    pub fn jsonStringify(self: Prepared, writer: anytype) @TypeOf(writer.*).Error!void {
        try writer.beginObject();
        inline for (@typeInfo(Prepared).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "selinux_approval")) {
                if (self.selinux_approval) |approval| {
                    try writer.objectField(field);
                    try local.storage_db_relational_integrity_json.write(approval, writer);
                }
            } else {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    pub fn validate(self: Prepared) !void {
        try self.intent.validate();
        try self.object.validate();
        if (self.intent.class == .procedure) {
            if (self.routine_generation == null or self.routine_generation.? == 0) return error.InvalidCatalogMutation;
        } else if (self.routine_generation != null) return error.InvalidCatalogMutation;
        try labels.validateProvider(self.provider_name);
        const kernel = std.mem.eql(u8, self.provider_name, "selinux");
        if (kernel != (self.selinux_approval != null)) return error.InvalidCatalogMutation;
        if (kernel and !std.mem.eql(u8, self.intent.provider orelse "", "selinux")) return error.InvalidCatalogMutation;
        if (self.object.class != self.intent.class or std.mem.allEqual(u8, &self.provider_digest, 0)) return error.InvalidCatalogMutation;
    }
};

/// The only fields owned by a SECURITY LABEL command. Every future payload
/// and generic topology/creation field is excluded unless reviewed here.
pub fn isEnvelopeField(comptime field: []const u8) bool {
    return std.mem.eql(u8, field, "version") or std.mem.eql(u8, field, "expected_revision") or
        std.mem.eql(u8, field, "mutation") or std.mem.eql(u8, field, "sql_security_label") or
        std.mem.eql(u8, field, "sql_catalog_admission");
}
pub fn validateCommand(command: Command) !void {
    inline for (@typeInfo(Command).@"struct".field_names) |field| {
        if (comptime isEnvelopeField(field)) continue;
        const value = @field(command, field);
        switch (@typeInfo(@TypeOf(value))) {
            .optional => if (value != null) return error.InvalidCatalogMutation,
            .pointer => |pointer| {
                if (comptime pointer.size != .slice) @compileError("new catalog envelope field needs explicit SECURITY LABEL exclusion");
                if (value.len != 0) return error.InvalidCatalogMutation;
            },
            else => @compileError("new catalog envelope field needs explicit SECURITY LABEL exclusion"),
        }
    }
    const prepared = command.sql_security_label orelse return error.InvalidCatalogMutation;
    try prepared.validate();
    const admission = command.sql_catalog_admission orelse return error.InvalidCatalogMutation;
    try admission.validate();
    if (command.version != command_version or command.mutation.table_id != 0 or command.mutation.storage_name.len != 0 or !std.mem.eql(u8, &admission.incarnation, &prepared.object.incarnation) or
        !try catalog.matchesIntent(std.heap.page_allocator, command.mutation, prepared.intent.mutation())) return error.InvalidCatalogMutation;
}

fn owner(a: A, resource: domain.Resource) !auth.Principal {
    if (resource.kind != .routine) return resource.sql_owner orelse .bootstrap;
    var record = try local.sql_catalog.routines.records.Record.decode(a, resource.source_json);
    defer record.deinit();
    return switch (record.value.definition.owner) {
        .auth_disabled => .bootstrap,
        .user => |principal| .{ .kind = .user, .instance = principal.instance_id },
        .role => |principal| .{ .kind = .role, .instance = principal.instance_id },
    };
}
fn requireOwner(a: A, resource: domain.Resource, intent: Intent) !void {
    if (intent.native_administrator or intent.actor.principal().kind == .bootstrap) return;
    if (!(try owner(a, resource)).eql(intent.actor.principal())) return error.Forbidden;
}

/// One transaction resolves database, namespace and overloaded procedure using
/// point lookups. No caller can substitute physical IDs or a display-name owner.
fn resolve(a: A, txn: *local.storage_docstore.DocStore.Txn, group: u64, intent: Intent, meta: domain.Meta) !struct { resource: domain.Resource, database_id: u64 } {
    const view: storage.View = .{ .alloc = a, .txn = txn, .group_id = group, .meta = meta };
    const database = (try view.lookup(.database, 0, intent.target.database)) orelse return error.DatabaseNotFound;
    const resource: domain.Resource = switch (intent.class) {
        .namespace => (try view.lookup(.namespace, database.id, intent.target.table)) orelse return error.NamespaceNotFound,
        .table => blk: {
            const namespace = try view.namespaceFor(intent.target.database, intent.target.namespace);
            break :blk (try view.lookup(.table, namespace.id, intent.target.table)) orelse return if (intent.expected_table_id != null) error.CatalogSourceUntracked else error.TableNotFound;
        },
        .procedure => blk: {
            const namespace = try view.namespaceFor(intent.target.database, intent.target.namespace);
            const signature = intent.signature orelse absent: {
                const candidates = try storage.functionOverloads(a, txn, group, namespace.id, intent.target.table);
                var selected: ?[]const u8 = null;
                for (candidates) |candidate| {
                    var candidate_scratch = std.heap.ArenaAllocator.init(a);
                    defer candidate_scratch.deinit();
                    const scratch = candidate_scratch.allocator();
                    const candidate_resource = (try storage.getById(scratch, txn, group, .routine, candidate.id)) orelse return error.InvalidCatalogRecord;
                    const candidate_kind = try std.json.parseFromSliceLeaky(struct { kind: local.sql_catalog.routines.records.Kind }, scratch, candidate_resource.value.source_json, .{ .ignore_unknown_fields = true });
                    if (candidate_kind.kind != .procedure) continue;
                    if (selected != null) return error.SqlAmbiguousFunction;
                    selected = candidate.signature;
                }
                break :absent selected orelse return error.SqlUndefinedProcedure;
            };
            const found = (try storage.findFunction(a, txn, group, namespace.id, intent.target.table, signature)) orelse return error.SqlUndefinedProcedure;
            var decoded = try local.sql_catalog.routines.records.Record.decode(a, found.value.source_json);
            defer decoded.deinit();
            if (decoded.value.kind != .procedure) return error.SqlWrongObjectType;
            break :blk found.value;
        },
    };
    if (resource.kind == .table and resource.sql_relation_id == null) return error.CatalogSourceUntracked;
    if (intent.expected_table_id) |id| if (resource.id != id) return error.CatalogGenerationChanged;
    if (resource.native_service != null or resource.sql_temporary_owner != null) return error.Forbidden;
    return .{ .resource = resource, .database_id = database.id };
}
fn relabel(a: A, resource: domain.Resource, provider: labels.Provider, prepared: Prepared) !domain.Resource {
    try requireOwner(a, resource, prepared.intent);
    const previous = try labels.previousLabel(provider, resource.sql_security_labels);
    const authority: labels.NativeAuthority = .{ .administrator = prepared.intent.native_administrator or prepared.intent.actor.principal().kind == .bootstrap };
    try provider.check(provider.context, .{ .object = prepared.object, .previous = previous, .proposed = prepared.intent.label, .authority = &authority });
    return replaceLabel(a, resource, provider.name, provider.digest, prepared.intent.label);
}

fn relabelSelinux(
    a: A,
    txn: *local.storage_docstore.DocStore.Txn,
    group: u64,
    resource: domain.Resource,
    command: Command,
) !domain.Resource {
    const prepared = command.sql_security_label.?;
    try requireOwner(a, resource, prepared.intent);
    const authority = (try storage.readSelinuxAuthority(a, txn, group)) orelse return error.Forbidden;
    if (!std.mem.eql(u8, &authority.policy.digest, &prepared.provider_digest)) return error.CatalogGenerationChanged;
    const previous = try labels.previousKernelLabel(resource.sql_security_labels);
    authority.verify(.{
        .object = prepared.object,
        .revision = command.expected_revision,
        .nonce = command.sql_catalog_admission.?.request.nonce,
        .actor = try actorDigest(prepared.intent.actor),
        .previous = previous,
        .proposed = prepared.intent.label,
    }, prepared.selinux_approval.?) catch |err| switch (err) {
        error.SelinuxAdmissionChanged => return error.CatalogGenerationChanged,
        else => return error.Forbidden,
    };
    return replaceLabel(a, resource, "selinux", authority.policy.digest, prepared.intent.label);
}

fn replaceLabel(a: A, resource: domain.Resource, provider: []const u8, policy: [32]u8, proposed: ?[]const u8) !domain.Resource {
    var entries: std.ArrayList(labels.Entry) = .empty;
    for (resource.sql_security_labels) |entry| if (!std.mem.eql(u8, entry.provider, provider)) try entries.append(a, entry);
    if (proposed) |label| try entries.append(a, .{ .provider = provider, .policy_digest = policy, .label = label });
    std.mem.sort(labels.Entry, entries.items, {}, struct {
        fn less(_: void, left: labels.Entry, right: labels.Entry) bool {
            return std.mem.lessThan(u8, left.provider, right.provider);
        }
    }.less);
    try labels.validateEntries(entries.items);
    var result = resource;
    result.sql_security_labels = try entries.toOwnedSlice(a);
    return result;
}

/// Only the private preparation route returns this value. An unsigned
/// challenge is not a catalog command and cannot be submitted to Raft.
pub const SelinuxChallenge = struct {
    revision: u64,
    prepared: Prepared,
    admission: catalog.Admission,
    authority: labels.KernelRelabel.Authority,
    previous: ?[]const u8,

    pub fn cut(self: SelinuxChallenge) !labels.KernelRelabel.Authority.CatalogCut {
        return .{
            .object = self.prepared.object,
            .revision = self.revision,
            .nonce = self.admission.request.nonce,
            .actor = try actorDigest(self.prepared.intent.actor),
            .previous = self.previous,
            .proposed = self.prepared.intent.label,
        };
    }

    pub fn finish(self: SelinuxChallenge, approval: labels.KernelRelabel.Approval) !Command {
        if (!std.mem.eql(u8, &self.prepared.provider_digest, &self.authority.policy.digest)) return error.CatalogGenerationChanged;
        try self.authority.verify(try self.cut(), approval);
        var prepared = self.prepared;
        prepared.selinux_approval = approval;
        return makeCommand(self.revision, prepared, self.admission);
    }
};

fn actorDigest(actor: auth.Actor) ![32]u8 {
    return catalog.canonicalDigest("antfly:selinux-sql-actor:v1", actor);
}

fn makeCommand(revision: u64, prepared: Prepared, admission: catalog.Admission) !Command {
    const command: Command = .{
        .version = command_version,
        .expected_revision = revision,
        .mutation = prepared.intent.mutation(),
        .sql_security_label = prepared,
        .sql_catalog_admission = admission,
    };
    try validateCommand(command);
    return command;
}

/// Called with the authority's actual native transaction and incarnation.
/// Native providers retain their original command response.
pub fn prepareTxn(a: A, txn: *local.storage_docstore.DocStore.Txn, group: u64, input: catalog.Preparation, meta: domain.Meta, incarnation: [16]u8) !union(enum) {
    command: Command,
    selinux: SelinuxChallenge,
} {
    const intent = input.security_label orelse return error.InvalidCatalogMutation;
    if (input.view != null or input.relation != null) return error.InvalidCatalogMutation;
    const kernel = std.mem.eql(u8, intent.provider orelse "", "selinux");
    const provider = if (kernel) null else try labels.nativeRegistry().select(intent.provider);
    const authority = if (kernel)
        (try storage.readSelinuxAuthority(a, txn, group)) orelse return error.SecurityLabelProviderNotLoaded
    else
        null;
    try intent.validate();
    if (!try catalog.matchesIntent(a, intent.mutation(), input.request.mutation)) return error.InvalidCatalogMutation;
    if (intent.read_revision) |revision| if (revision != meta.revision) return error.CatalogGenerationChanged;
    const found = try resolve(a, txn, group, intent, meta);
    const prepared: Prepared = .{
        .intent = intent,
        .object = .{ .incarnation = incarnation, .database_id = found.database_id, .class = intent.class, .id = found.resource.effectiveLogicalRelationId() },
        .provider_name = if (provider) |native| native.name else "selinux",
        .provider_digest = if (authority) |trust| trust.policy.digest else provider.?.digest,
        .routine_generation = if (intent.class == .procedure) try routineGeneration(a, found.resource) else null,
    };
    const admission: catalog.Admission = .{
        .request = .{ .nonce = input.nonce, .head = meta.sequence_outcome_head orelse 0, .command_fence = input.command_fence },
        .incarnation = incarnation,
    };
    if (authority) |trust| {
        try requireOwner(a, found.resource, intent);
        return .{ .selinux = .{
            .revision = meta.revision,
            .prepared = prepared,
            .admission = admission,
            .authority = trust,
            .previous = try labels.previousKernelLabel(found.resource.sql_security_labels),
        } };
    }
    _ = try relabel(a, found.resource, provider.?, prepared);
    return .{ .command = try makeCommand(meta.revision, prepared, admission) };
}

fn routineGeneration(a: A, resource: domain.Resource) !u64 {
    var record = try local.sql_catalog.routines.records.Record.decode(a, resource.source_json);
    defer record.deinit();
    return record.value.generation;
}

pub const Admitted = struct {
    meta: domain.Meta,
    upserts: [1]domain.Resource,
    outcome: catalog.Outcome,

    fn delta(self: *Admitted) domain.Delta {
        return .{ .upserts = &self.upserts, .removes = &.{}, .next_id = self.meta.next_id };
    }

    pub fn apply(self: *Admitted, a: A, txn: *local.storage_docstore.DocStore.Txn, group: u64, command: Command) !catalog.Outcome {
        try storage.applyDelta(a, txn, group, self.delta(), self.meta, try catalog.canonicalDigest("antfly:security-label-command:v1", command));
        return self.outcome;
    }
};

pub fn admitTxn(a: A, txn: *local.storage_docstore.DocStore.Txn, group: u64, command: Command) !Admitted {
    try validateCommand(command);
    const prepared = command.sql_security_label.?;
    const meta = try storage.readMeta(a, txn, group);
    if (meta.revision != command.expected_revision) return error.CatalogGenerationChanged;
    const revision = std.math.add(u64, meta.revision, 1) catch return error.CatalogRevisionExhausted;
    const found = try resolve(a, txn, group, prepared.intent, meta);
    if (found.database_id != prepared.object.database_id or found.resource.effectiveLogicalRelationId() != prepared.object.id) return error.CatalogGenerationChanged;
    if (prepared.routine_generation) |generation| if (try routineGeneration(a, found.resource) != generation) return error.CatalogGenerationChanged;
    const updated = if (prepared.selinux_approval != null)
        try relabelSelinux(a, txn, group, found.resource, command)
    else native: {
        const provider = try labels.nativeRegistry().select(prepared.intent.provider);
        if (!std.mem.eql(u8, provider.name, prepared.provider_name) or !std.mem.eql(u8, &provider.digest, &prepared.provider_digest)) return error.CatalogGenerationChanged;
        break :native try relabel(a, found.resource, provider, prepared);
    };
    var admitted: Admitted = .{
        .meta = meta,
        .upserts = .{updated},
        .outcome = .{ .success = .{
            .revision = revision,
            .identity = if (prepared.intent.class == .table) null else .{ .id = found.resource.id, .generation = revision },
            .removed = 0,
        } },
    };
    try storage.requireScopeDeltaWritable(a, txn, group, admitted.delta());
    return admitted;
}
