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

//! Immutable ancestor-drop membership and exact recovery progress. This is a
//! publication contract, not authority to delete a table or terminate a backend.
//! Physical retirement remains the responsibility of native owner protocols.
const std = @import("std");
const domain = @import("domain.zig");
const privileges = @import("privileges.zig");
const A = std.mem.Allocator;
pub const Id = [16]u8;
pub const Digest = [32]u8;
pub const max_resources = 4096;
pub const max_tables = 128;
pub const max_manifest_bytes = 16 * 1024 * 1024;

pub const Scope = struct {
    kind: enum { database, namespace },
    database: domain.Resource,
    namespace: ?domain.Resource = null,

    pub fn root(self: Scope) domain.Resource {
        return if (self.kind == .database) self.database else self.namespace.?;
    }
    pub fn validate(self: Scope) !void {
        if (self.database.kind != .database or self.database.id == 0 or self.database.parent_id != 0 or
            (self.kind == .namespace) != (self.namespace != null)) return error.InvalidScopeRetirement;
        try domain.validateResourceName(.database, self.database.name);
        if (self.namespace) |ns| {
            if (ns.kind != .namespace or ns.id == 0 or ns.parent_id != self.database.id) return error.InvalidScopeRetirement;
            try domain.validateResourceName(.namespace, ns.name);
        }
        const selected = self.root();
        if ((selected.kind == .database and selected.id == domain.default_database_id) or
            (selected.kind == .namespace and selected.id == domain.default_namespace_id)) return error.ProtectedCatalogResource;
        if (selected.native_service != null or self.database.native_service != null) return error.ProtectedCatalogResource;
    }
};

/// A table's complete definition/range cut is persisted by the native child
/// retirement manifest. The scope records that immutable digest rather than
/// duplicating schemas, index declarations and placement arrays at every step.
pub const Table = struct {
    id: u64,
    physical_name: []const u8,
    definition: Digest,
    ranges: Digest,
    /// Owned immutable metadata image, retained across children and restart.
    /// A digest alone cannot reconstruct an earlier schema after CASCADE has
    /// rewritten another selected member. Native owner admission rejoins it.
    preimage: []const u8,
    pub fn validate(self: Table, a: A) !void {
        if (self.id == 0 or self.physical_name.len == 0 or self.physical_name.len > 256 or
            std.mem.indexOfScalar(u8, self.physical_name, 0) != null or !std.unicode.utf8ValidateSlice(self.physical_name) or
            std.mem.allEqual(u8, &self.definition, 0) or std.mem.allEqual(u8, &self.ranges, 0) or self.preimage.len == 0 or self.preimage.len > max_manifest_bytes) return error.InvalidScopeRetirement;
        if (!std.mem.eql(u8, &self.definition, &definitionDigest(self.preimage))) return error.InvalidScopeRetirement;
        var header = try std.json.parseFromSlice(struct { table_id: u64, name: []const u8 }, a, self.preimage, .{ .ignore_unknown_fields = true });
        defer header.deinit();
        if (header.value.table_id != self.id or !std.mem.eql(u8, header.value.name, self.physical_name)) return error.InvalidScopeRetirement;
    }
    pub fn definitionDigest(preimage: []const u8) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/scope-retirement-table/v1\x00");
        hash.update(preimage);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
};

const Key = struct { kind: domain.Kind, id: u64 };
fn key(resource: domain.Resource) Key {
    return .{ .kind = resource.kind, .id = resource.id };
}
fn resourceLess(_: void, left: domain.Resource, right: domain.Resource) bool {
    return if (left.kind == right.kind) left.id < right.id else @backingInt(left.kind) < @backingInt(right.kind);
}

pub const Manifest = struct {
    version: u8 = 1,
    metadata_group_id: u64,
    scope: Scope,
    metadata_incarnation: [16]u8,
    catalog_revision: u64,
    creator: privileges.Principal,
    cascade: bool,
    force: bool = false,
    /// Canonically sorted by (kind,id), excluding the selected scope root.
    /// Database manifests include their namespaces; namespace manifests do not.
    resources: []const domain.Resource,
    /// Canonically sorted by immutable table ID. No mutable-name lookup may
    /// replace these members after the operation's admission.
    tables: []const Table,

    pub fn validate(self: Manifest, backing: A) !void {
        try self.scope.validate();
        try self.creator.validate();
        if (self.version != 1 or self.metadata_group_id == 0 or self.catalog_revision == 0 or self.creator.kind == .public or
            std.mem.allEqual(u8, &self.metadata_incarnation, 0) or self.resources.len > max_resources or self.tables.len > max_tables or
            (self.force and self.scope.kind != .database) or (!self.cascade and self.scope.kind == .namespace and self.resources.len != 0)) return error.InvalidScopeRetirement;
        var arena: std.heap.ArenaAllocator = .init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        var members: std.AutoHashMapUnmanaged(Key, domain.Resource) = .empty;
        var namespaces: std.AutoHashMapUnmanaged(u64, void) = .empty;
        if (self.scope.namespace) |ns| try namespaces.put(a, ns.id, {});
        var bytes: usize = 0;
        for (self.resources, 0..) |resource, i| {
            if (resource.id == 0 or (i != 0 and !resourceLess({}, self.resources[i - 1], resource)) or
                resource.native_service != null or (resource.kind == self.scope.root().kind and resource.id == self.scope.root().id)) return error.InvalidScopeRetirement;
            try domain.validateResourceName(resource.kind, resource.name);
            const member = try members.getOrPut(a, key(resource));
            if (member.found_existing) return error.InvalidScopeRetirement;
            member.value_ptr.* = resource;
            if (resource.kind == .namespace) {
                if (self.scope.kind != .database or resource.parent_id != self.scope.database.id) return error.InvalidScopeRetirement;
                try namespaces.put(a, resource.id, {});
            }
            for ([_][]const u8{ resource.name, resource.storage_name, resource.source_json, resource.location_json, resource.signature orelse "", resource.description orelse "" }) |text| {
                bytes = std.math.add(usize, bytes, text.len) catch return error.InvalidScopeRetirement;
                if (bytes > max_manifest_bytes) return error.InvalidScopeRetirement;
            }
        }
        var count: usize = 0;
        for (self.resources) |resource| {
            switch (domain.parentScope(resource.kind)) {
                .global => return error.InvalidScopeRetirement,
                .database => if (self.scope.kind != .database or resource.parent_id != self.scope.database.id) return error.InvalidScopeRetirement,
                .namespace => if (!namespaces.contains(resource.parent_id)) return error.InvalidScopeRetirement,
            }
            if (resource.kind == .table) count += 1;
        }
        if (count != self.tables.len) return error.InvalidScopeRetirement;
        var physical: std.StringHashMapUnmanaged(void) = .empty;
        var previous: u64 = 0;
        for (self.tables) |table| {
            try table.validate(a);
            bytes = std.math.add(usize, bytes, table.preimage.len) catch return error.InvalidScopeRetirement;
            if (bytes > max_manifest_bytes) return error.InvalidScopeRetirement;
            if (table.id <= previous or !members.contains(.{ .kind = .table, .id = table.id }) or
                (try physical.getOrPut(a, table.physical_name)).found_existing) return error.InvalidScopeRetirement;
            previous = table.id;
            // Resolve from the same pinned membership, never from current
            // display names. This also rejects swapping two physical owners.
            const resource = members.get(.{ .kind = .table, .id = table.id }) orelse return error.InvalidScopeRetirement;
            if (!std.mem.eql(u8, resource.storage_name, table.physical_name)) return error.InvalidScopeRetirement;
        }
    }

    pub fn digest(self: Manifest, a: A) !Digest {
        try self.validate(a);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/scope-retirement-manifest/v1\x00");
        var discarded = std.Io.Writer.Discarding.init(&.{});
        var buffer: [4096]u8 = undefined;
        var writer = discarded.writer.hashed(&hash, &buffer);
        try std.json.Stringify.value(self, .{}, &writer.writer);
        try writer.writer.flush();
        if (discarded.fullCount() > max_manifest_bytes) return error.InvalidScopeRetirement;
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }

    pub fn reference(self: Manifest, a: A, id: Id) !Reference {
        const result: Reference = .{ .id = id, .digest = try self.digest(a), .database = self.scope.database.id, .root = self.scope.root().id, .kind = self.scope.kind, .tables = @intCast(self.tables.len), .resources = @intCast(self.resources.len) };
        try result.validate();
        return result;
    }
};

pub const Prepare = struct {
    kind: @FieldType(Scope, "kind"),
    database: []const u8 = domain.default_database_name,
    name: []const u8,
    creator: privileges.Principal,
    cascade: bool = false,
    force: bool = false,
    pub fn validate(self: Prepare) !void {
        try domain.validateName(self.database);
        try domain.validateResourceName(if (self.kind == .database) .database else .namespace, self.name);
        try self.creator.validate();
        if (self.creator.kind == .public or (self.force and self.kind != .database)) return error.InvalidScopeRetirement;
    }
};

/// Capture from one native read transaction. The reader uses the derived
/// parent/child and table/range directories; no user row or unrelated namespace
/// needs to be loaded. It rejects the next member before decoding its body if
/// either admission budget is exhausted. Overloads are never paged by name.
/// Every returned slice is owned by the caller's capture arena.
pub fn capture(a: A, reader: anytype, group: u64, scope: Scope, incarnation: [16]u8, revision: u64, request: Prepare) !Manifest {
    try request.validate();
    try scope.validate();
    if (request.kind != scope.kind or !std.mem.eql(u8, request.name, scope.root().name) or
        (request.kind == .namespace and !std.mem.eql(u8, request.database, scope.database.name))) return error.CatalogGenerationChanged;
    try request.creator.validate();
    if (request.creator.kind == .public) return error.InvalidScopeRetirement;
    if (request.force and scope.kind != .database) return error.InvalidScopeRetirement;
    var resources: std.ArrayList(domain.Resource) = .empty;
    var remaining_bytes: usize = max_manifest_bytes;
    var namespace_count: usize = 0;
    if (scope.kind == .database) {
        const namespaces = try reader.children(.namespace, scope.database.id, max_resources, remaining_bytes);
        try resources.appendSlice(a, namespaces);
        namespace_count = namespaces.len;
        try consumeBytes(&remaining_bytes, namespaces);
        inline for (comptime std.meta.tags(domain.Kind).*) |kind| {
            if (comptime kind != .namespace and domain.parentScope(kind) == .database) {
                const children = try reader.children(kind, scope.database.id, max_resources - resources.items.len, remaining_bytes);
                try resources.appendSlice(a, children);
                try consumeBytes(&remaining_bytes, children);
            }
        }
    }
    // Keep the directory stable while appending children to the output.
    const selected_namespaces = if (scope.namespace) |ns| try a.dupe(domain.Resource, &.{ns}) else try a.dupe(domain.Resource, resources.items[0..namespace_count]);
    for (selected_namespaces) |namespace| {
        if (namespace.kind != .namespace) return error.InvalidScopeRetirement;
        inline for (comptime std.meta.tags(domain.Kind).*) |kind| {
            if (comptime domain.parentScope(kind) == .namespace) {
                const children = try reader.children(kind, namespace.id, max_resources - resources.items.len, remaining_bytes);
                if (scope.kind == .namespace and !request.cascade and children.len != 0) return error.NamespaceNotEmpty;
                try resources.appendSlice(a, children);
                try consumeBytes(&remaining_bytes, children);
            }
        }
    }
    std.mem.sort(domain.Resource, resources.items, {}, resourceLess);
    var tables: std.ArrayList(Table) = .empty;
    for (resources.items) |resource| if (resource.kind == .table) {
        if (tables.items.len == max_tables) return error.SqlProgramLimitExceeded;
        try tables.append(a, try reader.table(resource));
    };
    std.mem.sort(Table, tables.items, {}, struct {
        fn less(_: void, left: Table, right: Table) bool {
            return left.id < right.id;
        }
    }.less);
    const result: Manifest = .{ .metadata_group_id = group, .scope = scope, .metadata_incarnation = incarnation, .catalog_revision = revision, .creator = request.creator, .cascade = request.cascade, .force = request.force, .resources = resources.items, .tables = tables.items };
    _ = try result.digest(a);
    return result;
}
fn consumeBytes(remaining: *usize, resources: []const domain.Resource) !void {
    var count = std.Io.Writer.Discarding.init(&.{});
    for (resources) |resource| try std.json.Stringify.value(resource, .{}, &count.writer);
    const bytes = count.fullCount();
    if (bytes > remaining.*) return error.SqlProgramLimitExceeded;
    remaining.* -= @intCast(bytes);
}

pub const Reference = struct {
    id: Id,
    digest: Digest,
    database: u64,
    root: u64,
    kind: @FieldType(Scope, "kind"),
    tables: u16,
    resources: u16,
    pub fn validate(self: Reference) !void {
        if (self.database == 0 or self.root == 0 or (self.kind == .database and self.root != self.database) or
            self.tables > max_tables or self.resources > max_resources or self.tables > self.resources or
            std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidScopeRetirement;
    }
    /// Stable before child admission: a lost child reply never requires a new
    /// plan or a new random ID to rediscover the already admitted retirement.
    pub fn childId(self: Reference, table: u64) !Id {
        try self.validate();
        if (table == 0) return error.InvalidScopeRetirement;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/scope-retirement-child/v1\x00");
        hash.update(&self.id);
        hash.update(&self.digest);
        var number: [8]u8 = undefined;
        std.mem.writeInt(u64, &number, table, .little);
        hash.update(&number);
        var result: Id = undefined;
        hash.final(&result);
        return result;
    }
};

pub const Phase = enum { draining_connections, retiring_tables, publishing_catalog, published, canceled };
pub const Progress = struct {
    reference: Reference,
    revision: u64 = 1,
    phase: Phase = .draining_connections,
    completed_tables: u16 = 0,
    /// Actual final native catalog publication, never an admission revision.
    catalog_revision: u64 = 0,
    pub fn validate(self: Progress) !void {
        try self.reference.validate();
        if (self.completed_tables > self.reference.tables or self.revision == 0 or
            (self.phase == .published) != (self.catalog_revision != 0) or
            (self.phase == .draining_connections and self.completed_tables != 0) or
            ((self.phase == .publishing_catalog or self.phase == .published) and self.completed_tables != self.reference.tables) or
            (self.phase == .retiring_tables and self.completed_tables == self.reference.tables) or
            (self.phase == .canceled and self.completed_tables != 0)) return error.InvalidScopeRetirement;
        const expected: u64 = switch (self.phase) {
            .draining_connections => 1,
            .retiring_tables, .publishing_catalog => 2 + @as(u64, self.completed_tables),
            .published => 3 + @as(u64, self.completed_tables),
            .canceled => 2,
        };
        if (self.revision != expected) return error.InvalidScopeRetirement;
    }
    pub fn drained(self: Progress) !Progress {
        try self.validate();
        if (self.phase != .draining_connections) return error.ScopeRetirementChanged;
        var result = self;
        result.revision += 1;
        result.phase = if (self.reference.tables == 0) .publishing_catalog else .retiring_tables;
        try result.validate();
        return result;
    }
    pub fn tablePublished(self: Progress, ordinal: u16) !Progress {
        try self.validate();
        if (self.phase != .retiring_tables or ordinal != self.completed_tables) return error.ScopeRetirementChanged;
        var result = self;
        result.revision += 1;
        result.completed_tables += 1;
        if (result.completed_tables == result.reference.tables) result.phase = .publishing_catalog;
        try result.validate();
        return result;
    }
    pub fn published(self: Progress, actual_revision: u64, admitted_revision: u64) !Progress {
        try self.validate();
        if (self.phase != .publishing_catalog or actual_revision <= admitted_revision) return error.ScopeRetirementChanged;
        var result = self;
        result.revision += 1;
        result.phase = .published;
        result.catalog_revision = actual_revision;
        try result.validate();
        return result;
    }
    pub fn cancelBeforeEffects(self: Progress) !Progress {
        try self.validate();
        if (self.phase != .draining_connections) return error.ScopeRetirementChanged;
        var result = self;
        result.phase = .canceled;
        result.revision += 1;
        try result.validate();
        return result;
    }
};

test "SQL scope retirement progress requires every exact native child and a final catalog cut" {
    const a = std.testing.allocator;
    const reference: Reference = .{ .id = @splat(1), .digest = @splat(2), .database = 9, .root = 10, .kind = .namespace, .tables = 2, .resources = 4 };
    var progress: Progress = .{ .reference = reference };
    try std.testing.expectError(error.ScopeRetirementChanged, progress.tablePublished(0));
    try std.testing.expectError(error.ScopeRetirementChanged, progress.published(100, 50));
    progress = try progress.drained();
    try std.testing.expectError(error.ScopeRetirementChanged, progress.cancelBeforeEffects());
    try std.testing.expectError(error.ScopeRetirementChanged, progress.tablePublished(1));
    for (0..2) |ordinal| {
        // A real wire/cold boundary between every state transition.
        const bytes = try std.json.Stringify.valueAlloc(a, progress, .{});
        defer a.free(bytes);
        var cold = try std.json.parseFromSlice(Progress, a, bytes, .{});
        defer cold.deinit();
        progress = try cold.value.tablePublished(@intCast(ordinal));
        try std.testing.expectError(error.ScopeRetirementChanged, progress.tablePublished(@intCast(ordinal)));
    }
    try std.testing.expectEqual(Phase.publishing_catalog, progress.phase);
    try std.testing.expectError(error.ScopeRetirementChanged, progress.published(50, 50));
    progress = try progress.published(51, 50);
    try progress.validate();
    try std.testing.expectEqual(@as(u64, 51), progress.catalog_revision);
    try std.testing.expectError(error.ScopeRetirementChanged, progress.published(52, 50));
    const child = try reference.childId(99);
    try std.testing.expectEqualDeep(child, try reference.childId(99));
    try std.testing.expect(!std.mem.eql(u8, &child, &try reference.childId(100)));
}

test "SQL scope retirement empty scope still requires drainage and durable final publication" {
    const reference: Reference = .{ .id = @splat(1), .digest = @splat(2), .database = 9, .root = 9, .kind = .database, .tables = 0, .resources = 0 };
    const initial: Progress = .{ .reference = reference };
    const canceled = try initial.cancelBeforeEffects();
    try canceled.validate();
    try std.testing.expectError(error.ScopeRetirementChanged, canceled.drained());
    const drained = try initial.drained();
    try std.testing.expectEqual(Phase.publishing_catalog, drained.phase);
    try std.testing.expectError(error.ScopeRetirementChanged, drained.tablePublished(0));
    _ = try drained.published(11, 10);
    var forged = drained;
    forged.revision += 1;
    try std.testing.expectError(error.InvalidScopeRetirement, forged.validate());
}

fn exerciseManifest(a: A) !void {
    const database: domain.Resource = .{ .kind = .database, .id = 9, .name = "tenant_ops" };
    const namespace: domain.Resource = .{ .kind = .namespace, .id = 10, .parent_id = 9, .name = "serving" };
    var resources = [_]domain.Resource{
        namespace,
        .{ .kind = .table, .id = 111, .parent_id = 10, .name = "events", .storage_name = "table:111" },
        .{ .kind = .sequence, .id = 12, .parent_id = 10, .name = "event_seq" },
        .{ .kind = .routine, .id = 13, .parent_id = 10, .name = "audit", .signature = "text" },
        .{ .kind = .routine, .id = 14, .parent_id = 10, .name = "audit", .signature = "bigint" },
    };
    std.mem.sort(domain.Resource, &resources, {}, resourceLess);
    const preimage = "{\"table_id\":111,\"name\":\"table:111\"}";
    const tables = [_]Table{.{ .id = 111, .physical_name = "table:111", .definition = Table.definitionDigest(preimage), .ranges = @splat(4), .preimage = preimage }};
    var manifest: Manifest = .{ .metadata_group_id = 7, .scope = .{ .kind = .database, .database = database }, .metadata_incarnation = @splat(1), .catalog_revision = 17, .creator = .bootstrap, .cascade = true, .resources = &resources, .tables = &tables };
    const reference = try manifest.reference(a, @splat(5));
    try std.testing.expectEqual(@as(u16, 1), reference.tables);
    try std.testing.expectEqual(@as(u16, 5), reference.resources);
    const encoded = try std.json.Stringify.valueAlloc(a, manifest, .{});
    defer a.free(encoded);
    var cold = try std.json.parseFromSlice(Manifest, a, encoded, .{});
    defer cold.deinit();
    try std.testing.expectEqualDeep(reference.digest, try cold.value.digest(a));
    manifest.metadata_group_id = 8;
    try std.testing.expect(!std.mem.eql(u8, &reference.digest, &try manifest.digest(a)));
    manifest.metadata_group_id = 7;
    // Overloads with the same display name remain distinct members. A changed
    // creator, signature, definition or immutable incarnation changes identity.
    resources[resources.len - 1].signature = "numeric";
    try std.testing.expect(!std.mem.eql(u8, &reference.digest, &try manifest.digest(a)));
    manifest.tables = &.{};
    try std.testing.expectError(error.InvalidScopeRetirement, manifest.validate(std.testing.allocator));
    manifest.tables = &tables;
    manifest.scope = .{ .kind = .namespace, .database = database, .namespace = namespace };
    try std.testing.expectError(error.InvalidScopeRetirement, manifest.validate(std.testing.allocator));
    manifest.scope = .{ .kind = .database, .database = domain.default_database };
    try std.testing.expectError(error.ProtectedCatalogResource, manifest.validate(std.testing.allocator));
}

test "SQL scope retirement manifest pins all overloads physical members and cold identity" {
    try exerciseManifest(std.testing.allocator);
}

test "SQL scope retirement manifest unwinds every allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, exerciseManifest, .{});
}

test "SQL scope retirement definition images cannot replace admitted physical identities" {
    const a = std.testing.allocator;
    const original = "{\"table_id\":17,\"name\":\"table\\u003a17\",\"schema_json\":\"{}\"}";
    var table: Table = .{ .id = 17, .physical_name = "table:17", .definition = Table.definitionDigest(original), .ranges = @splat(1), .preimage = original };
    try table.validate(a);
    table.id = 18;
    try std.testing.expectError(error.InvalidScopeRetirement, table.validate(a));
    table.id = 17;
    table.definition[0] ^= 1;
    try std.testing.expectError(error.InvalidScopeRetirement, table.validate(a));
    const changed = "{\"table_id\":18,\"name\":\"table:17\"}";
    table.definition = Table.definitionDigest(changed);
    table.preimage = changed;
    try std.testing.expectError(error.InvalidScopeRetirement, table.validate(a));
}
