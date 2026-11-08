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

//! Owned constraint effects for a local native SQL publication. These are not
//! durable distributed intents. The caller must publish every participating
//! namespace in one native transaction, after validating all participant plans.
const std = @import("std");
const integrity = @import("relational_integrity.zig");
const catalog_mod = @import("relational_integrity_catalog.zig");
const activation = @import("relational_integrity_activation.zig");
const types = @import("types.zig");
const scope_mod = @import("../native_publication.zig");
const A = std.mem.Allocator;

/// Preserve supported participant fields explicitly. Unsupported authority or
/// lifecycle requests keep their existing coordinator instead of dropping a
/// guard while translating into a local batch.
pub fn batchRequest(alloc: A, input: anytype) !?types.BatchRequest {
    const empty: @TypeOf(input) = .{ .table_name = input.table_name };
    inline for (comptime std.meta.fieldNames(@TypeOf(input))) |name| {
        if (comptime std.mem.eql(u8, name, "table_name") or std.mem.eql(u8, name, "writes") or
            std.mem.eql(u8, name, "deletes") or std.mem.eql(u8, name, "predicates") or
            std.mem.eql(u8, name, "schema_version") or std.mem.eql(u8, name, "relational_schema_version") or
            std.mem.eql(u8, name, "relational_integrity_generation_set") or std.mem.eql(u8, name, "integrity_commands")) continue;
        if (!std.meta.eql(@field(input, name), @field(empty, name))) return null;
    }
    const writes = try alloc.alloc(types.BatchWrite, input.writes.len);
    for (input.writes, writes) |write, *out| out.* = .{ .key = write.key, .value = write.value, .json_null_fields = write.json_null_fields };
    return .{
        .writes = writes,
        .deletes = input.deletes,
        .predicates = input.predicates,
        .schema_version = input.schema_version,
        .relational_schema_version = input.relational_schema_version,
        .relational_integrity_generation_set = input.relational_integrity_generation_set,
        .integrity_commands = input.integrity_commands,
    };
}

pub const Plan = struct {
    atomic_member: ?scope_mod.AtomicMember = null,
    snapshot_mutation: ?*const @import("snapshot_admission.zig").SnapshotAdmission.MutationLease = null,
    arena: std.heap.ArenaAllocator,
    effects: integrity.Effects,
    scope: scope_mod.Scope,
    namespace: @import("doc_identity.zig").Namespace,
    incarnation: u128,
    schema_namespace: u64,
    schema_version: u32,
    relational_schema_version: ?u32,
    catalog_digest: ?[32]u8,
    generation_set: ?[32]u8,
    primary_digest: [32]u8,

    pub fn deinit(self: *Plan) void {
        self.effects.deinit();
        self.arena.deinit();
        self.* = undefined;
    }

    /// Invoke under the namespace's apply fence and native writer admission.
    /// Rechecking reference-prefix invariants also detects phantoms introduced
    /// while preparation was detached. Existing prepared intent locks remain
    /// authoritative, including shared guards on keys this plan will modify.
    pub fn validate(self: *const Plan, alloc: A, db: anytype, request: types.BatchRequest) !void {
        if (try db.maintenanceRequiresOrderedApply()) return error.UnsupportedSqlExecution;
        return self.validateCore(alloc, db.core, db.root_incarnation, request);
    }

    pub fn validateCore(self: *const Plan, alloc: A, core: anytype, incarnation: u128, request: types.BatchRequest) !void {
        const expected_primary = primaryDigest(request);
        if (!primaryShape(request, false) or !std.meta.eql(self.namespace, core.identity_namespace) or
            self.incarnation != incarnation or
            self.schema_namespace != core.schemaNamespaceGeneration() or
            request.relational_schema_version != self.relational_schema_version or
            request.schema_version != self.schema_version or
            !std.meta.eql(request.relational_integrity_generation_set, self.generation_set) or
            !std.mem.eql(u8, &self.primary_digest, &expected_primary)) return error.PreparedGenerationChanged;
        if (core.byteRange().start.len != 0 or core.byteRange().end.len != 0) return error.UnsupportedSqlExecution;
        var scope = (try core.store.runtime_store.nativePublicationScope(alloc)) orelse return error.UnsupportedSqlExecution;
        defer scope.deinit(alloc);
        if (scope.inode != self.scope.inode or !std.mem.eql(u8, &scope.file_namespace, &self.scope.file_namespace) or
            !std.mem.eql(u8, scope.prefix, self.scope.prefix)) return error.PreparedGenerationChanged;
        var view = try integrity.CurrentView.init(core.store);
        defer view.deinit();
        view.intent_read_guard = @import("../transactions.zig").IntentReadGuard.init(alloc, null);
        const raw: ?[]const u8 = view.get(catalog_mod.key) catch |err| if (err == error.NotFound) null else return err;
        var catalog: ?catalog_mod.Catalog = null;
        defer if (catalog) |*value| value.deinit();
        if (self.catalog_digest) |expected| {
            const bytes_value = raw orelse return error.IntegrityCatalogChanged;
            var digest: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(bytes_value, &digest, .{});
            if (!std.mem.eql(u8, &digest, &expected)) return error.IntegrityCatalogChanged;
            catalog = try catalog_mod.decode(alloc, bytes_value);
        } else if (raw != null) return error.IntegrityCatalogChanged;
        try requireMutable(alloc, &view, catalog, request.writes.len != 0 or request.deletes.len != 0);
        var manager = try core.initTxnManager();
        defer manager.deinit();
        for (self.effects.operations) |operation| {
            try @import("online_integrity_shadow.zig").requireServing(&view, core.byteRange(), operation.routing_key);
            if (operation.kind != .guard) try manager.checkOrdinaryWriteConflict(operation.key);
        }
        try integrity.validatePreparedEffects(alloc, &view, self.effects.operations);
    }
};

/// Capture while the request retains its originating runtime. Preparation may
/// run after releasing writer admission, but must release these read scopes
/// before admission resumes and can replace that runtime.
pub const Inputs = struct {
    backing: A,
    arena: ?std.heap.ArenaAllocator,
    view: ?integrity.CurrentView,
    scope: scope_mod.Scope,
    namespace: @import("doc_identity.zig").Namespace,
    incarnation: u128,
    schema_namespace: u64,
    schema_version: u32,
    relational_schema_version: ?u32,
    primary_digest: [32]u8,
    catalog_bytes: ?[]const u8,
    generation_set: ?[32]u8,
    commands: []const integrity.Command,
    changes_rows: bool,

    pub fn capture(alloc: A, db: anytype, request: types.BatchRequest) !?Inputs {
        const version = request.schema_version orelse return null;
        const generation = request.relational_integrity_generation_set;
        if (!primaryShape(request, true) or (request.relational_schema_version != null and request.relational_schema_version.? != version) or request.relational_repair or
            request.relational_activation != null or request.relational_retirement != null or
            request.relational_index_maintenance != null or request.integrity.len != 0 or
            request.transforms.len != 0 or request.graph_writes.len != 0 or request.graph_deletes.len != 0 or
            db.core.byteRange().start.len != 0 or db.core.byteRange().end.len != 0 or
            try db.maintenanceRequiresOrderedApply()) return null;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        const scope = (try db.core.store.runtime_store.nativePublicationScope(a)) orelse {
            arena.deinit();
            return null;
        };
        var schema = db.core.acquireSchemaView() orelse return error.IntegrityCatalogUnavailable;
        defer schema.release();
        if (schema.version() != version) return error.PreparedGenerationChanged;
        if (schema.tableSchema().storage_mode == .relational) {
            if (request.relational_schema_version != version) return null;
        } else if (request.relational_schema_version != null or schema.hasCoordinatedConstraints()) return null;
        if (generation == null and (schema.hasCoordinatedConstraints() or request.integrity_commands.len != 0)) return error.IntegrityCatalogChanged;
        var view = try integrity.CurrentView.init(db.core.store);
        errdefer view.deinit();
        // This allocator must survive moving Inputs; never store an allocator
        // whose context points into the movable arena field.
        view.intent_read_guard = @import("../transactions.zig").IntentReadGuard.init(alloc, null);
        const raw: ?[]const u8 = view.get(catalog_mod.key) catch |err| if (err == error.NotFound) null else return err;
        const owned_catalog = if (raw) |value| try a.dupe(u8, value) else null;
        return .{
            .backing = alloc,
            .arena = arena,
            .view = view,
            .scope = scope,
            .namespace = db.core.identity_namespace,
            .incarnation = db.root_incarnation,
            .schema_namespace = db.core.schemaNamespaceGeneration(),
            .schema_version = version,
            .relational_schema_version = request.relational_schema_version,
            .primary_digest = primaryDigest(request),
            .catalog_bytes = owned_catalog,
            .generation_set = generation,
            .commands = request.integrity_commands,
            .changes_rows = request.writes.len != 0 or request.deletes.len != 0,
        };
    }

    pub fn deinit(self: *Inputs) void {
        self.releaseRead();
        if (self.arena) |*arena| arena.deinit();
        self.* = undefined;
    }

    fn releaseRead(self: *Inputs) void {
        if (self.view) |*view| view.deinit();
        self.view = null;
    }

    pub fn prepare(self: *Inputs) !Plan {
        defer self.releaseRead();
        var catalog: ?catalog_mod.Catalog = if (self.catalog_bytes) |raw| try catalog_mod.decode(self.backing, raw) else null;
        defer if (catalog) |*value| value.deinit();
        if (catalog) |value| {
            const incarnation = try catalog_mod.incarnationFromTableId(self.namespace.table_id);
            if (value.schema_version != self.schema_version) return error.PreparedGenerationChanged;
            if (!std.mem.eql(u8, &incarnation, &value.incarnation)) return error.IntegrityCatalogIncarnationMismatch;
            if (self.generation_set) |expected| {
                const generation = activation.generationSet(value);
                if (!std.mem.eql(u8, &generation, &expected)) return error.IntegrityCatalogChanged;
            } else {
                for (value.bindings) |binding| if (!binding.retired) return error.IntegrityCatalogChanged;
            }
        } else if (self.generation_set != null or self.commands.len != 0) return error.IntegrityCatalogUnavailable;
        const view = &self.view.?;
        try requireMutable(self.backing, view, catalog, self.changes_rows);
        try @import("relational_integrity_retirement.zig").admitCommands(view, self.commands);
        for (self.commands) |command| {
            try @import("online_integrity_shadow.zig").requireServing(view, .{ .start = "", .end = "" }, &command.address.routing);
            const entry = (catalog orelse return error.IntegrityCatalogUnavailable).findGeneration(command.address.generation) orelse return error.IntegrityCatalogChanged;
            if (entry.definition.kind != .unique) return error.InvalidIntegrityOperation;
            if (entry.retired and (command.operation == .establish or command.operation == .attach)) return error.IntegrityCatalogChanged;
        }
        var effects = try integrity.prepare(self.backing, view, self.commands);
        errdefer effects.deinit();
        try integrity.validatePreparedEffects(self.backing, view, effects.operations);
        var digest: ?[32]u8 = null;
        if (self.catalog_bytes) |raw| {
            var value: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(raw, &value, .{});
            digest = value;
        }
        const result = Plan{
            .arena = self.arena.?,
            .effects = effects,
            .scope = self.scope,
            .namespace = self.namespace,
            .incarnation = self.incarnation,
            .schema_namespace = self.schema_namespace,
            .schema_version = self.schema_version,
            .relational_schema_version = self.relational_schema_version,
            .catalog_digest = digest,
            .generation_set = self.generation_set,
            .primary_digest = self.primary_digest,
        };
        self.arena = null;
        return result;
    }
};

fn requireMutable(alloc: A, view: anytype, catalog: ?catalog_mod.Catalog, changes_rows: bool) !void {
    try @import("restore_staging.zig").requireMutableScope(alloc, view, null);
    try @import("relational_integrity_topology.zig").requireUnfenced(view);
    try @import("online_integrity_shadow.zig").requireCatalogMutable(view);
    try @import("relational_integrity_retirement.zig").requireMutable(view);
    if (changes_rows) if (catalog) |value| try activation.requireReady(view, value);
}

/// Bind effects to the exact primary postimages and observations selected by
/// the SQL planner. Metadata cannot be transplanted onto another row request.
pub fn primaryDigest(request: types.BatchRequest) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-sql-primary-v1\x00");
    number(&hash, request.timestamp_ns);
    number(&hash, @backingInt(request.sync_level));
    number(&hash, request.writes.len);
    for (request.writes) |write| {
        bytes(&hash, write.key);
        bytes(&hash, write.value);
        number(&hash, write.json_null_fields.len);
        for (write.json_null_fields) |name| bytes(&hash, name);
    }
    number(&hash, request.deletes.len);
    for (request.deletes) |key| bytes(&hash, key);
    number(&hash, request.predicates.len);
    for (request.predicates) |predicate| {
        bytes(&hash, predicate.key);
        number(&hash, predicate.expected_version);
        hash.update(&.{@intFromBool(predicate.unique_absence)});
        if (predicate.expected_content_digest) |digest| {
            hash.update(&.{1});
            hash.update(&digest);
        } else hash.update(&.{0});
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn primaryShape(request: types.BatchRequest, allow_commands: bool) bool {
    const empty = types.BatchRequest{};
    inline for (comptime std.meta.fieldNames(types.BatchRequest)) |name| {
        if (comptime std.mem.eql(u8, name, "writes") or std.mem.eql(u8, name, "deletes") or
            std.mem.eql(u8, name, "predicates") or std.mem.eql(u8, name, "timestamp_ns") or
            std.mem.eql(u8, name, "sync_level") or std.mem.eql(u8, name, "schema_version") or
            std.mem.eql(u8, name, "relational_schema_version") or
            std.mem.eql(u8, name, "relational_integrity_generation_set")) continue;
        if (comptime std.mem.eql(u8, name, "integrity_commands")) {
            if (!allow_commands and request.integrity_commands.len != 0) return false;
        } else if (!std.meta.eql(@field(request, name), @field(empty, name))) return false;
    }
    return true;
}

fn number(hash: *std.crypto.hash.Blake3, value: u64) void {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, value, .little);
    hash.update(&encoded);
}

fn bytes(hash: *std.crypto.hash.Blake3, value: []const u8) void {
    number(hash, value.len);
    hash.update(value);
}
