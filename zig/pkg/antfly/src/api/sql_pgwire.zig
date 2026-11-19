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

//! Native pgwire adapter. No HTTP self-requests, durable SQL side catalog,
//! password retention, or automatic mutation replay. Listener registration is
//! deliberately separate from this authenticated engine boundary.
const std = @import("std");
const wire = @import("../pgwire/backend.zig");
const wire_values = @import("../pgwire/values.zig");
const storage_schema = @import("antfly_local_sources").storage_schema;
const http = @import("http_server.zig");
const execution = @import("sql_execution.zig");
const compiler = @import("antfly_local_sources").sql_compiler;
const describe_sql = @import("antfly_local_sources").sql_describe;
const native = @import("antfly_local_sources").sql_runtime;
const catalog = @import("antfly_local_sources").sql_catalog;
const ast = @import("antfly_local_sources").sql_ast;
const operation = @import("antfly_local_sources").api_operation;
const usermgr = @import("../usermgr/mod.zig");
const io_abi = @import("antfly_runtime_abi").io_abi;
const Mac = std.crypto.auth.hmac.sha2.HmacSha256;
const credential_domain = "antfly.pgwire.password-session.v1";

test "SQL NUMERIC public result descriptors parse through generated OpenAPI contracts" {
    const a = std.testing.allocator;
    for ([_]ast.ColumnType{ .number, .array }) |kind| {
        const encoded = try std.json.Stringify.valueAlloc(a, describe_sql.Column{ .name = "n", .type = kind, .element_type = .numeric, .numeric_modifier = .{ .precision = 2, .scale = -3 } }, .{});
        defer a.free(encoded);
        const parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").SQLColumn, a, encoded, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(i64, 2), parsed.value.numeric_modifier.?.precision);
        try std.testing.expectEqual(@as(i64, -3), parsed.value.numeric_modifier.?.scale);
    }
}

/// Stable API-owned callback and listener storage. Shutdown joins all native
/// work before releasing the adapter, user manager, or backend runtime.
pub const Listener = struct {
    alloc: std.mem.Allocator,
    adapter: Adapter,
    replication: ?CatalogReplicationFactory = null,
    server: @import("../pgwire/server.zig").Server,

    pub fn start(api: *http.ApiHttpServer, config: @import("antfly_local_sources").common_config.Config.PgwireConfig) !*Listener {
        if (api.cfg.user_manager == null and (api.cfg.trusted_principal_secret == null or api.cfg.trusted_principal_issuer == null)) return error.PgwireRequiresAuthentication;
        if (api.cfg.backend_runtime == null) return error.PgwireRequiresBackendRuntime;
        const io = api.sharedApiNetworkIo() orelse return error.PgwireRequiresBackendRuntime;
        const self = try api.owner_alloc.create(Listener);
        errdefer api.owner_alloc.destroy(self);
        self.alloc = api.owner_alloc;
        self.adapter = .{ .server = api };
        self.replication = null;
        if (config.replication_publication) |name| {
            self.replication = try CatalogReplicationFactory.configured(api, io, name);
            self.adapter.native_replication = self.replication.?.factory();
        }
        self.server = try @import("../pgwire/server.zig").start(self.alloc, .{
            .io = io,
            .backend = self.adapter.backend(),
            .bind_host = config.bind_host orelse "127.0.0.1",
            .bind_port = config.bind_port,
            .max_connections = config.max_connections,
            .allow_insecure_non_loopback = config.externally_protected_transport,
        });
        return self;
    }

    pub fn deinit(self: *Listener) void {
        self.server.deinit();
        self.alloc.destroy(self);
    }
};

fn resolveOnPath(native_backend: catalog.Backend, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action, path: ?*const @import("../pgwire/search_path.zig").Path) !catalog.Table {
    if (name.namespace != null or name.database != null or path == null) return native_backend.vtable.resolve(native_backend.ptr, alloc, name, action);
    // All candidates use the same native catalog-revision fence. A missing
    // relation may advance; auth, schema churn, and runtime failures may not.
    const selected = path.?;
    for (selected.entries[0..selected.len]) |*entry| {
        var scoped = name;
        scoped.namespace = entry.slice();
        const table = native_backend.vtable.resolve(native_backend.ptr, alloc, scoped, action) catch |err| switch (err) {
            error.TableNotFound => continue,
            else => return err,
        };
        return table;
    }
    return error.TableNotFound;
}

test "SQL pgwire ordered native resolver falls back only on exact table absence" {
    // sql-0038 / sql-0040: SET SESSION and SET LOCAL share this ordered,
    // authorization-fenced native binding path after pgwire scope selection.
    const search_path = @import("../pgwire/search_path.zig");
    const Fixture = struct {
        calls: usize = 0,
        seen: [4]search_path.Namespace = undefined,
        first_error: ?anyerror = null,

        fn resolve(raw: *anyopaque, _: std.mem.Allocator, name: ast.Name, _: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.seen[self.calls] = try search_path.Namespace.init(name.namespace orelse "public");
            self.calls += 1;
            if (self.calls == 1) if (self.first_error) |err| return err;
            return .{ .id = 7, .physical_name = "second_table", .schema_version = 1, .columns = &.{} };
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = undefined, .mutate = undefined, .checkpoint = undefined } };
        }
    };
    var path: search_path.Path = .{};
    try path.append(try search_path.Namespace.init("tenant_schema"));
    try path.append(try search_path.Namespace.init("public"));
    var fixture: Fixture = .{ .first_error = error.TableNotFound };
    const table = try resolveOnPath(fixture.backend(), std.testing.allocator, .{ .table = "t" }, .read, &path);
    try std.testing.expectEqual(@as(u64, 7), table.id);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    try std.testing.expectEqualStrings("tenant_schema", fixture.seen[0].slice());
    try std.testing.expectEqualStrings("public", fixture.seen[1].slice());

    for ([_]anyerror{ error.Forbidden, error.CatalogGenerationChanged, error.QueryCanceled }) |failure| {
        fixture = .{ .first_error = failure };
        try std.testing.expectError(failure, resolveOnPath(fixture.backend(), std.testing.allocator, .{ .table = "t" }, .read, &path));
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    }
    fixture = .{ .first_error = error.TableNotFound };
    try std.testing.expectError(error.TableNotFound, resolveOnPath(fixture.backend(), std.testing.allocator, .{ .namespace = "explicit", .table = "t" }, .read, &path));
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    try std.testing.expectEqualStrings("explicit", fixture.seen[0].slice());
}

test "SQL pgwire owner rejects unauthenticated and runtime-less startup" {
    var api: http.ApiHttpServer = undefined;
    api.cfg = .{};
    try std.testing.expectError(error.PgwireRequiresAuthentication, Listener.start(&api, .{ .enabled = true }));
    var manager: usermgr.UserManager = undefined;
    api.cfg.user_manager = &manager;
    try std.testing.expectError(error.PgwireRequiresBackendRuntime, Listener.start(&api, .{ .enabled = true }));
}

/// The replication owner checks its explicit privilege and publication policy
/// against the current connection identity at each authorization checkpoint.
pub const ReplicationIdentity = struct {
    context: *anyopaque,
    read: *const fn (*anyopaque, std.mem.Allocator) anyerror!http.AuthenticatedIdentity,

    /// Authorize an already resolved publication's exact logical table scopes.
    /// The caller supplies catalog-qualified names and verifies their immutable
    /// bindings. This path serves whole tuples and rejects effective filters.
    pub fn requireWholeTables(self: ReplicationIdentity, alloc: std.mem.Allocator, tables: []const []const u8) !void {
        var identity = try self.read(self.context, alloc);
        defer identity.deinit(alloc);
        try requireWholeTablesForIdentity(alloc, identity, tables);
    }

    fn requireWholeTablesForIdentity(alloc: std.mem.Allocator, identity: http.AuthenticatedIdentity, tables: []const []const u8) !void {
        const control = @import("../metadata/changefeed_control.zig");
        if (tables.len == 0 or tables.len > control.max_tables) return error.InvalidChangefeed;
        var checks: [control.max_tables]usermgr.UserManager.PermissionCheck = undefined;
        for (tables, checks[0..tables.len]) |table, *check| {
            if (table.len == 0) return error.InvalidChangefeed;
            if (http.hasEffectiveRowFilter(identity, table)) return error.Forbidden;
            check.* = .{ .resource_type = .table, .resource = table, .permission_type = .replicate };
        }
        if (!try http.resourcePermissionsCurrentlyAllowed(alloc, identity, checks[0..tables.len])) return error.Forbidden;
    }
};

/// Catalog-resolved publication members. The slot binds this whole immutable
/// projection, including the retained relation identity and old-key contract.
pub const ReplicationPublication = struct {
    const feed = @import("../metadata/changefeed_control.zig");
    const slot = feed.pg_slot;
    const relation_values = @import("../pgwire/values.zig");

    const publication = @import("../metadata/pg_publication.zig");
    pub const Member = publication.Member;

    database: feed.Digest,
    publications: []const []const u8,
    members: []const Member,
    publication_revisions: []const feed.Digest = &.{},

    pub fn binding(self: ReplicationPublication) !slot.Binding {
        if (std.mem.allEqual(u8, &self.database, 0) or self.members.len == 0 or self.members.len > feed.max_tables)
            return error.InvalidChangefeed;
        try publication.validateMembers(self.members);
        if (self.publication_revisions.len != 0 and self.publication_revisions.len != self.publications.len) return error.InvalidChangefeed;
        const value: slot.Binding = .{
            .publications = self.publications,
            .projection = if (self.publication_revisions.len == 0)
                feed.hash(.{ .domain = "antfly:pgoutput-publication-projection:v1", .database = self.database, .members = self.members })
            else
                feed.hash(.{ .domain = "antfly:pgoutput-publication-projection:v2", .database = self.database, .members = self.members, .revisions = self.publication_revisions }),
        };
        try value.validate();
        return value;
    }

    /// Re-read the connection credential each time a slot opens, builds, or
    /// serves committed tuples. A changed projection requires a fresh slot.
    pub fn authorize(self: ReplicationPublication, authority: ReplicationIdentity, a: std.mem.Allocator, state: feed.State, expected: slot.Binding) !void {
        const current = try self.binding();
        const owner_slot = state.slot orelse return error.ChangefeedCapabilityUnsupported;
        const definition = state.definition orelse return error.ChangefeedConflict;
        if (!std.meta.eql(current.identity(), expected.identity()) or !std.meta.eql(self.database, owner_slot.database) or
            definition.projection != .rows or definition.tables.len != self.members.len)
            return error.ChangefeedResynchronizationRequired;
        if (owner_slot.binding) |recorded| {
            if (!std.meta.eql(current.identity(), recorded.identity())) return error.ChangefeedResynchronizationRequired;
        }
        var names: [feed.max_tables][]const u8 = undefined;
        for (self.members, definition.tables, 0..) |member, table, index| {
            if (!std.meta.eql(feed.hash(member.table), feed.hash(table))) return error.ChangefeedResynchronizationRequired;
            names[index] = member.permission_name;
        }
        var identity = try authority.read(authority.context, a);
        defer identity.deinit(a);
        const owner = try @import("changefeed_public.zig").durableOwner(identity);
        if (!std.crypto.timing_safe.eql(feed.Digest, owner, definition.owner)) return error.Forbidden;
        try ReplicationIdentity.requireWholeTablesForIdentity(a, identity, names[0..self.members.len]);
    }

    pub fn historicalRelation(self: ReplicationPublication, a: std.mem.Allocator, table_id: u64, schema: @import("antfly_local_sources").storage_schema.TableSchema) !relation_values.HistoricalRelation {
        _ = try self.binding();
        const member = for (self.members) |member| {
            if (member.table.table_id == table_id) break member;
        } else return error.ChangefeedResynchronizationRequired;
        if (schema.storage_mode != .relational or schema.external_base_source != null) return error.ChangefeedCapabilityUnsupported;
        const count = member.key_columns.len;
        const ordinals = try a.alloc(u16, count);
        errdefer a.free(ordinals);
        var filled: usize = 0;
        for (schema.relational_columns, 0..) |column, index| {
            for (member.key_columns) |name| {
                if (!std.mem.eql(u8, column.name, name)) continue;
                if (filled == count) return error.ChangefeedResynchronizationRequired;
                ordinals[filled] = std.math.cast(u16, index) orelse return error.ChangefeedCapabilityUnsupported;
                filled += 1;
                break;
            }
        }
        if (filled != count) return error.ChangefeedResynchronizationRequired;
        return .{ .oid = member.oid, .namespace = member.namespace, .name = member.name, .identity = member.identity, .key_ordinals = ordinals };
    }
};

/// Resolve durable publications against the current catalog and credential.
pub const CatalogReplicationPublication = struct {
    const domain = @import("antfly_local_sources").system_catalog_domain;

    server: *http.ApiHttpServer,
    identity: ReplicationIdentity,
    database_name: []const u8,

    pub fn resolver(self: *@This()) LiveReplicationPublication.Resolver {
        return .{ .ptr = self, .resolve = resolveOpaque };
    }

    fn resolveOpaque(raw: *anyopaque, a: std.mem.Allocator, database: publication_control.Digest, names: []const []const u8, context: operation.RequestContext) !ReplicationPublication {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.resolve(a, database, names, context);
    }

    pub fn resolve(self: *@This(), a: std.mem.Allocator, database: publication_control.Digest, names: []const []const u8, context: operation.RequestContext) !ReplicationPublication {
        try context.ensureActive();
        const port = self.server.source.pg_publications orelse return error.UnsupportedReplicationMode;
        const sorted = try publication_control.pg_slot.bindingAlloc(a, names, @splat(1));
        var identity = try self.identity.read(self.identity.context, a);
        defer identity.deinit(a);
        var maintenance = context;
        maintenance.principal = .{ .kind = .internal, .subject = "pg-publication-maintenance" };

        var records: [wire.replication_commands.max_publications]std.json.Parsed(publication_metadata.Record) = undefined;
        var loaded: usize = 0;
        defer for (records[0..loaded]) |*record| record.deinit();
        const revisions = try a.alloc(publication_control.Digest, sorted.publications.len);
        var members: std.ArrayList(publication_metadata.Member) = .empty;
        for (sorted.publications, revisions) |name, *revision| {
            const record = (try port.read(port.ptr, a, database, name, maintenance)) orelse return error.ChangefeedResynchronizationRequired;
            records[loaded] = record;
            loaded += 1;
            if (!std.meta.eql(record.value.database, database) or !std.mem.eql(u8, record.value.name, name)) return error.ChangefeedCorrupt;
            const current = record.value.members orelse return error.ChangefeedResynchronizationRequired;
            revision.* = publication_control.hash(record.value);
            for (current) |member| {
                for (members.items) |prior| {
                    if (prior.table.table_id != member.table.table_id) continue;
                    if (!std.meta.eql(publication_control.hash(prior), publication_control.hash(member))) return error.ChangefeedResynchronizationRequired;
                    break;
                } else {
                    if (members.items.len == publication_control.max_tables) return error.ChangefeedCapabilityUnsupported;
                    try members.append(a, member);
                }
            }
        }
        std.mem.sort(publication_metadata.Member, members.items, {}, publicationMemberLess);
        const targets = try a.alloc(domain.Target, members.items.len);
        for (members.items, targets) |member, *target| target.* = .{ .database = self.database_name, .namespace = member.namespace, .table = member.name };
        const request: domain.ResolveMany = .{ .targets = targets, .publication_database = self.database_name, .include_query_definitions = true };
        const response = try self.server.source.systemCatalog(a, context, .{ .resolve_many = request });
        const resolved = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, response, .{ .allocate = .alloc_always });
        const cut = try resolved.requirePublicationCut(request);
        if (!std.meta.eql(publicationDatabaseKey(cut), database)) return error.ChangefeedResynchronizationRequired;
        const current = resolvePublicationMembers(self.server, a, identity, context, targets, resolved, cut, members.items) catch |err| switch (err) {
            error.TableNotFound => return error.ChangefeedResynchronizationRequired,
            else => return err,
        };
        if (!std.meta.eql(publication_control.hash(members.items), publication_control.hash(current))) return error.ChangefeedResynchronizationRequired;

        // Two linearizable passes bracket the catalog validation. A revision
        // can only advance, so a match proves the same publication set at it.
        for (sorted.publications, revisions) |name, revision| {
            var check = (try port.read(port.ptr, a, database, name, maintenance)) orelse return error.ChangefeedResynchronizationRequired;
            defer check.deinit();
            if (!std.meta.eql(publication_control.hash(check.value), revision)) return error.ChangefeedResynchronizationRequired;
        }
        try context.ensureActive();
        return .{ .database = database, .publications = sorted.publications, .members = current, .publication_revisions = revisions };
    }
};

/// A connection resolves its database to a durable identity once, then
/// revalidates that identity before addressing a slot by name.
pub const CatalogReplicationSlots = struct {
    const domain = @import("antfly_local_sources").system_catalog_domain;
    const control = publication_control;

    server: *http.ApiHttpServer,
    identity: ReplicationIdentity,
    database_name: []const u8,
    database: control.Digest,
    system_identifier: u64,
    database_oid: ?u32 = null,

    pub fn open(server: *http.ApiHttpServer, identity: ReplicationIdentity, a: std.mem.Allocator, database_name: []const u8, context: operation.RequestContext) !CatalogReplicationSlots {
        var authenticated = try identity.read(identity.context, a);
        defer authenticated.deinit(a);
        _ = try @import("changefeed_public.zig").durableOwner(authenticated);
        const cut = try readDatabaseCut(server, a, database_name, context);
        const cluster = control.hash(.{ .domain = "antfly:pg-system-identity:v1", .group = cut.metadata_group_id, .incarnation = cut.metadata_incarnation });
        const system = @max(1, std.mem.readInt(u64, cluster[0..8], .big));
        return .{ .server = server, .identity = identity, .database_name = database_name, .database = publicationDatabaseKey(cut), .system_identifier = system, .database_oid = std.math.cast(u32, cut.database_id) };
    }

    fn readDatabase(server: *http.ApiHttpServer, a: std.mem.Allocator, name: []const u8, context: operation.RequestContext) !control.Digest {
        return publicationDatabaseKey(try readDatabaseCut(server, a, name, context));
    }

    fn readDatabaseCut(server: *http.ApiHttpServer, a: std.mem.Allocator, name: []const u8, context: operation.RequestContext) !domain.PublicationCatalogCut {
        try domain.validateName(name);
        const request: domain.ResolveMany = .{ .publication_database = name, .include_query_definitions = true };
        const bytes = try server.source.systemCatalog(a, context, .{ .resolve_many = request });
        defer a.free(bytes);
        const resolved = try std.json.parseFromSlice(domain.ResolvedMany, a, bytes, .{});
        defer resolved.deinit();
        var cut = try resolved.value.requirePublicationCut(request);
        cut.bindings = &.{};
        return cut;
    }

    pub fn readOwned(self: CatalogReplicationSlots, a: std.mem.Allocator, name: []const u8, context: operation.RequestContext) !control.Parsed {
        try context.ensureActive();
        if (!control.pg_slot.validName(name)) return error.InvalidReplicationSlotName;
        return self.readOwnedId(a, control.pg_slot.feedId(self.database, name), name, context);
    }

    fn readOwnedId(self: CatalogReplicationSlots, a: std.mem.Allocator, id: control.Id, expected_name: ?[]const u8, context: operation.RequestContext) !control.Parsed {
        try context.ensureActive();
        const metadata = self.server.source.changefeeds orelse return error.UnsupportedReplicationMode;
        const current = try readDatabase(self.server, a, self.database_name, context);
        if (!std.meta.eql(current, self.database)) return error.ChangefeedResynchronizationRequired;
        var identity = try self.identity.read(self.identity.context, a);
        defer identity.deinit(a);
        const owner = try @import("changefeed_public.zig").durableOwner(identity);
        const bytes = try metadata.read(a, id, maintenanceContext(context));
        defer a.free(bytes);
        var state = try control.parse(a, bytes);
        errdefer state.deinit();
        const definition = state.value.definition orelse return error.ReplicationSlotNotFound;
        if (!std.crypto.timing_safe.eql(control.Digest, owner, definition.owner)) return error.Forbidden;
        const slot = state.value.slot orelse return error.ReplicationSlotNotFound;
        if (!std.meta.eql(slot.database, self.database) or !std.meta.eql(state.value.id, id)) return error.ChangefeedCorrupt;
        if (expected_name) |name| if (!std.mem.eql(u8, slot.name, name)) return error.ChangefeedCorrupt;
        try context.ensureActive();
        return state;
    }

    fn qualifySnapshot(self: CatalogReplicationSlots, a: std.mem.Allocator, snapshot: control.SlotSnapshot, context: operation.RequestContext) !control.Parsed {
        var observed = try self.readOwnedId(a, snapshot.feed, null, context);
        errdefer observed.deinit();
        const state = observed.value;
        const slot = state.slot.?;
        const binding = slot.binding orelse return error.ChangefeedCapabilityUnsupported;
        var resolver: CatalogReplicationPublication = .{ .server = self.server, .database_name = self.database_name, .identity = self.identity };
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const publication = try resolver.resolve(scratch, self.database, binding.publications, context);
        try publication.authorize(self.identity, scratch, state, binding);
        const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_s;
        // readOwnedId and authorize have independently checked the live owner.
        try snapshot.verify(state, self.database, state.definition.?.owner, now);
        return observed;
    }

    /// Read the existing SQL transaction and qualified replication cut before
    /// the importer installs a read provider. No transaction is created here.
    pub fn qualifyTransactionSnapshot(self: CatalogReplicationSlots, a: std.mem.Allocator, identity: wire.Identity, request: wire.Request, name: []const u8) !ImportedSnapshot {
        try request.check();
        if (self.identity.context != identity.context or self.identity.read != &Adapter.replicationIdentity) return error.Forbidden;
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        try credential.validate();
        const encoded = request.session_id orelse return error.NoActiveSqlTransaction;
        const id = @import("distributed_txn.zig").parseTxnIdHex(encoded) catch return error.SqlTransactionNotActive;
        if (try self.server.txn_sessions.principalAccess(a, id, credential.sessionPrincipal()) != .allowed)
            return error.SqlTransactionNotActive;
        var transaction = (try self.server.txn_sessions.getSqlState(a, id)) orelse return error.SqlTransactionNotActive;
        defer transaction.deinit(a);
        try validateSnapshotTransaction(transaction, self.database_name, request.session_namespace orelse request.namespace orelse "public");
        if (!std.mem.eql(u8, self.database_name, request.database orelse "default")) return error.SqlTransactionNotActive;
        const snapshot = try control.SlotSnapshot.parse(name);
        var qualified = try self.qualifySnapshot(a, snapshot, try @import("../pgwire/retained_stream.zig").metadataContext(request));
        defer qualified.deinit();
        if (qualified.value.roots.len != 1) return error.ChangefeedCapabilityUnsupported;
        try request.check();
        return .{ .transaction = id, .token = snapshot, .namespace = qualified.value.roots[0].namespace };
    }

    fn validateSnapshotTransaction(transaction: @import("transactions.zig").SessionRegistry.SqlState, database: []const u8, namespace: []const u8) !void {
        const metadata = transaction.metadata;
        if (transaction.terminal != null or transaction.terminal_abort or
            !std.mem.eql(u8, metadata.database, database) or !std.mem.eql(u8, metadata.namespace, namespace))
            return error.SqlTransactionNotActive;
        if (metadata.failed) return error.InFailedSqlTransaction;
        if (transaction.execution_started or transaction.savepoints != 0) return error.ActiveSqlTransaction;
        if (metadata.isolation == .read_committed or metadata.accepted_lake_reads)
            return error.UnsupportedSqlExecution;
    }

    /// Qualified raw rows for a SQL snapshot importer. The importer still
    /// owns transaction lifetime and conversion to authorized SQL tuples.
    pub fn readExportedSnapshot(self: CatalogReplicationSlots, a: std.mem.Allocator, name: []const u8, namespace: control.Namespace, after: u32, rows: u32, bytes: u32, context: operation.RequestContext) !@import("history_page_operations.zig").pages.BootstrapPage {
        const snapshot = try control.SlotSnapshot.parse(name);
        var observed = try self.qualifySnapshot(a, snapshot, context);
        defer observed.deinit();
        const state = observed.value;
        const source = self.server.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
        const port = source.bootstrap_pages orelse return error.ChangefeedCapabilityUnsupported;
        const root = state.roots[try state.rootIndex(namespace)];
        const pin = root.snapshot.?;
        const maintenance = maintenanceContext(context);
        var captured = try source.capture(a, state.definition.?, maintenance);
        defer captured.deinit();
        if (captured.roots.len != state.roots.len) return error.ChangefeedResynchronizationRequired;
        for (state.roots, captured.roots) |expected, current|
            if (!std.meta.eql(expected.namespace, current.namespace)) return error.ChangefeedResynchronizationRequired;
        const selected = try captured.find(namespace);
        var page = try port.read(a, selected.fence, selected.table_name, .{
            .version = 1,
            .request = .{
                .reference = .{ .namespace = namespace, .authority = state.authority, .consumer = state.consumerId(), .epoch = root.consumer_epoch, .grant = root.grant_sequence, .definition = state.definition.?.identity() },
                .digest = pin.digest,
                .after_rows = after,
                .max_rows = rows,
                .max_bytes = bytes,
                .now_seconds = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_s,
            },
        }, maintenance);
        errdefer page.deinit();
        var rechecked = try self.qualifySnapshot(a, snapshot, context);
        rechecked.deinit();
        try context.ensureActive();
        return page;
    }

    fn normalizeSnapshotSchema(page: *@import("history_page_operations.zig").pages.BootstrapPage) !void {
        const sources = @import("antfly_local_sources");
        const a = page.arena.allocator();
        const parsed = try sources.schema_mod.parseValidatedTableSchema(a, page.schema);
        const schema = try sources.schema_mod.deriveRuntimeTableSchema(a, parsed);
        page.schema = try sources.storage_schema.serializeSchema(a, schema);
    }

    pub fn readExportedSnapshotRows(self: CatalogReplicationSlots, a: std.mem.Allocator, name: []const u8, namespace: control.Namespace, after: u32, rows: u32, bytes: u32, context: operation.RequestContext) !@import("../pgwire/values.zig").HistoricalSnapshot {
        var raw = try self.readExportedSnapshot(a, name, namespace, after, rows, bytes, context);
        defer raw.deinit();
        try normalizeSnapshotSchema(&raw);
        return @import("../pgwire/values.zig").HistoricalSnapshot.decode(a, raw);
    }

    pub fn readExportedSnapshotCatalogPage(self: CatalogReplicationSlots, a: std.mem.Allocator, name: []const u8, namespace: control.Namespace, table: catalog.Table, after: ?[]const u8, rows: u32, bytes: u32, context: operation.RequestContext) !catalog.Page {
        if (std.mem.readInt(u64, namespace[0..8], .big) != table.id) return error.ChangefeedResynchronizationRequired;
        const offset = if (after) |cursor| std.fmt.parseUnsigned(u32, cursor, 10) catch return error.HistoryCheckpointConflict else 0;
        var raw = try self.readExportedSnapshot(a, name, namespace, offset, rows, bytes, context);
        defer raw.deinit();
        try normalizeSnapshotSchema(&raw);
        return @import("../pgwire/values.zig").historicalCatalogPage(a, raw, table);
    }

    pub fn dropWrite(self: CatalogReplicationSlots, a: std.mem.Allocator, name: []const u8, now: u64, context: operation.RequestContext) !control.Write {
        var observed = try self.readOwned(a, name, context);
        defer observed.deinit();
        const state = observed.value;
        if (state.phase == .deleted) return error.ReplicationSlotNotFound;
        const definition = state.definition.?;
        return .{
            .id = state.id,
            .authority = state.authority,
            .owner = definition.owner,
            .authorization = definition.authorization,
            .expected_revision = state.revision,
            .expected_epoch = state.epoch,
            .mutation = .{ .slot = .{ .drop = .{ .database = self.database, .name = name, .now = now } } },
        };
    }

    pub fn drop(self: CatalogReplicationSlots, a: std.mem.Allocator, command: wire.replication_commands.Command, request: wire.Request, context: operation.RequestContext) !wire.Result {
        if (command != .drop_slot) return error.InvalidChangefeed;
        const metadata = self.server.source.changefeeds orelse return error.UnsupportedReplicationMode;
        const Refresh = struct {
            slots: CatalogReplicationSlots,
            name: []const u8,
            context: operation.RequestContext,

            fn read(raw: *anyopaque, alloc: std.mem.Allocator, current: wire.Request) !control.Write {
                const refresh: *@This() = @ptrCast(@alignCast(raw));
                try current.check();
                const ns = std.Io.Clock.now(.real, current.io).nanoseconds;
                if (ns <= 0) return error.ChangefeedResynchronizationRequired;
                const now = std.math.cast(u64, @divFloor(ns, std.time.ns_per_s)) orelse return error.ChangefeedResynchronizationRequired;
                return refresh.slots.dropWrite(alloc, refresh.name, now, refresh.context);
            }
        };
        var refresh: Refresh = .{ .slots = self, .name = command.drop_slot.slot, .context = context };
        const write = try Refresh.read(&refresh, a, request);
        return SlotCommands.dropWait(metadata, a, command, write, request, maintenanceContext(context), .{ .ptr = &refresh, .read = Refresh.read });
    }

    pub fn maintenanceContext(context: operation.RequestContext) operation.RequestContext {
        var maintenance = context;
        maintenance.principal = .{ .kind = .internal, .subject = "changefeed-maintenance" };
        return maintenance;
    }
};

/// Request-scoped policy used by both the spool uploader and its CopyBoth
/// reader. The resolver must return the complete current publication set.
pub const LiveReplicationPublication = struct {
    const feed = @import("../metadata/changefeed_control.zig");
    const Binding = feed.pg_slot.Binding;

    pub const Resolver = struct {
        ptr: *anyopaque,
        resolve: *const fn (*anyopaque, std.mem.Allocator, feed.Digest, []const []const u8, operation.RequestContext) anyerror!ReplicationPublication,
    };

    allocator: std.mem.Allocator,
    io: std.Io,
    identity: ReplicationIdentity,
    resolver: Resolver,
    expected: Binding,

    fn resolve(self: *LiveReplicationPublication, a: std.mem.Allocator, state: feed.State, request: operation.RequestContext) !ReplicationPublication {
        try request.ensureActive();
        const owner_slot = state.slot orelse return error.ChangefeedCapabilityUnsupported;
        const current = try self.resolver.resolve(self.resolver.ptr, a, owner_slot.database, self.expected.publications, request);
        try current.authorize(self.identity, a, state, self.expected);
        try request.ensureActive();
        return current;
    }

    pub fn authorize(self: *LiveReplicationPublication, state: feed.State, binding: Binding, request: operation.RequestContext) !void {
        if (!std.meta.eql(self.expected.identity(), binding.identity())) return error.ChangefeedResynchronizationRequired;
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        _ = try self.resolve(arena.allocator(), state, request);
    }

    pub fn nowSeconds(self: *LiveReplicationPublication, request: operation.RequestContext) !u64 {
        try request.ensureActive();
        const now = std.Io.Clock.now(.real, self.io).nanoseconds;
        if (now <= 0) return error.ChangefeedResynchronizationRequired;
        return std.math.cast(u64, @divFloor(now, std.time.ns_per_s)) orelse error.ChangefeedResynchronizationRequired;
    }

    pub const Upload = struct {
        owner: *LiveReplicationPublication,

        pub fn authorize(self: *Upload, state: feed.State, request: operation.RequestContext) !void {
            try self.owner.authorize(state, self.owner.expected, request);
        }

        pub fn nowSeconds(self: *Upload, request: operation.RequestContext) !u64 {
            return self.owner.nowSeconds(request);
        }

        pub fn context(_: *Upload, request: operation.RequestContext) !operation.RequestContext {
            return request;
        }

        pub fn relation(self: *Upload, a: std.mem.Allocator, state: feed.State, header: @import("antfly_local_sources").storage_committed_history.frame.Header, request: operation.RequestContext) !ReplicationPublication.relation_values.HistoricalRelation {
            const source = try self.owner.resolve(a, state, request);
            const schema_mod = @import("antfly_local_sources").schema_mod;
            const raw = header.schema_after orelse header.schema_before orelse return error.ChangefeedCapabilityUnsupported;
            const parsed = try schema_mod.parseValidatedTableSchema(a, raw);
            const schema = try schema_mod.deriveRuntimeTableSchema(a, parsed);
            const id = std.mem.readInt(u64, header.namespace[0..8], .big);
            return source.historicalRelation(a, id, schema);
        }
    };
};

/// Bind a qualified durable slot to the frontend stream. Spool publication
/// runs independently of the CopyBoth reader and shares its session fence.
pub const CatalogRetainedStream = struct {
    const Bridge = @import("../pgwire/retained_stream.zig");
    const retained = @import("../metadata/pg_spool_stream.zig");
    const Registry = @import("../metadata/pg_spool_artifacts.zig").Registry;
    const control = publication_control;

    allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    service: *const @import("changefeed_metadata.zig").Port,
    registry: Registry,
    id: control.Id,
    actor: control.Id,
    config: retained.Config,
    catalog_resolver: CatalogReplicationPublication,
    policy: LiveReplicationPublication,

    publication_worker: ?*PublicationWorker = null,
    collected_history_sequence: ?u64 = null,
    publication_group: std.Io.Group = .init,

    const PublicationWorker = struct {
        const upload = @import("../metadata/pg_spool_upload.zig");
        const producer = @import("../metadata/pg_retained_transaction.zig");
        const allocator = std.heap.smp_allocator;

        host: *CatalogRetainedStream,
        arena: std.heap.ArenaAllocator,
        credential: Credential,
        session: control.pg_slot.Session,
        source: @import("history_roots.zig").Source,
        context: operation.RequestContext,
        canceled: std.atomic.Value(bool) = .init(false),
        finished: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,

        fn run(self: *PublicationWorker) void {
            defer self.finished.store(true, .release);
            self.drive() catch |err| {
                self.failure = err;
            };
        }

        const CleanupRegistry = struct {
            storage: Registry,
            policy: *LiveReplicationPublication,
            session: control.pg_slot.Session,

            pub fn artifactStore(self: CleanupRegistry, scope: control.pg_slot.spool_control.Scope, context: operation.RequestContext) !@import("../serverless/artifacts/store.zig").ArtifactStore {
                return self.storage.artifactStore(scope, context);
            }

            pub fn authorizeCleanup(self: CleanupRegistry, state: control.State, context: operation.RequestContext) !void {
                try self.policy.authorize(state, self.policy.expected, context);
                const slot = state.slot orelse return error.ChangefeedResynchronizationRequired;
                try slot.verifySession(self.session, try self.policy.nowSeconds(context));
            }
        };

        const HistoryPolicy = struct {
            source: @import("history_roots.zig").Source,
            selected: CleanupRegistry,
            io: std.Io,

            pub fn authorizeRoot(self: *HistoryPolicy, state: control.State, _: control.Namespace, context_value: operation.RequestContext) !void {
                try self.selected.authorizeCleanup(state, context_value);
            }

            pub fn validateRoot(self: *HistoryPolicy, state: control.State, namespace: control.Namespace, fence: @import("../metadata/api.zig").CatalogRouteFence, table: []const u8, context_value: operation.RequestContext) !void {
                try self.source.validate(allocator, state, namespace, fence, table, context_value);
            }

            pub fn newAttempt(self: *HistoryPolicy, _: control.State, _: control.Namespace, context_value: operation.RequestContext) !control.Id {
                var id: control.Id = undefined;
                while (true) {
                    try context_value.ensureActive();
                    self.io.random(&id);
                    if (!std.mem.allEqual(u8, &id, 0)) return id;
                }
            }

            pub fn context(_: *HistoryPolicy, value: operation.RequestContext) !operation.RequestContext {
                return value;
            }
        };

        fn reclaimHistory(self: *PublicationWorker, selected: CleanupRegistry) !void {
            const host = self.host;
            var client: @import("changefeed_metadata.zig").Client = .{ .port = host.service.* };
            const bytes = try client.readChangefeedState(allocator, host.id, self.context);
            defer allocator.free(bytes);
            var observed = try control.parse(allocator, bytes);
            defer observed.deinit();
            const state = observed.value;
            try selected.authorizeCleanup(state, self.context);
            if (state.roots.len != 1) return error.ChangefeedCapabilityUnsupported;
            const root = state.roots[0];
            const target = root.acknowledged.sequence;
            if (target == root.start or host.collected_history_sequence == target) return;
            var captured = try self.source.capture(allocator, state.definition.?, self.context);
            defer captured.deinit();
            var history_policy: HistoryPolicy = .{ .source = self.source, .selected = selected, .io = selected.policy.io };
            const more = try @import("history_roots.zig").flushAcknowledged(&client, &history_policy, allocator, state, try captured.find(root.namespace), self.context);
            if (!more) host.collected_history_sequence = target;
        }

        fn drive(self: *PublicationWorker) !void {
            const host = self.host;
            var admission = try host.catalog_resolver.server.acquireSqlExecution(false);
            defer admission.release();
            const identity: ReplicationIdentity = .{ .context = &self.credential, .read = Adapter.replicationIdentity };
            var resolver = host.catalog_resolver;
            resolver.identity = identity;
            var policy = host.policy;
            policy.allocator = allocator;
            policy.identity = identity;
            policy.resolver = resolver.resolver();
            const cleanup_registry: CleanupRegistry = .{ .storage = host.registry, .policy = &policy, .session = self.session };
            try self.reclaimHistory(cleanup_registry);
            const cleanup = try @import("../metadata/pg_spool_cleanup.zig").cleanup(host.service, cleanup_registry, allocator, host.id, .{}, self.context);
            switch (cleanup.status) {
                .checkpointed, .collected => return,
                .idle, .waiting, .retained => {},
            }
            var selected: LiveReplicationPublication.Upload = .{ .owner = &policy };
            var transaction: producer.Producer(LiveReplicationPublication.Upload) = .{
                .policy = &selected,
                .allocator = allocator,
                .source = self.source,
                .byte_limit = @import("history_page_operations.zig").pages.max_source_bytes,
            };
            _ = try upload.drive(host.service, host.registry, &transaction, allocator, host.id, self.session, .{ .require_shared = host.config.require_shared }, self.context);
        }

        fn deinit(self: *PublicationWorker) void {
            std.crypto.secureZero(u8, &self.credential.verifier_mac);
            self.arena.deinit();
            allocator.destroy(self);
        }
    };

    pub fn schedulePublication(self: *CatalogRetainedStream, session: control.pg_slot.Session, request_value: wire.Request) !void {
        const source = self.catalog_resolver.server.cfg.native_history_roots orelse return;
        // Only the native credential callback can provide an isolated
        // verifier snapshot. Embedded replication hosts own their producer.
        if (self.policy.identity.read != &Adapter.replicationIdentity) return;
        const io = self.policy.io;
        if (self.publication_worker) |previous| {
            if (!previous.finished.load(.acquire)) return;
            try self.publication_group.await(io);
            const failure = previous.failure;
            previous.deinit();
            self.publication_worker = null;
            if (failure) |err| switch (err) {
                error.ChangefeedConflict, error.ChangefeedBackpressure, error.MetadataMutationOutcomeUnknown, error.ReadUnavailable, error.SqlWriteCapacityUnavailable, error.DeadlineExceeded, error.Canceled, error.Cancelled => {},
                else => {
                    std.log.warn("logical slot publication failed: {s}", .{@errorName(err)});
                    return err;
                },
            };
        }
        var context = try Bridge.metadataContext(request_value);
        const worker = try PublicationWorker.allocator.create(PublicationWorker);
        errdefer PublicationWorker.allocator.destroy(worker);
        worker.arena = std.heap.ArenaAllocator.init(PublicationWorker.allocator);
        errdefer worker.arena.deinit();
        const a = worker.arena.allocator();
        const credential: *Credential = @ptrCast(@alignCast(self.policy.identity.context));
        try credential.validate();
        worker.credential = credential.*;
        errdefer std.crypto.secureZero(u8, &worker.credential.verifier_mac);
        worker.credential.username = try a.dupe(u8, credential.username);
        worker.credential.principal = try a.dupe(u8, credential.principal);
        worker.credential.trusted_identity = try http.cloneCatalogIdentity(a, credential.trusted_identity);
        worker.credential.trusted_issuer = if (credential.trusted_issuer) |issuer| try a.dupe(u8, issuer) else null;
        worker.host = self;
        worker.source = source;
        worker.session = session;
        worker.finished = .init(false);
        worker.canceled = .init(false);
        worker.failure = null;
        // The CopyBoth callback has a short transport budget. Artifact I/O
        // has its own bounded deadline and is canceled when the stream closes.
        context.deadline_ns = @import("antfly_platform").time.monotonicNs() +| 15 * std.time.ns_per_s;
        context.cancellation = operation.CancellationToken.fromAtomic(&worker.canceled);
        worker.context = context;
        self.publication_group.concurrent(io, PublicationWorker.run, .{worker}) catch {
            worker.deinit();
            return;
        };
        self.publication_worker = worker;
    }

    pub fn open(slots: CatalogReplicationSlots, registry: Registry, config: retained.Config, a: std.mem.Allocator, start: wire.replication_commands.Start, request_value: wire.Request) !wire.ReplicationStream {
        const context = try Bridge.metadataContext(request_value);
        const service = if (slots.server.source.changefeeds) |*port| port else return error.UnsupportedReplicationMode;
        const retained_alloc = request_value.scratch_allocator orelse a;
        const self = try retained_alloc.create(CatalogRetainedStream);
        errdefer retained_alloc.destroy(self);
        self.arena = std.heap.ArenaAllocator.init(retained_alloc);
        errdefer self.arena.deinit();
        const owned = self.arena.allocator();
        var state = try slots.readOwned(retained_alloc, start.slot, context);
        defer state.deinit();
        self.catalog_resolver = .{ .server = slots.server, .database_name = try owned.dupe(u8, slots.database_name), .identity = slots.identity };
        var scratch = std.heap.ArenaAllocator.init(retained_alloc);
        defer scratch.deinit();
        const publication = try self.catalog_resolver.resolve(scratch.allocator(), slots.database, start.publications, context);
        const binding_value = try publication.binding();
        const names = try owned.alloc([]const u8, binding_value.publications.len);
        for (binding_value.publications, names) |name, *copy| copy.* = try owned.dupe(u8, name);
        const expected: control.pg_slot.Binding = .{ .publications = names, .projection = binding_value.projection };
        if (state.value.phase == .bootstrap) {
            try publication.authorize(slots.identity, scratch.allocator(), state.value, expected);
            try config.validate();
            try registry.qualify(state.value, config.require_shared, context);
            const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_s;
            try SlotCommands.startExported(service.*, scratch.allocator(), state.value, @backingInt(start.position), now, CatalogReplicationSlots.maintenanceContext(context));
        }
        var actor: control.Id = undefined;
        while (true) {
            try request_value.check();
            request_value.io.random(&actor);
            if (!std.mem.allEqual(u8, &actor, 0)) break;
        }
        self.allocator = retained_alloc;
        self.service = service;
        self.registry = registry;
        self.id = state.value.id;
        self.actor = actor;
        self.config = config;
        self.publication_worker = null;
        self.collected_history_sequence = null;
        self.publication_group = .init;
        self.policy = .{ .allocator = retained_alloc, .io = request_value.io, .identity = slots.identity, .resolver = self.catalog_resolver.resolver(), .expected = expected };
        return Bridge.Bridge(CatalogRetainedStream).open(self, a, request_value, start);
    }

    pub fn binding(self: *CatalogRetainedStream, _: std.mem.Allocator, start: wire.replication_commands.Start, request_value: wire.Request) !control.pg_slot.Binding {
        try request_value.check();
        if (start.publications.len != self.policy.expected.publications.len) return error.ChangefeedResynchronizationRequired;
        return self.policy.expected;
    }

    pub fn request(_: *CatalogRetainedStream, value: wire.Request) !operation.RequestContext {
        return Bridge.metadataContext(value);
    }

    pub fn authorize(self: *CatalogRetainedStream, state: control.State, selected: control.pg_slot.Binding, context: operation.RequestContext) !void {
        try self.policy.authorize(state, selected, context);
    }

    pub fn nowSeconds(self: *CatalogRetainedStream, context: operation.RequestContext) !u64 {
        return self.policy.nowSeconds(context);
    }

    fn closeContext() operation.RequestContext {
        const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s;
        return CatalogReplicationSlots.maintenanceContext(.{ .deadline_ns = deadline });
    }

    pub fn failedOpen(self: *CatalogRetainedStream) void {
        const io = self.policy.io;
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        retained.releaseActor(self.service, self, self.allocator, self.id, self.actor, closeContext()) catch |err| {
            std.log.warn("logical slot lease release after failed open deferred: {s}", .{@errorName(err)});
        };
    }

    pub fn closed(self: *CatalogRetainedStream, owner: *retained.Owner) void {
        if (self.publication_worker) |worker| {
            worker.canceled.store(true, .release);
            self.publication_group.cancel(self.policy.io);
            worker.deinit();
            self.publication_worker = null;
        }
        const io = self.policy.io;
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        owner.release(self.service, self, self.allocator, closeContext()) catch |err| {
            std.log.warn("logical slot lease release deferred: {s}", .{@errorName(err)});
        };
        const a = self.allocator;
        self.arena.deinit();
        a.destroy(self);
    }
};

/// Command completion for a replication owner that has resolved the database,
/// authorized the publication and captured the exact metadata owner revision.
pub const SlotCommands = struct {
    const control = @import("../metadata/changefeed_control.zig");
    const Metadata = @import("changefeed_metadata.zig").Port;

    pub fn drop(
        metadata: Metadata,
        alloc: std.mem.Allocator,
        command: wire.replication_commands.Command,
        write: control.Write,
        context: operation.RequestContext,
    ) !wire.Result {
        if (command != .drop_slot or write.mutation != .slot or write.mutation.slot != .drop) return error.InvalidChangefeed;
        if (!std.mem.eql(u8, command.drop_slot.slot, write.mutation.slot.drop.name)) return error.InvalidChangefeed;
        try metadata.dropSlot(alloc, write, context);
        return .{ .command_tag = "DROP_REPLICATION_SLOT", .mutation_outcome = .committed };
    }

    pub const DropRefresh = struct {
        ptr: *anyopaque,
        /// Reauthorize the same slot and capture its latest metadata revision
        /// and the current trusted lease clock under the original request.
        read: *const fn (*anyopaque, std.mem.Allocator, wire.Request) anyerror!control.Write,
    };

    pub fn dropWait(
        metadata: Metadata,
        alloc: std.mem.Allocator,
        command: wire.replication_commands.Command,
        original: control.Write,
        request: wire.Request,
        context: operation.RequestContext,
        refresh: DropRefresh,
    ) !wire.Result {
        if (command != .drop_slot or original.mutation != .slot or original.mutation.slot != .drop) return error.InvalidChangefeed;
        var write = original;
        while (true) {
            try request.check();
            return drop(metadata, alloc, command, write, context) catch |err| {
                if (!command.drop_slot.wait or err != error.ReplicationSlotActive) return err;
                try request.io.sleep(.fromMilliseconds(25), .awake);
                try request.check();
                try context.ensureActive();
                var scratch = std.heap.ArenaAllocator.init(alloc);
                defer scratch.deinit();
                const current = try refresh.read(refresh.ptr, scratch.allocator(), request);
                try current.validate();
                if (current.mutation != .slot or current.mutation.slot != .drop) return error.InvalidChangefeed;
                const next = current.mutation.slot.drop;
                const previous = write.mutation.slot.drop;
                if (!std.meta.eql(current.id, original.id) or !std.meta.eql(current.authority, original.authority) or
                    !std.meta.eql(current.owner, original.owner) or !std.meta.eql(current.authorization, original.authorization) or
                    current.expected_epoch != original.expected_epoch or current.expected_revision < write.expected_revision or
                    !std.meta.eql(next.database, previous.database) or !std.mem.eql(u8, next.name, previous.name) or
                    next.now < previous.now) return error.ChangefeedConflict;
                // Only these scalar values survive the refresh arena.
                write.expected_revision = current.expected_revision;
                write.mutation.slot.drop.now = next.now;
                continue;
            };
        }
    }

    fn startExported(metadata: Metadata, a: std.mem.Allocator, state: control.State, requested: u64, now: u64, context: operation.RequestContext) !void {
        const slot = state.slot orelse return error.ReplicationSlotNotFound;
        const definition = state.definition orelse return error.ChangefeedConflict;
        const name = try control.SlotSnapshot.capture(state, slot.database, definition.owner, now);
        _ = try slot.restartAt(requested);
        try metadata.mutate(a, .{
            .id = state.id,
            .authority = state.authority,
            .owner = definition.owner,
            .authorization = definition.authorization,
            .expected_revision = state.revision,
            .expected_epoch = state.epoch,
            .mutation = .{ .start_exported_slot = .{ .name = name, .now = now, .requested_lsn = requested } },
        }, context);
    }

    pub fn create(
        metadata: Metadata,
        alloc: std.mem.Allocator,
        command: wire.replication_commands.Create,
        write: control.Write,
        now: u64,
        context: operation.RequestContext,
    ) !wire.Result {
        if (command.temporary) return error.UnsupportedReplicationOption;
        if (write.mutation != .seal) return error.InvalidChangefeed;
        const configuration = write.mutation.seal.slot orelse return error.InvalidChangefeed;
        if (!std.mem.eql(u8, command.slot, configuration.name)) return error.InvalidChangefeed;
        if (command.snapshot == .use_snapshot) return error.UnsupportedReplicationOption;
        const snapshot_name = if (command.snapshot == .export_snapshot) try alloc.alloc(u8, control.SlotSnapshot.name_size) else null;
        errdefer if (snapshot_name) |bytes| alloc.free(bytes);
        const point = try wire.replication_commands.formatLsn(alloc, @fromBackingInt(configuration.consistent_lsn));
        errdefer alloc.free(point);
        const values = try alloc.dupe(std.json.Value, &.{
            .{ .string = command.slot },
            .{ .string = point },
            if (snapshot_name) |name| .{ .string = name } else .null,
            .{ .string = "pgoutput" },
        });
        errdefer alloc.free(values);
        const rows = try alloc.dupe([]const std.json.Value, &.{values});
        errdefer alloc.free(rows);
        // Allocate the wire result before committing so allocation failure has
        // a definite outcome. The result borrows the command and caller arena.
        const sealed = try metadata.sealSlot(alloc, write, now, context);
        if (sealed.consistent_lsn != configuration.consistent_lsn or (sealed.snapshot != null) != (snapshot_name != null))
            return error.MetadataMutationOutcomeUnknown;
        if (snapshot_name) |bytes| @memcpy(bytes, &sealed.snapshot.?.name());

        return .{
            .columns = &.{
                .{ .name = "slot_name", .type = .string },
                .{ .name = "consistent_point", .type = .string },
                .{ .name = "snapshot_name", .type = .string },
                .{ .name = "output_plugin", .type = .string },
            },
            .rows = rows,
            .command_tag = "CREATE_REPLICATION_SLOT",
            .mutation_outcome = .committed,
        };
    }
};

pub const ReplicationFactory = struct {
    context: *anyopaque,
    open_fn: *const fn (*anyopaque, std.mem.Allocator, ReplicationIdentity, wire.Request) anyerror!wire.ReplicationSession,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,

    const Boundary = @import("antfly_local_sources").runtime_callback_abi.Boundary(VTable);
    const VTable = struct { open: @FieldType(ReplicationFactory, "open_fn") };

    fn open(self: ReplicationFactory, alloc: std.mem.Allocator, identity: ReplicationIdentity, request: wire.Request) !wire.ReplicationSession {
        return Boundary.call("open", self.boundary_dispatch, self.open_fn, .{ self.context, alloc, identity, request });
    }
};

/// Enroll a resolved publication at one native root for a no-snapshot slot.
/// A host must supply the exact publication set before it offers this mode.
pub const CatalogSlotEnrollment = struct {
    const control = publication_control;
    const roots = @import("history_roots.zig");
    const metadata = @import("changefeed_metadata.zig");

    const Policy = struct {
        slots: CatalogReplicationSlots,
        source: roots.Source,
        services: *CatalogReplicationPublication,
        expected: control.pg_slot.Binding,
        owner: control.Digest,
        allocator: std.mem.Allocator,
        io: std.Io,

        pub fn authorizeRoot(self: *Policy, state: control.State, _: control.Namespace, context_value: operation.RequestContext) !void {
            var scratch = std.heap.ArenaAllocator.init(self.allocator);
            defer scratch.deinit();
            const a = scratch.allocator();
            var identity = try self.slots.identity.read(self.slots.identity.context, a);
            defer identity.deinit(a);
            const owner = try @import("changefeed_public.zig").durableOwner(identity);
            if (!std.crypto.timing_safe.eql(control.Digest, owner, self.owner) or
                !std.crypto.timing_safe.eql(control.Digest, owner, state.definition.?.owner))
                return error.Forbidden;
            const current = try self.services.resolve(a, self.slots.database, self.expected.publications, context_value);
            if (!std.meta.eql((try current.binding()).identity(), self.expected.identity())) return error.ChangefeedResynchronizationRequired;
        }

        pub fn validateRoot(self: *Policy, state: control.State, namespace: control.Namespace, fence: @import("../metadata/api.zig").CatalogRouteFence, table: []const u8, context_value: operation.RequestContext) !void {
            try self.source.validate(self.allocator, state, namespace, fence, table, context_value);
        }

        pub fn newAttempt(self: *Policy, _: control.State, _: control.Namespace, context_value: operation.RequestContext) !control.Id {
            var id: control.Id = undefined;
            while (true) {
                try context_value.ensureActive();
                self.io.random(&id);
                if (!std.mem.allEqual(u8, &id, 0)) return id;
            }
        }

        pub fn context(_: *Policy, value: operation.RequestContext) !operation.RequestContext {
            return value;
        }
    };

    pub fn create(slots: CatalogReplicationSlots, a: std.mem.Allocator, names: []const []const u8, command: wire.replication_commands.Create, request: wire.Request, context: operation.RequestContext) !wire.Result {
        if (command.snapshot == .use_snapshot or command.temporary) return error.UnsupportedReplicationOption;
        if (!control.pg_slot.validName(command.slot)) return error.InvalidReplicationSlotName;
        const source = slots.server.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
        const export_snapshot = command.snapshot == .export_snapshot;
        if (export_snapshot and source.bootstrap_pages == null) return error.ChangefeedCapabilityUnsupported;
        const port = slots.server.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
        const maintenance = CatalogReplicationSlots.maintenanceContext(context);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var resolver: CatalogReplicationPublication = .{ .server = slots.server, .identity = slots.identity, .database_name = slots.database_name };
        const publication = try resolver.resolve(scratch, slots.database, names, maintenance);
        const binding = try publication.binding();
        var identity = try slots.identity.read(slots.identity.context, scratch);
        defer identity.deinit(scratch);
        const owner = try @import("changefeed_public.zig").durableOwner(identity);
        const tables = try scratch.alloc(control.Table, publication.members.len);
        for (publication.members, tables) |member, *table| table.* = member.table;
        const definition: control.Definition = .{
            .owner = owner,
            .authorization = binding.identity(),
            .tables = tables,
            .projection = .rows,
            .start = if (export_snapshot) .snapshot_then_changes else .now,
        };
        var captured = try source.capture(scratch, definition, maintenance);
        defer captured.deinit();
        if (captured.roots.len != 1) return error.ChangefeedCapabilityUnsupported;
        const root = captured.roots[0];
        const authority = try @import("history_policy_provisioning.zig").authority(root.fence);
        if (try port.authority(scratch, maintenance)) |registered| {
            if (!std.meta.eql(registered.incarnation, authority)) return error.ChangefeedResynchronizationRequired;
        }
        const id = control.pg_slot.feedId(slots.database, command.slot);
        var client: metadata.Client = .{ .port = port };
        var policy: Policy = .{ .slots = slots, .source = source, .services = &resolver, .expected = binding, .owner = owner, .allocator = a, .io = request.io };
        const ns = std.Io.Clock.now(.real, request.io).nanoseconds;
        if (ns <= 0) return error.ChangefeedResynchronizationRequired;
        const now = std.math.cast(u64, @divFloor(ns, std.time.ns_per_s)) orelse return error.ChangefeedResynchronizationRequired;
        for (0..8) |_| {
            try request.check();
            var iteration = std.heap.ArenaAllocator.init(a);
            defer iteration.deinit();
            const work = iteration.allocator();
            const bytes = try port.read(work, id, maintenance);
            var parsed = try control.parse(work, bytes);
            defer parsed.deinit();
            const state = parsed.value;
            if (state.phase != .empty and (!std.meta.eql(state.authority, authority) or
                !std.meta.eql(state.definition.?.identity(), definition.identity()))) return error.ChangefeedConflict;
            const current = if (state.phase == .empty) control.State{ .definition = definition } else state;
            try policy.authorizeRoot(current, root.namespace, maintenance);
            var write: control.Write = .{ .id = id, .authority = authority, .owner = owner, .authorization = definition.authorization, .expected_revision = state.revision, .expected_epoch = state.epoch, .mutation = undefined };
            if (state.phase == .empty) {
                write.mutation = .{ .create = .{ .id = id, .definition = definition, .now = now, .expires_at = now +| 3600, .bootstrap_expires_at = if (export_snapshot) now +| control.max_bootstrap_seconds else 0 } };
            } else if (state.phase == .enrolling and !state.expiryDue(now)) {
                if (state.roots.len == 0) {
                    write.mutation = .{ .plan_roots = try captured.initialPlans(work) };
                } else {
                    if (state.roots.len != 1 or !std.meta.eql(state.roots[0].namespace, root.namespace)) return error.ChangefeedResynchronizationRequired;
                    const enrolled = state.roots[0];
                    if (enrolled.consumer_epoch == 0) {
                        try roots.enrollRoot(&client, &policy, work, state, root, tables[0].policy_revision, maintenance);
                        continue;
                    }
                    try policy.validateRoot(state, root.namespace, root.fence, root.table_name, maintenance);
                    const continuation = control.hash(.{ .domain = "antfly:pg-slot-enrollment:v1", .authority = authority, .definition = definition.identity(), .root = root.namespace, .consumer_epoch = enrolled.consumer_epoch, .start = enrolled.start });
                    const cut = if (enrolled.snapshot) |pin|
                        control.hash(.{ .domain = "antfly:pg-slot-snapshot-enrollment:v1", .continuation = continuation, .pin = pin })
                    else
                        continuation;
                    write.mutation = .{ .seal = .{ .cut = cut, .slot = .{ .database = slots.database, .name = command.slot, .binding = binding } } };
                    return SlotCommands.create(port, a, command, write, now, maintenance);
                }
            } else return error.ChangefeedConflict;
            try port.mutate(work, write, maintenance);
        }
        return error.MetadataMutationOutcomeUnknown;
    }
};

/// Native pgwire session callbacks for a host that supplies the database
/// bootstrap authority and qualified spool namespace.
pub const CatalogReplicationFactory = struct {
    const control = publication_control;
    const pg = wire.replication_commands;

    server: *http.ApiHttpServer,
    registry: CatalogRetainedStream.Registry,
    stream_config: CatalogRetainedStream.retained.Config = .{},
    bootstrap: Bootstrap,
    publication: ?[]const u8 = null,

    pub const System = struct {
        id: u64,
        timeline: i32,
        position: pg.Lsn,
    };

    pub const Bootstrap = struct {
        ptr: *anyopaque,
        identify: *const fn (*anyopaque, CatalogReplicationSlots, std.mem.Allocator, operation.RequestContext) anyerror!System,
        create: *const fn (*anyopaque, CatalogReplicationSlots, std.mem.Allocator, pg.Create, wire.Request, operation.RequestContext) anyerror!wire.Result,
    };

    fn configured(api: *http.ApiHttpServer, io: std.Io, publication: []const u8) !CatalogReplicationFactory {
        if (api.cfg.pgwire_replication != null or api.source.changefeeds == null or api.source.pg_publications == null or
            api.cfg.native_history_roots == null) return error.ChangefeedCapabilityUnsupported;
        var key_buffer: [256]u8 = undefined;
        _ = try publication_metadata.key(&key_buffer, 1, @splat(1), publication);
        const shared = api.cfg.deployment_mode != .standalone;
        const store = if (api.cfg.pgwire_spool_store orelse api.cfg.document_artifact_store) |configured_store| configured_store else blk: {
            if (shared) return error.PgSpoolStorageUnsupported;
            const files = if (api.document_artifact_files) |*value| value else return error.PgSpoolStorageUnsupported;
            break :blk try files.spoolArtifactStore(.{});
        };
        const registry: CatalogRetainedStream.Registry = .{ .store = store, .io = io };
        try registry.requireAvailable(shared, operation.RequestContext{});
        return .{
            .server = api,
            .registry = registry,
            .stream_config = .{ .require_shared = shared },
            .bootstrap = .{ .ptr = undefined, .identify = identifyNative, .create = createNative },
            .publication = publication,
        };
    }

    fn identifyNative(_: *anyopaque, slots: CatalogReplicationSlots, _: std.mem.Allocator, context: operation.RequestContext) !System {
        try context.ensureActive();
        // Slot LSNs are local to the slot. There is no database-wide WAL
        // position to advertise before the client selects one.
        return .{ .id = slots.system_identifier, .timeline = 1, .position = @fromBackingInt(0) };
    }

    fn createNative(raw: *anyopaque, slots: CatalogReplicationSlots, a: std.mem.Allocator, command: pg.Create, request: wire.Request, context: operation.RequestContext) !wire.Result {
        const self: *CatalogReplicationFactory = @ptrCast(@alignCast(raw));
        return CatalogSlotEnrollment.create(slots, a, &.{self.publication.?}, command, request, context);
    }

    pub fn factory(self: *CatalogReplicationFactory) ReplicationFactory {
        if (self.publication != null) self.bootstrap.ptr = self;
        return .{ .context = self, .open_fn = Session.open };
    }

    const Session = struct {
        host: *CatalogReplicationFactory,
        allocator: std.mem.Allocator,
        slots: CatalogReplicationSlots,
        database_name: []u8,
        owner: control.Digest,

        fn open(raw: *anyopaque, a: std.mem.Allocator, identity: ReplicationIdentity, request: wire.Request) !wire.ReplicationSession {
            const host: *CatalogReplicationFactory = @ptrCast(@alignCast(raw));
            const context = try CatalogRetainedStream.Bridge.metadataContext(request);
            if (host.server.source.changefeeds == null or host.server.source.pg_publications == null)
                return error.UnsupportedReplicationMode;
            try host.registry.requireAvailable(host.stream_config.require_shared, context);
            const self = try a.create(Session);
            errdefer a.destroy(self);
            const name = try a.dupe(u8, request.database orelse "default");
            errdefer a.free(name);
            const slots = try CatalogReplicationSlots.open(host.server, identity, a, name, context);
            var authenticated = try identity.read(identity.context, a);
            defer authenticated.deinit(a);
            const owner = try @import("changefeed_public.zig").durableOwner(authenticated);
            self.* = .{ .host = host, .allocator = a, .slots = slots, .database_name = name, .owner = owner };
            return .{ .context = self, .authorize = authorize, .command = command, .open_stream = openStream, .close = close };
        }

        fn authorize(raw: *anyopaque, a: std.mem.Allocator, request: wire.Request, _: ?pg.Command) !void {
            const self: *Session = @ptrCast(@alignCast(raw));
            try request.check();
            if (!std.mem.eql(u8, self.database_name, request.database orelse "default")) return error.Forbidden;
            var identity = try self.slots.identity.read(self.slots.identity.context, a);
            defer identity.deinit(a);
            const current = try @import("changefeed_public.zig").durableOwner(identity);
            if (!std.crypto.timing_safe.eql(control.Digest, current, self.owner)) return error.Forbidden;
        }

        fn command(raw: *anyopaque, a: std.mem.Allocator, request: wire.Request, input: pg.Command) !wire.Result {
            const self: *Session = @ptrCast(@alignCast(raw));
            try authorize(raw, a, request, input);
            const context = try CatalogRetainedStream.Bridge.metadataContext(request);
            switch (input) {
                .start => return error.UnsupportedReplicationMode,
                .drop_slot => return self.slots.drop(a, input, request, context),
                .create_slot, .identify_system => {},
            }
            const current = try CatalogReplicationSlots.readDatabase(self.host.server, a, self.database_name, context);
            if (!std.meta.eql(current, self.slots.database)) return error.ChangefeedResynchronizationRequired;
            const bootstrap = self.host.bootstrap;
            if (input == .create_slot) return bootstrap.create(bootstrap.ptr, self.slots, a, input.create_slot, request, context);
            const system = try bootstrap.identify(bootstrap.ptr, self.slots, a, context);
            if (system.id == 0 or system.timeline <= 0) return error.InvalidResult;
            const values = try a.dupe(std.json.Value, &.{
                .{ .string = try std.fmt.allocPrint(a, "{d}", .{system.id}) },
                .{ .integer = system.timeline },
                .{ .string = try pg.formatLsn(a, system.position) },
                .{ .string = self.database_name },
            });
            return .{
                .command_tag = "IDENTIFY_SYSTEM",
                .columns = &.{
                    .{ .name = "systemid", .type = .string },
                    .{ .name = "timeline", .type = .integer },
                    .{ .name = "xlogpos", .type = .string },
                    .{ .name = "dbname", .type = .string },
                },
                .rows = try a.dupe([]const std.json.Value, &.{values}),
            };
        }

        fn openStream(raw: *anyopaque, a: std.mem.Allocator, request: wire.Request, start: pg.Start) !wire.ReplicationStream {
            const self: *Session = @ptrCast(@alignCast(raw));
            try authorize(raw, a, request, .{ .start = start });
            return CatalogRetainedStream.open(self.slots, self.host.registry, self.host.stream_config, a, start, request);
        }

        fn close(raw: *anyopaque) void {
            const self: *Session = @ptrCast(@alignCast(raw));
            const a = self.allocator;
            a.free(self.database_name);
            a.destroy(self);
        }
    };
};

pub const Adapter = struct {
    server: *http.ApiHttpServer,
    native_replication: ?ReplicationFactory = null,

    pub fn backend(self: *Adapter) wire.Backend {
        return .{ .context = self, .vtable = if (self.replicationFactory() != null) &replication_backend else &sql_backend };
    }

    fn replicationFactory(self: *Adapter) ?ReplicationFactory {
        return self.native_replication orelse self.server.cfg.pgwire_replication;
    }

    const sql_backend: wire.Backend.VTable = .{
        .native_parameters = true,
        .authenticate = authenticate,
        .refresh = refresh,
        .describe = describe,
        .execute = execute,
        .load_settings = loadSettings,
        .validate_namespace = validateNamespace,
        .evaluate_parameters = evaluateParameters,
        .evaluate_parameter_cells = evaluateParameterCells,
        .fail_transaction = failTransaction,
        .import_snapshot = importSnapshot,
        .set_transaction_options = setTransactionOptions,
        .open_stream = openStream,
        .disconnect = disconnect,
        .completion_safe = completionSafe,
        .pin_completion = pinCompletion,
        .reserve_completion = reserveCompletion,
        .completion_ready = completionReady,
    };
    const replication_backend: wire.Backend.VTable = blk: {
        var enabled = sql_backend;
        enabled.open_replication_session = openReplication;
        break :blk enabled;
    };

    fn openReplication(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !wire.ReplicationSession {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const factory = self.replicationFactory() orelse return error.UnsupportedReplicationMode;
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        try request.check();
        try credential.validate();
        const session = try factory.open(alloc, .{ .context = credential, .read = replicationIdentity }, request);
        errdefer session.close(session.context);
        try request.check();
        return session;
    }

    fn replicationIdentity(raw: *anyopaque, alloc: std.mem.Allocator) anyerror!http.AuthenticatedIdentity {
        const credential: *Credential = @ptrCast(@alignCast(raw));
        return credential.identity(alloc);
    }

    fn loadSettings(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !@import("antfly_local_sources").sql_setting_catalog.RawSnapshot {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        var authenticated: ?http.AuthenticatedIdentity = try credential.identity(self.server.alloc);
        defer authenticated.?.deinit(self.server.alloc);
        var authority: Authority = .{ .credential = credential, .identity = &authenticated, .request = request };
        var context = try authority.context();
        context.setting_read_principal = credential.sessionPrincipal();
        const scope: @import("antfly_local_sources").sql_setting_catalog.Scope = .{ .principal = credential.sessionPrincipal(), .database = request.database orelse "default" };
        const bytes = try self.server.source.systemCatalog(alloc, context, .{ .setting_snapshot = scope });
        defer alloc.free(bytes);
        return std.json.parseFromSliceLeaky(@import("antfly_local_sources").sql_setting_catalog.RawSnapshot, alloc, bytes, .{ .allocate = .alloc_always });
    }

    fn setTransactionOptions(raw: *anyopaque, identity: wire.Identity, request: wire.Request, options: wire.TransactionOptions) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        const encoded = request.session_id orelse return error.NoActiveSqlTransaction;
        const id = @import("distributed_txn.zig").parseTxnIdHex(encoded) catch return error.SqlTransactionNotActive;
        if (options.isolation) |isolation| {
            if (isolation != .read_committed and !@import("sql_execution.zig").supportsRangeGuards(self.server))
                return error.UnsupportedSqlExecution;
        }
        try self.updateTransactionOptions(credential, request, id, options);
    }

    fn updateTransactionOptions(self: *Adapter, credential: *Credential, request: wire.Request, id: [16]u8, options: wire.TransactionOptions) !void {
        try request.check();
        try credential.validate();
        const lease = self.server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
        defer lease.release();
        try self.server.txn_sessions.setSqlTransactionOptions(self.server.alloc, id, self.server.localSessionNodeId(), .{
            .principal = credential.sessionPrincipal(),
            .database = request.database orelse "default",
            .namespace = request.session_namespace orelse request.namespace orelse "public",
        }, options.isolation, options.mode);
    }

    fn importSnapshot(raw: *anyopaque, a: std.mem.Allocator, identity: wire.Identity, request: wire.Request, name: []const u8) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        if (self.server.cfg.native_history_roots == null or self.server.cfg.native_history_roots.?.bootstrap_pages == null)
            return error.ChangefeedCapabilityUnsupported;
        const context = try @import("../pgwire/retained_stream.zig").metadataContext(request);
        if (credential.imported_snapshot) |existing| {
            if (request.session_id) |encoded| {
                const id = try @import("distributed_txn.zig").parseTxnIdHex(encoded);
                if (std.mem.eql(u8, &id, &existing.transaction)) return error.ActiveSqlTransaction;
            }
        }
        const slots = try CatalogReplicationSlots.open(self.server, .{ .context = credential, .read = replicationIdentity }, a, request.database orelse "default", context);
        const snapshot = try slots.qualifyTransactionSnapshot(a, identity, request, name);
        try self.updateTransactionOptions(credential, request, snapshot.transaction, .{ .mode = .read_only });
        credential.imported_snapshot = snapshot;
    }

    fn failTransaction(raw: *anyopaque, identity: wire.Identity, request: wire.Request) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        const encoded = request.session_id orelse return error.SqlTransactionNotActive;
        const id = try @import("distributed_txn.zig").parseTxnIdHex(encoded);
        if (try self.server.txn_sessions.principalAccess(self.server.alloc, id, credential.sessionPrincipal()) != .allowed) return error.SqlTransactionNotActive;
        var state = (try self.server.txn_sessions.getSqlState(self.server.alloc, id)) orelse return error.SqlTransactionNotActive;
        defer state.deinit(self.server.alloc);
        if (state.terminal != null or !std.mem.eql(u8, state.metadata.database, request.database orelse "default") or !std.mem.eql(u8, state.metadata.namespace, request.session_namespace orelse request.namespace orelse "public")) return error.SqlTransactionNotActive;
        try self.server.txn_sessions.setSqlFailed(self.server.alloc, id, true);
    }

    fn evaluateParameters(_: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request, expressions: []const []const u8) ![]const std.json.Value {
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        try credential.validate();
        return evaluateScalarParameters(alloc, request, expressions);
    }

    fn evaluateParameterCells(_: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request, expressions: []const []const u8) ![]const @import("antfly_local_sources").sql_scalar.Datum {
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        try credential.validate();
        return evaluateScalarCells(alloc, request, expressions);
    }

    fn evaluateScalarCells(alloc: std.mem.Allocator, request: wire.Request, expressions: []const []const u8) ![]const @import("antfly_local_sources").sql_scalar.Datum {
        if (expressions.len != request.parameter_types.len) return error.InvalidSqlParameters;
        const scalar = @import("antfly_local_sources").sql_scalar;
        const result = try alloc.alloc(scalar.Datum, expressions.len);
        if (request.parameter_descriptors.len != 0 and request.parameter_descriptors.len != expressions.len) return error.InvalidSqlParameters;
        for (expressions, request.parameter_types, result, 0..) |expression, kind, *value, index| {
            try request.check();
            var compiled = try compiler.compileScalar(alloc, expression, .{});
            defer compiled.deinit();
            if (compiled.parameter_count != 0) return error.InvalidSqlParameters;
            const expected: ?ast.ColumnType = switch (kind) {
                .unknown => null,
                inline else => |tag| @field(ast.ColumnType, @tagName(tag)),
            };
            // Empty binding environment forbids table reads/correlated names;
            // the scalar compiler rejects subqueries and statement commands.
            const descriptor: ?scalar.Type = if (request.parameter_descriptors.len != 0) request.parameter_descriptors[index] else if (expected) |type_| .{ .kind = type_ } else null;
            var program = try scalar.bindTypedExpectedWithSettings(alloc, compiled.expression, &.{}, &.{}, descriptor, .{}, null);
            defer program.deinit();
            const evaluated = try program.evaluate(alloc, &.{}, &.{}, .{});
            var retained = evaluated;
            retained.patterns = null;
            value.* = try @import("antfly_local_sources").sql_operators.cloneDatum(alloc, retained);
        }
        try request.check();
        return result;
    }

    fn evaluateScalarParameters(alloc: std.mem.Allocator, request: wire.Request, expressions: []const []const u8) ![]const std.json.Value {
        const cells = try evaluateScalarCells(alloc, request, expressions);
        const result = try alloc.alloc(std.json.Value, cells.len);
        for (cells, request.parameter_types, result) |evaluated, kind, *value| {
            value.* = if (evaluated.array) |array|
                try @import("antfly_local_sources").sql_array_wire.toJsonLeaky(alloc, array.*, .{})
            else if (evaluated.numeric) |number| numeric: {
                const exact = @import("antfly_local_sources").sql_numeric_value;
                var context: exact.Context = .{ .alloc = alloc };
                break :numeric .{ .number_string = try exact.format(&context, number.*) };
            } else if (kind == .json and !evaluated.sql_null)
                .{ .string = try std.json.Stringify.valueAlloc(alloc, evaluated.value, .{}) }
            else
                try native.clone(alloc, evaluated.value);
        }
        try request.check();
        return result;
    }

    fn openStream(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !?wire.ReadStream {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        if (try wire.replication_commands.parsePublication(alloc, request.statement, publication_control.max_tables)) |publication| {
            var parsed = publication;
            parsed.deinit();
            return null;
        }
        var job = StreamJob{ .adapter = self, .alloc = alloc, .credential = @ptrCast(@alignCast(identity.context)), .request = request };
        try job.dispatch();
        return job.opened;
    }

    fn validateNamespace(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        var job = StreamJob{ .adapter = self, .alloc = alloc, .credential = @ptrCast(@alignCast(identity.context)), .request = request, .namespace_only = true };
        try job.dispatch();
    }

    fn refresh(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, token: []const u8) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const current: *Credential = @ptrCast(@alignCast(identity.context));
        if (current.trusted_server != self.server) return error.Unauthorized;
        const next = try Credential.authenticateTrusted(alloc, self.server, self.server.cfg.user_manager, token);
        defer Credential.release(next, alloc);
        // Renewal never changes the owner of prepared statements or transactions.
        if (!std.mem.eql(u8, current.principal, next.principal) or !std.mem.eql(u8, current.username, next.username) or !std.meta.eql(current.user_instance_id, next.user_instance_id)) return error.Unauthorized;
        std.mem.swap(?http.AuthenticatedIdentity, &current.trusted_identity, &next.trusted_identity);
        std.mem.swap(?[]u8, &current.trusted_issuer, &next.trusted_issuer);
        current.verifier_mac = next.verifier_mac;
        current.trusted_expires = next.trusted_expires;
    }

    fn authenticate(raw: *anyopaque, alloc: std.mem.Allocator, username: []const u8, password: []const u8) !wire.Identity {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        // A password callback is mandatory even if HTTP auth is disabled.
        // Neither a startup username nor database name creates authority.
        const manager = self.server.cfg.user_manager;
        if (username.len == 0 or username.len > 256 or password.len > 4096) return error.Unauthorized;
        const credential = if (std.mem.eql(u8, username, trusted_principal_username)) blk: {
            break :blk try Credential.authenticateTrusted(alloc, self.server, manager, password);
        } else try Credential.authenticate(alloc, manager orelse return error.Unauthorized, username, password);
        return .{ .context = credential, .release = Credential.release, .renewable = credential.trusted_server != null };
    }

    fn describe(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !wire.Description {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        var job = Job{ .adapter = self, .alloc = alloc, .credential = @ptrCast(@alignCast(identity.context)), .request = request, .kind = .describe };
        try self.dispatch(&job);
        return job.description.?;
    }

    fn execute(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !wire.Result {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        var job = Job{ .adapter = self, .alloc = alloc, .credential = @ptrCast(@alignCast(identity.context)), .request = request, .kind = .execute };
        try self.dispatch(&job);
        return job.result.?;
    }

    fn completionReady(raw: *anyopaque) bool {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        return @import("sql_completion.zig").capableJoined(self.server, self.server.sharedApiNetworkIo() orelse return false);
    }
    fn reserveCompletion(raw: *anyopaque, completion: *@import("../pgwire/completion.zig").Completion, desired: usize) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        try @import("sql_completion.zig").reserveJoined(self.server, completion, desired, self.server.sharedApiNetworkIo() orelse return error.SqlWriteCapacityUnavailable);
    }
    fn pinCompletion(raw: *anyopaque, alloc: std.mem.Allocator, completion: *@import("../pgwire/completion.zig").Completion) void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const network = self.server.sharedApiNetworkIo() orelse {
            completion.unknown = true;
            return;
        };
        @import("sql_completion.zig").pinJoined(self.server, alloc, completion, network);
    }

    fn completionSafe(_: *anyopaque, identity: wire.Identity) bool {
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        return credential.session_cleanup_complete;
    }

    fn disconnect(raw: *anyopaque, identity: wire.Identity, session_id: ?[]const u8) void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const encoded = session_id orelse return;
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        credential.session_cleanup_complete = false;
        const id = @import("distributed_txn.zig").parseTxnIdHex(encoded) catch return;
        const io = http.ApiHttpServer.configuredDurableIo(self.server.cfg) orelse return;
        const network_io = self.server.sharedApiNetworkIo() orelse return;
        const Cleanup = struct {
            server: *http.ApiHttpServer,
            principal: []const u8,
            id: [16]u8,
            completion_io: std.Io,
            done: std.Io.Event = .unset,
            credential: *Credential,

            fn run(job: *@This()) void {
                defer job.done.set(job.completion_io);
                const access = job.server.txn_sessions.principalAccess(job.server.alloc, job.id, job.principal) catch return;
                if (access == .missing) {
                    job.credential.session_cleanup_complete = true;
                    return;
                }
                if (access != .allowed) return;
                // Never erase a started distributed decision; durable session
                // maintenance retains it after this connection disappears.
                job.credential.session_cleanup_complete = job.server.txn_sessions.removeBeforeExecution(job.server.alloc, job.id);
            }
        };
        var job = Cleanup{ .server = self.server, .principal = credential.sessionPrincipal(), .id = id, .completion_io = network_io, .credential = credential };
        var future = io.concurrent(Cleanup.run, .{&job}) catch return;
        job.done.waitUncancelable(network_io);
        @import("protected_future.zig").wait(io, &future);
    }

    fn dispatch(self: *Adapter, job: *Job) !void {
        try validateRequest(job.request);
        if (self.server.cfg.user_manager != job.credential.manager) return error.Unauthorized;
        const io = http.ApiHttpServer.configuredDurableIo(self.server.cfg) orelse return error.SqlWriteCapacityUnavailable;
        var future = io.concurrent(Job.run, .{job}) catch return error.SqlWriteCapacityUnavailable;
        // Keep borrowed request storage alive through a durable decision. The
        // listener sets cancellation/deadline on shutdown, native checkpoints
        // observe it, and this adapter joins rather than abandoning the job.
        job.done.waitUncancelable(job.request.io);
        @import("protected_future.zig").wait(io, &future);
        if (job.failure) |err| return err;
    }
};

const Pull = @import("antfly_local_sources").sql_read_stream;

fn cloneSettingOverlay(alloc: std.mem.Allocator, entries: []const @import("antfly_local_sources").sql_setting_catalog.OverlayEntry) ![]const @import("antfly_local_sources").sql_setting_catalog.OverlayEntry {
    const result = try alloc.alloc(@import("antfly_local_sources").sql_setting_catalog.OverlayEntry, entries.len);
    for (entries, result) |entry, *out| {
        out.* = entry;
        if (entry.value == .string) out.value = .{ .string = try alloc.dupe(u8, entry.value.string) };
    }
    return result;
}

test "pgwire native array page delivery preserves dimensions NULLs and exact integers" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const alloc = arena.allocator();
            const sources = @import("antfly_local_sources");
            const array = (try sources.sql_array_text.decodeLeaky(alloc, .int64, "[-1:1]={9007199254740993,NULL,7}", .{})).value;
            const values = [_]sources.sql_scalar.Datum{.{ .array = &array, .sql_null = false }};
            var owner: OwnedRead.PageOwner = .{ .alloc = a, .page = .{
                .arena = std.heap.ArenaAllocator.init(a),
                .columns = &.{.{ .name = "a", .type = .array, .element_type = .int64 }},
                .values = .{ .rows = &.{&values} },
                .exhausted = true,
            } };
            defer owner.page.deinit();
            const cell = try OwnedRead.PageOwner.cell(&owner, alloc, 0, 0);
            try std.testing.expect(!cell.sql_null);
            try std.testing.expectEqual(std.json.Value.null, cell.value);
            try std.testing.expect(cell.array != null);
            var work: sources.sql_array_value.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try array.compare(cell.array.?.*, &work));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

/// The portal owns this entire capsule. No cursor retains Job.runInner's stack
/// identity, authority, schema binding or temporary request context.
const OwnedRead = struct {
    alloc: std.mem.Allocator,
    adapter: *Adapter,
    arena: std.heap.ArenaAllocator,
    identity: ?http.AuthenticatedIdentity,
    authority: Authority,
    native_adapter: execution.Adapter,
    decision_runtime: ?@import("antfly_local_sources").functions_runtime.Runtime = null,
    session_id: ?[]u8 = null,
    staged: @import("transactions.zig").OwnedTransactionCommitRequest = .{},
    range_guards: @import("transactions.zig").OwnedTransactionCommitRequest = .{},
    guarded: GuardedCatalog,
    plan: @import("../sql/plan_cache.zig").Lease,
    admission: http.RequestAdmission.Lease,
    stream: *Pull.Stream,
    columns: []const wire.Column,
    policies: []const Policy,
    detached: bool = false,
    const Policy = struct { table: []const u8, filter: ?[]const u8, name: ?ast.Name = null, id: u64 = 0, schema_version: u32 = 0 };

    fn open(adapter: *Adapter, alloc: std.mem.Allocator, credential: *Credential, request: wire.Request) !?wire.ReadStream {
        const server = adapter.server;
        var preparation = server.sql_preparation_admission.tryAcquireLease() orelse return error.SqlWriteCapacityUnavailable;
        defer preparation.release();
        var diagnostic: compiler.Diagnostic = .{};
        var plan = try server.sqlPlanCache().acquire(server.sqlPlanCacheIo(), .{ .statement = request.statement, .principal = credential.principal, .database = request.database orelse "default", .namespace = request.namespace orelse "public" }, &diagnostic);
        var moved = false;
        defer if (!moved) plan.release(server.sqlPlanCacheIo());
        if (plan.compiled().statement != .select) return null;
        var admission = try server.acquireSqlExecution(false);
        defer if (!moved) admission.release();
        const self = try alloc.create(OwnedRead);
        errdefer alloc.destroy(self);
        self.alloc = alloc;
        self.session_id = null;
        self.staged = .{};
        self.range_guards = .{};
        self.detached = false;
        self.adapter = adapter;
        self.arena = std.heap.ArenaAllocator.init(alloc);
        errdefer self.arena.deinit();
        errdefer self.staged.deinit(alloc);
        errdefer self.range_guards.deinit(server.alloc);
        const arena = self.arena.allocator();
        if (request.session_id) |encoded| self.session_id = try server.alloc.dupe(u8, encoded);
        errdefer if (self.session_id) |id| server.alloc.free(id);
        // Native catalog resolution extends this identity with aliases using
        // server.alloc; every allocation in the mutable identity must share it.
        self.identity = try credential.identity(server.alloc);
        errdefer self.identity.?.deinit(server.alloc);
        // Statement, parameter and binding-guard data are portal-owned. Keep a
        // private request capsule so I/O borrows point to a stable address.
        self.authority = .{ .server = server, .credential = credential, .identity = &self.identity, .request = request };
        self.authority.request.statement = try arena.dupe(u8, request.statement);
        self.authority.request.database = try arena.dupe(u8, request.database orelse "default");
        self.authority.request.namespace = try arena.dupe(u8, request.namespace orelse "public");
        self.authority.request.session_namespace = if (request.session_namespace) |scope| try arena.dupe(u8, scope) else null;
        self.authority.request.binding_guard = if (request.binding_guard) |guard| try arena.dupe(u8, guard) else null;
        self.authority.request.setting_overlay = try cloneSettingOverlay(arena, request.setting_overlay);
        self.authority.request.session_id = self.session_id;
        try request.read_requirements.validate(.sql);
        self.native_adapter = .{ .server = server, .identity = &self.identity, .context = try self.authority.context(), .database = self.authority.request.database.?, .namespace = self.authority.request.namespace.?, .session_id = self.session_id, .read_requirements = request.read_requirements, .shared_receipt_wait_budget = request.read_receipt_budget, .lake_visibility = if (request.read_requirements.visibility) |visibility| if (visibility == .accepted) .accepted else .committed else @fromBackingInt(@intCast(@backingInt(request.lake_visibility))), .setting_overlay = self.authority.request.setting_overlay, .setting_overlay_source = .connection, .expected_setting_epoch = self.authority.request.setting_epoch };
        var transaction_lease: ?@import("transactions.zig").SessionRegistry.CommitExecution = null;
        defer if (transaction_lease) |lease| lease.release();
        if (self.session_id) |id_hex| {
            const id = @import("distributed_txn.zig").parseTxnIdHex(id_hex) catch return error.SqlTransactionNotActive;
            if (try server.txn_sessions.principalAccess(server.alloc, id, credential.sessionPrincipal()) != .allowed) return error.SqlTransactionNotActive;
            transaction_lease = server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
            var state = (try server.txn_sessions.getSqlState(server.alloc, id)) orelse return error.SqlTransactionNotActive;
            defer state.deinit(server.alloc);
            if (state.metadata.failed or state.terminal != null) return error.SqlTransactionAborted;
            if (!std.mem.eql(u8, state.metadata.database, request.database orelse "default") or !std.mem.eql(u8, state.metadata.namespace, request.session_namespace orelse request.namespace orelse "public")) return error.SqlTransactionNotActive;
            self.staged = try server.txn_sessions.cloneSqlStaged(alloc, id);
            if (state.metadata.accepted_lake_reads) {
                if (request.read_requirements.visibility == .committed) return error.ReadSnapshotTooOld;
                self.native_adapter.lake_visibility = .accepted;
            }
            self.native_adapter.accepted_lake_repeatable = self.native_adapter.lake_visibility == .accepted and (state.metadata.isolation != .serializable or state.metadata.mode == .read_only);
            self.native_adapter.active_transaction = id;
            self.native_adapter.staged = &self.staged;
            if (state.metadata.isolation != .read_committed) self.native_adapter.range_reads = &self.range_guards;
        }
        self.decision_runtime = try self.native_adapter.decisionRuntime();
        errdefer if (self.decision_runtime) |*active| active.deinit();
        if (self.decision_runtime) |*active| self.native_adapter.decision_provider = active.provider();
        self.guarded = .{ .native = self.native_adapter.backend(), .authority = &self.authority, .revision = &self.native_adapter.revision, .expected_guard = self.authority.request.binding_guard };
        const parameters = if (request.parameter_cells != null) request.parameters else try normalizeParameters(arena, request.parameters, request.parameter_types);
        self.native_adapter.graph_parameters = parameters;
        self.native_adapter.graph_parameter_cells = request.parameter_cells;
        var stream_backend = self.guarded.backend();
        stream_backend.parameter_descriptor_hints = request.parameter_descriptors;
        stream_backend.setting_capture = try self.guarded.settingCapture(arena, &self.native_adapter, plan.compiled());
        // Stream.open captures the owner view once; loadSettings validates the
        // prepared epoch on that same snapshot before binding or reading rows.
        const opened = try Pull.Stream.openOwned(try @import("sql_memory_owner.zig").create(adapter.server.alloc, alloc, adapter.server.shared_resource_manager orelse &adapter.server.local_resource_manager), stream_backend, plan.compiled(), parameters, .{ .result_rows = request.limit, .page_rows = 256, .parameter_cells = request.parameter_cells, .parameter_owner = request.parameter_owner });
        if (opened == null) {
            if (self.decision_runtime) |*active| active.deinit();
            self.identity.?.deinit(server.alloc);
            self.arena.deinit();
            self.staged.deinit(alloc);
            self.range_guards.deinit(server.alloc);
            if (self.session_id) |id| server.alloc.free(id);
            alloc.destroy(self);
            return null;
        }
        // Capture the admission budget before this owner outlives the request.
        // A later statement can then reset its connection-owned budget safely.
        if (request.read_receipt_budget) |budget| self.native_adapter.receipt_wait_budget = budget.*;
        self.native_adapter.shared_receipt_wait_budget = null;
        self.authority.request.read_receipt_budget = null;
        self.stream = opened.?;
        // Future lazy graph opens borrow the stream-owned canonical frame,
        // rather than the request's transient native cell slice.
        if (self.stream.parameter_invocation) |invocation| if (invocation.frame) |frame| {
            self.native_adapter.graph_parameter_cells = frame.values;
        };
        errdefer self.stream.close();
        if (self.session_id) |id_hex| if (self.range_guards.tables.len != 0) {
            const id = try @import("distributed_txn.zig").parseTxnIdHex(id_hex);
            _ = (try server.txn_sessions.stage(server.alloc, id, &self.range_guards)) orelse return error.SqlTransactionNotActive;
        };
        if (transaction_lease) |lease| {
            lease.release();
            transaction_lease = null;
        }
        const binding = self.stream.context.binding;
        var policies: std.ArrayList(Policy) = .empty;
        const table_count: usize = if (binding.relation) |relation| relation.scans.len else if (binding.table != null) 1 else 0;
        for (0..table_count) |i| {
            const table = if (binding.relation) |relation| relation.scans[i].table else binding.table.?;
            // The guarded catalog checks the current credential. A built-in
            // relation has no user table ACL or native catalog binding.
            if (GuardedCatalog.system.owns(table)) continue;
            var policy: Policy = .{ .table = try arena.dupe(u8, table.physical_name), .filter = try http.resolveEffectiveRowFilterJson(arena, self.identity, table.physical_name) };
            if (table.scope) |scope| {
                policy.name = .{ .database = try arena.dupe(u8, scope.database), .namespace = try arena.dupe(u8, scope.namespace), .table = try arena.dupe(u8, scope.name) };
                policy.id = table.id;
                policy.schema_version = table.schema_version;
            }
            try policies.append(arena, policy);
        }
        self.policies = try policies.toOwnedSlice(arena);
        const columns = try arena.alloc(wire.Column, self.stream.context.binding.columns.len);
        for (columns, self.stream.context.binding.columns) |*out, column| out.* = .{ .name = try arena.dupe(u8, column.name), .type = try wireType(column.type), .element_type = column.element_type, .numeric_modifier = column.numeric_modifier };
        self.columns = columns;
        self.plan = plan;
        self.admission = admission;
        moved = true;
        return .{ .context = self, .columns = columns, .next = next, .close = close, .detach = detach, .validate = validate };
    }

    pub fn validate(raw: *anyopaque, alloc: std.mem.Allocator, request: wire.Request) !void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        var job = StreamJob{ .adapter = self.adapter, .alloc = alloc, .credential = self.authority.credential, .request = request, .owner = self, .validate_only = true };
        try job.dispatch();
    }

    fn validateCursor(self: *OwnedRead, alloc: std.mem.Allocator, request: wire.Request) !void {
        try request.check();
        try self.authority.credential.validate();
        var admission: ?http.RequestAdmission.Lease = if (self.detached) try self.adapter.server.acquireSqlExecution(false) else null;
        defer if (admission) |*lease| lease.release();
        const original = self.authority.request;
        if (!std.mem.eql(u8, original.database.?, request.database orelse "default") or !std.mem.eql(u8, original.namespace.?, request.namespace orelse "public") or
            !std.meta.eql(original.search_path, request.search_path) or
            !std.mem.eql(u8, original.binding_guard orelse "", request.binding_guard orelse "")) return error.CatalogGenerationChanged;
        // Detached rows retain immutable source identity, not permission to
        // follow a renamed/replaced logical resource into a different table.
        // Resolve definitions only: never reacquire or rerun the row snapshot.
        var current: ?http.AuthenticatedIdentity = try self.authority.credential.identity(self.adapter.server.alloc);
        defer current.?.deinit(self.adapter.server.alloc);
        try validatePolicies(self.adapter.server.alloc, &current.?, self.identity.?, self.policies);
        var authority: Authority = .{ .credential = self.authority.credential, .identity = &current, .request = request };
        var adapter: execution.Adapter = .{ .server = self.adapter.server, .identity = &current, .context = try authority.context(), .database = original.database.?, .namespace = original.namespace.? };
        const catalog_backend = adapter.backend();
        for (self.policies) |policy| if (policy.name) |name| {
            const table = try catalog_backend.vtable.resolve(catalog_backend.ptr, alloc, name, .read);
            if (table.id != policy.id or table.schema_version != policy.schema_version or !std.mem.eql(u8, table.physical_name, policy.table)) return error.CatalogGenerationChanged;
        };
    }

    fn next(raw: *anyopaque, alloc: std.mem.Allocator, request: wire.Request, limit: u32) !wire.StreamPage {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        var job = StreamJob{ .adapter = self.adapter, .alloc = alloc, .credential = self.authority.credential, .request = request, .owner = self, .limit = limit };
        try job.dispatch();
        return job.page.?;
    }

    fn pull(self: *OwnedRead, alloc: std.mem.Allocator, request: wire.Request, limit: u32) !wire.StreamPage {
        if (self.detached) return error.InvalidSqlBackendResponse;
        try request.check();
        // Retained snapshots have a hard initial statement deadline, even if
        // later Execute messages carry a newer per-request deadline.
        try self.authority.request.check();
        try self.authority.credential.validate();
        if (self.session_id) |id_hex| {
            const id = try @import("distributed_txn.zig").parseTxnIdHex(id_hex);
            if (try self.adapter.server.txn_sessions.principalAccess(self.adapter.server.alloc, id, self.authority.credential.sessionPrincipal()) != .allowed) return error.SqlTransactionNotActive;
            var state = (try self.adapter.server.txn_sessions.getSqlState(self.adapter.server.alloc, id)) orelse return error.SqlTransactionNotActive;
            defer state.deinit(self.adapter.server.alloc);
            if (state.metadata.failed or state.terminal != null) return error.SqlTransactionNotActive;
        }
        var fresh = try self.authority.credential.identity(alloc);
        defer fresh.deinit(alloc);
        try validatePolicies(alloc, &fresh, self.identity.?, self.policies);
        var page = try self.stream.nextBatch(limit);
        errdefer page.deinit();
        const owner = try alloc.create(PageOwner);
        owner.* = .{ .alloc = alloc, .page = page };
        return .{ .exhausted = page.exhausted, .result = .{
            .columns = self.columns,
            .cells = .{ .context = owner, .count = page.values.len(), .width = self.columns.len, .read = PageOwner.cell },
            .command_tag = "SELECT",
            .owner = .{ .context = owner, .release = releasePage },
        } };
    }

    fn validatePolicies(alloc: std.mem.Allocator, fresh: *http.AuthenticatedIdentity, admitted: http.AuthenticatedIdentity, policies: []const Policy) !void {
        // The catalog resolves logical resources to private physical names.
        // Reapply only the pinned aliases to fresh authority, never its old
        // permissions or filters. Buffered operator rows need this check too:
        // they need not cause another native cursor.next call.
        for (admitted.catalog_aliases) |alias| try http.projectCatalogIdentity(alloc, fresh, alias.logical, alias.physical);
        for (policies) |policy| {
            if (!(try http.tablePermissionCurrentlyAllowed(fresh.*, policy.table, .read))) return error.Forbidden;
            const filter = try http.resolveEffectiveRowFilterJson(alloc, fresh.*, policy.table);
            defer if (filter) |value| alloc.free(value);
            if ((filter == null) != (policy.filter == null) or
                (filter != null and !std.mem.eql(u8, filter.?, policy.filter.?))) return error.Forbidden;
        }
    }

    fn releasePage(raw: *anyopaque) void {
        const owner: *PageOwner = @ptrCast(@alignCast(raw));
        owner.page.deinit();
        owner.alloc.destroy(owner);
    }

    const PageOwner = struct {
        alloc: std.mem.Allocator,
        page: Pull.BatchPage,
        fn cell(raw: *anyopaque, alloc: std.mem.Allocator, row: usize, column: usize) anyerror!wire.Cell {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (try self.page.values.frozenJson(row, column)) |value| {
                if (self.page.columns[column].type != .json) return error.InvalidSqlBackendResponse;
                return .{ .frozen_json = value, .sql_null = false };
            }
            if (try self.page.values.encodedJson(row, column)) |bytes| {
                if (self.page.columns[column].type != .json) return error.InvalidSqlBackendResponse;
                return .{ .json_bytes = bytes, .sql_null = false };
            }
            const value = try self.page.values.cell(alloc, row, column);
            if (value.array) |array| {
                const definition = self.page.columns[column];
                if (value.sql_null or value.value != .null or value.patterns != null or definition.type != .array or definition.element_type != array.element_type) return error.InvalidSqlBackendResponse;
                return .{ .array = array, .sql_null = false };
            }
            if (value.numeric) |number| {
                const definition = self.page.columns[column];
                if (value.sql_null or value.value != .null or value.patterns != null or definition.type != .number or definition.element_type != .numeric) return error.InvalidSqlBackendResponse;
                return .{ .numeric = number, .sql_null = false };
            }
            return .{ .value = if (!value.sql_null and self.page.columns[column].type == .datetime) try datetimeResult(alloc, value.value) else value.value, .sql_null = value.sql_null };
        }
    };

    fn detach(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        if (self.detached) return;
        self.detached = true;
        self.stream.close();
        if (self.decision_runtime) |*active| active.deinit();
        self.plan.release(self.adapter.server.sqlPlanCacheIo());
        self.admission.release();
        self.staged.deinit(self.alloc);
        self.range_guards.deinit(self.adapter.server.alloc);
    }

    fn close(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        detach(raw);
        self.identity.?.deinit(self.adapter.server.alloc);
        if (self.session_id) |id| self.adapter.server.alloc.free(id);
        self.arena.deinit();
        self.alloc.destroy(self);
    }
};

const StreamJob = struct {
    adapter: *Adapter,
    alloc: std.mem.Allocator,
    credential: *Credential,
    request: wire.Request,
    owner: ?*OwnedRead = null,
    limit: u32 = 0,
    validate_only: bool = false,
    namespace_only: bool = false,
    opened: ?wire.ReadStream = null,
    page: ?wire.StreamPage = null,
    failure: ?anyerror = null,
    done: std.Io.Event = .unset,

    fn dispatch(self: *StreamJob) !void {
        try validateRequest(self.request);
        if (self.adapter.server.cfg.user_manager != self.credential.manager) return error.Unauthorized;
        const io = http.ApiHttpServer.configuredDurableIo(self.adapter.server.cfg) orelse return error.SqlWriteCapacityUnavailable;
        var future = io.concurrent(run, .{self}) catch return error.SqlWriteCapacityUnavailable;
        self.done.waitUncancelable(self.request.io);
        @import("protected_future.zig").wait(io, &future);
        if (self.failure) |err| return err;
    }
    fn run(self: *StreamJob) void {
        defer self.done.set(self.request.io);
        self.runInner() catch |err| {
            self.failure = err;
            if (self.request.diagnostics) |diagnostic| {
                const safe = execution.diagnostic(err);
                var message: [256]u8 = undefined;
                diagnostic.set(safe.code, execution.diagnosticMessage(err, &message), null, safe.retryable);
            }
        };
    }
    fn runInner(self: *StreamJob) !void {
        if (self.namespace_only) {
            const server = self.adapter.server;
            const domain = @import("antfly_local_sources").system_catalog_domain;
            const database = self.request.database orelse "default";
            const namespace = self.request.namespace orelse "public";
            try domain.validateName(database);
            try domain.validateName(namespace);
            var identity: ?http.AuthenticatedIdentity = try self.credential.identity(server.alloc);
            defer identity.?.deinit(server.alloc);
            const resource = try @import("../system_catalog/routes.zig").resourceNameAlloc(self.alloc, .{ .kind = .namespace, .database = database, .name = namespace });
            defer self.alloc.free(resource);
            if (!http.permissionsAllow(identity.?.permissions, .namespace, resource, .read)) return error.Forbidden;
            var admission = try server.acquireSqlExecution(false);
            defer admission.release();
            var authority: Authority = .{ .credential = self.credential, .identity = &identity, .request = self.request };
            const call = server.source.vtable.system_catalog orelse return error.UnsupportedSqlExecution;
            const response = try call(server.source.ptr, self.alloc, try authority.context(), .{ .read = .{ .kind = .namespace, .database = database, .name = namespace } });
            self.alloc.free(response);
            return;
        }
        if (self.validate_only) return self.owner.?.validateCursor(self.alloc, self.request);
        if (self.owner) |owner| self.page = try owner.pull(self.alloc, self.request, self.limit) else self.opened = try OwnedRead.open(self.adapter, self.alloc, self.credential, self.request);
    }
};

/// Reserved proxy login. A username alone never grants access: the password
/// must be a valid token signed with this instance's configured HTTP authority.
pub const trusted_principal_username = "__antfly_trusted_principal__";

const ImportedSnapshot = struct {
    transaction: [16]u8,
    token: publication_control.SlotSnapshot,
    namespace: publication_control.Namespace,
};

const Credential = struct {
    manager: ?*usermgr.UserManager,
    username: []u8,
    principal: []u8,
    verifier_mac: [Mac.mac_length]u8,
    trusted_server: ?*http.ApiHttpServer = null,
    trusted_identity: ?http.AuthenticatedIdentity = null,
    trusted_issuer: ?[]u8 = null,
    trusted_expires: i64 = 0,
    session_cleanup_complete: bool = true,
    imported_snapshot: ?ImportedSnapshot = null,
    user_instance_id: ?[16]u8 = null,

    fn sessionPrincipal(self: *const Credential) []const u8 {
        // Durable Basic SQL sessions use the username across mixed-version
        // rollouts. Credential-scoped caches and grants still use `principal`.
        return if (self.trusted_identity != null) self.principal else self.username;
    }

    fn authenticate(alloc: std.mem.Allocator, manager: *usermgr.UserManager, username: []const u8, password: []const u8) !*Credential {
        // Snapshot only while locked; bcrypt must not serialize policy reads
        // and writes. Both ingress paths use UserManager's shared verifier.
        var user = snapshotUser(manager, username) catch return error.Unauthorized;
        defer user.deinit(manager.alloc);
        try usermgr.verifyPassword(user.password_hash, password);
        if (std.mem.allEqual(u8, &user.instance_id, 0)) return error.Unauthorized;
        const self = try alloc.create(Credential);
        errdefer alloc.destroy(self);
        const name = try alloc.dupe(u8, username);
        errdefer alloc.free(name);
        const principal = try std.fmt.allocPrint(alloc, "basic:{s}", .{username});
        errdefer alloc.free(principal);
        var verifier_mac: [Mac.mac_length]u8 = undefined;
        Mac.create(&verifier_mac, credential_domain, user.password_hash);
        self.* = .{ .manager = manager, .username = name, .principal = principal, .verifier_mac = verifier_mac, .user_instance_id = user.instance_id };
        // Detect rotation/deletion between snapshot, expensive verification,
        // and session publication without retaining either password or hash.
        try self.validate();
        return self;
    }

    fn authenticateTrusted(alloc: std.mem.Allocator, server: *http.ApiHttpServer, manager: ?*usermgr.UserManager, token: []const u8) !*Credential {
        if (token.len == 0 or token.len > 4096) return error.Unauthorized;
        var admitted = try server.authenticateRequest(.{ .trusted_principal = token });
        defer admitted.deinit(server.alloc);
        if (admitted.is_internal_service) return error.Unauthorized;
        var parts = std.mem.splitScalar(u8, token, '.');
        _ = parts.next() orelse return error.Unauthorized;
        const encoded = parts.next() orelse return error.Unauthorized;
        const decoded = try alloc.alloc(u8, std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(encoded) catch return error.Unauthorized);
        defer alloc.free(decoded);
        std.base64.url_safe_no_pad.Decoder.decode(decoded, encoded) catch return error.Unauthorized;
        var payload = try std.json.parseFromSlice(std.json.Value, alloc, decoded, .{});
        defer payload.deinit();
        const exp = payload.value.object.get("exp") orelse return error.Unauthorized;
        if (exp != .integer) return error.Unauthorized;
        const self = try alloc.create(Credential);
        errdefer alloc.destroy(self);
        const username = try alloc.dupe(u8, admitted.username);
        errdefer alloc.free(username);
        const principal = try alloc.dupe(u8, admitted.credential_principal);
        errdefer alloc.free(principal);
        var owned_identity = (try http.cloneCatalogIdentity(alloc, admitted)).?;
        errdefer owned_identity.deinit(alloc);
        const issuer = try alloc.dupe(u8, server.cfg.trusted_principal_issuer orelse return error.Unauthorized);
        errdefer alloc.free(issuer);
        var fingerprint: [Mac.mac_length]u8 = undefined;
        Mac.create(&fingerprint, credential_domain, server.cfg.trusted_principal_secret orelse return error.Unauthorized);
        self.* = .{ .manager = manager, .username = username, .principal = principal, .verifier_mac = fingerprint, .trusted_server = server, .trusted_identity = owned_identity, .trusted_issuer = issuer, .trusted_expires = exp.integer, .user_instance_id = admitted.user_instance_id };
        return self;
    }

    pub fn validate(self: *const Credential) !void {
        if (self.trusted_server) |server| {
            // Fast page/work checkpoint: expiry and authority rotation never
            // take the native user-manager lock or decode the immutable JWT.
            const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_s;
            if (now >= self.trusted_expires) return error.Unauthorized;
            if (!std.mem.eql(u8, self.trusted_issuer.?, server.cfg.trusted_principal_issuer orelse return error.Unauthorized)) return error.Unauthorized;
            var current: [Mac.mac_length]u8 = undefined;
            Mac.create(&current, credential_domain, server.cfg.trusted_principal_secret orelse return error.Unauthorized);
            if (!std.crypto.timing_safe.eql([Mac.mac_length]u8, current, self.verifier_mac)) return error.Unauthorized;
            return;
        }
        const current = if (self.user_instance_id) |instance|
            self.manager.?.destinationGrantMacForUserInstance(self.principal, credential_domain, instance) catch return error.Unauthorized
        else
            self.manager.?.destinationGrantMac(self.principal, credential_domain) catch return error.Unauthorized;
        if (!std.crypto.timing_safe.eql([Mac.mac_length]u8, current, self.verifier_mac)) return error.Unauthorized;
    }

    fn release(raw: *anyopaque, alloc: std.mem.Allocator) void {
        const self: *Credential = @ptrCast(@alignCast(raw));
        std.crypto.secureZero(u8, &self.verifier_mac);
        if (self.trusted_identity) |*cached| cached.deinit(alloc);
        if (self.trusted_issuer) |issuer| alloc.free(issuer);
        alloc.free(self.username);
        alloc.free(self.principal);
        alloc.destroy(self);
    }

    fn identity(self: *Credential, alloc: std.mem.Allocator) !http.AuthenticatedIdentity {
        try self.validate();
        if (self.trusted_identity) |cached| {
            if (cached.user_instance_id != null) if (cached.live_user_manager) |manager| {
                if (!try manager.permissionsCurrentlyAllowedWithUserInstance(cached.username, cached.credential_principal, &.{}, cached.user_instance_id)) return error.Unauthorized;
            };
            return (try http.cloneCatalogIdentity(alloc, cached)).?;
        }
        var lease = try self.manager.?.acquireSnapshotLease();
        // Copy credentials and policies under one lease without recursively
        // acquiring the user-manager mutex or retaining a login-time grant.
        const result = blk: {
            defer lease.release();
            var user = try lease.getUser(self.username);
            defer user.deinit(self.manager.?.alloc);
            if (self.user_instance_id) |expected| {
                if (!std.mem.eql(u8, &expected, &user.instance_id)) return error.Unauthorized;
            }
            const permissions = try self.manager.?.getPermissionsForUser(self.username);
            defer http.freePermissions(self.manager.?.alloc, permissions);
            const filters = try self.manager.?.getRowFilters(self.username);
            defer http.freeRowFilters(self.manager.?.alloc, filters);
            const roles = try self.manager.?.getRolesForUser(self.username);
            defer {
                for (roles) |role| self.manager.?.alloc.free(role);
                self.manager.?.alloc.free(roles);
            }
            break :blk (try http.cloneCatalogIdentity(alloc, .{
                .user_instance_id = user.instance_id,
                .username = user.username,
                .credential_principal = self.principal,
                .permissions = permissions,
                .row_filter = filters,
                .metadata_json = user.metadata_json,
                .roles = roles,
                .live_user_manager = self.manager.?,
            })).?;
        };
        var owned = result;
        errdefer owned.deinit(alloc);
        try self.validate();
        return owned;
    }
};

fn snapshotUser(manager: *usermgr.UserManager, username: []const u8) !usermgr.User {
    var lease = try manager.acquireSnapshotLease();
    defer lease.release();
    return lease.getUser(username);
}

fn validateRequest(request: wire.Request) !void {
    try request.check();
    if (request.deadline.clock != .awake) return error.UnsupportedSqlExecution;
    if (request.limit == 0 or request.limit > 4096 or request.parameters.len > 1024) return error.InvalidSqlParameters;
}

const publication_metadata = @import("../metadata/pg_publication.zig");
const publication_control = @import("../metadata/changefeed_control.zig");
const PublicationCommand = wire.replication_commands.PublicationCommand;

fn executePublication(
    server: *http.ApiHttpServer,
    a: std.mem.Allocator,
    authority: *Authority,
    command: PublicationCommand,
) !wire.Result {
    const domain = @import("antfly_local_sources").system_catalog_domain;
    const port = server.source.pg_publications orelse return error.UnsupportedReplicationMode;
    const identity = authority.identity.*.?;
    const owner = try @import("changefeed_public.zig").durableOwner(identity);
    const context = try authority.context();
    const database = authority.request.database orelse domain.default_database_name;
    const namespace = authority.request.namespace orelse domain.default_namespace_name;
    const targets = try a.alloc(domain.Target, command.tables.len);
    for (command.tables, targets) |table, *target| {
        target.* = .{ .database = database, .namespace = table.schema orelse namespace, .table = table.name };
        try target.validate();
    }
    const request: domain.ResolveMany = .{ .targets = targets, .include_query_definitions = true, .publication_database = database };
    const response = try server.source.systemCatalog(a, context, .{ .resolve_many = request });
    const resolved_catalog = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, response, .{ .allocate = .alloc_always });
    const cut = try resolved_catalog.requirePublicationCut(request);
    const database_key = publicationDatabaseKey(cut);
    var maintenance = context;
    maintenance.principal = .{ .kind = .internal, .subject = "pg-publication-maintenance" };
    var previous = try port.read(port.ptr, a, database_key, command.name, maintenance);
    defer if (previous) |*record| record.deinit();
    const old = if (previous) |record| record.value else null;
    if (old) |record| if (!std.meta.eql(record.owner, owner)) return error.Forbidden;
    const exists = old != null and old.?.members != null;
    const tag: []const u8 = switch (command.action) {
        .create => "CREATE PUBLICATION",
        .replace_members => "ALTER PUBLICATION",
        .drop => "DROP PUBLICATION",
    };
    if (command.action == .create and exists) return error.PgPublicationAlreadyExists;
    if (command.action != .create and !exists) {
        if (command.if_exists) return .{ .command_tag = tag };
        return error.PgPublicationNotFound;
    }
    const previous_members = if (old) |record| record.members orelse &.{} else &.{};
    const members = if (command.action == .drop) null else try resolvePublicationMembers(server, a, identity, context, targets, resolved_catalog, cut, previous_members);
    const revision = if (old) |record| record.revision else 0;
    try authority.credential.validate();
    try port.mutate(port.ptr, a, .{
        .expected_revision = revision,
        .expected_catalog_revision = resolved_catalog.revision,
        .next = .{ .database = database_key, .name = command.name, .owner = owner, .revision = std.math.add(u64, revision, 1) catch return error.ProgramLimitExceeded, .members = members },
    }, maintenance);
    return .{ .command_tag = tag, .mutation_outcome = .committed };
}

fn publicationDatabaseKey(cut: @import("antfly_local_sources").system_catalog_domain.PublicationCatalogCut) publication_control.Digest {
    return publication_control.hash(.{ .domain = "antfly:pg-publication-database:v1", .group = cut.metadata_group_id, .incarnation = cut.metadata_incarnation, .database = cut.database_id });
}

fn publicationMemberLess(_: void, lhs: publication_metadata.Member, rhs: publication_metadata.Member) bool {
    return lhs.table.table_id < rhs.table.table_id;
}

/// Relation ids are 32-bit wire aliases in one publication. Keep the
/// persisted alias when the same table survives a publication revision.
fn assignPublicationOids(members: []publication_metadata.Member, previous: []const publication_metadata.Member) void {
    var candidate: u32 = 16384;
    for (members) |*member| {
        for (previous) |prior| {
            if (member.table.table_id == prior.table.table_id) {
                member.oid = prior.oid;
                break;
            }
        } else {
            // The previous set is bounded and candidate only advances.
            while (true) : (candidate += 1) {
                for (previous) |prior| {
                    if (prior.oid == candidate) break;
                } else break;
            }
            member.oid = candidate;
            candidate += 1;
        }
    }
}

fn resolvePublicationMembers(
    server: *http.ApiHttpServer,
    a: std.mem.Allocator,
    identity: http.AuthenticatedIdentity,
    context: operation.RequestContext,
    targets: []const @import("antfly_local_sources").system_catalog_domain.Target,
    resolved_catalog: @import("antfly_local_sources").system_catalog_domain.ResolvedMany,
    cut: @import("antfly_local_sources").system_catalog_domain.PublicationCatalogCut,
    previous: []const publication_metadata.Member,
) ![]const publication_metadata.Member {
    const names = try a.alloc([]const u8, targets.len);
    for (targets, names) |target, *name| name.* = try target.resourceNameAlloc(a);
    try ReplicationIdentity.requireWholeTablesForIdentity(a, identity, names);
    const members = try a.alloc(publication_metadata.Member, targets.len);
    for (targets, names, resolved_catalog.tables, cut.bindings, members) |target, name, resolved, binding, *member| {
        const table = resolved orelse return error.TableNotFound;
        const definition = table.query_definition.?;
        if (table.native_service != null or definition.storage_engine != .local) return error.ChangefeedCapabilityUnsupported;
        var parsed = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(a, definition.schema_json);
        defer parsed.deinit(a);
        if (parsed.storage_mode != .relational or parsed.external_base_source != null) return error.ChangefeedCapabilityUnsupported;
        if (http.hasEffectiveRowFilter(identity, table.name)) return error.Forbidden;
        const policy = try server.rowPolicyReadProof(a, &identity, context, table.table_id, table.name, target.database, parsed.version);
        if (policy != null) return error.Forbidden;
        var keys: []const []const u8 = &.{};
        if (parsed.unique_constraints) |constraints| for (constraints.value) |constraint| {
            if (!(constraint.primary orelse false)) continue;
            const columns = constraint.columns orelse return error.ChangefeedCapabilityUnsupported;
            const ordered = try a.alloc([]const u8, columns.len);
            for (columns, ordered) |column, *slot| slot.* = try a.dupe(u8, column);
            std.mem.sort([]const u8, ordered, {}, struct {
                fn less(_: void, lhs: []const u8, rhs: []const u8) bool {
                    return std.mem.order(u8, lhs, rhs) == .lt;
                }
            }.less);
            keys = ordered;
            break;
        };
        if (parsed.replica_identity == .default and keys.len == 0) return error.ChangefeedCapabilityUnsupported;
        member.* = .{
            .table = .{ .table_id = table.table_id, .binding = binding.?, .contract = .native_relational, .policy_revision = 1 },
            .permission_name = name,
            .namespace = try a.dupe(u8, target.namespace),
            .name = try a.dupe(u8, target.table),
            .oid = 0,
            .identity = if (parsed.replica_identity == .full or keys.len == 0) .full else .default,
            .key_columns = keys,
        };
    }
    std.mem.sort(publication_metadata.Member, members, {}, publicationMemberLess);
    assignPublicationOids(members, previous);
    try publication_metadata.validateMembers(members);
    return members;
}

const Job = struct {
    adapter: *Adapter,
    alloc: std.mem.Allocator,
    credential: *Credential,
    request: wire.Request,
    kind: enum { describe, execute },
    description: ?wire.Description = null,
    result: ?wire.Result = null,
    failure: ?anyerror = null,
    compile_diagnostic: compiler.Diagnostic = .{},
    compile_diagnostic_valid: bool = false,
    done: std.Io.Event = .unset,

    fn run(self: *Job) void {
        defer self.done.set(self.request.io);
        self.runInner() catch |err| {
            if (self.request.diagnostics) |diagnostic| if (diagnostic.code == null) {
                if (self.compile_diagnostic_valid) {
                    const safe = execution.diagnostic(err);
                    diagnostic.set(safe.code, self.compile_diagnostic.message, null, safe.retryable);
                } else {
                    var message_buffer: [256]u8 = undefined;
                    const safe = execution.diagnostic(err);
                    switch (err) {
                        error.PgReplicationSyntax => diagnostic.set("42601", "Invalid publication statement", null, false),
                        error.PgPublicationAlreadyExists => diagnostic.set("42710", "Publication already exists", null, false),
                        error.PgPublicationNotFound => diagnostic.set("42704", "Publication does not exist", null, false),
                        error.PgPublicationConflict => diagnostic.set("40001", "Publication changed concurrently", null, true),
                        error.ChangefeedCapabilityUnsupported, error.UnsupportedReplicationMode => diagnostic.set("0A000", "Publication requires supported native relational tables and replication authority", null, false),
                        else => diagnostic.set(safe.code, execution.diagnosticMessage(err, &message_buffer), null, safe.retryable),
                    }
                }
            };
            self.failure = err;
        };
    }

    fn runInner(self: *Job) !void {
        try validateRequest(self.request);
        var preparation = self.adapter.server.sql_preparation_admission.tryAcquireLease() orelse return error.SqlWriteCapacityUnavailable;
        defer preparation.release();
        if (try wire.replication_commands.parsePublication(self.alloc, self.request.statement, publication_control.max_tables)) |publication| {
            var parsed = publication;
            defer parsed.deinit();
            if (self.request.session_id != null) return error.UnsupportedSqlExecution;
            if (self.request.parameters.len != 0 or self.request.parameter_types.len != 0 or self.request.parameter_descriptors.len != 0)
                return error.InvalidSqlParameters;
            if (self.kind == .describe) {
                self.description = .{};
                return;
            }
            const server = self.adapter.server;
            var admission = try server.acquireSqlExecution(true);
            defer admission.release();
            preparation.release();
            var identity: ?http.AuthenticatedIdentity = try self.credential.identity(server.alloc);
            defer identity.?.deinit(server.alloc);
            var authority: Authority = .{ .credential = self.credential, .identity = &identity, .request = self.request };
            self.result = executePublication(server, self.alloc, &authority, parsed.command) catch |err| {
                std.log.debug("pgwire publication command failed action={s} err={s}", .{ @tagName(parsed.command.action), @errorName(err) });
                return err;
            };
            return;
        }
        var lease = self.adapter.server.sqlPlanCache().acquire(self.adapter.server.sqlPlanCacheIo(), .{
            .statement = self.request.statement,
            .principal = self.credential.principal,
            .database = self.request.database orelse "default",
            .namespace = self.request.namespace orelse "public",
        }, &self.compile_diagnostic) catch |err| {
            self.compile_diagnostic_valid = err != error.SqlPlanCacheBusy and err != error.InvalidSqlPlanCacheConfig;
            return err;
        };
        defer lease.release(self.adapter.server.sqlPlanCacheIo());
        const compiled = lease.compiled();
        const write = self.kind == .execute and switch (compiled.statement) {
            .select, .explain => false,
            .insert, .update, .delete, .merge, .create_table, .drop_table, .catalog_ddl, .policy_ddl, .begin, .commit, .rollback, .savepoint, .rollback_to_savepoint, .release_savepoint, .set_constraints => true,
        };
        const server = self.adapter.server;
        var admission = try server.acquireSqlExecution(write);
        defer admission.release();
        preparation.release();
        var identity: ?http.AuthenticatedIdentity = try self.credential.identity(server.alloc);
        defer identity.?.deinit(server.alloc);
        var authority = Authority{ .server = server, .credential = self.credential, .identity = &identity, .request = self.request };
        const context = try authority.context();
        var native_adapter = execution.Adapter{
            .server = server,
            .identity = &identity,
            .context = context,
            .database = self.request.database orelse "default",
            .namespace = self.request.namespace orelse "public",
            .read_requirements = self.request.read_requirements,
            .shared_receipt_wait_budget = self.request.read_receipt_budget,
            .lake_visibility = @fromBackingInt(@intCast(@backingInt(self.request.lake_visibility))),
            .ddl_search_path = if (self.request.search_path) |*path| path else null,
            .session_id = self.request.session_id,
            .session_namespace = self.request.session_namespace,
            .setting_overlay = self.request.setting_overlay,
            .setting_overlay_source = .connection,
            .expected_setting_epoch = self.request.setting_epoch,
        };
        // describe/execute capture a single settings view. The adapter's
        // loader checks expected_setting_epoch on that exact captured epoch.
        var guarded = GuardedCatalog{
            .native = native_adapter.backend(),
            .authority = &authority,
            .revision = &native_adapter.revision,
            .expected_guard = self.request.binding_guard,
        };
        var settings_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer settings_arena.deinit();
        const setting_capture = try guarded.settingCapture(settings_arena.allocator(), &native_adapter, compiled);
        if (self.kind == .describe) {
            switch (compiled.statement) {
                .begin, .commit, .rollback, .savepoint, .rollback_to_savepoint, .release_savepoint, .set_constraints => {
                    if (self.request.parameter_types.len != 0) return error.InvalidSqlParameters;
                    self.description = .{ .columns = &.{} };
                    return;
                },
                else => {},
            }
            const hints = try self.alloc.alloc(?ast.ColumnType, self.request.parameter_types.len);
            defer self.alloc.free(hints);
            for (self.request.parameter_types, hints) |kind, *hint| hint.* = switch (kind) {
                .unknown => null,
                inline else => |tag| @field(ast.ColumnType, @tagName(tag)),
            };
            var describe_backend = guarded.backend();
            describe_backend.parameter_descriptor_hints = self.request.parameter_descriptors;
            describe_backend.setting_capture = setting_capture;
            var description = try describe_sql.describe(self.request.scratch_allocator orelse self.alloc, describe_backend, compiled, hints);
            defer description.deinit();
            const columns = try self.alloc.alloc(wire.Column, description.binding.columns.len);
            for (columns, description.binding.columns) |*out, column| out.* = .{ .name = try self.alloc.dupe(u8, column.name), .type = try wireType(column.type), .element_type = column.element_type, .numeric_modifier = column.numeric_modifier };
            const parameter_types = try self.alloc.alloc(wire.Type, description.binding.parameter_types.len);
            for (parameter_types, description.binding.parameter_types) |*out, kind| out.* = if (kind) |value| try wireType(value) else .unknown;
            self.description = .{
                .columns = columns,
                .parameter_types = parameter_types,
                .parameter_descriptors = try self.alloc.dupe(wire.Parameter, description.binding.parameter_descriptors),
                .binding_guard = try statementBindingGuard(self.alloc, native_adapter.revision, description.binding),
                .setting_epoch = if (description.settings) |view| view.epoch else null,
            };
            return;
        }
        const parameters = if (self.request.parameter_cells != null) self.request.parameters else try normalizeParameters(self.alloc, self.request.parameters, self.request.parameter_types);
        var execution_backend = guarded.backend();
        execution_backend.parameter_descriptor_hints = self.request.parameter_descriptors;
        execution_backend.setting_capture = setting_capture;
        var result = native_adapter.execute(self.request.scratch_allocator orelse self.alloc, compiled, parameters, .{ .result_rows = self.request.limit, .native_result = true, .parameter_cells = self.request.parameter_cells, .parameter_owner = self.request.parameter_owner }, execution_backend) catch |err| {
            if (self.request.diagnostics) |diagnostic| diagnostic.transaction_status = @fromBackingInt(@backingInt(native_adapter.transaction_status));
            if (err == error.SqlMutationOutcomeUnknown or err == error.SqlTransactionOutcomeUnknown or err == error.SessionLeaseLost) if (self.request.diagnostics) |diagnostic|
                diagnostic.set("40003", "transaction outcome is unknown; do not replay this statement", native_adapter.outcome_transaction_id, false);
            return err;
        };
        var transferred = false;
        defer if (!transferred) result.deinit();
        if (self.request.diagnostics) |diagnostic| diagnostic.transaction_status = @fromBackingInt(@backingInt(native_adapter.transaction_status));
        errdefer if (native_adapter.result_session_id) |id| {
            const transaction_id = @import("distributed_txn.zig").parseTxnIdHex(&id) catch unreachable;
            server.txn_sessions.setSqlFailed(server.alloc, transaction_id, true) catch {};
            if (self.request.diagnostics) |diagnostic| diagnostic.transaction_status = .failed;
        };
        // After a durable commit, even acknowledgement-allocation failure must
        // preserve uncertainty instead of looking like a pre-admission abort.
        errdefer if (result.output.mutation_outcome != null) {
            if (self.request.diagnostics) |diagnostic|
                diagnostic.set("40003", "mutation committed but its acknowledgement could not be prepared; do not replay", native_adapter.outcome_transaction_id, false);
        };
        // Only datetime cells require representation adaptation. Mutate the
        // exclusively owned row cells, not a second copy of every result row.
        for (result.output.columns, 0..) |column, index| if (column.type == .datetime) {
            for (0..result.output.rows.len) |row_index| {
                const row = result.mutableRow(row_index);
                row[index] = try datetimeResult(self.alloc, row[index]);
            }
        };
        const columns = try self.alloc.alloc(wire.Column, result.output.columns.len);
        for (columns, result.output.columns) |*out, column| out.* = .{ .name = column.name, .type = try wireType(column.type), .element_type = column.element_type, .numeric_modifier = column.numeric_modifier };
        const native_owner = if (result.output.native_rows != null) try result.state.arena.allocator().create(NativeResultCells) else null;
        if (native_owner) |owner| owner.* = .{ .result = result };
        self.result = .{
            .columns = columns,
            .cells = if (native_owner) |owner| .{ .context = owner, .count = result.output.rowCount(), .width = columns.len, .read = NativeResultCells.read } else null,
            .rows = result.output.rows,
            .sql_nulls = result.output.sql_nulls,
            .rows_affected = result.output.rows_affected,
            .command_tag = try commandTag(self.alloc, result.output.command_tag, result.output.rowCount(), result.output.rows_affected),
            .mutation_outcome = if (result.output.mutation_outcome) |outcome| switch (outcome) {
                inline else => |kind| @field(wire.MutationOutcome, @tagName(kind)),
            } else null,
            .transaction_id = native_adapter.outcome_transaction_id,
            .ddl_receipt_json = if (result.output.ddl_receipt) |receipt| try std.json.Stringify.valueAlloc(self.alloc, receipt, .{ .emit_null_optional_fields = false }) else null,
            .session_id = if (native_adapter.result_session_id) |id| try self.alloc.dupe(u8, &id) else null,
            .transaction_status = @fromBackingInt(@backingInt(native_adapter.transaction_status)),
            .owner = .{ .context = result.state, .release = releaseResult },
        };
        transferred = true;
    }
};

test "SQL pgwire dispatch preserves imported executor authority including unavailable views" {
    const Probe = struct {
        attempts: usize = 0,
        fn concurrent(raw: ?*anyopaque, _: usize, _: std.mem.Alignment, _: []const u8, _: std.mem.Alignment, _: *const fn (*const anyopaque, *anyopaque) void) std.Io.ConcurrentError!*std.Io.AnyFuture {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.attempts += 1;
            return error.ConcurrencyUnavailable;
        }
    };
    var raw_probe: Probe = .{};
    var imported_probe: Probe = .{};
    var vtable = std.Io.failing.vtable.*;
    vtable.concurrent = Probe.concurrent;
    const raw_io: std.Io = .{ .userdata = &raw_probe, .vtable = &vtable };
    const imported_io: std.Io = .{ .userdata = &imported_probe, .vtable = &vtable };
    var runtime = try @import("antfly_local_sources").storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{
        .backend = .manual,
        .borrowed_io = .{ .general = raw_io },
    });
    defer runtime.deinit();
    // Dispatch must reject or schedule before dereferencing either object.
    var manager: usermgr.UserManager = undefined;
    var credential: Credential = undefined;
    credential.manager = &manager;
    var api: http.ApiHttpServer = undefined;
    api.cfg = .{ .backend_runtime = runtime.ptr(), .user_manager = &manager, .imported_runtime_io = .{} };
    var adapter: Adapter = .{ .server = &api };
    var canceled: std.atomic.Value(bool) = .init(false);
    const request: wire.Request = .{
        .statement = "SELECT id FROM docs",
        .limit = 1,
        .io = std.testing.io,
        .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } },
        .cancel_requested = &canceled,
    };
    for ([_]?std.Io{ null, imported_io }, 0..) |executor, i| {
        api.cfg.imported_runtime_io.?.durable = executor;
        var job: Job = .{ .adapter = &adapter, .alloc = std.testing.allocator, .credential = &credential, .request = request, .kind = .execute };
        try std.testing.expectError(error.SqlWriteCapacityUnavailable, adapter.dispatch(&job));
        var stream: StreamJob = .{ .adapter = &adapter, .alloc = std.testing.allocator, .credential = &credential, .request = request };
        try std.testing.expectError(error.SqlWriteCapacityUnavailable, stream.dispatch());
        try std.testing.expectEqual(@as(usize, 0), raw_probe.attempts);
        try std.testing.expectEqual(i * 2, imported_probe.attempts);
    }
}

test "SQL pgwire native execute arguments use bounded scalar semantics without table access" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var canceled: std.atomic.Value(bool) = .init(false);
    const request: wire.Request = .{ .statement = "SELECT $1, $2", .parameter_types = &.{ .integer, .string }, .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } }, .cancel_requested = &canceled };
    const result = try Adapter.evaluateScalarParameters(arena.allocator(), request, &.{ "9007199254740991 + 2", "concat('a,b', upper('x'))" });
    try std.testing.expectEqual(@as(i64, 9007199254740993), result[0].integer);
    try std.testing.expectEqualStrings("a,bX", result[1].string);
    try std.testing.expectError(error.InvalidSqlParameters, Adapter.evaluateScalarParameters(arena.allocator(), request, &.{ "$1", "'x'" }));
    var json_request = request;
    json_request.parameter_types = &.{ .json, .json };
    const json_values = try Adapter.evaluateScalarParameters(arena.allocator(), json_request, &.{ "'null'::json", "'\"text\"'::json" });
    try std.testing.expectEqualStrings("null", json_values[0].string);
    try std.testing.expectEqualStrings("\"text\"", json_values[1].string);
    const null_values = try Adapter.evaluateScalarParameters(arena.allocator(), json_request, &.{ "NULL", "NULL::json" });
    try std.testing.expect(null_values[0] == .null and null_values[1] == .null);
    var array_request = request;
    array_request.parameter_types = &.{ .array, .integer };
    array_request.parameter_descriptors = &.{ .{ .kind = .array, .element_type = .int64 }, .{ .kind = .integer, .element_type = .int32 } };
    const array_values = try Adapter.evaluateScalarParameters(arena.allocator(), array_request, &.{ "'[-1:1]={9007199254740993,NULL,2}'::bigint[]", "42" });
    var decoded = try @import("antfly_local_sources").sql_array_wire.decode(std.testing.allocator, .int64, array_values[0], .{});
    defer decoded.deinit();
    try std.testing.expectEqual(@as(i32, -1), decoded.value.dimensions[0].lower);
    try std.testing.expectEqual(@as(i64, 9007199254740993), decoded.value.elements[0].value.integer);
    try std.testing.expect(decoded.value.elements[1].sql_null);
    const native_array = try Adapter.evaluateScalarCells(arena.allocator(), array_request, &.{ "'[-1:1]={9007199254740993,NULL,2}'::bigint[]", "42" });
    try std.testing.expectEqual(@as(i64, 9007199254740993), native_array[0].array.?.flatCell(0).value.integer);
    try std.testing.expect(native_array[0].array.?.flatCell(1).sql_null);
    const native_json = try Adapter.evaluateScalarCells(arena.allocator(), json_request, &.{ "'null'::json", "NULL::json" });
    try std.testing.expect(!native_json[0].sql_null and native_json[1].sql_null);
    canceled.store(true, .release);
    try std.testing.expectError(error.QueryCanceled, Adapter.evaluateScalarParameters(arena.allocator(), request, &.{ "1", "'x'" }));
}

fn normalizeParameters(alloc: std.mem.Allocator, input: []const std.json.Value, types: []const wire.Type) ![]const std.json.Value {
    if (input.len != types.len) return error.InvalidSqlParameters;
    var normalized: ?[]std.json.Value = null;
    for (input, types, 0..) |value, kind, index| {
        if (kind != .datetime or value == .null or value == .string) continue;
        if (normalized == null) normalized = try alloc.dupe(std.json.Value, input);
        normalized.?[index] = .{ .string = try storage_schema.formatDateTimeNsAlloc(alloc, try wire_values.timestampNanos(value)) };
    }
    return normalized orelse input;
}

fn datetimeResult(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    if (value == .null) return value;
    const nanos = if (value == .string)
        storage_schema.parseDateTimeToNs(value.string) orelse return error.InvalidSqlBackendResponse
    else
        try wire_values.timestampNanos(value);
    return wire_values.timestampValue(alloc, nanos);
}

fn wireType(kind: ast.ColumnType) !wire.Type {
    return switch (kind) {
        inline else => |tag| @field(wire.Type, @tagName(tag)),
    };
}

fn commandTag(alloc: std.mem.Allocator, command: []const u8, rows: usize, affected: u64) ![]const u8 {
    if (std.mem.eql(u8, command, "DDL PENDING")) return alloc.dupe(u8, command);
    if (std.mem.eql(u8, command, "SELECT")) return std.fmt.allocPrint(alloc, "SELECT {d}", .{rows});
    if (std.mem.eql(u8, command, "INSERT")) return std.fmt.allocPrint(alloc, "INSERT 0 {d}", .{affected});
    if (std.mem.eql(u8, command, "UPDATE") or std.mem.eql(u8, command, "DELETE") or std.mem.eql(u8, command, "MERGE")) return std.fmt.allocPrint(alloc, "{s} {d}", .{ command, affected });
    if (std.mem.startsWith(u8, command, "CREATE ") or std.mem.startsWith(u8, command, "DROP ") or std.mem.startsWith(u8, command, "ALTER ")) return alloc.dupe(u8, command);
    inline for (.{ "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "SET CONSTRAINTS", "TRUNCATE TABLE" }) |tag| if (std.mem.eql(u8, command, tag)) return alloc.dupe(u8, command);
    return error.UnsupportedSqlExecution;
}

test "SQL pgwire MERGE completion reports committed affected rows" {
    const tag = try commandTag(std.testing.allocator, "MERGE", 0, 3);
    defer std.testing.allocator.free(tag);
    try std.testing.expectEqualStrings("MERGE 3", tag);
}

test "SQL pgwire native materialized cells match public text and binary encodings" {
    const Probe = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator) !void {
            var marker: u8 = 0;
            const backend: catalog.Backend = .{ .ptr = &marker, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = checkpoint } };
            var compiled = try compiler.compile(a, "SELECT ARRAY[9007199254740993::bigint,NULL::bigint,-7::bigint] AS a, '9007199254740993.1200'::numeric AS n, 'null'::jsonb AS j, NULL::numeric AS z", .{});
            defer compiled.deinit();
            var result = try native.execute(a, backend, &compiled, &.{}, .{ .native_result = true });
            defer result.deinit();
            var baseline = try native.execute(a, backend, &compiled, &.{}, .{});
            defer baseline.deinit();
            var owner: NativeResultCells = .{ .result = result };
            var columns: [4]wire.Column = undefined;
            for (&columns, result.output.columns) |*out, column| out.* = .{ .name = column.name, .type = try wireType(column.type), .element_type = column.element_type };
            const published: wire.Result = .{ .columns = &columns, .cells = .{ .context = &owner, .count = result.output.rowCount(), .width = columns.len, .read = NativeResultCells.read }, .command_tag = "SELECT 1" };
            const view = try published.view(0, 1);
            for ([_]u16{ 0, 1 }) |format| for (columns, 0..) |column, index| {
                const cell = try view.cell(a, 0, index);
                const expected_null = baseline.output.sql_nulls.?[0][index];
                try std.testing.expectEqual(expected_null, cell.sql_null);
                if (cell.sql_null) continue;
                var bytes: std.Io.Writer.Allocating = .init(a);
                defer bytes.deinit();
                var expected: std.Io.Writer.Allocating = .init(a);
                defer expected.deinit();
                wire_values.encodeCellInto(a, &bytes.writer, column, format, cell, 1 << 20) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
                wire_values.encodeColumnInto(a, &expected.writer, column, format, baseline.output.rows[0][index], 1 << 20) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
                try std.testing.expectEqualSlices(u8, expected.written(), bytes.written());
            };
            try std.testing.expectError(error.InvalidResult, published.cell(a, 1, 0));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

const NativeResultCells = struct {
    result: native.Result,
    fn read(raw: *anyopaque, alloc: std.mem.Allocator, row: usize, column: usize) !wire.Cell {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const rows = self.result.output.native_rows orelse return error.InvalidSqlBackendResponse;
        if (row >= rows.len or column >= self.result.output.columns.len or rows[row].len != self.result.output.columns.len) return error.InvalidSqlBackendResponse;
        const datum = rows[row][column];
        return .{ .value = if (!datum.sql_null and self.result.output.columns[column].type == .datetime) try datetimeResult(alloc, datum.value) else datum.value, .sql_null = datum.sql_null, .array = datum.array, .numeric = datum.numeric };
    }
};

fn releaseResult(raw: *anyopaque) void {
    var result = native.Result{ .state = @ptrCast(@alignCast(raw)), .output = undefined };
    result.deinit();
}

const Authority = struct {
    server: ?*http.ApiHttpServer = null,
    credential: *Credential,
    identity: *?http.AuthenticatedIdentity,
    request: wire.Request,

    fn checkpoint(self: *Authority) !void {
        try self.request.check();
    }

    fn context(self: *Authority) !operation.RequestContext {
        try self.checkpoint();
        try self.credential.validate();
        // Normalize once at ingress before catalog/storage owner boundaries,
        // which carry native absolute deadlines rather than an Io clock borrow.
        // Unrepresentable positive deadlines are effectively unbounded; check()
        // above still rejects expired requests and explicit cancellation.
        const deadline = std.math.cast(u64, self.request.deadline.raw.nanoseconds);
        const native_context: operation.RequestContext = .{
            .sql_completion = if (self.request.completion != null) .{ .ptr = self, .prepare_transaction = prepareTransaction, .prepare_metadata = prepareMetadata, .reject_metadata = rejectMetadata } else null,
            .deadline_ns = deadline,
            .deadline_io = if (deadline != null) io_abi.Borrow.init(&self.request.io) else null,
            .fanout_io = io_abi.Borrow.init(&self.request.io),
            .cancellation = operation.CancellationToken.fromAtomic(self.request.cancel_requested),
            .principal = .{ .kind = .user, .subject = self.identity.*.?.username },
            .destination_authorization_principal = self.credential.principal,
            // The authenticated, credential-fenced identity is borrowed for
            // this statement only; owner write proofs are minted before the
            // durable transaction plan is sealed.
            .row_policy_credential = self.identity,
            .table_write_authorization = .{ .ptr = self, .allows = allowsWrite },
        };
        return native_context.platformDeadline();
    }

    fn prepareTransaction(raw: *anyopaque, id: [16]u8) !void {
        const self: *Authority = @ptrCast(@alignCast(raw));
        try @import("sql_completion.zig").prepareTransaction(self.credential.trusted_server orelse return error.DurabilityUnavailable, self.request.completion_allocator orelse return error.InvalidArgument, self.request.completion.?, id);
    }
    fn prepareMetadata(raw: *anyopaque, owner: operation.SQLCompletionOwner) !void {
        const self: *Authority = @ptrCast(@alignCast(raw));
        try @import("sql_completion.zig").prepareMetadata(self.credential.trusted_server orelse return error.DurabilityUnavailable, self.request.completion_allocator orelse return error.InvalidArgument, self.request.completion.?, owner);
    }
    fn rejectMetadata(raw: *anyopaque, owner: operation.SQLCompletionOwner) !void {
        const self: *Authority = @ptrCast(@alignCast(raw));
        try @import("sql_completion.zig").rejectMetadata(self.credential.trusted_server orelse return error.DurabilityUnavailable, self.request.completion_allocator orelse return error.InvalidArgument, self.request.completion.?, owner);
    }
    fn importedSnapshot(self: *const Authority) ?ImportedSnapshot {
        const snapshot = self.credential.imported_snapshot orelse return null;
        const encoded = self.request.session_id orelse return null;
        const id = @import("distributed_txn.zig").parseTxnIdHex(encoded) catch return null;
        return if (std.mem.eql(u8, &snapshot.transaction, &id)) snapshot else null;
    }

    fn allowsWrite(raw: *const anyopaque, table: []const u8) bool {
        const self: *const Authority = @ptrCast(@alignCast(raw));
        if (self.importedSnapshot() != null) return false;
        self.credential.validate() catch return false;
        return http.tablePermissionCurrentlyAllowed(self.identity.*, table, .write) catch false;
    }
};

const GuardedCatalog = struct {
    const system = @import("pgwire_catalog.zig");

    native: catalog.Backend,
    authority: *Authority,
    revision: *const ?u64,
    expected_guard: ?[]const u8,
    fn backend(self: *GuardedCatalog) catalog.Backend {
        var result = self.native;
        result.ptr = self;
        if (self.authority.importedSnapshot() != null) {
            result.distributed_join = null;
            result.supports_search_relations = false;
            result.coordinated_point_reads = false;
            result.coordinated_index_reads = false;
            result.dynamic_statement_read_set = false;
        }
        result.vtable = &.{ .resolve = resolve, .resolve_graph = resolveGraph, .scan = scan, .supports_scan_order = true, .open_scan = openScan, .open_statement = openStatement, .mutate = mutate, .mutate_prepared = mutatePrepared, .prepare_mutations = prepareMutations, .resolve_conflict_owners = resolveConflictOwners, .generate_row_id = generateRowId, .checkpoint = checkpoint, .ddl = ddl };
        return result;
    }
    fn resolveGraph(raw: *anyopaque, alloc: std.mem.Allocator, definition: ast.GraphTable) !catalog.Table {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (self.authority.importedSnapshot() != null) return error.UnsupportedSqlShape;
        const resolve_graph = self.native.vtable.resolve_graph orelse return error.UnsupportedSqlExecution;
        const table = try resolve_graph(self.native.ptr, alloc, definition);
        if (self.expected_guard) |guard| try verifyBindingGuard(guard, self.revision.* orelse return error.InvalidSqlBackendResponse, table);
        try self.checkTableRead(table);
        return table;
    }
    fn checkTableRead(self: *GuardedCatalog, table: catalog.Table) !void {
        if (system.owns(table)) {
            try checkpoint(self);
            try self.authority.credential.validate();
            return;
        }
        if (table.graph) |binding| {
            try checkpoint(self);
            if (!try http.resourcePermissionCurrentlyAllowed(self.authority.identity.*, .graph, binding.graph.name, .read)) return error.Forbidden;
            for (binding.dependencies) |dependency| try self.checkRead(dependency.physical_name);
        } else try self.checkRead(table.physical_name);
    }
    fn copyDependencies(alloc: std.mem.Allocator, names: []const []const u8) ![][]u8 {
        if (names.len == 0) return &.{};
        const copied = try alloc.alloc([]u8, names.len);
        errdefer alloc.free(copied);
        var count: usize = 0;
        errdefer for (copied[0..count]) |name| alloc.free(name);
        for (names, copied) |name, *out| {
            out.* = try alloc.dupe(u8, name);
            count += 1;
        }
        return copied;
    }
    fn dependencies(alloc: std.mem.Allocator, table: catalog.Table) ![][]u8 {
        const binding = table.graph orelse return &.{};
        const names = try alloc.alloc([]const u8, binding.dependencies.len);
        defer alloc.free(names);
        for (binding.dependencies, names) |dependency, *name| name.* = dependency.physical_name;
        return copyDependencies(alloc, names);
    }
    fn freeDependencies(alloc: std.mem.Allocator, names: [][]u8) void {
        if (names.len == 0) return;
        for (names) |name| alloc.free(name);
        alloc.free(names);
    }
    fn generateRowId(raw: *anyopaque, alloc: std.mem.Allocator) ![]const u8 {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        const generate = self.native.vtable.generate_row_id orelse return error.UnsupportedSqlExecution;
        return generate(self.native.ptr, alloc);
    }
    fn resolveConflictOwners(raw: *anyopaque, alloc: std.mem.Allocator, scratch_alloc: std.mem.Allocator, table: catalog.Table, target: catalog.ConflictTarget, mutations: []const catalog.Mutation) ![]const catalog.ConflictOwner {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.checkTableRead(table);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        const resolve_owners = self.native.vtable.resolve_conflict_owners orelse return error.UnsupportedSqlShape;
        return resolve_owners(self.native.ptr, alloc, scratch_alloc, table, target, mutations);
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.authority.checkpoint();
        try self.native.vtable.checkpoint(self.native.ptr);
    }
    fn resolve(raw: *anyopaque, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        try self.authority.credential.validate();
        const table = (try system.resolve(name, action, self.authority.request.database orelse "default")) orelse
            try resolveOnPath(self.native, alloc, name, action, if (self.authority.request.search_path) |*path| path else null);
        if (!system.owns(table)) if (self.expected_guard) |guard| try verifyBindingGuard(guard, self.revision.* orelse return error.InvalidSqlBackendResponse, table);
        return table;
    }
    fn scanPinned(self: *GuardedCatalog, alloc: std.mem.Allocator, snapshot: ImportedSnapshot, table: catalog.Table, request: catalog.Scan) !catalog.Page {
        if (request.search != null or request.index_equality != null or request.index_range != null or
            request.primary_key != null or request.before != null or request.physical_selection != null or request.row_refs != null or
            request.include_document or request.include_primary_digest or request.conditions.len != 0 or request.order.len != 0)
            return error.UnsupportedSqlShape;
        const authority = self.authority;
        const server = authority.server orelse return error.ChangefeedCapabilityUnsupported;
        const context = try @import("../pgwire/retained_stream.zig").metadataContext(authority.request);
        const slots = try CatalogReplicationSlots.open(server, .{ .context = authority.credential, .read = Adapter.replicationIdentity }, alloc, authority.request.database orelse "default", context);
        return slots.readExportedSnapshotCatalogPage(alloc, &snapshot.token.name(), snapshot.namespace, table, request.after, @min(request.limit, 256), 4 * 1024 * 1024, context) catch |err| {
            std.log.warn("exported snapshot page unavailable err={s}", .{@errorName(err)});
            return err;
        };
    }

    const PinnedCursor = struct {
        alloc: std.mem.Allocator,
        guard: *GuardedCatalog,
        table: catalog.Table,
        request: catalog.Scan,
        snapshot: ImportedSnapshot,
        after: ?[]u8 = null,
        done: bool = false,

        fn next(raw: *anyopaque, a: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *PinnedCursor = @ptrCast(@alignCast(raw));
            if (self.done) return .{ .rows = &.{} };
            try self.guard.checkTableRead(self.table);
            var request = self.request;
            request.limit = limit;
            request.after = self.after;
            const page = try self.guard.scanPinned(a, self.snapshot, self.table, request);
            errdefer page.deinit();
            const after = if (page.after) |position| try self.alloc.dupe(u8, position) else null;
            if (self.after) |old| self.alloc.free(old);
            self.after = after;
            self.done = after == null;
            return page;
        }

        fn close(raw: *anyopaque) void {
            const self: *PinnedCursor = @ptrCast(@alignCast(raw));
            if (self.after) |after| self.alloc.free(after);
            self.alloc.destroy(self);
        }
    };

    fn openPinned(self: *GuardedCatalog, a: std.mem.Allocator, snapshot: ImportedSnapshot, table: catalog.Table, request: catalog.Scan) !catalog.Cursor {
        const cursor = try a.create(PinnedCursor);
        errdefer a.destroy(cursor);
        const after = if (request.after) |position| try a.dupe(u8, position) else null;
        cursor.* = .{ .alloc = a, .guard = self, .table = table, .request = request, .snapshot = snapshot, .after = after };
        return .{ .ptr = cursor, .next = PinnedCursor.next, .close = PinnedCursor.close };
    }

    const PinnedStatement = struct {
        alloc: std.mem.Allocator,
        cursors: []catalog.Cursor,
        fn close(raw: *anyopaque) void {
            const self: *PinnedStatement = @ptrCast(@alignCast(raw));
            for (self.cursors) |cursor| cursor.close(cursor.ptr);
            self.alloc.free(self.cursors);
            self.alloc.destroy(self);
        }
    };

    fn openPinnedStatement(self: *GuardedCatalog, a: std.mem.Allocator, snapshot: ImportedSnapshot, scans: []const catalog.StatementScan) !catalog.StatementRead {
        const statement = try a.create(PinnedStatement);
        errdefer a.destroy(statement);
        const cursors = try a.alloc(catalog.Cursor, scans.len);
        errdefer a.free(cursors);
        var opened: usize = 0;
        errdefer for (cursors[0..opened]) |cursor| cursor.close(cursor.ptr);
        for (scans, cursors) |selection, *cursor| {
            cursor.* = try self.openPinned(a, snapshot, selection.table, selection.request);
            opened += 1;
        }
        statement.* = .{ .alloc = a, .cursors = cursors };
        return .{ .ptr = statement, .cursors = cursors, .close = PinnedStatement.close };
    }

    fn scan(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Page {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.checkTableRead(table);
        if (system.isRoles(table)) return system.scanRole(alloc, request, try self.catalogRole());
        if (system.isEmpty(table)) return .{ .rows = &.{} };
        if (system.isReplicationSlots(table)) return self.replicationSlotPage(alloc, request);
        if (system.isPublications(table)) return self.publicationPage(alloc, request);
        if (system.isClasses(table)) {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            return system.scanClasses(alloc, request, try self.catalogClasses(arena.allocator(), request.conditions));
        }
        if (system.isNamespaces(table)) {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            return system.scanNamespaces(alloc, request, try self.catalogNamespaces(arena.allocator(), request.conditions));
        }
        if (system.owns(table)) return system.scan(alloc, request);
        if (self.authority.importedSnapshot()) |snapshot| return self.scanPinned(alloc, snapshot, table, request);
        return self.native.vtable.scan(self.native.ptr, alloc, table, request);
    }
    fn checkRead(self: *GuardedCatalog, table: []const u8) !void {
        try checkpoint(self);
        try self.authority.credential.validate();
        if (!(try http.tablePermissionCurrentlyAllowed(self.authority.identity.*, table, .read))) return error.Forbidden;
    }
    const ReadCursor = struct {
        alloc: std.mem.Allocator,
        guard: *GuardedCatalog,
        table: []u8,
        is_graph: bool = false,
        graph_dependencies: [][]u8 = &.{},
        inner: catalog.Cursor,

        fn checkAuthority(self: *@This()) !void {
            if (!self.is_graph) return self.guard.checkRead(self.table);
            try GuardedCatalog.checkpoint(self.guard);
            if (!try http.resourcePermissionCurrentlyAllowed(self.guard.authority.identity.*, .graph, self.table, .read)) return error.Forbidden;
            for (self.graph_dependencies) |name| try self.guard.checkRead(name);
        }
        fn releaseFields(self: *@This()) void {
            self.alloc.free(self.table);
            GuardedCatalog.freeDependencies(self.alloc, self.graph_dependencies);
        }
        fn next(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkAuthority();
            return self.inner.next(self.inner.ptr, alloc, limit);
        }
        fn nextColumns(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.ColumnPage {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkAuthority();
            return self.inner.next_columns.?(self.inner.ptr, alloc, limit);
        }
        fn countRows(raw: *anyopaque) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkAuthority();
            return self.inner.count_rows.?(self.inner.ptr);
        }
        fn enableWeights(raw: *anyopaque, multiplicity: catalog.Multiplicity) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkAuthority();
            const enabled = try self.inner.enable_weights.?(self.inner.ptr, multiplicity);
            try self.checkAuthority();
            return enabled;
        }
        fn checkGroups(ptr: *anyopaque, _: std.mem.Allocator) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.checkAuthority();
        }
        fn enableGroups(ptr: *anyopaque, requests: []const catalog.AggregateWeight, orders: []const @import("cypher").runtime.NodeCursor.GroupOrder, grouped: *@import("antfly_local_sources").sql_operators.Grouped) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.checkAuthority();
            const enabled = try self.inner.enable_groups.?(self.inner.ptr, requests, orders, grouped);
            if (enabled) try @import("sql_group_guard.zig").wrap(self.alloc, grouped, .{ .ptr = self, .call = checkGroups });
            try self.checkAuthority();
            return enabled;
        }
        fn enableAggregateWeights(raw: *anyopaque, requests: []const catalog.AggregateWeight) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkAuthority();
            const enabled = try self.inner.enable_aggregate_weights.?(self.inner.ptr, requests);
            try self.checkAuthority();
            return enabled;
        }
        fn wantsExactFilter(raw: *anyopaque, columns: []const @import("antfly_local_sources").sql_dynamic_filter.Column) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.checkRead(self.table);
            return self.inner.wants_exact_filter.?(self.inner.ptr, columns);
        }
        fn setDynamicFilter(raw: *anyopaque, filter: *const @import("antfly_local_sources").sql_dynamic_filter.Filter) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkAuthority();
            return self.inner.set_dynamic_filter.?(self.inner.ptr, filter);
        }
        fn splitScan(raw: *anyopaque, alloc: std.mem.Allocator, workers: usize) !?[]catalog.Cursor {
            return splitImpl(raw, alloc, workers, false);
        }
        fn splitOrdered(raw: *anyopaque, alloc: std.mem.Allocator, workers: usize) !?[]catalog.Cursor {
            return splitImpl(raw, alloc, workers, true);
        }
        fn splitImpl(raw: *anyopaque, alloc: std.mem.Allocator, workers: usize, ordered: bool) !?[]catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkAuthority();
            const split = if (ordered) self.inner.split_ordered.? else self.inner.split_scan.?;
            const children = (try split(self.inner.ptr, alloc, workers)) orelse return null;
            defer alloc.free(children);
            var wrapped: usize = 0;
            errdefer for (children[wrapped..]) |child| child.close(child.ptr);
            const cursors = try alloc.alloc(catalog.Cursor, children.len);
            errdefer alloc.free(cursors);
            errdefer for (cursors[0..wrapped]) |child| child.close(child.ptr);
            for (children, cursors) |child, *out| {
                const name = try alloc.dupe(u8, self.table);
                errdefer alloc.free(name);
                const deps = try GuardedCatalog.copyDependencies(alloc, self.graph_dependencies);
                errdefer GuardedCatalog.freeDependencies(alloc, deps);
                const owner = try alloc.create(ReadCursor);
                owner.* = .{ .alloc = alloc, .guard = self.guard, .table = name, .is_graph = self.is_graph, .graph_dependencies = deps, .inner = child };
                out.* = owner.cursor(close);
                wrapped += 1;
            }
            return cursors;
        }
        fn cursor(self: *@This(), release: *const fn (*anyopaque) void) catalog.Cursor {
            return .{
                .column_statistics = self.inner.column_statistics,
                .estimated_filtered_rows = self.inner.estimated_filtered_rows,
                .estimated_rows = self.inner.estimated_rows,
                .estimated_bytes = self.inner.estimated_bytes,
                .ptr = self,
                .next = next,
                .close = release,
                .next_columns = if (self.inner.next_columns != null) nextColumns else null,
                .count_rows = if (self.inner.count_rows != null) countRows else null,
                .enable_weights = if (self.inner.enable_weights != null) enableWeights else null,
                .enable_groups = if (self.inner.enable_groups != null) enableGroups else null,
                .enable_aggregate_weights = if (self.inner.enable_aggregate_weights != null) enableAggregateWeights else null,
                .set_dynamic_filter = if (self.inner.set_dynamic_filter != null) setDynamicFilter else null,
                .wants_exact_filter = if (self.inner.wants_exact_filter != null) wantsExactFilter else null,
                .split_scan = if (self.inner.split_scan != null) splitScan else null,
                .split_ordered = if (self.inner.split_ordered != null) splitOrdered else null,
            };
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inner.close(self.inner.ptr);
            self.releaseFields();
            self.alloc.destroy(self);
        }
    };
    fn catalogRole(self: *GuardedCatalog) !system.Role {
        const identity = self.authority.identity.* orelse return error.Forbidden;
        const can_replicate = for (identity.permissions) |permission| {
            if (permission.resource_type != .table and permission.resource_type != .@"*") continue;
            if (permission.deny_mutation or !permission.type.allows(.replicate)) continue;
            if (try http.tablePermissionCurrentlyAllowed(identity, permission.resource, .replicate)) break true;
        } else false;
        return .{ .name = self.authority.credential.sessionPrincipal(), .replicate = can_replicate };
    }
    fn openSystemScan(self: *GuardedCatalog, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Cursor {
        if (system.isRoles(table)) return system.openRole(alloc, request, try self.catalogRole());
        if (system.isEmpty(table)) return system.openEmpty(self);
        if (system.isReplicationSlots(table)) return system.openSingleRow(alloc, try self.replicationSlotPage(alloc, request));
        if (system.isPublications(table)) return system.openSingleRow(alloc, try self.publicationPage(alloc, request));
        if (system.isClasses(table)) {
            var arena = std.heap.ArenaAllocator.init(alloc);
            const items = self.catalogClasses(arena.allocator(), request.conditions) catch |err| {
                arena.deinit();
                return err;
            };
            return system.openClasses(request, arena, items);
        }
        if (system.isNamespaces(table)) {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            return system.openNamespaces(alloc, request, try self.catalogNamespaces(arena.allocator(), request.conditions));
        }
        return system.open(alloc, request);
    }
    fn publicationPage(self: *GuardedCatalog, alloc: std.mem.Allocator, request: catalog.Scan) !catalog.Page {
        const name = (try system.keyedName(request, "pubname")) orelse return .{ .rows = &.{} };
        if (name.len == 0 or name.len > 63 or std.mem.indexOfScalar(u8, name, 0) != null or request.limit == 0 or request.after != null) return .{ .rows = &.{} };
        const server = self.authority.server orelse return error.UnsupportedReplicationMode;
        const port = server.source.pg_publications orelse return .{ .rows = &.{} };
        const identity = self.authority.identity.* orelse return error.Forbidden;
        const owner = try @import("changefeed_public.zig").durableOwner(identity);
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var context = try self.authority.context();
        const database = try CatalogReplicationSlots.readDatabase(server, a, self.authority.request.database orelse "default", context);
        context.principal = .{ .kind = .internal, .subject = "pg-publication-maintenance" };
        var record = (try port.read(port.ptr, a, database, name, context)) orelse return .{ .rows = &.{} };
        defer record.deinit();
        const value = record.value;
        if (!std.meta.eql(value.database, database) or !std.mem.eql(u8, value.name, name)) return error.ChangefeedCorrupt;
        if (!std.crypto.timing_safe.eql(publication_control.Digest, owner, value.owner) or value.members == null) return .{ .rows = &.{} };
        try self.authority.credential.validate();
        const digest = publication_control.hash(.{ .database = database, .name = name });
        const oid = 16384 + std.mem.readInt(u32, digest[0..4], .big) % (std.math.maxInt(u32) - 16384);
        return system.publicationPage(alloc, request, name, oid, @import("antfly_local_sources").sql_scalar.postgresRoleOid(self.authority.credential.sessionPrincipal()));
    }

    fn replicationSlotPage(self: *GuardedCatalog, alloc: std.mem.Allocator, request: catalog.Scan) !catalog.Page {
        const name = (try system.keyedName(request, "slot_name")) orelse return .{ .rows = &.{} };
        if (!publication_control.pg_slot.validName(name) or request.limit == 0 or request.after != null) return .{ .rows = &.{} };
        const server = self.authority.server orelse return error.UnsupportedReplicationMode;
        if (server.source.changefeeds == null) return .{ .rows = &.{} };
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const identity: ReplicationIdentity = .{ .context = self.authority.credential, .read = Adapter.replicationIdentity };
        const context = try self.authority.context();
        const slots = try CatalogReplicationSlots.open(server, identity, a, self.authority.request.database orelse "default", context);
        var observed = slots.readOwned(a, name, context) catch |err| switch (err) {
            error.ReplicationSlotNotFound, error.Forbidden => return .{ .rows = &.{} },
            else => return err,
        };
        defer observed.deinit();
        const slot = observed.value.slot.?;
        const phase = observed.value.phase;
        // DROP becomes visible at the closing transition. Physical spool
        // reclamation and the final tombstone can follow independently.
        if (phase == .closing or phase == .deleted) return .{ .rows = &.{} };
        const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_s;
        return system.replicationSlotPage(alloc, request, .{
            .name = slot.name,
            .database = slots.database_name,
            .database_oid = slots.database_oid,
            .confirmed_lsn = slot.confirmed_lsn,
            .active = slot.session != null and slot.session_expires_at > now,
            .lost = phase == .reset_required or observed.value.expiryDue(now),
        });
    }

    fn settingCapture(self: *GuardedCatalog, alloc: std.mem.Allocator, adapter: *execution.Adapter, compiled: *const compiler.Compiled) !@FieldType(catalog.Backend, "setting_capture") {
        if (!compiled.uses_current_setting) return null;
        var capture = adapter.settingCapture().?;
        if (compiled.uses_transaction_xid) if (self.authority.request.session_id) |encoded| {
            const transaction = try @import("distributed_txn.zig").parseTxnIdHex(encoded);
            const digest = publication_control.hash(.{ .domain = "antfly:pg-sql-transaction-id:v1", .transaction = transaction });
            // Keep the compatibility ID in JDBC's signed long range.
            capture.transaction_xid = 3 + std.mem.readInt(u64, digest[0..8], .big) % (@as(u64, std.math.maxInt(i64)) - 2);
        };
        if (compiled.uses_snapshot_lsn) if (self.authority.importedSnapshot()) |snapshot| {
            const identity: ReplicationIdentity = .{ .context = self.authority.credential, .read = Adapter.replicationIdentity };
            const context = try self.authority.context();
            const slots = try CatalogReplicationSlots.open(self.authority.server orelse return error.UnsupportedReplicationMode, identity, alloc, self.authority.request.database orelse "default", context);
            var state = try slots.qualifySnapshot(alloc, snapshot.token, context);
            defer state.deinit();
            capture.snapshot_lsn = state.value.slot.?.consistent_lsn;
        };
        if (!compiled.uses_current_schemas) return capture;
        const namespaces = try self.catalogNamespaces(alloc, &.{});
        const path = if (self.authority.request.search_path) |*configured| configured.entries[0..configured.len] else null;
        var effective: std.ArrayList([]const u8) = .empty;
        for (0..if (path) |entries| entries.len else 1) |index| {
            const name = if (path) |entries| entries[index].slice() else self.authority.request.namespace orelse "public";
            for (namespaces) |candidate| {
                if (!std.mem.eql(u8, name, candidate.name)) continue;
                try effective.append(alloc, candidate.name);
                break;
            }
        }
        capture.schema_search_path = try effective.toOwnedSlice(alloc);
        return capture;
    }

    fn catalogClasses(self: *GuardedCatalog, a: std.mem.Allocator, conditions: []const catalog.Condition) ![]const system.Class {
        const domain = @import("antfly_local_sources").system_catalog_domain;
        const Relation = compiler.PostgresCatalogRelation;
        var result: std.ArrayList(system.Class) = .empty;
        inline for (std.meta.tags(Relation)) |tag| {
            const item: system.Class = .{
                .oid = @backingInt(tag),
                .name = @tagName(tag),
                .namespace = system.builtin_namespace.oid,
                .kind = switch (tag) {
                    .pg_roles, .pg_replication_slots => "v",
                    else => "r",
                },
            };
            if (try item.matches(conditions)) try result.append(a, item);
        }
        const server = self.authority.server orelse return result.items;
        if (server.source.vtable.system_catalog == null) return result.items;
        const database = self.authority.request.database orelse domain.default_database_name;
        const context = try self.authority.context();
        const identity = self.authority.identity.* orelse return error.Forbidden;
        var revision: ?u64 = null;
        var visited: usize = 0;
        for (try self.catalogNamespaces(a, &.{})) |namespace| {
            if (namespace.oid == system.builtin_namespace.oid) continue;
            var list: domain.TableList = .{ .database = database, .namespace = namespace.name, .limit = 128, .revision = revision };
            while (true) {
                try context.ensureActive();
                var page_arena = std.heap.ArenaAllocator.init(server.alloc);
                defer page_arena.deinit();
                const scratch = page_arena.allocator();
                const bytes = try server.source.systemCatalog(scratch, context, .{ .list_tables = list });
                const page = try std.json.parseFromSliceLeaky(@import("../system_catalog/projection.zig").TableListing, scratch, bytes, .{});
                if (revision) |expected| if (expected != page.revision) return error.CatalogGenerationChanged;
                revision = page.revision;
                list.revision = revision;
                list.legacy_membership = page.legacy_membership;
                for (page.entries) |entry| {
                    visited += 1;
                    if (visited > 4096) return error.SqlProgramLimitExceeded;
                    const key = try (domain.Target{ .database = database, .namespace = namespace.name, .table = entry.name }).resourceNameAlloc(scratch);
                    if (!try http.tablePermissionCurrentlyAllowed(identity, key, .read)) continue;
                    const digest = publication_control.hash(.{ .domain = "antfly:pg-class-oid:v1", .table = entry.table.table_id });
                    const item: system.Class = .{ .oid = 16384 + std.mem.readInt(u32, digest[0..4], .big) % (std.math.maxInt(u32) - 16384), .name = entry.name, .namespace = namespace.oid };
                    if (try item.matches(conditions)) {
                        var retained = item;
                        retained.name = try a.dupe(u8, item.name);
                        try result.append(a, retained);
                    }
                }
                const next = page.next_after orelse break;
                if (list.after) |prior| if (std.mem.eql(u8, prior, next) and list.after_table_id == page.next_table_id) return error.InvalidSqlBackendResponse;
                list.after = try a.dupe(u8, next);
                list.after_table_id = page.next_table_id;
            }
        }
        std.mem.sort(system.Class, result.items, {}, struct {
            fn less(_: void, lhs: system.Class, rhs: system.Class) bool {
                return lhs.oid < rhs.oid;
            }
        }.less);
        for (result.items, 0..) |item, index| {
            if (index > 0 and item.oid == result.items[index - 1].oid) return error.InvalidSqlBackendResponse;
        }
        return result.items;
    }

    fn catalogNamespaces(self: *GuardedCatalog, a: std.mem.Allocator, conditions: []const catalog.Condition) ![]const system.Namespace {
        var result: std.ArrayList(system.Namespace) = .empty;
        if (try system.namespaceMatches(system.builtin_namespace, conditions)) try result.append(a, system.builtin_namespace);
        const server = self.authority.server orelse return result.items;
        if (server.source.vtable.system_catalog == null) return result.items;
        const domain = @import("antfly_local_sources").system_catalog_domain;
        const database = self.authority.request.database orelse domain.default_database_name;
        const bytes = try server.source.systemCatalog(a, try self.authority.context(), .{ .read = .{ .kind = .namespace, .database = database } });
        const state = try std.json.parseFromSliceLeaky(domain.State, a, bytes, .{});
        const identity = self.authority.identity.* orelse return error.Forbidden;
        for (state.resources) |resource| {
            try checkpoint(self);
            if (resource.kind != .namespace or std.mem.eql(u8, resource.name, system.builtin_namespace.name)) continue;
            const logical = try @import("../system_catalog/routes.zig").resourceNameAlloc(a, .{ .kind = .namespace, .database = database, .name = resource.name });
            if (!try http.resourcePermissionCurrentlyAllowed(identity, .namespace, logical, .read)) continue;
            const item: system.Namespace = .{ .oid = system.namespaceOid(resource.id), .name = resource.name };
            if (try system.namespaceMatches(item, conditions)) try result.append(a, item);
        }
        std.mem.sort(system.Namespace, result.items, {}, struct {
            fn less(_: void, left: system.Namespace, right: system.Namespace) bool {
                return left.oid < right.oid;
            }
        }.less);
        for (result.items, 0..) |item, i| {
            if (i != 0 and item.oid == result.items[i - 1].oid) return error.InvalidSqlBackendResponse;
        }
        return result.items;
    }
    fn openScan(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.checkTableRead(table);
        if (system.owns(table)) return try self.openSystemScan(alloc, table, request);
        if (self.authority.importedSnapshot()) |snapshot| return try self.openPinned(alloc, snapshot, table, request);
        const open = self.native.vtable.open_scan orelse return null;
        const inner = (try open(self.native.ptr, alloc, table, request)) orelse return null;
        errdefer inner.close(inner.ptr);
        const name = try alloc.dupe(u8, table.physical_name);
        errdefer alloc.free(name);
        const deps = try dependencies(alloc, table);
        errdefer freeDependencies(alloc, deps);
        const cursor = try alloc.create(ReadCursor);
        cursor.* = .{ .alloc = alloc, .guard = self, .table = name, .is_graph = table.graph != null, .graph_dependencies = deps, .inner = inner };
        // Materialized execution closes before returning; streaming execution
        // retains the heap-owned guard and authority in its OwnedRead capsule.
        return cursor.cursor(ReadCursor.close);
    }
    fn mutate(raw: *anyopaque, alloc: std.mem.Allocator, scratch_alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        return self.native.vtable.mutate(self.native.ptr, alloc, scratch_alloc, table, input);
    }

    fn mutatePrepared(raw: *anyopaque, alloc: std.mem.Allocator, scratch_alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        const commit = self.native.vtable.mutate_prepared orelse return error.UnsupportedSqlExecution;
        return commit(self.native.ptr, alloc, scratch_alloc, table, input);
    }

    fn prepareMutations(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) ![]const catalog.Mutation {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        const prepare = self.native.vtable.prepare_mutations orelse return error.UnsupportedSqlExecution;
        return prepare(self.native.ptr, alloc, table, input);
    }

    const StatementRead = struct {
        alloc: std.mem.Allocator,
        inner: catalog.StatementRead,
        wrappers: []ReadCursor,
        cursors: []catalog.Cursor,
        fn borrowedClose(_: *anyopaque) void {}
        fn close(raw: *anyopaque) void {
            const self: *StatementRead = @ptrCast(@alignCast(raw));
            self.inner.close(self.inner.ptr);
            for (self.wrappers) |*wrapper| wrapper.releaseFields();
            self.alloc.free(self.wrappers);
            self.alloc.free(self.cursors);
            self.alloc.destroy(self);
        }
    };

    fn openStatement(raw: *anyopaque, alloc: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        var system_count: usize = 0;
        for (scans) |scan_request| {
            try self.checkTableRead(scan_request.table);
            if (system.owns(scan_request.table)) system_count += 1;
        }
        if (system_count != 0) {
            if (system_count != scans.len) return error.UnsupportedSqlShape;
            const owner = try alloc.create(PinnedStatement);
            errdefer alloc.destroy(owner);
            const cursors = try alloc.alloc(catalog.Cursor, scans.len);
            errdefer alloc.free(cursors);
            var opened: usize = 0;
            errdefer for (cursors[0..opened]) |cursor| cursor.close(cursor.ptr);
            for (scans, cursors) |selection, *cursor| {
                cursor.* = try self.openSystemScan(alloc, selection.table, selection.request);
                opened += 1;
            }
            owner.* = .{ .alloc = alloc, .cursors = cursors };
            return .{ .ptr = owner, .cursors = cursors, .close = PinnedStatement.close };
        }
        if (self.authority.importedSnapshot()) |snapshot| return self.openPinnedStatement(alloc, snapshot, scans);
        const open = self.native.vtable.open_statement orelse return error.SqlStatementSnapshotRequired;
        const inner = try open(self.native.ptr, alloc, scans);
        errdefer inner.close(inner.ptr);
        if (inner.cursors.len != scans.len) return error.InvalidSqlBackendResponse;
        const owner = try alloc.create(StatementRead);
        errdefer alloc.destroy(owner);
        const wrappers = try alloc.alloc(ReadCursor, scans.len);
        errdefer alloc.free(wrappers);
        const cursors = try alloc.alloc(catalog.Cursor, scans.len);
        errdefer alloc.free(cursors);
        var count: usize = 0;
        errdefer for (wrappers[0..count]) |*wrapper| wrapper.releaseFields();
        for (scans, inner.cursors, wrappers, cursors) |scan_request, cursor, *wrapper, *out| {
            const deps = try dependencies(alloc, scan_request.table);
            errdefer freeDependencies(alloc, deps);
            wrapper.* = .{ .alloc = alloc, .guard = self, .table = try alloc.dupe(u8, scan_request.table.physical_name), .is_graph = scan_request.table.graph != null, .graph_dependencies = deps, .inner = cursor };
            count += 1;
            out.* = wrapper.cursor(StatementRead.borrowedClose);
        }
        owner.* = .{ .alloc = alloc, .inner = inner, .wrappers = wrappers, .cursors = cursors };
        return .{ .ptr = owner, .cursors = cursors, .close = StatementRead.close };
    }

    fn ddl(raw: *anyopaque, allocators: catalog.DdlAllocators, input: catalog.Ddl) !catalog.DdlOutcome {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (self.authority.importedSnapshot() != null) return error.SqlReadOnlyTransaction;
        const dispatch_ddl = self.native.vtable.ddl orelse return error.UnsupportedSqlExecution;
        // Refresh current grants before native DDL authorizes resource, rename
        // destination and tablespace. Prepared metadata never grants authority.
        var current = try self.authority.credential.identity(allocators.scratch);
        defer current.deinit(allocators.scratch);
        const prior = self.authority.identity.*;
        self.authority.identity.* = current;
        defer self.authority.identity.* = prior;
        return dispatch_ddl(self.native.ptr, allocators, input);
    }
};

fn statementBindingGuard(alloc: std.mem.Allocator, revision: ?u64, binding: describe_sql.BoundStatement) !?[]const u8 {
    var scans: [64]catalog.StatementScan = undefined;
    var count: usize = 0;
    try collectBindingTables(binding, &scans, &count, 0);
    if (count == 0) return null;
    return try bindingGuards(alloc, revision orelse return error.InvalidSqlBackendResponse, scans[0..count]);
}

fn collectBindingTable(table: catalog.Table, scans: *[64]catalog.StatementScan, count: *usize) !void {
    // Built-in catalog layouts are owned by this server binary. They do not
    // resolve through the native catalog or advance its route revision.
    if (GuardedCatalog.system.owns(table)) return;
    for (scans[0..count.*]) |scan| {
        if (scan.table.id == table.id and scan.table.schema_version == table.schema_version and std.mem.eql(u8, scan.table.physical_name, table.physical_name)) return;
    }
    if (count.* == scans.len) return error.SqlProgramLimitExceeded;
    scans[count.*] = .{ .table = table, .request = .{ .fields = &.{}, .limit = 1 } };
    count.* += 1;
}

fn collectBindingTables(binding: describe_sql.BoundStatement, scans: *[64]catalog.StatementScan, count: *usize, depth: usize) anyerror!void {
    if (depth >= 64) return error.SqlProgramLimitExceeded;
    if (binding.relation) |relation| {
        // binding.table describes the synthesized output layout in this case.
        for (relation.scans) |scan| try collectBindingTable(scan.table, scans, count);
    } else if (binding.table) |table| try collectBindingTable(table, scans, count);
    if (binding.insert_source) |source| try collectBindingTables(source.*, scans, count, depth + 1);
    if (binding.joined_mutation) |joined| try collectBindingTables(joined.input.*, scans, count, depth + 1);
    if (binding.returning) |returning| try collectBindingTables(returning.*, scans, count, depth + 1);
}

fn bindingGuard(alloc: std.mem.Allocator, revision: u64, table: catalog.Table) ![]const u8 {
    return bindingGuards(alloc, revision, &.{.{ .table = table, .request = .{ .fields = &.{}, .limit = 1 } }});
}

fn bindingGuards(alloc: std.mem.Allocator, revision: u64, scans: []const catalog.StatementScan) ![]const u8 {
    if (scans.len == 0 or scans.len > 64) return error.SqlProgramLimitExceeded;
    var length: usize = 10;
    for (scans) |scan| length = try std.math.add(usize, length, 16 + scan.table.physical_name.len);
    const bytes = try alloc.alloc(u8, length);
    std.mem.writeInt(u64, bytes[0..8], revision, .little);
    std.mem.writeInt(u16, bytes[8..10], @intCast(scans.len), .little);
    var offset: usize = 10;
    for (scans) |scan| {
        const entry = bytes[offset..];
        std.mem.writeInt(u32, entry[0..4], @intCast(scan.table.physical_name.len), .little);
        std.mem.writeInt(u64, entry[4..12], scan.table.id, .little);
        std.mem.writeInt(u32, entry[12..16], scan.table.schema_version, .little);
        @memcpy(entry[16..][0..scan.table.physical_name.len], scan.table.physical_name);
        offset += 16 + scan.table.physical_name.len;
    }
    return bytes;
}

fn verifyBindingGuard(guard: []const u8, revision: u64, table: catalog.Table) !void {
    if (guard.len < 10 or std.mem.readInt(u64, guard[0..8], .little) != revision) return error.CatalogGenerationChanged;
    const count = std.mem.readInt(u16, guard[8..10], .little);
    if (count == 0 or count > 64) return error.CatalogGenerationChanged;
    var rest = guard[10..];
    var found = false;
    for (0..count) |_| {
        if (rest.len < 16) return error.CatalogGenerationChanged;
        const length = std.mem.readInt(u32, rest[0..4], .little);
        if (length > rest.len - 16) return error.CatalogGenerationChanged;
        if (std.mem.eql(u8, rest[16..][0..length], table.physical_name)) {
            if (std.mem.readInt(u64, rest[4..12], .little) != table.id or
                std.mem.readInt(u32, rest[12..16], .little) != table.schema_version) return error.CatalogGenerationChanged;
            found = true;
        }
        rest = rest[16 + length ..];
    }
    if (rest.len != 0 or !found) return error.CatalogGenerationChanged;
}

test "SQL pgwire prepared identity rejects same shaped table replacement before execution" {
    const table = catalog.Table{ .id = 42, .physical_name = "physical_docs", .schema_version = 1, .columns = &.{} };
    const guard = try bindingGuard(std.testing.allocator, 7, table);
    defer std.testing.allocator.free(guard);
    try verifyBindingGuard(guard, 7, table);
    var replacement = table;
    replacement.id = 43;
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(guard, 7, replacement));
    replacement = table;
    replacement.schema_version = 2;
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(guard, 7, replacement));
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(guard, 8, table));
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard("malformed", 7, table));
    const other: catalog.Table = .{ .id = 55, .physical_name = "other_docs", .schema_version = 3, .columns = &.{} };
    const many = try bindingGuards(std.testing.allocator, 7, &.{
        .{ .table = table, .request = .{ .fields = &.{}, .limit = 1 } },
        .{ .table = other, .request = .{ .fields = &.{}, .limit = 1 } },
    });
    defer std.testing.allocator.free(many);
    try verifyBindingGuard(many, 7, other);
    try verifyBindingGuard(many, 7, table);
    for (0..many.len) |length| try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(many[0..length], 7, other));
}

test "SQL pgwire binding manifest includes recursive mutation sources and deduplicates targets" {
    const target: catalog.Table = .{ .id = 1, .physical_name = "target", .schema_version = 1, .columns = &.{} };
    const source: catalog.Table = .{ .id = 2, .physical_name = "source", .schema_version = 1, .columns = &.{} };
    const source_binding: describe_sql.BoundStatement = .{ .table = source, .action = .read, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty };
    var binding: describe_sql.BoundStatement = .{ .table = target, .action = .read, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty, .insert_source = &source_binding, .returning = &source_binding };
    const inserted = (try statementBindingGuard(std.testing.allocator, 9, binding)).?;
    defer std.testing.allocator.free(inserted);
    try std.testing.expectEqual(@as(u16, 2), std.mem.readInt(u16, inserted[8..10], .little));
    try verifyBindingGuard(inserted, 9, target);
    try verifyBindingGuard(inserted, 9, source);
    var replaced = source;
    replaced.id = 3;
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(inserted, 9, replaced));
    var joined: @import("antfly_local_sources").sql_joined_mutation.Bound = undefined;
    joined.input = &source_binding;
    binding.insert_source = null;
    binding.returning = null;
    binding.joined_mutation = &joined;
    const updated = (try statementBindingGuard(std.testing.allocator, 9, binding)).?;
    defer std.testing.allocator.free(updated);
    try std.testing.expectEqualSlices(u8, inserted, updated);
}

test "SQL pgwire credential snapshot observes policy revocation and password rotation" {
    const alloc = std.testing.allocator;
    const casbin = @import("antfly_casbin");
    var store = usermgr.MemoryStore.init(alloc);
    defer store.deinit();
    var policies = casbin.MemoryAdapter.init(alloc);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(alloc, store.iface(), try usermgr.initDefaultEnforcer(alloc, policies.iface()));
    defer manager.deinit();
    var read = try usermgr.Permission.initOwned(alloc, .table, "docs", .read);
    defer read.deinit(alloc);
    var user = try manager.createUser("alice", "secret", &.{read});
    defer user.deinit(alloc);
    const SnapshotAllocation = struct {
        fn check(failing: std.mem.Allocator, original: *usermgr.UserManager) !void {
            // Read-only copy shares map data but routes only snapshot ownership
            // through the failing allocator. Never deinitialize the borrowed maps.
            var borrowed = original.*;
            borrowed.alloc = failing;
            var snapshot = try snapshotUser(&borrowed, "alice");
            defer snapshot.deinit(failing);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, SnapshotAllocation.check, .{&manager});
    try std.testing.expectError(error.Unauthorized, Credential.authenticate(alloc, &manager, "missing", "secret"));
    try std.testing.expectError(error.InvalidPassword, Credential.authenticate(alloc, &manager, "alice", "wrong"));
    const credential = try Credential.authenticate(alloc, &manager, "alice", "secret");
    defer Credential.release(credential, alloc);
    var admitted = try credential.identity(alloc);
    defer admitted.deinit(alloc);
    try std.testing.expectEqual(user.instance_id, admitted.user_instance_id.?);
    try std.testing.expectEqual(user.instance_id, credential.user_instance_id.?);
    try std.testing.expect(try http.tablePermissionCurrentlyAllowed(admitted, "docs", .read));
    var projected = (try http.cloneCatalogIdentity(alloc, admitted)).?;
    defer projected.deinit(alloc);
    try http.projectCatalogIdentity(alloc, &projected, "docs", "private-table-17");
    const retained_policies = [_]OwnedRead.Policy{.{ .table = "private-table-17", .filter = null }};
    {
        var current = try credential.identity(alloc);
        defer current.deinit(alloc);
        try OwnedRead.validatePolicies(alloc, &current, projected, &retained_policies);
    }
    const NativeCursor = struct {
        pages: usize = 0,
        columns: usize = 0,
        filters: usize = 0,
        counts: usize = 0,
        splits: usize = 0,
        closes: usize = 0,
        fn open(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
            return cursor(ptr);
        }
        fn cursor(ptr: *anyopaque) catalog.Cursor {
            return .{ .estimated_filtered_rows = 7, .estimated_rows = 99, .estimated_bytes = 1024, .ptr = ptr, .next = next, .next_columns = nextColumns, .count_rows = countRows, .set_dynamic_filter = setFilter, .split_scan = split, .split_ordered = split, .close = close };
        }
        fn next(ptr: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.pages += 1;
            return .{ .rows = &.{}, .after = "next" };
        }
        fn nextColumns(ptr: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.ColumnPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.columns += 1;
            return .{ .batch = .{ .snapshot = .{ .table_id = "docs", .snapshot_id = "v1" }, .row_refs = &.{}, .columns = &.{} }, .selection = &.{} };
        }
        fn countRows(ptr: *anyopaque) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counts += 1;
            return 99;
        }
        fn setFilter(ptr: *anyopaque, _: *const @import("antfly_local_sources").sql_dynamic_filter.Filter) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.filters += 1;
            return true;
        }
        fn split(ptr: *anyopaque, a: std.mem.Allocator, _: usize) !?[]catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const children = try a.alloc(catalog.Cursor, 2);
            @memset(children, cursor(ptr));
            self.splits += 1;
            return children;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var native_cursor: NativeCursor = .{};
    var identity: ?http.AuthenticatedIdentity = admitted;
    var cancellation: std.atomic.Value(bool) = .init(false);
    var authority = Authority{ .credential = credential, .identity = &identity, .request = .{
        .statement = "SELECT * FROM docs",
        .limit = 1,
        .io = std.testing.io,
        .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } },
        .cancel_requested = &cancellation,
    } };
    {
        const saved = authority.request;
        defer authority.request = saved;
        const OffsetClock = struct {
            fn now(_: ?*anyopaque, clock: std.Io.Clock) std.Io.Timestamp {
                if (clock == .awake) return .{ .nanoseconds = std.time.ns_per_s };
                return std.testing.io.vtable.now(std.testing.io.userdata, clock);
            }
        };
        var vtable = std.testing.io.vtable.*;
        vtable.now = OffsetClock.now;
        authority.request.io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
        authority.request.deadline.raw.nanoseconds = 10 * std.time.ns_per_s;
        const before = @import("antfly_platform").time.monotonicNs();
        const normalized = try authority.context();
        const after = @import("antfly_platform").time.monotonicNs();
        try std.testing.expect(normalized.deadline_io == null);
        try std.testing.expect(normalized.deadline_ns.? >= before + 9 * std.time.ns_per_s);
        try std.testing.expect(normalized.deadline_ns.? <= after + 9 * std.time.ns_per_s);
        try normalized.ensureActive();
        authority.request.deadline.raw.nanoseconds = std.math.maxInt(i96);
        try std.testing.expect((try authority.context()).deadline_ns == null);
        authority.request.deadline.raw.nanoseconds = std.time.ns_per_s;
        try std.testing.expectError(error.QueryCanceled, authority.context());
        authority.request.deadline.raw.nanoseconds = 10 * std.time.ns_per_s;
        cancellation.store(true, .release);
        defer cancellation.store(false, .release);
        try std.testing.expectError(error.QueryCanceled, authority.context());
    }
    const revision: ?u64 = 1;
    var guarded: GuardedCatalog = .{
        .native = .{ .ptr = &native_cursor, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .open_scan = NativeCursor.open, .checkpoint = NativeCursor.checkpoint } },
        .authority = &authority,
        .revision = &revision,
        .expected_guard = null,
    };
    const backend = guarded.backend();
    const cursor = (try backend.vtable.open_scan.?(backend.ptr, alloc, .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = &.{} }, .{ .fields = &.{}, .limit = 1 })).?;
    var cursor_closed = false;
    defer if (!cursor_closed) cursor.close(cursor.ptr);
    const page = try cursor.next(cursor.ptr, alloc, 1);
    page.deinit();
    try std.testing.expectEqual(@as(usize, 1), native_cursor.pages);
    try std.testing.expectEqual(@as(?u64, 7), cursor.estimated_filtered_rows);
    try std.testing.expectEqual(@as(?u64, 99), cursor.estimated_rows);
    try std.testing.expectEqual(@as(?u64, 1024), cursor.estimated_bytes);
    _ = try cursor.next_columns.?(cursor.ptr, alloc, 1);
    try std.testing.expectEqual(@as(?u64, 99), try cursor.count_rows.?(cursor.ptr));
    const filter = try @import("antfly_local_sources").sql_dynamic_filter.Filter.create(alloc, &.{}, 128);
    defer filter.close();
    try std.testing.expect(try cursor.set_dynamic_filter.?(cursor.ptr, filter));
    const children = (try cursor.split_scan.?(cursor.ptr, alloc, 2)).?;
    defer alloc.free(children);
    var children_closed = false;
    defer if (!children_closed) for (children) |child| child.close(child.ptr);
    for (children) |child| {
        _ = try child.next_columns.?(child.ptr, alloc, 1);
        try std.testing.expectEqual(@as(?u64, 99), try child.count_rows.?(child.ptr));
    }
    const SplitSweep = struct {
        fn run(a: std.mem.Allocator, parent: catalog.Cursor, fixture: *NativeCursor, ordered: bool) !void {
            const before_splits = fixture.splits;
            const before_closes = fixture.closes;
            defer std.debug.assert(fixture.closes - before_closes == 2 * (fixture.splits - before_splits));
            const split = if (ordered) parent.split_ordered.? else parent.split_scan.?;
            const parts = (try split(parent.ptr, a, 2)).?;
            defer a.free(parts);
            defer for (parts) |part| part.close(part.ptr);
            for (parts) |part| try std.testing.expectEqual(@as(?u64, 99), part.estimated_rows);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, SplitSweep.run, .{ cursor, &native_cursor, false });
    try std.testing.checkAllAllocationFailures(alloc, SplitSweep.run, .{ cursor, &native_cursor, true });
    const closes_before_revoke = native_cursor.closes;
    try manager.removePermissionFromUser("alice", "docs", .table);
    {
        var current = try credential.identity(alloc);
        defer current.deinit(alloc);
        try std.testing.expectError(error.Forbidden, OwnedRead.validatePolicies(alloc, &current, projected, &retained_policies));
    }
    try std.testing.expectError(error.Forbidden, cursor.next(cursor.ptr, alloc, 1));
    try std.testing.expectError(error.Forbidden, cursor.next_columns.?(cursor.ptr, alloc, 1));
    try std.testing.expectError(error.Forbidden, cursor.count_rows.?(cursor.ptr));
    try std.testing.expectError(error.Forbidden, cursor.set_dynamic_filter.?(cursor.ptr, filter));
    try std.testing.expectError(error.Forbidden, cursor.split_scan.?(cursor.ptr, alloc, 2));
    try std.testing.expectError(error.Forbidden, cursor.split_ordered.?(cursor.ptr, alloc, 2));
    for (children) |child| {
        try std.testing.expectError(error.Forbidden, child.next_columns.?(child.ptr, alloc, 1));
        try std.testing.expectError(error.Forbidden, child.count_rows.?(child.ptr));
        child.close(child.ptr);
    }
    children_closed = true;
    try std.testing.expectEqual(@as(usize, 1), native_cursor.pages);
    try std.testing.expectEqual(@as(usize, 3), native_cursor.columns);
    try std.testing.expectEqual(@as(usize, 3), native_cursor.counts);
    try std.testing.expectEqual(@as(usize, 1), native_cursor.filters);
    cursor.close(cursor.ptr);
    cursor_closed = true;
    try std.testing.expectEqual(closes_before_revoke + 3, native_cursor.closes);
    try std.testing.expect(!(try http.tablePermissionCurrentlyAllowed(admitted, "docs", .read)));
    var fresh = try credential.identity(alloc);
    defer fresh.deinit(alloc);
    try std.testing.expect(!(try http.tablePermissionCurrentlyAllowed(fresh, "docs", .read)));
    try manager.updatePassword("alice", "new-secret");
    try std.testing.expectError(error.Unauthorized, credential.validate());
    try std.testing.expectError(error.Unauthorized, credential.identity(alloc));
    const replacement = try Credential.authenticate(alloc, &manager, "alice", "new-secret");
    defer Credential.release(replacement, alloc);
    var replacement_identity = try replacement.identity(alloc);
    defer replacement_identity.deinit(alloc);
    try std.testing.expect(!(try http.tablePermissionCurrentlyAllowed(replacement_identity, "docs", .read)));
}

test "SQL pgwire native replication requires live whole-table export authority" {
    const a = std.testing.allocator;
    const Fixture = struct {
        identity: http.AuthenticatedIdentity,
        reads: usize = 0,
        fn read(raw: *anyopaque, alloc: std.mem.Allocator) anyerror!http.AuthenticatedIdentity {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            return (try http.cloneCatalogIdentity(alloc, self.identity)).?;
        }
    };
    var grant = try usermgr.Permission.initOwned(a, .table, "docs", .read);
    defer grant.deinit(a);
    var fixture: Fixture = .{ .identity = .{ .username = @constCast("exporter"), .permissions = (&grant)[0..1] } };
    const authority: ReplicationIdentity = .{ .context = &fixture, .read = Fixture.read };
    try std.testing.expectError(error.Forbidden, authority.requireWholeTables(a, &.{"docs"}));
    grant.type = .replicate;
    try authority.requireWholeTables(a, &.{"docs"});
    try std.testing.expectError(error.Forbidden, authority.requireWholeTables(a, &.{ "docs", "other" }));
    var filter = try usermgr.RowFilterEntry.initOwned(a, "docs", "{}");
    defer filter.deinit(a);
    fixture.identity.row_filter = (&filter)[0..1];
    try std.testing.expectError(error.Forbidden, authority.requireWholeTables(a, &.{"docs"}));
    try std.testing.expectEqual(@as(usize, 4), fixture.reads);
}

test "SQL pgwire native adapter admits sessions for principal scoped native validation" {
    // Force semantic compilation of every production callback without needing
    // a listener or a live distributed backend in this owner-layer test.
    const a = std.testing.allocator;
    const server = try a.create(http.ApiHttpServer);
    defer a.destroy(server);
    server.cfg = .{};
    var adapter: Adapter = .{ .server = server };
    const callbacks = adapter.backend();
    try std.testing.expect(callbacks.context == @as(*anyopaque, @ptrCast(&adapter)));
    try std.testing.expect(callbacks.vtable.open_replication_session == null);
    const Denied = struct {
        fn open(_: *anyopaque, _: std.mem.Allocator, _: ReplicationIdentity, _: wire.Request) anyerror!wire.ReplicationSession {
            return error.Forbidden;
        }
    };
    server.cfg.pgwire_replication = .{ .context = server, .open_fn = Denied.open };
    try std.testing.expect(adapter.backend().vtable.open_replication_session != null);
    var cancelled = std.atomic.Value(bool).init(false);
    const request = wire.Request{
        .statement = "SELECT * FROM docs",
        .limit = 4096,
        .session_id = "untrusted-session",
        .io = std.testing.io,
        .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } },
        .cancel_requested = &cancelled,
    };
    // Session identity is checked by the durable native owner, not rejected by
    // ingress merely because this request carries a transaction id.
    try validateRequest(request);
}

test "SQL pgwire command tags carry exact PostgreSQL counts" {
    const cases = [_]struct { command: []const u8, expected: []const u8 }{
        .{ .command = "SELECT", .expected = "SELECT 3" },
        .{ .command = "INSERT", .expected = "INSERT 0 7" },
        .{ .command = "UPDATE", .expected = "UPDATE 7" },
        .{ .command = "DELETE", .expected = "DELETE 7" },
        .{ .command = "SET CONSTRAINTS", .expected = "SET CONSTRAINTS" },
        .{ .command = "TRUNCATE TABLE", .expected = "TRUNCATE TABLE" },
    };
    for (cases) |case| {
        const tag = try commandTag(std.testing.allocator, case.command, 3, 7);
        defer std.testing.allocator.free(tag);
        try std.testing.expectEqualStrings(case.expected, tag);
    }
}

test "SQL pgwire binary datetime parameters cross native coercion without precision loss" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    // Native storage accepts unsigned nanoseconds; the high case must not
    // narrow through i64 even though the PostgreSQL wire uses signed micros.
    for ([_]u64{ 0, 946684800000000000, 18000000000123456000, std.math.maxInt(u64) / 1000 * 1000 }) |nanos| {
        const wire_value = try wire_values.timestampValue(alloc, nanos);
        const bytes = try wire_values.encode(alloc, .datetime, 1, wire_value);
        const decoded = try wire_values.decode(alloc, 1184, 1, bytes);
        const parameters = try normalizeParameters(alloc, &.{decoded}, &.{.datetime});
        const coerced = try describe_sql.coerce(parameters[0], .datetime);
        try std.testing.expect(coerced == .string);
        try std.testing.expectEqual(@as(?u64, nanos), storage_schema.parseDateTimeToNs(coerced.string));
        // The native SELECT representation is ISO; the adapter normalizes just
        // that owned cell and then produces the exact original binary micros.
        const returned = try datetimeResult(alloc, coerced);
        try std.testing.expectEqualSlices(u8, bytes, try wire_values.encode(alloc, .datetime, 1, returned));
        const text = try wire_values.encode(alloc, .datetime, 0, returned);
        try std.testing.expectEqual(@as(?u64, nanos), storage_schema.parseDateTimeToNs(text));
    }
    const text_value = std.json.Value{ .string = "2000-01-01T01:00:00+01:00" };
    const text_parameters = try normalizeParameters(alloc, &.{ text_value, .null }, &.{ .datetime, .datetime });
    try std.testing.expectEqualStrings(text_value.string, text_parameters[0].string);
    try std.testing.expect(text_parameters[1] == .null);
    const returned = try datetimeResult(alloc, text_value);
    try std.testing.expectEqual(@as(i64, 0), std.mem.readInt(i64, (try wire_values.encode(alloc, .datetime, 1, returned))[0..8], .big));
    const too_precise = try datetimeResult(alloc, .{ .string = "2000-01-01T00:00:00.000000001Z" });
    try std.testing.expectError(error.UnsupportedResultPrecision, wire_values.encode(alloc, .datetime, 1, too_precise));
    try std.testing.expectError(error.UnsupportedResultPrecision, wire_values.encode(alloc, .datetime, 0, too_precise));
    try std.testing.expectError(error.InvalidSqlBackendResponse, datetimeResult(alloc, .{ .string = "1969-01-01T00:00:00Z" }));
}

test "SQL pgwire native frozen JSON renders exact numbers without scalar delivery" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            const sources = @import("antfly_local_sources");
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const alloc = arena.allocator();
            for ([_]bool{ false, true }) |parse_numbers| {
                const root = try std.json.parseFromSliceLeaky(std.json.Value, alloc, "{\"x\":[9007199254740993,1.25],\"s\":\"a\\nb\"}", .{ .parse_numbers = parse_numbers });
                const rows = [_]sources.sql_operators.Row{.{ .values = &.{sources.sql_scalar.Datum.json(root)}, .keys = &.{}, .ordinal = 0 }};
                for ([_]sources.sql_spill.ColumnarVersion{ .v1, .v2 }) |version| {
                    const bytes = try sources.sql_spill.encodeColumnarBlockVersionAlloc(alloc, &rows, 1 << 20, version);
                    const block = try sources.sql_spill.decodeColumnarBlockWithCache(alloc, a, bytes, 1 << 20);
                    defer block.deinit();
                    const Reader = struct {
                        value: sources.sql_execution_batch.FrozenJson,
                        fn cell(_: *anyopaque, _: std.mem.Allocator, _: usize, _: usize) !sources.sql_scalar.Datum {
                            return error.UnexpectedScalarMaterialization;
                        }
                        fn frozen(raw: *anyopaque, _: usize, _: usize) !?sources.sql_execution_batch.FrozenJson {
                            return @as(*@This(), @ptrCast(@alignCast(raw))).value;
                        }
                    };
                    var reader: Reader = .{ .value = (try block.frozenJson(0, 0, false)).? };
                    var owner: OwnedRead.PageOwner = .{ .alloc = a, .page = .{
                        .arena = .init(a),
                        .columns = &.{.{ .name = "j", .type = .json }},
                        .values = .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.frozen, .count = 1, .width = 1 } },
                        .exhausted = true,
                    } };
                    defer owner.page.deinit();
                    var scratch = std.heap.FixedBufferAllocator.init(&.{});
                    const cell = try OwnedRead.PageOwner.cell(&owner, scratch.allocator(), 0, 0);
                    try std.testing.expect(!cell.sql_null and cell.value == .null and cell.json_bytes == null and cell.frozen_json != null);
                    const expected = try std.json.Stringify.valueAlloc(alloc, root, .{});
                    for ([_]u16{ 0, 1 }) |format| {
                        var buffer: [256]u8 = undefined;
                        var output: std.Io.Writer = .fixed(&buffer);
                        try wire_values.encodeCellInto(scratch.allocator(), &output, .{ .name = "j", .type = .json }, format, cell, buffer.len);
                        const prefix: usize = @intFromBool(format == 1);
                        if (prefix != 0) try std.testing.expectEqual(@as(u8, 1), output.buffered()[0]);
                        try std.testing.expectEqualStrings(expected, output.buffered()[prefix..]);
                    }
                }
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL pgwire native trusted principals preserve scoped permissions and expire" {
    const alloc = std.testing.allocator;
    var manager: usermgr.UserManager = undefined; // borrowed, unused by trusted authentication
    var server = http.ApiHttpServer.init(alloc, .{
        .user_manager = &manager,
        .trusted_principal_secret = "cloud-test-authority",
        .trusted_principal_issuer = "cloudaf",
    }, .{ .ptr = undefined, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    const payload = "{\"iss\":\"cloudaf\",\"sub\":\"cloud-key-1\",\"tenant\":\"instance-1\",\"tables\":[\"docs\"],\"operations\":[\"read\"],\"row_filter\":{\"docs\":{\"term\":{\"tenant_id\":\"t1\"}}},\"exp\":4102444800}";
    const token = try testTrustedToken(alloc, "cloud-test-authority", payload);
    defer alloc.free(token);
    var adapter: Adapter = .{ .server = &server };
    const authenticated = try Adapter.authenticate(&adapter, alloc, trusted_principal_username, token);
    defer authenticated.release(authenticated.context, alloc);
    const credential: *Credential = @ptrCast(@alignCast(authenticated.context));
    var identity = try credential.identity(alloc);
    defer identity.deinit(alloc);
    try std.testing.expectEqualStrings("cloud-key-1", identity.username);
    try std.testing.expectEqualStrings("trusted:cloudaf:cloud-key-1", credential.sessionPrincipal());
    try std.testing.expect(http.permissionsAllow(identity.permissions, .table, "docs", .read));
    try std.testing.expect(!http.permissionsAllow(identity.permissions, .table, "other", .read));
    try std.testing.expect(!http.permissionsAllow(identity.permissions, .table, "docs", .write));
    try std.testing.expectEqual(@as(usize, 1), identity.row_filter.len);
    server.cfg.trusted_principal_secret = "rotated";
    try std.testing.expectError(error.Unauthorized, credential.validate());
    try std.testing.expectError(error.Unauthorized, credential.identity(alloc));
    server.cfg.trusted_principal_secret = "cloud-test-authority";
    credential.trusted_expires = 0;
    try std.testing.expectError(error.Unauthorized, credential.validate());
    try std.testing.expectError(error.Unauthorized, Adapter.authenticate(&adapter, alloc, trusted_principal_username, "not-a-token"));
    server.cfg.trusted_principal_secret = null;
    try std.testing.expectError(error.Unauthorized, Adapter.authenticate(&adapter, alloc, trusted_principal_username, token));
}

fn testTrustedToken(alloc: std.mem.Allocator, secret: []const u8, payload: []const u8) ![]u8 {
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
    const h = try alloc.alloc(u8, encoder.calcSize(header.len));
    defer alloc.free(h);
    _ = encoder.encode(h, header);
    const p = try alloc.alloc(u8, encoder.calcSize(payload.len));
    defer alloc.free(p);
    _ = encoder.encode(p, payload);
    const input = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ h, p });
    defer alloc.free(input);
    var mac: [Mac.mac_length]u8 = undefined;
    Mac.create(&mac, input, secret);
    var signature: [43]u8 = undefined;
    _ = encoder.encode(&signature, &mac);
    return std.fmt.allocPrint(alloc, "{s}.{s}", .{ input, signature });
}

test "SQL pgwire native trusted-only renewal preserves owners without native bootstrap or repeated token decoding" {
    const a = std.testing.allocator;
    var server = http.ApiHttpServer.init(a, .{ .trusted_principal_secret = "renewal-key", .trusted_principal_issuer = "cloudaf" }, .{ .ptr = undefined, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    try std.testing.expectError(error.PgwireRequiresBackendRuntime, Listener.start(&server, .{ .enabled = true }));
    var adapter: Adapter = .{ .server = &server };
    try std.testing.expectError(error.Unauthorized, Adapter.authenticate(&adapter, a, "admin", "password"));
    const payload = "{\"iss\":\"cloudaf\",\"sub\":\"key-1\",\"tables\":[\"docs\"],\"operations\":[\"read\"],\"row_filter\":{\"docs\":{\"term\":{\"tenant\":\"t1\"}}},\"exp\":4102444800}";
    const token = try testTrustedToken(a, "renewal-key", payload);
    defer a.free(token);
    const authenticated = try Adapter.authenticate(&adapter, a, trusted_principal_username, token);
    defer authenticated.release(authenticated.context, a);
    try std.testing.expect(authenticated.renewable);
    const credential: *Credential = @ptrCast(@alignCast(authenticated.context));
    // The API allocator would fail any new JWT decode. Cached identity reads
    // need only the caller's allocator, so this still succeeds.
    var no_decode = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    server.alloc = no_decode.allocator();
    {
        defer server.alloc = a;
        var snapshot = try credential.identity(a);
        defer snapshot.deinit(a);
        try std.testing.expect(http.permissionsAllow(snapshot.permissions, .table, "docs", .read));
        try std.testing.expectEqual(@as(usize, 1), snapshot.row_filter.len);
        try std.testing.expect(!no_decode.has_induced_failure);
    }
    credential.trusted_expires = 0;
    try std.testing.expectError(error.Unauthorized, credential.validate());
    try Adapter.refresh(&adapter, a, authenticated, token);
    try credential.validate();
    try std.testing.expectEqualStrings("trusted:cloudaf:key-1", credential.sessionPrincipal());
    const other = try testTrustedToken(a, "renewal-key", "{\"iss\":\"cloudaf\",\"sub\":\"key-2\",\"admin\":true,\"exp\":4102444800}");
    defer a.free(other);
    try std.testing.expectError(error.Unauthorized, Adapter.refresh(&adapter, a, authenticated, other));
    try std.testing.expectEqualStrings("trusted:cloudaf:key-1", credential.sessionPrincipal());
    server.cfg.trusted_principal_issuer = "rotated";
    try std.testing.expectError(error.Unauthorized, credential.validate());
}

test "SQL pgwire native trusted credential and identity allocation failures release owned snapshots" {
    const a = std.testing.allocator;
    var server = http.ApiHttpServer.init(a, .{ .trusted_principal_secret = "allocation-key", .trusted_principal_issuer = "cloudaf" }, .{ .ptr = undefined, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    const token = try testTrustedToken(a, "allocation-key", "{\"iss\":\"cloudaf\",\"sub\":\"key-1\",\"tables\":[\"docs\"],\"operations\":[\"read\"],\"exp\":4102444800}");
    defer a.free(token);
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator, api: *http.ApiHttpServer, jwt: []const u8) !void {
            const credential = try Credential.authenticateTrusted(alloc, api, null, jwt);
            defer Credential.release(credential, alloc);
            var snapshot = try credential.identity(alloc);
            defer snapshot.deinit(alloc);
            var adapter: Adapter = .{ .server = api };
            try Adapter.refresh(&adapter, alloc, .{ .context = credential, .release = Credential.release, .renewable = true }, jwt);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{ &server, token });
}

test "system catalog SQL pgwire retained policies survive repeated qualified projection" {
    const a = std.testing.allocator;
    var permission = try usermgr.Permission.initOwned(a, .table, "docs", .read);
    defer permission.deinit(a);
    var restrictions = [_]usermgr.RowFilterEntry{
        try usermgr.RowFilterEntry.initOwned(a, "*", "{\"term\":{\"tenant\":\"a\"}}"),
        try usermgr.RowFilterEntry.initOwned(a, "docs", "{\"term\":{\"status\":\"open\"}}"),
    };
    defer for (&restrictions) |*entry| entry.deinit(a);
    var permissions = [_]usermgr.Permission{permission};
    var admitted = (try http.cloneCatalogIdentity(a, .{ .username = @constCast("reader"), .permissions = &permissions, .row_filter = &restrictions })).?;
    defer admitted.deinit(a);
    try http.projectCatalogIdentity(a, &admitted, "docs", "table:immutable");
    try http.projectCatalogIdentity(a, &admitted, "\x00catalog:7:default6:public4:docs", "table:immutable");
    const filter = (try http.resolveEffectiveRowFilterJson(a, admitted, "table:immutable")).?;
    defer a.free(filter);
    const policies = [_]OwnedRead.Policy{.{ .table = "table:immutable", .filter = filter }};
    var fresh = (try http.cloneCatalogIdentity(a, .{ .username = @constCast("reader"), .permissions = &permissions, .row_filter = &restrictions })).?;
    defer fresh.deinit(a);
    try OwnedRead.validatePolicies(a, &fresh, admitted, &policies);
    try OwnedRead.validatePolicies(a, &fresh, admitted, &policies);
    var changed = try http.cloneCatalogIdentity(a, .{ .username = @constCast("reader"), .permissions = &permissions, .row_filter = restrictions[0..1] });
    defer changed.?.deinit(a);
    try std.testing.expectError(error.Forbidden, OwnedRead.validatePolicies(a, &changed.?, admitted, &policies));
}

test "system catalog SQL pgwire retained policies keep namespace scopes isolated" {
    const a = std.testing.allocator;
    var permission = try usermgr.Permission.initOwned(a, .table, "*", .read);
    defer permission.deinit(a);
    var permissions = [_]usermgr.Permission{permission};
    const tenant_table = "\x00catalog:6:tenant7:allowed6:events";
    var filters = [_]usermgr.RowFilterEntry{
        try usermgr.RowFilterEntry.initOwned(a, "\x00catalog:7:default6:public*", "{\"term\":{\"visibility\":\"public\"}}"),
        try usermgr.RowFilterEntry.initOwned(a, tenant_table, "{\"term\":{\"status\":\"open\"}}"),
    };
    defer for (&filters) |*entry| entry.deinit(a);
    var admitted = (try http.cloneCatalogIdentity(a, .{ .username = @constCast("reader"), .permissions = &permissions, .row_filter = &filters })).?;
    defer admitted.deinit(a);
    try http.projectCatalogIdentity(a, &admitted, "docs", "table:default");
    try http.projectCatalogIdentity(a, &admitted, tenant_table, "table:tenant");
    const retained = [_]OwnedRead.Policy{.{ .table = "table:tenant", .filter = filters[1].filter }};
    var fresh = (try http.cloneCatalogIdentity(a, .{ .username = @constCast("reader"), .permissions = &permissions, .row_filter = &filters })).?;
    defer fresh.deinit(a);
    try OwnedRead.validatePolicies(a, &fresh, admitted, &retained);
    try OwnedRead.validatePolicies(a, &fresh, admitted, &retained);
}

test "SQL pgwire credential identity rejects username reuse with an identical password verifier" {
    const a = std.testing.allocator;
    const casbin = @import("antfly_casbin");
    var store = usermgr.MemoryStore.init(a);
    defer store.deinit();
    var policies = casbin.MemoryAdapter.init(a);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(a, store.iface(), try usermgr.initDefaultEnforcer(a, policies.iface()));
    defer manager.deinit();
    var read = try usermgr.Permission.initOwned(a, .table, "docs", .read);
    defer read.deinit(a);
    var original = try manager.createUser("alice", "secret", &.{read});
    defer original.deinit(a);
    const credential = try Credential.authenticate(a, &manager, "alice", "secret");
    defer Credential.release(credential, a);
    var identity = try credential.identity(a);
    defer identity.deinit(a);
    var projected = (try http.cloneCatalogIdentity(a, identity)).?;
    defer projected.deinit(a);
    try http.projectCatalogIdentity(a, &projected, "docs", "private-table-17");
    try std.testing.expectEqual(original.instance_id, projected.user_instance_id.?);
    try manager.deleteUser("alice");
    var replacement = try manager.createUser("alice", "different-secret", &.{read});
    defer replacement.deinit(a);
    try std.testing.expect(!std.mem.eql(u8, &original.instance_id, &replacement.instance_id));
    // Model an operator reinstalling the exact persisted verifier. The old
    // MAC then matches, so only the immutable incarnation can fence reuse.
    const hash = try a.dupe(u8, original.password_hash);
    const current = manager.users.getPtr("alice").?;
    a.free(current.*);
    current.* = hash;
    try std.testing.expectEqual(credential.verifier_mac, try manager.destinationGrantMac(credential.principal, credential_domain));
    try std.testing.expectError(error.Unauthorized, credential.validate());
    try std.testing.expectError(error.Unauthorized, credential.identity(a));
    try std.testing.expect(!try http.tablePermissionCurrentlyAllowed(projected, "private-table-17", .read));
    try std.testing.expect(!try http.resourcePermissionsCurrentlyAllowed(a, projected, &.{.{ .resource_type = .table, .resource = "private-table-17", .permission_type = .read }}));
    const renewed = try Credential.authenticate(a, &manager, "alice", "secret");
    defer Credential.release(renewed, a);
    var renewed_identity = try renewed.identity(a);
    defer renewed_identity.deinit(a);
    try std.testing.expectEqual(replacement.instance_id, renewed_identity.user_instance_id.?);
    try std.testing.expect(try http.tablePermissionCurrentlyAllowed(renewed_identity, "docs", .read));
}

test "SQL pgwire native replication fences publication changes and live credential policy" {
    const a = std.testing.allocator;
    const feeds = ReplicationPublication.feed;
    var store = usermgr.MemoryStore.init(a);
    defer store.deinit();
    var policies = @import("antfly_casbin").MemoryAdapter.init(a);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(a, store.iface(), try usermgr.initDefaultEnforcer(a, policies.iface()));
    defer manager.deinit();
    var permission = try usermgr.Permission.initOwned(a, .table, "docs", .replicate);
    defer permission.deinit(a);
    var user = try manager.createUser("replicator", "secret", &.{permission});
    defer user.deinit(a);
    const credential = try Credential.authenticate(a, &manager, "replicator", "secret");
    defer Credential.release(credential, a);
    var admitted = try credential.identity(a);
    defer admitted.deinit(a);
    const owner = try @import("changefeed_public.zig").durableOwner(admitted);
    const member_table: feeds.Table = .{ .table_id = 77, .binding = @splat(5), .policy_revision = 1, .contract = .native_relational };
    var members = [_]ReplicationPublication.Member{.{
        .table = member_table,
        .permission_name = "docs",
        .namespace = "public",
        .name = "docs",
        .oid = 42,
        .identity = .default,
        .key_columns = &.{"id"},
    }};
    const Resolver = struct {
        projection: ReplicationPublication,
        fn resolve(raw: *anyopaque, _: std.mem.Allocator, db: feeds.Digest, publications: []const []const u8, _: operation.RequestContext) !ReplicationPublication {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.projection.database, db);
            try std.testing.expectEqualStrings("docs_pub", publications[0]);
            return self.projection;
        }
    };
    var resolver: Resolver = .{ .projection = .{ .database = @splat(3), .publications = &.{"docs_pub"}, .members = &members } };
    const frozen = try resolver.projection.binding();
    const state: feeds.State = .{
        .phase = .streaming,
        .definition = .{ .owner = owner, .authorization = @splat(6), .tables = &.{member_table}, .projection = .rows },
        .slot = .{ .database = @splat(3), .name = "docs_slot", .acknowledged_cursor = @splat(7), .mapped_cursor = @splat(7), .binding = frozen },
    };
    var policy: LiveReplicationPublication = .{ .allocator = a, .io = std.testing.io, .identity = .{ .context = credential, .read = Adapter.replicationIdentity }, .resolver = .{ .ptr = &resolver, .resolve = Resolver.resolve }, .expected = frozen };
    try policy.authorize(state, frozen, .{});
    var upload_policy: LiveReplicationPublication.Upload = .{ .owner = &policy };
    const schema =
        \\{"version":7,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"extra":{"type":"integer"},"id":{"type":"integer"}},"required":["extra","id"],"additionalProperties":false}}}}
    ;
    var namespace: feeds.Namespace = @splat(0);
    std.mem.writeInt(u64, namespace[0..8], member_table.table_id, .big);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const relation = try upload_policy.relation(arena.allocator(), state, .{
        .namespace = namespace,
        .sequence = 10,
        .transaction_id = @splat(8),
        .schema_after = schema,
    }, .{});
    try std.testing.expectEqual(@as(u32, 42), relation.oid);
    try std.testing.expectEqualSlices(u16, &.{1}, relation.key_ordinals);
    members[0].oid = 43;
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, policy.authorize(state, frozen, .{}));
    members[0].oid = 42;
    try manager.setRowFilter("replicator", "docs", "{\"term\":{\"id\":1}}");
    try std.testing.expectError(error.Forbidden, upload_policy.authorize(state, .{}));
}

test "SQL pgwire native publication DDL uses catalog identity and revision CAS" {
    const a = std.testing.allocator;
    const domain = @import("antfly_local_sources").system_catalog_domain;
    const Fixture = struct {
        record: ?publication_metadata.Record = null,
        writes: usize = 0,
        slot_reads: usize = 0,
        slot_state: publication_control.State = .{},
        scratch: std.mem.Allocator = undefined,
        database_id: u64 = 1,

        fn identify(_: *anyopaque, slots: CatalogReplicationSlots, _: std.mem.Allocator, _: operation.RequestContext) !CatalogReplicationFactory.System {
            try std.testing.expectEqualStrings("default", slots.database_name);
            return .{ .id = 719, .timeline = 1, .position = @fromBackingInt(1) };
        }

        fn createSlot(_: *anyopaque, _: CatalogReplicationSlots, _: std.mem.Allocator, command: wire.replication_commands.Create, _: wire.Request, _: operation.RequestContext) !wire.Result {
            try std.testing.expectEqualStrings("docs_slot", command.slot);
            return error.TestExpectedBootstrapCallback;
        }

        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return error.TestUnexpectedResult;
        }

        fn catalogCall(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            if (call == .policy_publication_status) return alloc.dupe(u8, "null");
            const self: *@This() = @ptrCast(@alignCast(raw));
            const input = call.resolve_many;
            try std.testing.expectEqualStrings("default", input.publication_database.?);
            const schema =
                \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer","nullable":false}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"pk","columns":["id"],"primary":true,"timing":"immediate"}]}
            ;
            const table: domain.ResolvedTable = .{ .table_id = 0x1234_0000_0047, .name = "physical", .query_definition = .{ .table_id = 0x1234_0000_0047, .schema_json = schema, .read_schema_json = "", .indexes_json = "{}" } };
            const has_members = input.targets.len != 0;
            if (has_members) try std.testing.expectEqualStrings("docs", input.targets[0].table);
            const cut: domain.ResolvedMany = .{
                .revision = 9,
                .tables = if (has_members) &.{table} else &.{},
                .publication_cut = .{ .metadata_group_id = 3, .metadata_incarnation = @splat('1'), .database_id = self.database_id, .bindings = if (has_members) &.{@as([32]u8, @splat(2))} else &.{} },
            };
            return std.json.Stringify.valueAlloc(alloc, cut, .{});
        }

        fn readSlot(raw: *anyopaque, alloc: std.mem.Allocator, id: publication_control.Id, context: operation.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqualStrings("changefeed-maintenance", context.principal.?.subject);
            try std.testing.expectEqual(publication_control.pg_slot.feedId(self.record.?.database, "docs_slot"), id);
            self.slot_reads += 1;
            return publication_control.encode(alloc, self.slot_state);
        }

        fn mutateSlot(raw: *anyopaque, _: std.mem.Allocator, write: publication_control.Write, context: operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqualStrings("changefeed-maintenance", context.principal.?.subject);
            const bytes = try publication_control.encodeWrite(self.scratch, write);
            const persisted = try std.json.parseFromSliceLeaky(publication_control.Write, self.scratch, bytes, .{ .allocate = .alloc_always });
            self.slot_state = try persisted.apply(self.scratch, self.slot_state, self.slot_state.authority_sequence + 1);
        }

        fn slotAuthority(_: *anyopaque, _: std.mem.Allocator, _: operation.RequestContext) !?publication_control.Stamp {
            return error.TestUnexpectedResult;
        }

        fn read(raw: *anyopaque, alloc: std.mem.Allocator, database: publication_control.Digest, name: []const u8, context: operation.RequestContext) !?std.json.Parsed(publication_metadata.Record) {
            try publication_metadata.requireAuthority(context);
            const self: *@This() = @ptrCast(@alignCast(raw));
            const record = self.record orelse return null;
            try std.testing.expectEqual(record.database, database);
            try std.testing.expectEqualStrings(record.name, name);
            const bytes = try std.json.Stringify.valueAlloc(alloc, record, .{});
            defer alloc.free(bytes);
            return try publication_metadata.parseRecord(alloc, bytes);
        }

        fn mutate(raw: *anyopaque, _: std.mem.Allocator, write: publication_metadata.Write, context: operation.RequestContext) !void {
            try publication_metadata.requireAuthority(context);
            const self: *@This() = @ptrCast(@alignCast(raw));
            try write.validate();
            try std.testing.expect(write.applies(self.record));
            try std.testing.expectEqual(@as(?u64, 9), write.expected_catalog_revision);
            self.record = write.next;
            self.writes += 1;
        }
    };
    var store = usermgr.MemoryStore.init(a);
    defer store.deinit();
    var policies = @import("antfly_casbin").MemoryAdapter.init(a);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(a, store.iface(), try usermgr.initDefaultEnforcer(a, policies.iface()));
    defer manager.deinit();
    var permission = try usermgr.Permission.initOwned(a, .table, "docs", .replicate);
    defer permission.deinit(a);
    var user = try manager.createUser("replicator", "secret", &.{permission});
    defer user.deinit(a);
    const credential = try Credential.authenticate(a, &manager, "replicator", "secret");
    defer Credential.release(credential, a);
    var identity: ?http.AuthenticatedIdentity = try credential.identity(a);
    defer identity.?.deinit(a);
    var fixture: Fixture = .{};
    const server = try a.create(http.ApiHttpServer);
    defer a.destroy(server);
    server.alloc = a;
    server.source = .{
        .ptr = &fixture,
        .vtable = &.{ .status = Fixture.status, .system_catalog = Fixture.catalogCall },
        .changefeeds = .{ .ptr = &fixture, .read_fn = Fixture.readSlot, .mutate_fn = Fixture.mutateSlot, .authority_fn = Fixture.slotAuthority },
        .pg_publications = .{ .ptr = &fixture, .read = Fixture.read, .mutate = Fixture.mutate },
    };
    const cancelled: std.atomic.Value(bool) = .init(false);
    var authority: Authority = .{ .credential = credential, .identity = &identity, .request = .{
        .statement = "",
        .limit = 1,
        .io = std.testing.io,
        .deadline = std.Io.Clock.Timestamp.now(std.testing.io, .awake).addDuration(.{ .raw = .fromSeconds(5), .clock = .awake }),
        .cancel_requested = &cancelled,
    } };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const alloc = scratch.allocator();
    const create: PublicationCommand = .{ .action = .create, .name = "feed", .tables = &.{.{ .schema = null, .name = "docs" }} };
    const result = try executePublication(server, alloc, &authority, create);
    try std.testing.expectEqualStrings("CREATE PUBLICATION", result.command_tag);
    try std.testing.expectEqualStrings("id", fixture.record.?.members.?[0].key_columns[0]);
    try std.testing.expectError(error.PgPublicationAlreadyExists, executePublication(server, alloc, &authority, create));
    var resolver: CatalogReplicationPublication = .{
        .server = server,
        .database_name = "default",
        .identity = .{ .context = credential, .read = Adapter.replicationIdentity },
    };
    const database = fixture.record.?.database;
    const live = resolver.resolver();
    const before = try live.resolve(live.ptr, alloc, database, &.{"feed"}, .{});
    try std.testing.expectEqualStrings("docs", before.members[0].name);
    const replaced = try executePublication(server, alloc, &authority, .{ .action = .replace_members, .name = create.name, .tables = create.tables });
    try std.testing.expectEqualStrings("ALTER PUBLICATION", replaced.command_tag);
    const after = try live.resolve(live.ptr, alloc, database, &.{"feed"}, .{});
    try std.testing.expectEqual(@as(u32, 16384), before.members[0].oid);
    try std.testing.expectEqual(before.members[0].oid, after.members[0].oid);
    try std.testing.expect(!std.meta.eql((try before.binding()).identity(), (try after.binding()).identity()));
    const slots = try CatalogReplicationSlots.open(server, resolver.identity, alloc, "default", .{});
    try std.testing.expectEqual(database, slots.database);
    try std.testing.expectError(error.InvalidReplicationSlotName, slots.readOwned(alloc, "INVALID", .{}));
    try std.testing.expectError(error.ReplicationSlotNotFound, slots.dropWrite(alloc, "docs_slot", 1, .{}));
    try std.testing.expectError(error.ReplicationSlotNotFound, slots.drop(alloc, .{ .drop_slot = .{ .slot = "docs_slot", .wait = true } }, authority.request, .{}));
    const configuration: publication_control.pg_slot.Configuration = .{ .database = database, .name = "docs_slot" };
    const id = publication_control.pg_slot.feedId(database, configuration.name);
    const definition: publication_control.Definition = .{
        .owner = fixture.record.?.owner,
        .authorization = @splat(2),
        .projection = .rows,
        .tables = &.{before.members[0].table},
    };
    var namespace: publication_control.Namespace = @splat(1);
    std.mem.writeInt(u64, namespace[0..8], before.members[0].table.table_id, .big);
    const incarnation: publication_control.Digest = @splat(9);
    const now: u64 = @intCast(@divFloor(std.Io.Clock.now(.real, std.testing.io).nanoseconds, std.time.ns_per_s));
    var state = try (publication_control.Mutation{ .create = .{ .id = id, .definition = definition, .now = now, .expires_at = now + 600 } }).apply(alloc, .{}, .{ .incarnation = incarnation, .sequence = 1 });
    state = try (publication_control.Mutation{ .plan_roots = &.{.{ .namespace = namespace, .ownership_epoch = 1 }} }).apply(alloc, state, .{ .incarnation = incarnation, .sequence = 2 });
    state = try (publication_control.Mutation{ .enroll = .{ .namespace = namespace, .ownership_epoch = 1, .grant_sequence = state.roots[0].grant_sequence, .consumer_epoch = 1, .start = 10 } }).apply(alloc, state, .{ .incarnation = incarnation, .sequence = 3 });
    fixture.slot_state = try (publication_control.Mutation{ .seal = .{ .cut = @splat(7), .slot = configuration } }).apply(alloc, state, .{ .incarnation = incarnation, .sequence = 4 });
    fixture.scratch = alloc;
    fixture.slot_state.definition.?.owner = @splat(8);
    try std.testing.expectError(error.Forbidden, slots.readOwned(alloc, "docs_slot", .{}));
    fixture.slot_state.definition.?.owner = definition.owner;
    {
        const prior = fixture.slot_state;
        defer fixture.slot_state = prior;
        var snapshot_definition = definition;
        snapshot_definition.start = .snapshot_then_changes;
        snapshot_definition.tables = &.{after.members[0].table};
        snapshot_definition.authorization = (try after.binding()).identity();
        var bootstrap = try (publication_control.Mutation{ .create = .{ .id = id, .definition = snapshot_definition, .now = now, .expires_at = now + 600, .bootstrap_expires_at = now + 300 } }).apply(alloc, .{}, .{ .incarnation = incarnation, .sequence = 1 });
        bootstrap = try (publication_control.Mutation{ .plan_roots = &.{.{ .namespace = namespace, .ownership_epoch = 1 }} }).apply(alloc, bootstrap, .{ .incarnation = incarnation, .sequence = 2 });
        bootstrap = try (publication_control.Mutation{ .enroll = .{ .namespace = namespace, .ownership_epoch = 1, .grant_sequence = 2, .consumer_epoch = 1, .start = 10, .snapshot = .{ .id = bootstrap.consumerId(), .digest = @splat(4), .expires_at = now + 300 } } }).apply(alloc, bootstrap, .{ .incarnation = incarnation, .sequence = 3 });
        var pinned = configuration;
        pinned.binding = try after.binding();
        fixture.slot_state = bootstrap;
        const writes = fixture.writes;
        defer fixture.writes = writes;
        const exported = try SlotCommands.create(server.source.changefeeds.?, alloc, .{ .slot = pinned.name, .snapshot = .export_snapshot }, .{
            .id = id,
            .authority = incarnation,
            .owner = snapshot_definition.owner,
            .authorization = snapshot_definition.authorization,
            .expected_epoch = bootstrap.epoch,
            .expected_revision = bootstrap.revision,
            .mutation = .{ .seal = .{ .cut = @splat(7), .slot = pinned } },
        }, now, CatalogReplicationSlots.maintenanceContext(.{}));
        try std.testing.expectEqualStrings("CREATE_REPLICATION_SLOT", exported.command_tag);
        try std.testing.expectEqualStrings("0/1", exported.rows[0][1].string);
        const token = try publication_control.SlotSnapshot.parse(exported.rows[0][2].string);
        try token.verify(fixture.slot_state, database, snapshot_definition.owner, now);
        var qualified = try slots.qualifySnapshot(alloc, token, .{});
        defer qualified.deinit();
        try std.testing.expectEqual(@as(u64, 10), qualified.value.roots[0].start);
        try std.testing.expectError(error.NoActiveSqlTransaction, slots.qualifyTransactionSnapshot(alloc, .{ .context = credential, .release = Credential.release }, authority.request, &token.name()));
        var txn: @import("transactions.zig").SessionRegistry.SqlState = .{ .metadata = .{ .database = "default", .namespace = "public", .isolation = .repeatable_read, .mode = .read_only }, .owner_node_id = 1, .execution_started = false, .terminal = null, .savepoints = 0 };
        try CatalogReplicationSlots.validateSnapshotTransaction(txn, "default", "public");
        try std.testing.expectError(error.SqlTransactionNotActive, CatalogReplicationSlots.validateSnapshotTransaction(txn, "other", "public"));
        txn.metadata.mode = .read_write;
        try CatalogReplicationSlots.validateSnapshotTransaction(txn, "default", "public");
        txn.metadata.isolation = .read_committed;
        try std.testing.expectError(error.UnsupportedSqlExecution, CatalogReplicationSlots.validateSnapshotTransaction(txn, "default", "public"));
        txn.metadata.isolation = .repeatable_read;
        txn.execution_started = true;
        try std.testing.expectError(error.ActiveSqlTransaction, CatalogReplicationSlots.validateSnapshotTransaction(txn, "default", "public"));

        try std.testing.expectError(error.InvalidReplicationSnapshot, slots.readExportedSnapshot(alloc, "invalid", namespace, 0, 1, 4096, .{}));
        try std.testing.expectError(error.ChangefeedCapabilityUnsupported, slots.readExportedSnapshotRows(alloc, &token.name(), namespace, 0, 1, 4096, .{}));
        const sql_table: catalog.Table = .{ .id = after.members[0].table.table_id, .physical_name = "physical", .schema_version = 1, .columns = &.{} };
        const transaction: [16]u8 = @splat(9);
        credential.imported_snapshot = .{ .transaction = transaction, .token = token, .namespace = namespace };
        defer credential.imported_snapshot = null;
        var wrong_request = authority.request;
        var bound_authority: Authority = .{ .credential = credential, .identity = &identity, .request = wrong_request };
        try std.testing.expect(bound_authority.importedSnapshot() == null);
        const session_hex = std.fmt.bytesToHex(transaction, .lower);
        wrong_request.session_id = &session_hex;
        bound_authority.request = wrong_request;
        try std.testing.expectEqualDeep(token, bound_authority.importedSnapshot().?.token);
        try std.testing.expect(!Authority.allowsWrite(&bound_authority, "physical"));
        try std.testing.expectError(error.HistoryCheckpointConflict, slots.readExportedSnapshotCatalogPage(alloc, &token.name(), namespace, sql_table, "bad", 1, 4096, .{}));
        try std.testing.expectError(error.ChangefeedCapabilityUnsupported, slots.readExportedSnapshotCatalogPage(alloc, &token.name(), namespace, sql_table, null, 1, 4096, .{}));
        try std.testing.expectError(error.ChangefeedConflict, SlotCommands.startExported(server.source.changefeeds.?, alloc, fixture.slot_state, 2, now, CatalogReplicationSlots.maintenanceContext(.{})));
        try SlotCommands.startExported(server.source.changefeeds.?, alloc, fixture.slot_state, 1, now, CatalogReplicationSlots.maintenanceContext(.{}));
        try std.testing.expectEqual(publication_control.Phase.streaming, fixture.slot_state.phase);
        try std.testing.expectEqual(@as(u64, 1), fixture.slot_state.slot.?.confirmed_lsn);
        try std.testing.expect(fixture.slot_state.roots[0].snapshot_acknowledged.done);
    }
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/pgwire-retained-spool", .{tmp.sub_path});
        var artifacts = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, path);
        defer artifacts.deinit();
        const spool_store = try artifacts.spoolArtifactStore(.{});
        const registry: CatalogRetainedStream.Registry = .{ .store = spool_store, .io = std.testing.io };
        const start: wire.replication_commands.Start = .{ .slot = "docs_slot", .position = @fromBackingInt(0), .publications = &.{"feed"} };
        try std.testing.expectError(error.PgSpoolStorageUnsupported, CatalogRetainedStream.open(slots, registry, .{}, a, start, authority.request));
        var factory: CatalogReplicationFactory = .{
            .server = server,
            .registry = registry,
            .stream_config = .{ .require_shared = false },
            .bootstrap = .{ .ptr = &fixture, .identify = Fixture.identify, .create = Fixture.createSlot },
        };
        server.cfg = .{ .pgwire_replication = factory.factory() };
        var adapter: Adapter = .{ .server = server };
        const backend = adapter.backend();
        const session = try backend.vtable.open_replication_session.?(backend.context, a, .{ .context = credential, .release = Credential.release }, authority.request);
        defer session.close(session.context);
        try session.authorize(session.context, a, authority.request, null);
        try std.testing.expectError(error.ChangefeedCapabilityUnsupported, CatalogSlotEnrollment.create(slots, alloc, &.{"feed"}, .{ .slot = "docs_slot", .snapshot = .nothing }, authority.request, .{}));
        const system = try session.command(session.context, alloc, authority.request, .identify_system);
        try std.testing.expectEqualStrings("719", system.rows[0][0].string);
        try std.testing.expectEqualStrings("0/1", system.rows[0][2].string);
        try std.testing.expectError(error.TestExpectedBootstrapCallback, session.command(session.context, alloc, authority.request, .{ .create_slot = .{ .slot = "docs_slot", .snapshot = .nothing } }));
        const stream = try session.open_stream.?(session.context, a, authority.request, start);
        defer stream.close(stream.context);
        try std.testing.expectEqual(@as(u64, 1), @backingInt(stream.initial_lsn));
        try std.testing.expect(fixture.slot_state.slot.?.session != null);
        var page = try stream.poll(stream.context, a, authority.request, stream.initial_lsn, 1024);
        defer page.deinit();
        try std.testing.expect(page.chunk == null);
        server.cfg = .{
            .deployment_mode = .standalone,
            .native_history_roots = .{ .catalog = undefined, .controls = undefined, .pages = undefined, .grants = undefined },
            .document_artifact_store = spool_store,
        };
        var native_factory = try CatalogReplicationFactory.configured(server, std.testing.io, "feed");
        adapter.native_replication = native_factory.factory();
        const configured_backend = adapter.backend();
        const native_session = try configured_backend.vtable.open_replication_session.?(configured_backend.context, a, .{ .context = credential, .release = Credential.release }, authority.request);
        defer native_session.close(native_session.context);
        const native_system = try native_session.command(native_session.context, alloc, authority.request, .identify_system);
        try std.testing.expectEqual(slots.system_identifier, try std.fmt.parseInt(u64, native_system.rows[0][0].string, 10));
        try std.testing.expectEqualStrings("0/0", native_system.rows[0][2].string);
        server.cfg.deployment_mode = .distributed;
        try std.testing.expectError(error.PgSpoolStorageUnsupported, CatalogReplicationFactory.configured(server, std.testing.io, "feed"));
    }
    try std.testing.expect(fixture.slot_state.slot.?.session == null);
    _ = try executePublication(server, alloc, &authority, .{ .action = .drop, .name = "feed" });
    try std.testing.expect(fixture.record.?.members == null);
    _ = try executePublication(server, alloc, &authority, .{ .action = .drop, .name = "feed", .if_exists = true });
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, live.resolve(live.ptr, alloc, database, &.{"feed"}, .{}));
    const dropped = try slots.drop(alloc, .{ .drop_slot = .{ .slot = "docs_slot", .wait = false } }, authority.request, .{});
    try std.testing.expectEqualStrings("DROP_REPLICATION_SLOT", dropped.command_tag);
    try std.testing.expectEqual(publication_control.Phase.closing, fixture.slot_state.phase);
    fixture.slot_state = try (publication_control.Mutation{ .release_root = .{
        .namespace = namespace,
        .ownership_epoch = 1,
        .proof = @splat(10),
    } }).apply(alloc, fixture.slot_state, .{ .incarnation = incarnation, .sequence = fixture.slot_state.authority_sequence + 1 });
    const metadata = server.source.changefeeds.?;
    const maintenance = CatalogReplicationSlots.maintenanceContext(.{});
    try @import("changefeed_public.zig").maintainNativeFeed(metadata, undefined, std.testing.io, alloc, id, maintenance);
    try std.testing.expectEqual(publication_control.Phase.deleted, fixture.slot_state.phase);
    fixture.database_id = 99;
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, slots.readOwned(alloc, "docs_slot", .{}));
    try std.testing.expect(fixture.slot_reads > 4);
    try std.testing.expectEqual(@as(usize, 3), fixture.writes);
}
