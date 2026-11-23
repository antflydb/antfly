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
const wire = @import("../../../pgwire/backend.zig");
const wire_values = @import("../../../pgwire/values.zig");
const storage_schema = @import("antfly_local_sources").storage_schema;
const http = @import("../../http_server.zig");
const execution = @import("../execution/execution.zig");
const compiler = @import("antfly_local_sources").sql_compiler;
const describe_sql = @import("antfly_local_sources").sql_describe;
const native = @import("antfly_local_sources").sql_runtime;
const catalog = @import("antfly_local_sources").sql_catalog;
// Called only after principal/session ownership checks. The raw begin time
// belongs to durable metadata, never to the request's SQL parameter envelope.
fn cursorStatementClock(source: catalog.Clock.Source, statement: catalog.Clock.Instant, transaction_start_ns: ?u64) !catalog.Clock.View {
    const view: catalog.Clock.View = .{
        .source = source,
        .statement = statement,
        .transaction = if (transaction_start_ns) |ns| try catalog.Clock.Instant.fromUnixMicroseconds(@divFloor(@as(i128, ns), 1000)) else statement,
    };
    try view.validate();
    return view;
}

test "SQL statement clock pgwire portals retain owner time without resampling on pages" {
    const Fake = struct {
        calls: usize = 0,
        fn sample(raw: ?*anyopaque) !i128 {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            return if (self.calls == 1) 20_000_000_123 else -1;
        }
    };
    var fake: Fake = .{};
    const source: catalog.Clock.Source = .{ .callback = .{ .ptr = &fake, .now = Fake.sample } };
    const statement = try source.sample();
    const explicit = try cursorStatementClock(source, statement, 10_000_000_999);
    const implicit = try cursorStatementClock(source, statement, null);
    for (0..1000) |_| {
        const page = explicit;
        try std.testing.expectEqual(@as(?i128, 10_000_000_000), (try page.value(.transaction, 6)).unixNanoseconds());
        try std.testing.expectEqual(@as(?i128, 20_000_000_000), (try page.value(.statement, 6)).unixNanoseconds());
    }
    try std.testing.expectEqual(statement.micros, implicit.transaction.micros);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(?i128, -1000), (try explicit.value(.wall, 6)).unixNanoseconds());
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectError(error.InvalidTransactionClock, cursorStatementClock(source, try catalog.Clock.Instant.init(std.math.maxInt(i64)), null));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
}

const ast = @import("antfly_local_sources").sql_ast;
const operation = @import("antfly_local_sources").api_operation;
const usermgr = @import("../../../usermgr/mod.zig");
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
    network: std.Io.Threaded,
    server: @import("../../../pgwire/server.zig").Server,

    pub fn startWithSelinux(api: *http.ApiHttpServer, config: @import("antfly_local_sources").common_config.Config.PgwireConfig, endpoint: ?@import("../../../pgwire/server.zig").SelinuxEndpoint) !*Listener {
        if (api.cfg.user_manager == null and (api.cfg.trusted_principal_secret == null or api.cfg.trusted_principal_issuer == null)) return error.PgwireRequiresAuthentication;
        if (api.cfg.backend_runtime == null) return error.PgwireRequiresBackendRuntime;
        const self = try api.owner_alloc.create(Listener);
        errdefer api.owner_alloc.destroy(self);
        self.alloc = api.owner_alloc;
        // Listener, connection, session, and watchdog tasks are long-lived.
        // CopyBoth adds an input reader, poll timer, and publication worker.
        // Give those tasks separate capacity from API and durable storage I/O.
        const workers = 1 + 6 * (@as(u32, config.max_connections) + 4);
        self.network = .init(self.alloc, .{
            .async_limit = .nothing,
            .concurrent_limit = .limited(@min(workers, @import("antfly_runtime_fs").threaded_io_limits.service)),
        });
        errdefer self.network.deinit();
        const io = self.network.io();
        self.adapter = .{ .server = api };
        self.replication = null;
        if (config.replication_publication) |name| {
            self.replication = try CatalogReplicationFactory.configured(api, io, name);
            self.adapter.native_replication = self.replication.?.factory();
        }
        self.server = try @import("../../../pgwire/server.zig").start(self.alloc, .{
            .io = io,
            .backend = self.adapter.backend(),
            .bind_host = config.bind_host orelse "127.0.0.1",
            .bind_port = config.bind_port,
            .selinux_endpoint = endpoint,
            .max_connections = config.max_connections,
            .allow_insecure_non_loopback = config.externally_protected_transport,
        });
        return self;
    }

    pub fn deinit(self: *Listener) void {
        self.server.deinit();
        self.network.deinit();
        self.alloc.destroy(self);
    }
};

const resolveOnPath = @import("../execution/lookup_path.zig").resolveOnPath;
const resolveReadOnPath = @import("../execution/lookup_path.zig").resolveReadOnPath;

test "SQL pgwire ordered native resolver falls back only on exact table absence" {
    // sql-0038 / sql-0040: SET SESSION and SET LOCAL share this ordered,
    // authorization-fenced native binding path after pgwire scope selection.
    const search_path = @import("../../../pgwire/search_path.zig");
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
        const control = @import("../../../metadata/changefeed_control.zig");
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
    const feed = @import("../../../metadata/changefeed_control.zig");
    const slot = feed.pg_slot;
    const relation_values = @import("../../../pgwire/values.zig");

    const publication = @import("../../../metadata/pg_publication.zig");
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
        const owner = try @import("../../changefeed_public.zig").durableOwner(identity);
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
            const record = (try port.readRecord(a, database, name, maintenance)) orelse return error.ChangefeedResynchronizationRequired;
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
            var check = (try port.readRecord(a, database, name, maintenance)) orelse return error.ChangefeedResynchronizationRequired;
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
        _ = try @import("../../changefeed_public.zig").durableOwner(authenticated);
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
        const owner = try @import("../../changefeed_public.zig").durableOwner(identity);
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
        const id = @import("../../distributed_txn.zig").parseTxnIdHex(encoded) catch return error.SqlTransactionNotActive;
        if (try self.server.txn_sessions.principalAccess(a, id, credential.sessionPrincipal()) != .allowed)
            return error.SqlTransactionNotActive;
        var transaction = (try self.server.txn_sessions.getSqlState(a, id)) orelse return error.SqlTransactionNotActive;
        defer transaction.deinit(a);
        try validateSnapshotTransaction(transaction, self.database_name, request.session_namespace orelse request.namespace orelse "public");
        if (!std.mem.eql(u8, self.database_name, request.database orelse "default")) return error.SqlTransactionNotActive;
        const snapshot = try control.SlotSnapshot.parse(name);
        var qualified = try self.qualifySnapshot(a, snapshot, try @import("../../../pgwire/retained_stream.zig").metadataContext(request));
        defer qualified.deinit();
        try request.check();
        return .{ .transaction = id, .token = snapshot };
    }

    fn validateSnapshotTransaction(transaction: @import("../../transactions.zig").SessionRegistry.SqlState, database: []const u8, namespace: []const u8) !void {
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
    pub fn readExportedSnapshot(self: CatalogReplicationSlots, a: std.mem.Allocator, name: []const u8, namespace: control.Namespace, after: u32, rows: u32, bytes: u32, context: operation.RequestContext) !@import("../../history_page_operations.zig").pages.BootstrapPage {
        const snapshot = try control.SlotSnapshot.parse(name);
        var observed = try self.qualifySnapshot(a, snapshot, context);
        defer observed.deinit();
        return self.readQualifiedSnapshot(a, snapshot, observed.value, namespace, after, rows, bytes, context);
    }

    fn readQualifiedSnapshot(self: CatalogReplicationSlots, a: std.mem.Allocator, snapshot: control.SlotSnapshot, state: control.State, namespace: control.Namespace, after: u32, rows: u32, bytes: u32, context: operation.RequestContext) !@import("../../history_page_operations.zig").pages.BootstrapPage {
        const source = self.server.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
        const port = source.bootstrap_pages orelse return error.ChangefeedCapabilityUnsupported;
        const root = state.roots[try state.rootIndex(namespace)];
        const pin = root.snapshot.?;
        const maintenance = maintenanceContext(context);
        var captured = try source.captureAdmission(a, state.definition.?, maintenance);
        defer captured.deinit();
        try captured.validateInitialRoutes(state.roots);
        const selected = try captured.find(namespace);
        var page = try port.read(a, selected.fence, selected.table_name, .{
            .version = 1,
            .request = .{
                .reference = .{ .namespace = root.storageNamespace(), .authority = state.authority, .consumer = state.consumerId(), .epoch = root.consumer_epoch, .grant = root.grant_sequence, .definition = state.definition.?.identity() },
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

    fn normalizeSnapshotSchema(page: *@import("../../history_page_operations.zig").pages.BootstrapPage) !void {
        const sources = @import("antfly_local_sources");
        const a = page.arena.allocator();
        const parsed = try sources.schema_mod.parseValidatedTableSchema(a, page.schema);
        const schema = try sources.schema_mod.deriveRuntimeTableSchema(a, parsed);
        page.schema = try sources.storage_schema.serializeSchema(a, schema);
    }

    pub fn readExportedSnapshotRows(self: CatalogReplicationSlots, a: std.mem.Allocator, name: []const u8, namespace: control.Namespace, after: u32, rows: u32, bytes: u32, context: operation.RequestContext) !@import("../../../pgwire/values.zig").HistoricalSnapshot {
        var raw = try self.readExportedSnapshot(a, name, namespace, after, rows, bytes, context);
        defer raw.deinit();
        try normalizeSnapshotSchema(&raw);
        return @import("../../../pgwire/values.zig").HistoricalSnapshot.decode(a, raw);
    }

    pub fn readExportedSnapshotCatalogPage(self: CatalogReplicationSlots, a: std.mem.Allocator, name: []const u8, table: catalog.Table, after: ?[]const u8, rows: u32, bytes: u32, context: operation.RequestContext) !catalog.Page {
        var root_index: usize = 0;
        var offset: u32 = 0;
        if (after) |cursor| {
            const separator = std.mem.indexOfScalar(u8, cursor, '/') orelse return error.HistoryCheckpointConflict;
            root_index = std.fmt.parseUnsigned(usize, cursor[0..separator], 10) catch return error.HistoryCheckpointConflict;
            offset = std.fmt.parseUnsigned(u32, cursor[separator + 1 ..], 10) catch return error.HistoryCheckpointConflict;
        }
        const snapshot = try control.SlotSnapshot.parse(name);
        var observed = try self.qualifySnapshot(a, snapshot, context);
        defer observed.deinit();
        const state = observed.value;
        const first = for (state.roots, 0..) |root, index| {
            if (std.mem.readInt(u64, root.namespace[0..8], .big) == table.id) break index;
        } else return error.ChangefeedResynchronizationRequired;
        var end = first;
        while (end < state.roots.len and std.mem.readInt(u64, state.roots[end].namespace[0..8], .big) == table.id) : (end += 1) {}
        if (after == null) root_index = first;
        if (root_index < first or root_index >= end) return error.HistoryCheckpointConflict;
        while (root_index < end) : (root_index += 1) {
            var raw = try self.readQualifiedSnapshot(a, snapshot, state, state.roots[root_index].namespace, offset, rows, bytes, context);
            defer raw.deinit();
            try normalizeSnapshotSchema(&raw);
            var page = try @import("../../../pgwire/values.zig").historicalCatalogPage(a, raw, table);
            errdefer page.deinit();
            const page_allocator = page.owned_arena.?.allocator();
            if (page.after) |next| {
                page.after = try std.fmt.allocPrint(page_allocator, "{d}/{s}", .{ root_index, next });
                return page;
            }
            if (page.rows.len != 0 or root_index + 1 == end) {
                if (root_index + 1 < end) page.after = try std.fmt.allocPrint(page_allocator, "{d}/0", .{root_index + 1});
                return page;
            }
            page.deinit();
            offset = 0;
        }
        unreachable;
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
    const feed = @import("../../../metadata/changefeed_control.zig");
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
    const Bridge = @import("../../../pgwire/retained_stream.zig");
    const retained = @import("../../../metadata/pg_spool_stream.zig");
    const Registry = @import("../../../metadata/pg_spool_artifacts.zig").Registry;
    const history_lease_seconds = 3600;

    fn publicationRetryable(err: anyerror) bool {
        return switch (err) {
            error.ChangefeedConflict,
            error.ChangefeedBackpressure,
            error.MetadataMutationOutcomeUnknown,
            error.ReadUnavailable,
            error.ReadIndexTimeout,
            error.GroupLeaderUnavailable,
            error.LeaderUnavailable,
            error.NotLeader,
            error.StorageReadTemporarilyUnavailable,
            error.CatalogRoutingUnavailable,
            error.CatalogRoutingSnapshotTimeout,
            error.MetadataSnapshotHeadMismatch,
            error.TopologyChanged,
            error.ConnectionRefused,
            error.ConnectionResetByPeer,
            error.NetworkUnreachable,
            error.Timeout,
            error.SqlWriteCapacityUnavailable,
            error.DeadlineExceeded,
            error.Canceled,
            error.Cancelled,
            => true,
            else => false,
        };
    }
    const control = publication_control;

    allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    service: *const @import("../../changefeed_metadata.zig").Port,
    registry: Registry,
    id: control.Id,
    actor: control.Id,
    config: retained.Config,
    catalog_resolver: CatalogReplicationPublication,
    policy: LiveReplicationPublication,

    publication_worker: ?*PublicationWorker = null,
    collected_history_delivery: ?u64 = null,
    verified_history_expiry: u64 = 0,
    publication_group: std.Io.Group = .init,

    const PublicationWorker = struct {
        const upload = @import("../../../metadata/pg_spool_upload.zig");
        const producer = @import("../../../metadata/pg_retained_transaction.zig");
        const allocator = std.heap.smp_allocator;

        host: *CatalogRetainedStream,
        arena: std.heap.ArenaAllocator,
        credential: Credential,
        session: control.pg_slot.Session,
        source: @import("../../history_roots.zig").Source,
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

            pub fn artifactStore(self: CleanupRegistry, scope: control.pg_slot.spool_control.Scope, context: operation.RequestContext) !@import("../../../serverless/artifacts/store.zig").ArtifactStore {
                return self.storage.artifactStore(scope, context);
            }

            pub fn authorizeCleanup(self: CleanupRegistry, state: control.State, context: operation.RequestContext) !void {
                try self.policy.authorize(state, self.policy.expected, context);
                const slot = state.slot orelse return error.ChangefeedResynchronizationRequired;
                try slot.verifySession(self.session, try self.policy.nowSeconds(context));
            }
        };

        const HistoryPolicy = struct {
            source: @import("../../history_roots.zig").Source,
            selected: CleanupRegistry,
            io: std.Io,

            pub fn authorizeRoot(self: *HistoryPolicy, state: control.State, _: control.Namespace, context_value: operation.RequestContext) !void {
                try self.selected.authorizeCleanup(state, context_value);
            }

            pub fn validateRoot(self: *HistoryPolicy, state: control.State, namespace: control.Namespace, fence: @import("../../../metadata/api.zig").CatalogRouteFence, table: []const u8, context_value: operation.RequestContext) !void {
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
            var phase: enum { read_state, authorize, capture, stage_split, stage_merge, renew_feed, progress_merge, progress_split, renew_roots, reclaim_roots } = .read_state;
            errdefer std.log.debug("logical slot history maintenance failed phase={s}", .{@tagName(phase)});
            const host = self.host;
            var client: @import("../../changefeed_metadata.zig").Client = .{ .port = host.service.* };
            const bytes = try client.readChangefeedState(allocator, host.id, self.context);
            defer allocator.free(bytes);
            var observed = try control.parse(allocator, bytes);
            defer observed.deinit();
            phase = .authorize;
            try selected.authorizeCleanup(observed.value, self.context);
            const feed_api = @import("../../changefeed_public.zig");
            phase = .capture;
            var captured = try self.source.capturePhysical(allocator, observed.value.definition.?, self.context);
            defer captured.deinit();
            phase = .stage_split;
            try feed_api.stageVisibleSplits(&client, allocator, self.source, &captured, &observed, observed.value.acknowledged_cursor, self.context);
            phase = .stage_merge;
            try feed_api.stageVisibleMerges(&client, allocator, self.source, &captured, &observed, observed.value.acknowledged_cursor, self.context);
            const state = observed.value;
            const now = try selected.policy.nowSeconds(self.context);
            if (state.expires_at -| now <= history_lease_seconds / 2) {
                phase = .renew_feed;
                const definition = state.definition orelse return error.ChangefeedResynchronizationRequired;
                try client.mutateChangefeedState(allocator, .{
                    .id = state.id,
                    .authority = state.authority,
                    .owner = definition.owner,
                    .authorization = definition.authorization,
                    .expected_revision = state.revision,
                    .expected_epoch = state.epoch,
                    .mutation = .{ .renew = .{ .now = now, .expires_at = now +| history_lease_seconds } },
                }, self.context);
                // The next round observes the committed revision and drives
                // any native renewals, including after an ambiguous reply.
                return;
            }
            const renew = host.verified_history_expiry != state.expires_at;
            const reclaim = state.acknowledged_delivery != 0 and host.collected_history_delivery != state.acknowledged_delivery;
            phase = .progress_merge;
            try feed_api.progressMergeDrain(&client, allocator, self.source, &captured, state, state.acknowledged_cursor, self.context);
            phase = .progress_split;
            try feed_api.progressSplitSuccessor(&client, allocator, self.source, &captured, state, state.acknowledged_cursor, self.context);
            if (!renew and !reclaim) return;
            var history_policy: HistoryPolicy = .{ .source = self.source, .selected = selected, .io = selected.policy.io };
            phase = .renew_roots;
            if (renew) {
                for (state.roots) |root| {
                    if (!root.released) try @import("../../history_roots.zig").renewRoot(&client, &history_policy, allocator, state, try captured.findInstalled(root), self.context);
                }
                host.verified_history_expiry = state.expires_at;
            }
            phase = .reclaim_roots;
            if (!reclaim) return;
            for (state.roots) |root| {
                if (root.released or root.acknowledged.sequence == root.start) continue;
                const route = try captured.findInstalled(root);
                const more = try @import("../../history_roots.zig").flushAcknowledged(&client, &history_policy, allocator, state, route, self.context);
                if (more) return;
            }
            host.collected_history_delivery = state.acknowledged_delivery;
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
            // An unconfirmed native collection keeps history pinned. The
            // publisher independently rechecks the slot and can still admit
            // new work within its retained-history and spool budgets.
            self.reclaimHistory(cleanup_registry) catch |err| {
                // A moving route vector must be qualified before preparing a
                // new spool against it. Already published spools still drain.
                if (err == error.TopologyChanged) return;
                if (!publicationRetryable(err)) return err;
            };
            const cleanup = @import("../../../metadata/pg_spool_cleanup.zig").cleanup(host.service, cleanup_registry, allocator, host.id, .{}, self.context) catch |err| {
                std.log.debug("logical slot spool cleanup failed err={s}", .{@errorName(err)});
                return err;
            };
            switch (cleanup.status) {
                .checkpointed, .collected => return,
                .idle, .waiting, .retained => {},
            }
            var selected: LiveReplicationPublication.Upload = .{ .owner = &policy };
            var transaction: producer.Producer(LiveReplicationPublication.Upload) = .{
                .policy = &selected,
                .allocator = allocator,
                .source = self.source,
                .byte_limit = @import("../../history_page_operations.zig").pages.max_source_bytes,
            };
            defer transaction.deinit();
            _ = upload.drive(host.service, host.registry, &transaction, allocator, host.id, self.session, .{ .require_shared = host.config.require_shared }, self.context) catch |err| {
                if (err == error.HistoryTooLarge or err == error.PgoutputMessageTooLarge) {
                    const bytes = try host.service.read(allocator, host.id, self.context);
                    defer allocator.free(bytes);
                    var parsed = try control.parse(allocator, bytes);
                    defer parsed.deinit();
                    const state = parsed.value;
                    try cleanup_registry.authorizeCleanup(state, self.context);
                    const definition = state.definition orelse return error.ChangefeedResynchronizationRequired;
                    try host.service.mutate(allocator, .{
                        .id = state.id,
                        .authority = state.authority,
                        .owner = definition.owner,
                        .authorization = definition.authorization,
                        .expected_revision = state.revision,
                        .expected_epoch = state.epoch,
                        .mutation = .{ .reset = .quota },
                    }, self.context);
                }
                std.log.debug("logical slot spool publication failed err={s}", .{@errorName(err)});
                return err;
            };
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
            if (failure) |err| {
                if (!publicationRetryable(err)) {
                    std.log.warn("logical slot publication failed: {s}", .{@errorName(err)});
                    return err;
                }
            }
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
        self.catalog_resolver = .{ .server = slots.server, .database_name = try owned.dupe(u8, slots.database_name), .identity = slots.identity };
        const id, const expected = binding: {
            var state = try slots.readOwned(retained_alloc, start.slot, context);
            defer state.deinit();
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
            break :binding .{ state.value.id, expected };
        };
        var actor: control.Id = undefined;
        while (true) {
            try request_value.check();
            request_value.io.random(&actor);
            if (!std.mem.allEqual(u8, &actor, 0)) break;
        }
        self.allocator = retained_alloc;
        self.service = service;
        self.registry = registry;
        self.id = id;
        self.actor = actor;
        self.config = config;
        self.publication_worker = null;
        self.collected_history_delivery = null;
        self.verified_history_expiry = 0;
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
    const control = @import("../../../metadata/changefeed_control.zig");
    const Metadata = @import("../../changefeed_metadata.zig").Port;

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
        return Boundary.callRemote("open", self.boundary_dispatch, self.open_fn, .{ self.context, alloc, identity, request });
    }
};

/// Enroll the complete retained root set for a resolved publication.
pub const CatalogSlotEnrollment = struct {
    const control = publication_control;
    const roots = @import("../../history_roots.zig");
    const metadata = @import("../../changefeed_metadata.zig");

    const Policy = struct {
        slots: CatalogReplicationSlots,
        source: roots.Source,
        services: *CatalogReplicationPublication,
        expected: control.pg_slot.Binding,
        owner: control.Digest,
        allocator: std.mem.Allocator,
        io: std.Io,
        retirement_collected: bool = false,

        pub fn deferRetirementSettlement(self: *const Policy) bool {
            return !self.retirement_collected;
        }

        pub fn authorizeRoot(self: *Policy, state: control.State, _: control.Namespace, context_value: operation.RequestContext) !void {
            var scratch = std.heap.ArenaAllocator.init(self.allocator);
            defer scratch.deinit();
            const a = scratch.allocator();
            var identity = try self.slots.identity.read(self.slots.identity.context, a);
            defer identity.deinit(a);
            const owner = try @import("../../changefeed_public.zig").durableOwner(identity);
            if (!std.crypto.timing_safe.eql(control.Digest, owner, self.owner) or
                !std.crypto.timing_safe.eql(control.Digest, owner, state.definition.?.owner))
                return error.Forbidden;
            const current = try self.services.resolve(a, self.slots.database, self.expected.publications, context_value);
            if (!std.meta.eql((try current.binding()).identity(), self.expected.identity())) return error.ChangefeedResynchronizationRequired;
        }

        pub fn validateRoot(self: *Policy, state: control.State, namespace: control.Namespace, fence: @import("../../../metadata/api.zig").CatalogRouteFence, table: []const u8, context_value: operation.RequestContext) !void {
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

    fn singleRootCut(state: control.State, root: roots.Root, authority: control.Digest) control.Digest {
        const enrolled = state.roots[0];
        const continuation = control.hash(.{ .domain = "antfly:pg-slot-enrollment:v1", .authority = authority, .definition = state.definition.?.identity(), .root = root.namespace, .consumer_epoch = enrolled.consumer_epoch, .start = enrolled.start });
        if (enrolled.snapshot) |pin| return control.hash(.{ .domain = "antfly:pg-slot-snapshot-enrollment:v1", .continuation = continuation, .pin = pin });
        return continuation;
    }

    pub fn create(slots: CatalogReplicationSlots, a: std.mem.Allocator, names: []const []const u8, command: wire.replication_commands.Create, request: wire.Request, context: operation.RequestContext) !wire.Result {
        if (command.snapshot == .use_snapshot or command.temporary) return error.UnsupportedReplicationOption;
        if (!control.pg_slot.validName(command.slot)) return error.InvalidReplicationSlotName;
        const source = slots.server.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
        const export_snapshot = command.snapshot == .export_snapshot;
        if (export_snapshot and source.bootstrap_pages == null) return error.ChangefeedCapabilityUnsupported;
        const port = slots.server.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
        const maintenance = CatalogReplicationSlots.maintenanceContext(context);
        const working_allocator = request.scratch_allocator orelse a;
        var arena = std.heap.ArenaAllocator.init(working_allocator);
        defer arena.deinit();
        const scratch = arena.allocator();
        var resolver: CatalogReplicationPublication = .{ .server = slots.server, .identity = slots.identity, .database_name = slots.database_name };
        const publication = try resolver.resolve(scratch, slots.database, names, maintenance);
        const binding = try publication.binding();
        var identity = try slots.identity.read(slots.identity.context, scratch);
        defer identity.deinit(scratch);
        const owner = try @import("../../changefeed_public.zig").durableOwner(identity);
        const tables = try scratch.alloc(control.Table, publication.members.len);
        for (publication.members, tables) |member, *table| table.* = member.table;
        const definition: control.Definition = .{
            .owner = owner,
            .authorization = binding.identity(),
            .tables = tables,
            .projection = .rows,
            .start = if (export_snapshot) .snapshot_then_changes else .now,
        };
        var captured = try source.captureAdmission(scratch, definition, maintenance);
        defer captured.deinit();
        if (captured.roots.len == 0 or captured.roots.len > control.max_advances)
            return error.ChangefeedCapabilityUnsupported;
        const root = captured.roots[0];
        const authority = try @import("../../history_policy_provisioning.zig").authority(root.fence);
        if (try port.authority(scratch, maintenance)) |registered| {
            if (!std.meta.eql(registered.incarnation, authority)) return error.ChangefeedResynchronizationRequired;
        }
        const id = control.pg_slot.feedId(slots.database, command.slot);
        var client: metadata.Client = .{ .port = port };
        var policy: Policy = .{ .slots = slots, .source = source, .services = &resolver, .expected = binding, .owner = owner, .allocator = working_allocator, .io = request.io };
        const ns = std.Io.Clock.now(.real, request.io).nanoseconds;
        if (ns <= 0) return error.ChangefeedResynchronizationRequired;
        const now = std.math.cast(u64, @divFloor(ns, std.time.ns_per_s)) orelse return error.ChangefeedResynchronizationRequired;
        // Each root can require separate preparation, dispatch and reconciliation.
        for (0..8 + 8 * captured.roots.len) |_| {
            try request.check();
            var iteration = std.heap.ArenaAllocator.init(working_allocator);
            defer iteration.deinit();
            const work = iteration.allocator();
            const bytes = try port.read(work, id, maintenance);
            var parsed = try control.parse(work, bytes);
            defer parsed.deinit();
            const state = parsed.value;
            if (state.phase == .closing and std.meta.eql(state.definition.?.owner, owner))
                return error.ReplicationSlotActive;
            if (state.phase == .deleted) {
                if (!std.meta.eql(state.authority, authority) or !std.meta.eql(state.definition.?.owner, owner))
                    return error.ChangefeedConflict;
                try policy.authorizeRoot(.{ .definition = definition }, root.namespace, maintenance);
                try port.mutate(work, .{
                    .id = id,
                    .authority = authority,
                    .owner = owner,
                    .authorization = state.definition.?.authorization,
                    .expected_revision = state.revision,
                    .expected_epoch = state.epoch,
                    .mutation = .{ .recreate_slot = .{
                        .definition = definition,
                        .now = now,
                        .expires_at = now +| CatalogRetainedStream.history_lease_seconds,
                        .bootstrap_expires_at = if (export_snapshot) now +| control.max_bootstrap_seconds else 0,
                    } },
                }, maintenance);
                continue;
            }
            if (state.phase != .empty and (!std.meta.eql(state.authority, authority) or
                !std.meta.eql(state.definition.?.identity(), definition.identity()))) return error.ChangefeedConflict;
            const current = if (state.phase == .empty) control.State{ .definition = definition } else state;
            try policy.authorizeRoot(current, root.namespace, maintenance);
            var write: control.Write = .{ .id = id, .authority = authority, .owner = owner, .authorization = definition.authorization, .expected_revision = state.revision, .expected_epoch = state.epoch, .mutation = undefined };
            if (state.phase == .empty) {
                write.mutation = .{ .create = .{ .id = id, .definition = definition, .now = now, .expires_at = now +| CatalogRetainedStream.history_lease_seconds, .bootstrap_expires_at = if (export_snapshot) now +| control.max_bootstrap_seconds else 0 } };
            } else if (state.phase == .reset_required and state.slot == null and
                state.reset_reason == .history_expired and !state.expiryDue(now))
            {
                if (try @import("../../changefeed_public.zig").retireOne(&client, &policy, work, state, maintenance)) continue;
                write.mutation = .{ .restart = .{
                    .definition = definition,
                    .now = now,
                    .expires_at = state.expires_at,
                    .bootstrap_expires_at = state.bootstrap_expires_at,
                } };
            } else if (state.phase == .enrolling and !state.expiryDue(now)) {
                if (state.roots.len == 0) {
                    write.mutation = .{ .plan_roots = try captured.initialPlans(work) };
                } else {
                    try captured.validateInitialRoutes(state.roots);
                    for (state.roots, captured.roots) |planned, route| {
                        if (planned.consumer_epoch != 0) continue;
                        const table = for (tables) |selected| {
                            if (selected.table_id == route.fence.table_id) break selected;
                        } else return error.ChangefeedResynchronizationRequired;
                        roots.enrollRoot(&client, &policy, work, state, route, table.policy_revision, maintenance) catch |err| {
                            if (err != error.MetadataMutationOutcomeUnknown) return err;
                            // The next iteration reloads the durable intent and
                            // reconciles an accepted grant before advancing.
                            try request.io.sleep(.fromMilliseconds(25), .awake);
                        };
                        break;
                    } else {
                        const cut = if (state.roots.len == 1) singleRootCut(state, root, authority) else roots.qualifyEnrollmentCut(work, source, &policy, &captured, state, maintenance) catch |err| switch (err) {
                            error.ChangefeedEnrollmentCutAdvanced => {
                                write.mutation = .{ .reset = .history_expired };
                                try port.mutate(work, write, maintenance);
                                continue;
                            },
                            else => return err,
                        };
                        try policy.validateRoot(state, root.namespace, root.fence, root.table_name, maintenance);
                        write.mutation = .{ .seal = .{ .cut = cut, .slot = .{ .database = slots.database, .name = command.slot, .binding = binding, .consistent_lsn = @max(1, state.slot_lsn_floor) } } };
                        return SlotCommands.create(port, a, command, write, now, maintenance);
                    }
                    continue;
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
            const owner = try @import("../../changefeed_public.zig").durableOwner(authenticated);
            self.* = .{ .host = host, .allocator = a, .slots = slots, .database_name = name, .owner = owner };
            return .{ .context = self, .authorize = authorize, .command = command, .open_stream = openStream, .close = close };
        }

        fn authorize(raw: *anyopaque, a: std.mem.Allocator, request: wire.Request, _: ?pg.Command) !void {
            const self: *Session = @ptrCast(@alignCast(raw));
            try request.check();
            if (!std.mem.eql(u8, self.database_name, request.database orelse "default")) return error.Forbidden;
            var identity = try self.slots.identity.read(self.slots.identity.context, a);
            defer identity.deinit(a);
            const current = try @import("../../changefeed_public.zig").durableOwner(identity);
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
        .open_replication = LogicalPublisher.open,
        .start_connection = startConnection,
        .discard_connection = discardConnection,
        .notifications = notifications,
        .has_notifications = hasNotifications,
        .authenticate = authenticate,
        .refresh = refresh,
        .describe = describe,
        .description_current = descriptionCurrent,
        .same_binding_targets = sameBindingTargets,
        .execute = execute,
        .load_settings = loadSettings,
        .validate_namespace = validateNamespace,
        .evaluate_parameters = evaluateParameters,
        .evaluate_parameter_cells = evaluateParameterCells,
        .fail_transaction = failTransaction,
        .import_snapshot = importSnapshot,
        .set_transaction_options = setTransactionOptions,
        .open_stream = openStream,
        .guarded_select_stream = true,
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

    fn startConnection(raw: *anyopaque, a: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        if (!self.server.source.vtable.supports_sql_command_ownership) return;
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        if (credential.trusted_identity) |admitted| {
            // A federated principal can administer its publication without
            // holding the native identity required for notification enrollment.
            // Native SQL execution still checks that identity at admission.
            if (admitted.live_user_manager == null or admitted.user_instance_id == null) return;
        }
        var job: Job = .{ .adapter = self, .alloc = a, .credential = credential, .request = request, .kind = .notification_connect };
        try self.dispatch(&job);
    }

    fn hasNotifications(_: *anyopaque, identity: wire.Identity) bool {
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        return credential.notification_interest;
    }

    fn notifications(raw: *anyopaque, a: std.mem.Allocator, identity: wire.Identity, request: wire.Request, control: @import("antfly_local_sources").sql_ast.notifications.DeliveryControl) !@import("antfly_local_sources").sql_ast.notifications.Page {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        var job: Job = .{ .adapter = self, .alloc = a, .credential = @ptrCast(@alignCast(identity.context)), .request = request, .kind = .notifications, .notification_control = control };
        try self.dispatch(&job);
        return job.notification_page;
    }

    fn discardConnection(raw: *anyopaque, a: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        var job: Job = .{ .adapter = self, .alloc = a, .credential = @ptrCast(@alignCast(identity.context)), .request = request, .kind = .discard_connection };
        try self.dispatch(&job);
    }

    fn descriptionCurrent(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request, description: wire.Description) !bool {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        try validateRequest(request);
        if (self.server.cfg.user_manager != credential.manager) return error.Unauthorized;
        const stamp = description.stamp orelse return false;
        if (stamp.authority != try credential.descriptionRevision()) return false;
        const previous = stamp.catalog orelse return true;
        var admission = self.server.sql_preparation_admission.tryAcquireLease() orelse return error.SqlWriteCapacityUnavailable;
        defer admission.release();
        var job: Job = .{ .adapter = self, .alloc = alloc, .credential = credential, .request = request, .kind = .probe };
        try self.dispatch(&job);
        const current = job.catalog_stamp orelse return false;
        return std.meta.eql(previous, current);
    }

    fn loadSettings(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !@import("antfly_local_sources").sql_setting_catalog.RawSnapshot {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        var authenticated: ?http.AuthenticatedIdentity = try credential.identity(self.server.alloc);
        defer authenticated.?.deinit(self.server.alloc);
        var authority: Authority = .{ .credential = credential, .identity = &authenticated, .request = request };
        const context = try authority.context();
        var capture_adapter: execution.Adapter = .{ .server = self.server, .identity = &authenticated, .context = context, .database = request.database orelse "default", .namespace = request.namespace orelse "public" };
        const capture = capture_adapter.settingCapture().?;
        if (!std.mem.eql(u8, capture.scope.principal, credential.sessionPrincipal())) return error.Forbidden;
        return capture.owner.load(capture.owner.ptr, alloc, capture.scope);
    }

    fn setTransactionOptions(raw: *anyopaque, identity: wire.Identity, request: wire.Request, options: wire.TransactionOptions) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        const encoded = request.session_id orelse return error.NoActiveSqlTransaction;
        const id = @import("../../distributed_txn.zig").parseTxnIdHex(encoded) catch return error.SqlTransactionNotActive;
        if (options.isolation) |isolation| {
            if (isolation != .read_committed and !execution.supportsRangeGuards(self.server))
                return error.UnsupportedSqlExecution;
        }
        try self.updateTransactionOptions(credential, request, id, options);
    }

    fn updateTransactionOptions(self: *Adapter, credential: *Credential, request: wire.Request, id: [16]u8, options: wire.TransactionOptions) !void {
        try request.check();
        try credential.validate();
        const lease = self.server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
        defer lease.release();
        try self.server.txn_sessions.setSqlTransactionOptions(self.server.alloc, try credential.transactionGuard(id), credential.native_connection, .{
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
        const context = try @import("../../../pgwire/retained_stream.zig").metadataContext(request);
        if (credential.imported_snapshot) |existing| {
            if (request.session_id) |encoded| {
                const id = try @import("../../distributed_txn.zig").parseTxnIdHex(encoded);
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
        const id = try @import("../../distributed_txn.zig").parseTxnIdHex(encoded);
        if (try self.server.txn_sessions.principalAccess(self.server.alloc, id, credential.sessionPrincipal()) != .allowed) return error.SqlTransactionNotActive;
        var state = (try self.server.txn_sessions.getSqlState(self.server.alloc, id)) orelse return error.SqlTransactionNotActive;
        defer state.deinit(self.server.alloc);
        if (state.terminal != null or !std.mem.eql(u8, state.metadata.database, request.database orelse "default") or !std.mem.eql(u8, state.metadata.namespace, request.session_namespace orelse request.namespace orelse "public")) return error.SqlTransactionNotActive;
        if (state.metadata.failed or state.execution_started or state.terminal_abort or state.terminal_prepare) return;
        if (state.connection_id) |connection| {
            var authenticated: ?http.AuthenticatedIdentity = try credential.identity(self.server.alloc);
            defer authenticated.?.deinit(self.server.alloc);
            // Cleanup cannot borrow the canceled/expired SQL request's
            // checkpoint. Principal access above and the exact command loan
            // authorize control; this context cannot execute user row writes.
            const context: operation.RequestContext = .{
                .setting_admin = true,
                .principal = .{ .kind = .user, .subject = authenticated.?.username },
                .destination_authorization_principal = credential.principal,
                .row_policy_credential = &authenticated,
                .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 3 * std.time.ns_per_s,
            };
            const io = self.server.embedding_provider_runtime.io;
            var lease = try self.server.sql_connection_admission.acquire(self.server.alloc, io, .init(connection, credential.sessionPrincipal()));
            defer lease.release(io);
            var command: @import("../session/command_execution.zig").Request = .{};
            defer command.finish() catch {};
            // Parse/Bind can fail before the execution owner takes a command.
            // This fresh loan runs control only; the rejected SQL is not run.
            if (try command.start(self.server, context, authenticated, &lease, .{ .failure_only = true, .transaction_id = id, .statement = request.statement })) {
                _ = try command.failed(authenticated, null);
                return;
            }
        }
        const guard = try credential.transactionGuard(id);
        try self.server.txn_sessions.setSqlFailed(self.server.alloc, guard, true);
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
            if (retained.patterns() != null) {
                retained.extension_kind = .none;
                retained.extension = null;
            }
            value.* = try @import("antfly_local_sources").sql_operators.cloneDatum(alloc, retained);
        }
        try request.check();
        return result;
    }

    fn evaluateScalarParameters(alloc: std.mem.Allocator, request: wire.Request, expressions: []const []const u8) ![]const std.json.Value {
        const cells = try evaluateScalarCells(alloc, request, expressions);
        const result = try alloc.alloc(std.json.Value, cells.len);
        for (cells, request.parameter_types, result) |evaluated, kind, *value| {
            value.* = if (evaluated.range()) |range| range_text: {
                var context: @import("antfly_local_sources").sql_range_value.Context = .{ .alloc = alloc };
                break :range_text .{ .string = try @import("antfly_local_sources").sql_range_value.formatLeaky(&context, range.*) };
            } else if (evaluated.array) |array|
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
        if (!std.mem.eql(u8, current.principal, next.principal) or !std.mem.eql(u8, current.username, next.username) or
            !std.meta.eql(current.user_instance_id, next.user_instance_id) or
            !std.meta.eql(current.trusted_identity.?.trusted_changefeed_owner, next.trusted_identity.?.trusted_changefeed_owner))
            return error.Unauthorized;
        std.mem.swap(?http.AuthenticatedIdentity, &current.trusted_identity, &next.trusted_identity);
        std.mem.swap(?[]u8, &current.trusted_issuer, &next.trusted_issuer);
        current.trusted_revision += 1;
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
        return @import("../session/completion.zig").capableJoined(self.server, self.server.sharedApiNetworkIo() orelse return false);
    }
    fn reserveCompletion(raw: *anyopaque, completion: *@import("../../../pgwire/completion.zig").Completion, desired: usize) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        try @import("../session/completion.zig").reserveJoined(self.server, completion, desired, self.server.sharedApiNetworkIo() orelse return error.SqlWriteCapacityUnavailable);
    }
    fn pinCompletion(raw: *anyopaque, alloc: std.mem.Allocator, completion: *@import("../../../pgwire/completion.zig").Completion) void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const network = self.server.sharedApiNetworkIo() orelse {
            completion.unknown = true;
            return;
        };
        @import("../session/completion.zig").pinJoined(self.server, alloc, completion, network);
    }

    fn completionSafe(_: *anyopaque, identity: wire.Identity) bool {
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        return credential.session_cleanup_complete;
    }

    fn disconnect(raw: *anyopaque, identity: wire.Identity, session_id: ?[]const u8) void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        if (credential.native_connection) |connection| {
            disconnectNative(self.server, credential, connection);
            return;
        }
        const encoded = session_id orelse return;
        credential.session_cleanup_complete = false;
        const id = @import("../../distributed_txn.zig").parseTxnIdHex(encoded) catch return;
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
                const info = job.server.txn_sessions.getInfo(job.id) orelse return;
                if (info.version.owner != job.server.localSessionNodeId()) return;
                job.credential.session_cleanup_complete = job.server.txn_sessions.retireBeforeExecution(job.server.alloc, job.credential.transactionGuard(job.id) catch return, info.version, false) catch false;
            }
        };
        var job = Cleanup{ .server = self.server, .principal = credential.sessionPrincipal(), .id = id, .completion_io = network_io, .credential = credential };
        var future = io.concurrent(Cleanup.run, .{&job}) catch return;
        job.done.waitUncancelable(network_io);
        @import("../../protected_future.zig").wait(io, &future);
    }

    fn disconnectNative(server: *http.ApiHttpServer, credential: *Credential, connection: [32]u8) void {
        credential.session_cleanup_complete = false;
        const io = http.ApiHttpServer.configuredDurableIo(server.cfg) orelse return;
        const network_io = server.sharedApiNetworkIo() orelse return;
        const Cleanup = struct {
            server: *http.ApiHttpServer,
            credential: *Credential,
            connection: [32]u8,
            completion_io: std.Io,
            done: std.Io.Event = .unset,
            fn run(job: *@This()) void {
                defer job.done.set(job.completion_io);
                job.retire() catch {};
            }
            fn retire(job: *@This()) !void {
                const producer = @import("../session/command_producer.zig");
                const store = job.server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
                const principal = job.credential.sessionPrincipal();
                // The authenticated socket already owns this exact connection.
                // Closing must remain possible after credential revocation.
                for (0..16) |_| {
                    var record = (try producer.load(store, job.server.alloc, job.connection)) orelse {
                        try job.closeLogical();
                        return;
                    };
                    defer record.deinit();
                    if (record.value.phase == .retired) {
                        try job.closeLogical();
                        return;
                    }
                    if (!record.value.recovery) {
                        producer.requestRecovery(store, principal, record.value) catch |err| switch (err) {
                            error.SqlConnectionChanged => continue,
                            else => return err,
                        };
                    } else try producer.advanceConnection(job.server, job.server.alloc, job.connection, .none);
                }
                // The source recovery marker is durable. Existing bounded
                // upkeep owns unfinished decisions and release after this cut.
                return error.SqlConnectionBusy;
            }
            fn closeLogical(job: *@This()) !void {
                const store = job.server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
                _ = @import("../session/connections.zig").mutate(store, .{ .close = job.connection }, job.credential.sessionPrincipal(), job.server.localSessionNodeId(), @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms) catch |err| switch (err) {
                    error.SqlConnectionNotFound => 0,
                    else => return err,
                };
                job.credential.native_connection = null;
                job.credential.session_cleanup_complete = true;
            }
        };
        var job: Cleanup = .{ .server = server, .credential = credential, .connection = connection, .completion_io = network_io };
        var future = io.concurrent(Cleanup.run, .{&job}) catch return;
        job.done.waitUncancelable(network_io);
        @import("../../protected_future.zig").wait(io, &future);
    }

    fn dispatch(self: *Adapter, job: *Job) !void {
        if (job.kind == .notifications or job.kind == .notification_connect) try job.request.io.checkCancel() else try validateRequest(job.request);
        if (self.server.cfg.user_manager != job.credential.manager) return error.Unauthorized;
        const io = http.ApiHttpServer.configuredDurableIo(self.server.cfg) orelse return error.SqlWriteCapacityUnavailable;
        var future = io.concurrent(Job.run, .{job}) catch return error.SqlWriteCapacityUnavailable;
        // Keep borrowed request storage alive through a durable decision. The
        // listener sets cancellation/deadline on shutdown, native checkpoints
        // observe it, and this adapter joins rather than abandoning the job.
        job.done.waitUncancelable(job.request.io);
        @import("../../protected_future.zig").wait(io, &future);
        if (job.failure) |err| return err;
    }
};

const LogicalPublisher = struct {
    const frontend = @import("../../../pgwire/logical_replication_frontend.zig");
    const journal = @import("../../../system_catalog/replication/decision_journal.zig");
    const installation = @import("../../../system_catalog/replication/installation.zig");
    const publisher = @import("antfly_local_sources").system_catalog_logical_replication_publisher;

    alloc: std.mem.Allocator,
    adapter: *Adapter,
    credential: *Credential,
    realm: publisher.Realm = undefined,
    connection: [32]u8 = undefined,
    uncertain: bool = false,
    stream: ?Stream = null,

    fn open(raw: *anyopaque, a: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !frontend.Port {
        const owner = try a.create(LogicalPublisher);
        errdefer a.destroy(owner);
        owner.* = .{ .alloc = a, .adapter = @ptrCast(@alignCast(raw)), .credential = @ptrCast(@alignCast(identity.context)) };
        var work: Work = .{ .owner = owner, .alloc = a, .request = request };
        try work.dispatch();
        return .{ .ptr = owner, .vtable = &.{ .command_boundary = commandBoundary, .command = command, .poll = poll, .feedback = feedback, .stop = stop, .heartbeat = heartbeat, .close = close } };
    }

    fn read(self: *LogicalPublisher, a: std.mem.Allocator, context: operation.RequestContext, comptime T: type, query: installation.SlotRead) !T {
        const bytes = try self.adapter.server.source.systemCatalog(a, context, .{ .sql_publication_capture_work = .{ .slot = query } });
        defer a.free(bytes);
        return std.json.parseFromSliceLeaky(T, a, bytes, .{ .allocate = .alloc_always });
    }

    fn issue(self: *LogicalPublisher, a: std.mem.Allocator, context: operation.RequestContext, head: u64, action: publisher.Control, issued: ?*journal.ControlRequest) !journal.ControlResult {
        var nonce: [16]u8 = undefined;
        try self.adapter.server.embedding_provider_runtime.io.randomSecure(&nonce);
        const control: journal.ControlRequest = .{ .realm = self.realm, .nonce = nonce, .observed_head = head, .action = action };
        if (issued) |out| out.* = control;
        const bytes = self.adapter.server.source.systemCatalog(a, context, .{ .sql_publication_slot_control = control }) catch |err| {
            if (err == error.MetadataMutationOutcomeUnknown) self.uncertain = true;
            return err;
        };
        defer a.free(bytes);
        const result = std.json.parseFromSliceLeaky(journal.ControlResult, a, bytes, .{ .allocate = .alloc_always }) catch {
            self.uncertain = true;
            return error.MetadataMutationOutcomeUnknown;
        };
        if (result == .refused) return switch (result.refused) {
            .InvalidLogicalPublisher => error.InvalidLogicalPublisher,
            .InvalidReplicationAcknowledgement => error.InvalidReplicationAcknowledgement,
            .ReplicationSlotActive => error.ReplicationSlotActive,
            .ReplicationSlotAlreadyExists => error.ReplicationSlotAlreadyExists,
            .ReplicationSlotNotFound => error.ReplicationSlotNotFound,
            .ReplicationSlotLeaseChanged => error.ReplicationSlotLeaseChanged,
            .ReplicationSlotCapacityExceeded => error.ReplicationSlotCapacityExceeded,
            .PublicationNotFound => error.PublicationNotFound,
            .SqlSnapshotCaptureChanged => error.SqlSnapshotCaptureChanged,
            .InvalidReplicationCutoverIntent => error.InvalidReplicationCutoverIntent,
            .ReplicationCutoverAuthorityLost => error.ReplicationCutoverAuthorityLost,
            .LogicalReplicationLifecyclePending => error.LogicalReplicationLifecyclePending,
            .ForeignProviderIdentityMismatch => error.ForeignProviderIdentityMismatch,
            .UnsupportedOperation => error.UnsupportedOperation,
        };
        return result;
    }

    fn command(raw: *anyopaque, a: std.mem.Allocator, request: wire.Request, input: frontend.commands.Command) !frontend.Reply {
        var work: Work = .{ .owner = @ptrCast(@alignCast(raw)), .alloc = a, .request = request, .input = input };
        try work.dispatch();
        return work.reply;
    }

    fn poll(raw: *anyopaque, a: std.mem.Allocator, request: wire.Request, writer: *std.Io.Writer) !bool {
        var work: Work = .{ .owner = @ptrCast(@alignCast(raw)), .alloc = a, .request = request, .stream_action = .{ .poll = writer } };
        try work.dispatch();
        return work.backlog;
    }

    fn feedback(raw: *anyopaque, a: std.mem.Allocator, request: wire.Request, bytes: []const u8, writer: *std.Io.Writer) !void {
        var work: Work = .{ .owner = @ptrCast(@alignCast(raw)), .alloc = a, .request = request, .stream_action = .{ .feedback = .{ .bytes = bytes, .writer = writer } } };
        try work.dispatch();
    }

    fn stop(raw: *anyopaque, a: std.mem.Allocator, request: wire.Request) !void {
        var work: Work = .{ .owner = @ptrCast(@alignCast(raw)), .alloc = a, .request = request, .stream_action = .stop };
        try work.dispatch();
    }

    fn heartbeat(_: *anyopaque, _: std.mem.Allocator, _: wire.Request) !void {}

    fn close(raw: *anyopaque) bool {
        const owner: *LogicalPublisher = @ptrCast(@alignCast(raw));
        const safe = !owner.uncertain and owner.stream == null;
        commandBoundary(raw);
        if (owner.stream) |*stream| stream.arena.deinit();
        owner.alloc.destroy(owner);
        return safe;
    }

    const delivery = @import("../../../pgwire/logical_replication_delivery.zig");
    const lease_ns = 30 * std.time.ns_per_s;
    const Stream = struct {
        arena: std.heap.ArenaAllocator,
        sender: delivery.Sender,
        names: []const []const u8,
        origin: @import("antfly_local_sources").system_catalog_logical_replication.Origin,
        expiry: u64,
        confirmed: u64,
        applied: u64,
        last_keepalive: u64 = 0,
    };
    const StreamAction = union(enum) {
        poll: *std.Io.Writer,
        feedback: struct { bytes: []const u8, writer: *std.Io.Writer },
        stop,
    };

    fn runStream(self: *LogicalPublisher, a: std.mem.Allocator, context: operation.RequestContext, head: u64, action: StreamAction) !bool {
        const stream = if (self.stream) |*stream| stream else return error.InvalidLogicalPublisher;
        const lease = stream.sender.lease;
        if (action == .stop) {
            const result = try self.issue(a, context, head, .{ .release = lease }, null);
            if (result != .none) return error.InvalidLogicalPublisher;
            stream.arena.deinit();
            self.stream = null;
            return false;
        }
        const now = @import("antfly_platform").time.realtimeNs();
        if (now >= stream.expiry) return error.ReplicationSlotLeaseChanged;
        if (action == .feedback) {
            const input = action.feedback;
            const ack = try stream.sender.feedback(input.bytes);
            if (ack.flushed != 0 and (ack.flushed != stream.confirmed or ack.applied != stream.applied)) {
                const result = try self.issue(a, context, head, .{ .acknowledge = .{
                    .lease = lease,
                    .received = ack.received,
                    .flushed = ack.flushed,
                    .applied = ack.applied,
                    .observed_at_unix_ns = now,
                } }, null);
                if (result != .slot) return error.InvalidLogicalPublisher;
                stream.confirmed = result.slot.confirmed_flush_lsn;
                stream.applied = result.slot.applied_lsn;
            }
            if (ack.reply_requested) {
                try delivery.keepalive(input.writer, stream.sender.delivered, now / 1000, false);
                try input.writer.flush();
            }
            return false;
        }
        if (stream.expiry - now <= lease_ns / 2) {
            const renewed = try self.issue(a, context, head, .{ .acquire = .{
                .database_id = self.realm.database_id,
                .slot = lease.slot,
                .connection = lease.connection,
                .nonce = lease.nonce,
                .observed_at_unix_ns = now,
                .expires_at_unix_ns = now + lease_ns,
            } }, null);
            if (renewed != .slot or renewed.slot.lease == null or !renewed.slot.lease.?.eql(lease))
                return error.ReplicationSlotLeaseChanged;
            stream.expiry = renewed.slot.lease_expires_at_unix_ns;
        }
        const ready = try self.read(a, context, journal.FeedReadiness, .{ .ready = .{ .realm = self.realm, .lease = lease, .after = stream.sender.delivered } });
        const writer = action.poll;
        if (!ready.available) {
            if (now -| stream.last_keepalive >= 10 * std.time.ns_per_s) {
                try delivery.keepalive(writer, stream.sender.delivered, now / 1000, true);
                try writer.flush();
                stream.last_keepalive = now;
            }
            return false;
        }
        var control: journal.ControlRequest = undefined;
        const offered = try self.issue(a, context, ready.control_head, .{ .feed = .{
            .lease = lease,
            .observed_at_unix_ns = @import("antfly_platform").time.realtimeNs(),
            .after = stream.sender.delivered,
            .publication_names = stream.names,
            .origin = stream.origin,
        } }, &control);
        if (offered != .offered) return error.InvalidLogicalPublisher;
        const page = try self.read(a, context, publisher.FeedPage, .{ .page = control });
        try stream.sender.deliver(a, self.adapter.server, writer, page, stream.origin, now / 1000, context);
        return page.entries.len != 0;
    }

    fn commandBoundary(raw: *anyopaque) void {
        const owner: *LogicalPublisher = @ptrCast(@alignCast(raw));
        const sessions = &owner.adapter.server.txn_sessions;
        sessions.snapshot_exports.retire(owner.connection);
        sessions.snapshot_retirement.drain();
    }

    const Work = struct {
        owner: *LogicalPublisher,
        alloc: std.mem.Allocator,
        request: wire.Request,
        input: ?frontend.commands.Command = null,
        stream_action: ?StreamAction = null,
        backlog: bool = false,
        reply: frontend.Reply = undefined,
        failure: ?anyerror = null,
        done: std.Io.Event = .unset,

        fn dispatch(self: *Work) !void {
            try validateRequest(self.request);
            const io = http.ApiHttpServer.configuredDurableIo(self.owner.adapter.server.cfg) orelse return error.SqlWriteCapacityUnavailable;
            var future = io.concurrent(run, .{self}) catch return error.SqlWriteCapacityUnavailable;
            self.done.waitUncancelable(self.request.io);
            @import("../../protected_future.zig").wait(io, &future);
            if (self.failure) |err| return err;
        }

        fn run(self: *Work) void {
            defer self.done.set(self.request.io);
            self.runInner() catch |err| {
                self.failure = err;
            };
        }

        fn runInner(self: *Work) !void {
            const owner = self.owner;
            const server = owner.adapter.server;
            if (owner.uncertain) return error.MetadataMutationOutcomeUnknown;
            if (server.cfg.user_manager != owner.credential.manager) return error.Unauthorized;
            var identity: ?http.AuthenticatedIdentity = try owner.credential.identity(server.alloc);
            defer identity.?.deinit(server.alloc);
            const database = self.request.database orelse "default";
            // Complete captured rows require current native admin authority.
            if (identity.?.row_filter.len != 0 or
                !try http.resourcePermissionCurrentlyAllowed(identity, .database, database, .admin) or
                !try http.resourcePermissionCurrentlyAllowed(identity, .table, "*", .read) or
                !try http.resourcePermissionCurrentlyAllowed(identity, .user, "*", .admin)) return error.Forbidden;
            var authority: Authority = .{ .credential = owner.credential, .identity = &identity, .request = self.request };
            var context = try authority.context();
            context.setting_admin = true;
            const scope = try owner.read(self.alloc, context, installation.PublisherView, .{ .database = database });
            if (self.stream_action) |action| {
                if (!scope.realm.eql(owner.realm)) return error.CatalogGenerationChanged;
                self.backlog = try owner.runStream(self.alloc, context, scope.control_head, action);
                return;
            }
            const input = self.input orelse {
                owner.realm = scope.realm;
                owner.connection = (try owner.credential.ensureNativeConnection(server, identity, self.request)) orelse return error.SqlConnectionUnavailable;
                return;
            };
            if (!scope.realm.eql(owner.realm)) return error.CatalogGenerationChanged;
            self.reply = try owner.runCommand(self.alloc, context, scope.control_head, database, &identity, input);
        }
    };

    fn runCommand(self: *LogicalPublisher, a: std.mem.Allocator, context: operation.RequestContext, head: u64, database: []const u8, identity: *?http.AuthenticatedIdentity, input: frontend.commands.Command) !frontend.Reply {
        const io = self.adapter.server.embedding_provider_runtime.io;
        switch (input) {
            .identify_system => {
                const view = (try self.read(a, context, ?journal.IdentityView, .{ .identity = self.realm })) orelse init: {
                    var candidate: [8]u8 = undefined;
                    try io.randomSecure(&candidate);
                    const result = try self.issue(a, context, head, .{ .initialize = @max(1, std.mem.readInt(u64, &candidate, .big)) }, null);
                    if (result != .identity) return error.InvalidLogicalPublisher;
                    break :init result.identity;
                };
                return .{ .identity = .{ .identity = view.identity, .lsn = view.lsn, .database = database } };
            },
            .create_slot => |value| {
                if (value.snapshot == .use_snapshot or value.temporary) return error.UnsupportedSqlExecution;
                const server = self.adapter.server;
                const exports = &server.txn_sessions.snapshot_exports;
                var expected_cut: ?publisher.SnapshotPoint = null;
                var token: [16]u8 = undefined;
                var snapshot_name: ?[]const u8 = null;
                if (value.snapshot == .export_snapshot) {
                    const point = try self.read(a, context, publisher.SnapshotPoint, .{ .snapshot_point = self.realm });
                    var adapter: execution.Adapter = .{ .server = server, .identity = identity, .context = context, .database = database, .namespace = "public", .revision = point.catalog_revision };
                    const cut = try adapter.captureDatabaseSnapshot(true);
                    defer server.txn_sessions.snapshot_retirement.drain();
                    defer cut.release();
                    try io.randomSecure(&token);
                    snapshot_name = try a.dupe(u8, &@import("../execution/snapshot_exports.zig").format(token));
                    try exports.reserve(server.alloc, self.connection, token, self.realm, cut);
                    expected_cut = point;
                }
                errdefer if (expected_cut != null) {
                    exports.retire(self.connection);
                    server.txn_sessions.snapshot_retirement.drain();
                };
                var nonce: [16]u8 = undefined;
                try io.randomSecure(&nonce);
                const result = try self.issue(a, context, head, .{ .create_slot = .{
                    .database_id = self.realm.database_id,
                    .name = value.slot,
                    .snapshot = .nothing,
                    .connection = self.connection,
                    .nonce = nonce,
                    .expected_cut = expected_cut,
                } }, null);
                if (result != .slot) return error.InvalidLogicalPublisher;
                if (snapshot_name != null) exports.publish(self.connection, token);
                return .{ .created = .{ .slot = result.slot, .consistent_point = result.slot.confirmed_flush_lsn, .snapshot_name = snapshot_name } };
            },
            .drop_slot => |value| {
                const view = try self.read(a, context, journal.SlotView, .{ .lookup = .{ .realm = self.realm, .name = value.slot } });
                const slot = view.slot orelse return error.ReplicationSlotNotFound;
                if (value.wait and slot.lease != null) return error.UnsupportedSqlExecution;
                const result = try self.issue(a, context, view.control_head, .{ .drop_slot = .{ .database_id = self.realm.database_id, .slot = slot.reference } }, null);
                if (result != .none) return error.InvalidLogicalPublisher;
                return .dropped;
            },
            .start => |value| {
                if (self.stream != null) return error.ReplicationSlotActive;
                const view = try self.read(a, context, journal.SlotView, .{ .lookup = .{ .realm = self.realm, .name = value.slot } });
                const slot = view.slot orelse return error.ReplicationSlotNotFound;
                if (value.lsn > slot.confirmed_flush_lsn) return error.InvalidReplicationAcknowledgement;
                var arena = std.heap.ArenaAllocator.init(self.alloc);
                errdefer arena.deinit();
                const names = try frontend.commands.publicationNames(arena.allocator(), value.publication_names);
                if (!try self.read(a, context, bool, .{ .check_publications = .{ .realm = self.realm, .names = names } }))
                    return error.PublicationNotFound;
                const name = try arena.allocator().dupe(u8, slot.reference.name);
                var nonce: [16]u8 = undefined;
                try io.randomSecure(&nonce);
                const now = @import("antfly_platform").time.realtimeNs();
                const result = try self.issue(a, context, view.control_head, .{ .acquire = .{
                    .database_id = self.realm.database_id,
                    .slot = slot.reference,
                    .connection = self.connection,
                    .nonce = nonce,
                    .observed_at_unix_ns = now,
                    .expires_at_unix_ns = now + lease_ns,
                } }, null);
                if (result != .slot) return error.InvalidLogicalPublisher;
                var lease = result.slot.lease orelse return error.InvalidLogicalPublisher;
                lease.slot.name = name;
                self.stream = .{
                    .arena = arena,
                    .sender = try delivery.Sender.init(self.realm, lease, result.slot.confirmed_flush_lsn),
                    .names = names,
                    .origin = value.origin,
                    .expiry = result.slot.lease_expires_at_unix_ns,
                    .confirmed = result.slot.confirmed_flush_lsn,
                    .applied = result.slot.applied_lsn,
                };
                return .started;
            },
        }
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

test "SQL transaction timestamp pgwire pages preserve exact native values and reject relabeling" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const alloc = arena.allocator();
            const sources = @import("antfly_local_sources");
            const temporal = sources.sql_temporal_value;
            for ([_]i64{ -946_684_800_000_001, temporal.end_timestamp - 1, std.math.minInt(i64), std.math.maxInt(i64) }) |micros| {
                const value: temporal.Value = .{ .timestamp_tz = try temporal.Timestamp.init(micros) };
                var values = [_]sources.sql_scalar.Datum{sources.sql_scalar.Datum.typedTemporal(&value)};
                var owner: OwnedRead.PageOwner = .{ .alloc = a, .page = .{
                    .arena = std.heap.ArenaAllocator.init(a),
                    .columns = &.{.{ .name = "t", .type = .datetime }},
                    .values = .{ .rows = &.{&values} },
                    .exhausted = true,
                } };
                defer owner.page.deinit();
                const cell = try OwnedRead.PageOwner.cell(&owner, alloc, 0, 0);
                try std.testing.expect(!cell.sql_null);
                const bytes = try wire_values.encode(alloc, .datetime, 1, cell.value);
                try std.testing.expectEqual(@as(usize, 8), bytes.len);
                try std.testing.expectEqual(micros, std.mem.readInt(i64, bytes[0..8], .big));
                const text = try wire_values.encode(alloc, .datetime, 0, cell.value);
                var expected: [sources.sql_temporal_text.output_capacity]u8 = undefined;
                try std.testing.expectEqualStrings(try sources.sql_temporal_text.format(value, &expected), text);
                values[0].sql_null = true;
                try std.testing.expectError(error.SqlTypeMismatch, OwnedRead.PageOwner.cell(&owner, alloc, 0, 0));
                values[0] = .{ .sql_null = true };
                try std.testing.expect((try OwnedRead.PageOwner.cell(&owner, alloc, 0, 0)).sql_null);
                const wrong: temporal.Value = .{ .timestamp = try temporal.Timestamp.init(micros) };
                values[0] = sources.sql_scalar.Datum.typedTemporal(&wrong);
                try std.testing.expectError(error.InvalidSqlBackendResponse, OwnedRead.PageOwner.cell(&owner, alloc, 0, 0));
            }
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
    native_cut: ?*@import("../execution/retained_snapshot.zig").Shared = null,
    decision_runtime: ?@import("antfly_local_sources").functions_runtime.Runtime = null,
    session_id: ?[]u8 = null,
    staged: @import("../../transactions.zig").OwnedTransactionCommitRequest = .{},
    range_guards: @import("../../transactions.zig").OwnedTransactionCommitRequest = .{},
    guarded: GuardedCatalog,
    plan: @import("../../../sql/plan_cache.zig").Lease,
    admission: http.RequestAdmission.Lease,
    stream: *Pull.Stream,
    columns: []const wire.Column,
    policies: []const Policy,
    view_authorities: []const *const @import("../execution/read_authority.zig").Context = &.{},
    view_bindings: []const @import("../session/prepared.zig").Binding = &.{},
    detached: bool = false,
    retained: ?RetainedRead,
    const Policy = struct {
        resource: []const u8,
        filter: ?[]const u8,
        name: ?ast.Name = null,
        id: u64 = 0,
        schema_version: u32 = 0,
        graph_revision: ?u64 = null,
        authority_table: ?catalog.Table = null,
        sql_authorization: ?@import("../../transactions.zig").TableCommitRequest.SqlAuthorization = null,

        fn capture(a: std.mem.Allocator, identity: ?http.AuthenticatedIdentity, table: catalog.Table) !Policy {
            if (table.graph) |graph| return .{
                .resource = try a.dupe(u8, graph.graph.name),
                .filter = null,
                .id = graph.graph.incarnation,
                .graph_revision = graph.graph.revision,
            };
            return .{
                .resource = try a.dupe(u8, table.physical_name),
                .filter = try http.resolveEffectiveRowFilterJson(a, identity, table.physical_name),
                .id = table.id,
                .schema_version = table.schema_version,
                .name = if (table.scope) |scope| .{ .database = try a.dupe(u8, scope.database), .namespace = try a.dupe(u8, scope.namespace), .table = try a.dupe(u8, scope.name) } else null,
            };
        }
    };

    fn open(adapter: *Adapter, alloc: std.mem.Allocator, credential: *Credential, request: wire.Request) !?wire.ReadStream {
        const server = adapter.server;
        const statement_source: catalog.Clock.Source = .{ .io = server.embedding_provider_runtime.io };
        const statement_instant = try statement_source.sample();
        var transaction_start_ns: ?u64 = null;
        var preparation = server.sql_preparation_admission.tryAcquireLease() orelse return error.SqlWriteCapacityUnavailable;
        defer preparation.release();
        var diagnostic: compiler.Diagnostic = .{};
        var plan = try server.sqlPlanCache().acquire(server.sqlPlanCacheIo(), .{ .statement = request.statement, .principal = credential.principal, .database = request.database orelse "default", .namespace = request.namespace orelse "public" }, &diagnostic);
        var moved = false;
        defer if (!moved) plan.release(server.sqlPlanCacheIo());
        if (plan.compiled().statement != .select and plan.compiled().statement != .copy_to) return null;
        if (@import("../copy/server.zig").target(plan.compiled().statement) != null) return null;
        var admission = try server.acquireSqlExecution(false);
        defer if (!moved) admission.release();
        const self = try alloc.create(OwnedRead);
        errdefer alloc.destroy(self);
        self.alloc = alloc;
        self.session_id = null;
        self.native_cut = null;
        errdefer if (self.native_cut) |cut| cut.release();
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
        const admitted_revision = credential.readRevision();
        self.identity = try credential.identity(server.alloc);
        errdefer self.identity.?.deinit(server.alloc);
        // Statement, parameter and binding-guard data are portal-owned. Keep a
        // private request capsule so I/O borrows point to a stable address.
        self.authority = .{ .server = server, .credential = credential, .identity = &self.identity, .request = request };
        self.authority.request.expected_stream_columns = null;
        self.authority.request.statement = try arena.dupe(u8, request.statement);
        self.authority.request.database = try arena.dupe(u8, request.database orelse "default");
        self.authority.request.namespace = try arena.dupe(u8, request.namespace orelse "public");
        self.authority.request.session_namespace = if (request.session_namespace) |scope| try arena.dupe(u8, scope) else null;
        self.authority.request.binding_guard = if (request.binding_guard) |guard| try arena.dupe(u8, guard) else null;
        self.authority.request.setting_overlay = try cloneSettingOverlay(arena, request.setting_overlay);
        self.authority.request.session_id = self.session_id;
        try request.read_requirements.validate(.sql);
        self.native_adapter = .{ .sequence_state = request.sequence_state, .server = server, .identity = &self.identity, .context = try self.authority.context(), .database = self.authority.request.database.?, .namespace = self.authority.request.namespace.?, .session_id = self.session_id, .read_requirements = request.read_requirements, .shared_receipt_wait_budget = request.read_receipt_budget, .lake_visibility = if (request.read_requirements.visibility) |visibility| if (visibility == .accepted) .accepted else .committed else @fromBackingInt(@intCast(@backingInt(request.lake_visibility))), .setting_overlay = self.authority.request.setting_overlay, .setting_overlay_source = .connection, .expected_setting_epoch = self.authority.request.setting_epoch };
        self.native_adapter.prepared_bindings = try viewBindings(arena, self.authority.request.binding_guard);
        var captured_authorities: std.ArrayListUnmanaged(*const @import("../execution/read_authority.zig").Context) = .empty;
        self.native_adapter.setting_baseline = self.authority.request.setting_baseline;
        self.native_adapter.collect_view_authorities = &captured_authorities;
        var captured_bindings: std.ArrayListUnmanaged(@import("../session/prepared.zig").Binding) = .empty;
        self.native_adapter.collect_prepared_bindings = &captured_bindings;
        var transaction_lease: ?@import("../../transactions.zig").SessionRegistry.CommitExecution = null;
        defer if (transaction_lease) |lease| lease.release();
        var tenure: ?u128 = null;
        if (self.session_id) |id_hex| {
            const id = @import("../../distributed_txn.zig").parseTxnIdHex(id_hex) catch return error.SqlTransactionNotActive;
            if (try server.txn_sessions.principalAccess(server.alloc, id, credential.sessionPrincipal()) != .allowed) return error.SqlTransactionNotActive;
            transaction_lease = server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
            var statement_admission = try server.txn_sessions.admitSqlStatementForOwner(server.alloc, alloc, id, credential.sessionPrincipal(), server.localSessionNodeId(), credential.native_connection, credential.transaction_guard);
            defer statement_admission.state.deinit(server.alloc);
            self.staged = statement_admission.staged;
            tenure = statement_admission.guard.tenure;
            const state = &statement_admission.state;
            if (!std.mem.eql(u8, state.metadata.database, request.database orelse "default") or !std.mem.eql(u8, state.metadata.namespace, request.session_namespace orelse request.namespace orelse "public")) return error.SqlTransactionNotActive;
            transaction_start_ns = state.begin_timestamp;
            if (state.metadata.accepted_lake_reads) {
                if (request.read_requirements.visibility == .committed) return error.ReadSnapshotTooOld;
                self.native_adapter.lake_visibility = .accepted;
            }
            self.native_adapter.accepted_lake_repeatable = self.native_adapter.lake_visibility == .accepted and (state.metadata.isolation != .serializable or state.metadata.mode == .read_only);
            self.native_adapter.active_transaction = id;
            self.native_adapter.active_subtransaction = state.current_subtransaction;
            self.native_adapter.admitted_mutation = statement_admission.guard;
            self.native_adapter.staged = &self.staged;
            if (state.metadata.isolation != .read_committed) {
                self.native_cut = try self.native_adapter.pinTransactionSnapshot(id);
                self.native_adapter.dynamic_snapshot = &self.native_cut.?.snapshot;
                self.native_adapter.range_reads = &self.range_guards;
            }
            try server.txn_sessions.lockSqlCharacteristics(server.alloc, id);
        }
        self.decision_runtime = try self.native_adapter.decisionRuntime();
        errdefer if (self.decision_runtime) |*active| active.deinit();
        if (self.decision_runtime) |*active| self.native_adapter.decision_provider = active.provider();
        self.guarded = .{ .native = self.native_adapter.backend(), .authority = &self.authority, .revision = &self.native_adapter.revision, .expected_guard = self.authority.request.binding_guard, .allow_revision_advance = request.expected_stream_columns != null and tableOnlyPreparedRead(plan.compiled()) };
        try self.guarded.indexLargeGuard(arena);
        const parameters = if (request.parameter_cells != null) request.parameters else try normalizeParameters(arena, request.parameters, request.parameter_types);
        self.native_adapter.graph_parameters = parameters;
        self.native_adapter.graph_parameter_cells = request.parameter_cells;
        var stream_backend = self.guarded.backend();
        stream_backend.statement_clock = try cursorStatementClock(statement_source, statement_instant, transaction_start_ns);
        stream_backend.transaction_start = stream_backend.statement_clock.?.transaction;
        stream_backend.parameter_descriptor_hints = request.parameter_descriptors;
        stream_backend.setting_capture = try self.guarded.settingCapture(arena, &self.native_adapter, plan.compiled());
        // Stream.open captures the owner view once; loadSettings validates the
        // prepared epoch on that same snapshot before binding or reading rows.
        var opening: CursorAdmission = .{ .out = arena, .adapter = adapter, .native = &self.native_adapter, .credential = credential, .admitted = admitted_revision, .lease_epoch = tenure, .expected_columns = request.expected_stream_columns, .expected_tables = if (self.guarded.allow_revision_advance) self.guarded.expected_guard else null };
        if (plan.compiled().statement == .select) opening.install(&stream_backend);
        const opened = try Pull.Stream.openOwned(try @import("../execution/memory_owner.zig").create(adapter.server.alloc, alloc, adapter.server.shared_resource_manager orelse &adapter.server.local_resource_manager), stream_backend, plan.compiled(), parameters, .{ .result_rows = if (plan.compiled().statement == .copy_to) std.math.maxInt(usize) else request.limit, .page_rows = 256, .parameter_cells = request.parameter_cells, .parameter_owner = request.parameter_owner });
        if (opened == null) {
            if (self.native_cut) |cut| cut.release();
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
        self.native_adapter.collect_view_authorities = null;
        self.native_adapter.collect_prepared_bindings = null;
        // Future lazy graph opens borrow the stream-owned canonical frame,
        // rather than the request's transient native cell slice.
        if (self.stream.parameter_invocation) |invocation| if (invocation.frame) |frame| {
            self.native_adapter.graph_parameter_cells = frame.values;
        };
        errdefer self.stream.close();
        var view_bindings: std.ArrayListUnmanaged(@import("../session/prepared.zig").Binding) = .empty;
        for (captured_bindings.items) |binding| {
            if (binding.kind != .view) continue;
            var owned = binding;
            owned.database = try arena.dupe(u8, binding.database);
            owned.namespace = try arena.dupe(u8, binding.namespace);
            owned.name = try arena.dupe(u8, binding.name);
            owned.physical_name = "";
            try view_bindings.append(arena, owned);
        }
        self.view_bindings = try view_bindings.toOwnedSlice(arena);
        const view_authorities = try arena.alloc(*const @import("../execution/read_authority.zig").Context, captured_authorities.items.len);
        for (captured_authorities.items, view_authorities) |value, *owned|
            owned.* = try @import("../execution/read_authority.zig").clone(arena, value);
        self.view_authorities = view_authorities;
        self.retained = if (plan.compiled().statement == .select) opening.captured orelse return error.InvalidSqlBackendResponse else null;
        if (self.session_id != null and self.range_guards.tables.len != 0) {
            _ = (try server.txn_sessions.stage(server.alloc, self.native_adapter.admitted_mutation.?, &self.range_guards)) orelse return error.SqlTransactionNotActive;
        }
        if (transaction_lease) |lease| {
            lease.release();
            transaction_lease = null;
        }
        const binding = self.stream.context.binding;
        const policies = try arena.alloc(Policy, if (binding.relation) |relation| relation.scans.len else if (binding.table != null) 1 else 0);
        for (policies, 0..) |*policy, i| {
            const table = if (binding.relation) |relation| relation.scans[i].table else binding.table.?;
            policy.* = .{ .resource = try arena.dupe(u8, table.physical_name), .filter = try @import("../execution/read_authority.zig").filter(arena, table, self.identity), .sql_authorization = try @import("../execution/authorization_runtime.zig").transactionIntent(&self.native_adapter, table) };
            if (@import("../execution/read_authority.zig").context(table)) |value| {
                var capsule = table;
                capsule.physical_name = policy.resource;
                capsule.columns = &.{};
                capsule.read_authority = try @import("../execution/read_authority.zig").clone(arena, value);
                if (table.scope) |scope| capsule.scope = .{
                    .database = try arena.dupe(u8, scope.database),
                    .namespace = try arena.dupe(u8, scope.namespace),
                    .name = try arena.dupe(u8, scope.name),
                    .revision = scope.revision,
                };
                policy.authority_table = capsule;
            }
            if (table.scope) |scope| {
                policy.name = .{ .database = try arena.dupe(u8, scope.database), .namespace = try arena.dupe(u8, scope.namespace), .table = try arena.dupe(u8, scope.name) };
                policy.id = table.id;
                policy.schema_version = table.schema_version;
            }
        }
        self.policies = policies;
        const columns = try arena.alloc(wire.Column, self.stream.context.binding.columns.len);
        var enum_views: std.AutoHashMapUnmanaged(*const catalog.enum_values.View, *const catalog.enum_values.View) = .empty;
        var domain_views: @import("antfly_local_sources").sql_scalar.DomainViews = .empty;
        for (columns, self.stream.context.binding.columns) |*out, column| out.* = .{ .name = try arena.dupe(u8, column.name), .type = try wireType(column.type), .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .enum_view = try @import("antfly_local_sources").sql_scalar.ownEnumView(arena, &enum_views, column.enum_view), .domain_view = try @import("antfly_local_sources").sql_scalar.ownDomainView(arena, &domain_views, column.domain_view), .temporal_kind = column.temporal_kind };
        self.columns = columns;
        self.plan = plan;
        self.admission = admission;
        moved = true;
        return .{ .context = self, .columns = columns, .next = next, .close = close, .detach = detach, .validate = validate, .copy_out = if (plan.compiled().statement == .copy_to) plan.compiled().statement.copy_to.options else null };
    }

    pub fn validate(raw: *anyopaque, alloc: std.mem.Allocator, request: wire.Request) !void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        var job = StreamJob{ .adapter = self.adapter, .alloc = alloc, .credential = self.authority.credential, .request = request, .owner = self, .validate_only = true };
        try job.dispatch();
    }

    fn validateCursor(self: *OwnedRead, alloc: std.mem.Allocator, request: wire.Request) !void {
        try request.check();
        try self.authority.credential.validate();
        try self.validateViewBindings(alloc, request);
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
        for (self.view_authorities) |value| try @import("../execution/read_authority.zig").validateGrants(alloc, value);
        try self.validateSqlPolicies(alloc, request, current.?);
        try validatePolicies(self.adapter.server.alloc, &current.?, self.identity.?, self.policies);
        var authority: Authority = .{ .credential = self.authority.credential, .identity = &current, .request = request };
        var adapter: execution.Adapter = .{ .server = self.adapter.server, .identity = &current, .context = try authority.context(), .database = original.database.?, .namespace = original.namespace.? };
        const catalog_backend = adapter.backend();
        for (self.policies) |policy| if (policy.name) |name| {
            var scoped = adapter;
            var effective: ?http.AuthenticatedIdentity = null;
            defer if (effective) |*identity| identity.deinit(self.adapter.server.alloc);
            if (policy.authority_table) |pinned| {
                effective = try http.cloneCatalogIdentity(self.adapter.server.alloc, @import("../execution/read_authority.zig").context(pinned).?.effective);
                scoped.identity = &effective;
            }
            const resolver = if (effective != null) scoped.backend() else catalog_backend;
            const table = try resolver.vtable.resolve(resolver.ptr, alloc, name, .read);
            if (table.id != policy.id or table.schema_version != policy.schema_version or !std.mem.eql(u8, table.physical_name, policy.resource)) return error.CatalogGenerationChanged;
        };
        if (self.retained) |*retained| try retained.check(alloc, self.authority.credential, request, false);
    }

    fn next(raw: *anyopaque, alloc: std.mem.Allocator, request: wire.Request, limit: u32) !wire.StreamPage {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        var job = StreamJob{ .adapter = self.adapter, .alloc = alloc, .credential = self.authority.credential, .request = request, .owner = self, .limit = limit };
        try job.dispatch();
        return job.page.?;
    }

    fn pull(self: *OwnedRead, alloc: std.mem.Allocator, request: wire.Request, limit: u32) !wire.StreamPage {
        if (self.detached) return error.InvalidSqlBackendResponse;
        // Retained snapshots have a hard initial statement deadline, even if
        // later Execute messages carry a newer per-request deadline.
        try self.authority.request.check();
        try self.authority.credential.validate();
        try self.validateViewBindings(alloc, request);
        if (self.session_id) |id_hex| {
            const id = try @import("../../distributed_txn.zig").parseTxnIdHex(id_hex);
            if (try self.adapter.server.txn_sessions.principalAccess(self.adapter.server.alloc, id, self.authority.credential.sessionPrincipal()) != .allowed) return error.SqlTransactionNotActive;
            var state = (try self.adapter.server.txn_sessions.getSqlState(self.adapter.server.alloc, id)) orelse return error.SqlTransactionNotActive;
            defer state.deinit(self.adapter.server.alloc);
            if (state.metadata.failed or state.terminal != null) return error.SqlTransactionNotActive;
            self.native_adapter.active_subtransaction = state.current_subtransaction;
        }
        var fresh = try self.authority.credential.identity(alloc);
        defer fresh.deinit(alloc);
        for (self.view_authorities) |value| try @import("../execution/read_authority.zig").validateGrants(alloc, value);
        try self.validateSqlPolicies(alloc, request, fresh);
        try validatePolicies(alloc, &fresh, self.identity.?, self.policies);
        var attached = request;
        attached.session_id = self.session_id;
        if (self.retained) |*retained| try retained.check(alloc, self.authority.credential, attached, false);
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

    fn validateViewBindings(self: *OwnedRead, alloc: std.mem.Allocator, request: wire.Request) !void {
        if (self.view_bindings.len == 0) return;
        const sources = @import("antfly_local_sources");
        const domain = sources.system_catalog_domain;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const lookups = try a.alloc(domain.SqlSourceLookup, self.view_bindings.len);
        for (self.view_bindings, lookups) |binding, *lookup| lookup.* = .{ .view_id = binding.id };
        var authority = self.authority;
        authority.request = request;
        const bytes = try self.adapter.server.source.systemCatalog(a, try authority.context(), .{ .resolve_many = .{ .sql_sources = lookups } });
        const current = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
        if (!current.sql_sources_resolved or current.sql_sources.len != lookups.len) return error.CatalogGenerationChanged;
        for (self.view_bindings, current.sql_sources) |binding, resolved| {
            const found = resolved orelse return error.CatalogGenerationChanged;
            if (found.value != .view or !std.mem.eql(u8, binding.database, found.target.database) or
                !std.mem.eql(u8, binding.namespace, found.target.namespace) or !std.mem.eql(u8, binding.name, found.target.table))
                return error.CatalogGenerationChanged;
            var record = try @import("../../../system_catalog/views.zig").Record.decode(a, found.value.view.source_json);
            defer record.deinit();
            try @import("../session/prepared.zig").Binding.verifyView(&.{binding}, @import("../session/prepared.zig").Binding.fromView(
                found.target,
                record.value.id,
                record.value.generation,
                record.value.attributes.epoch.digest,
                current.revision,
            ));
        }
    }

    fn validateSqlPolicies(self: *OwnedRead, alloc: std.mem.Allocator, request: wire.Request, fresh: http.AuthenticatedIdentity) !void {
        const sessions = @import("../../transactions.zig");
        var region = std.heap.ArenaAllocator.init(alloc);
        defer region.deinit();
        const a = region.allocator();
        var tables: std.ArrayList(sessions.TableCommitRequest) = .empty;
        var bindings: std.ArrayListUnmanaged(sessions.CatalogBinding) = .empty;
        for (self.policies) |policy| if (policy.sql_authorization) |intent| {
            const name = policy.name orelse return error.InvalidSqlBackendResponse;
            const logical = try (@import("antfly_local_sources").system_catalog_domain.Target{ .database = name.database.?, .namespace = name.namespace.?, .table = name.table }).resourceNameAlloc(a);
            try tables.append(a, .{ .table_name = logical, .schema_version = policy.schema_version, .sql_authorization = intent });
            try bindings.append(a, .{ .logical = logical, .physical = policy.resource });
        };
        if (tables.items.len == 0) return;
        var authority = self.authority;
        authority.request = request;
        _ = try @import("../execution/authorization_runtime.zig").TransactionScope.init(self.adapter.server, fresh, try authority.context(), a, .{ .tables = tables.items, .catalog_bindings = bindings });
    }

    fn validatePolicies(alloc: std.mem.Allocator, fresh: *http.AuthenticatedIdentity, admitted: http.AuthenticatedIdentity, policies: []const Policy) !void {
        // The catalog resolves logical resources to private physical names.
        // Reapply only the pinned aliases to fresh authority, never its old
        // permissions or filters. Buffered operator rows need this check too:
        // they need not cause another native cursor.next call.
        for (admitted.catalog_aliases) |alias| try http.projectCatalogIdentity(alloc, fresh, alias.logical, alias.physical);
        try validateProjectedPolicies(alloc, fresh, policies);
    }

    fn validateProjectedPolicies(alloc: std.mem.Allocator, fresh: *http.AuthenticatedIdentity, policies: []const Policy) !void {
        if (policies.len == 0) return;
        var local: [8]http.GraphPermissionCheck = undefined;
        const checks = if (policies.len <= local.len) local[0..policies.len] else try alloc.alloc(http.GraphPermissionCheck, policies.len);
        defer if (policies.len > local.len) alloc.free(checks);
        var check_count: usize = 0;
        for (policies) |policy| {
            if (policy.authority_table != null or policy.sql_authorization != null) continue;
            checks[check_count] = .{ .resource_type = if (policy.graph_revision != null) .graph else .table, .resource = policy.resource, .permission_type = .read };
            check_count += 1;
        }
        if (!try http.resourcePermissionsCurrentlyAllowed(alloc, fresh.*, checks[0..check_count])) return error.Forbidden;
        for (policies) |policy| {
            if (policy.authority_table) |table| {
                try @import("../execution/read_authority.zig").validateFilters(alloc, table, fresh.*, policy.filter);
                continue;
            }
            if (policy.graph_revision != null) continue;
            const filter = try http.resolveEffectiveRowFilterJson(alloc, fresh.*, policy.resource);
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
            if (value.bytes()) |bytes| {
                try value.validateTemporal();
                if (self.page.columns[column].type != .bytea) return error.InvalidSqlBackendResponse;
                return .{ .bytea = bytes, .sql_null = false };
            }
            if (value.enumeration()) |atom| {
                try value.validateTemporal();
                return .{ .enumeration = atom, .sql_null = false };
            }
            if (value.range()) |range| {
                try value.validateTemporal();
                if (@import("antfly_local_sources").sql_range_value.kindFromType(self.page.columns[column].type) != range.kind) return error.InvalidSqlBackendResponse;
                return .{ .range = range, .sql_null = false };
            }
            if (value.temporal()) |temporal_value| {
                const definition = self.page.columns[column];
                const expected_kind = definition.temporal_kind orelse .timestamp_tz;
                if (definition.type != .datetime or expected_kind != temporal_value.kind()) return error.InvalidSqlBackendResponse;
                if (expected_kind != .timestamp_tz) {
                    try value.validateTemporal();
                    var text: [@import("antfly_local_sources").sql_temporal_text.output_capacity]u8 = undefined;
                    return .{ .value = .{ .string = try alloc.dupe(u8, try @import("antfly_local_sources").sql_temporal_text.format(temporal_value.*, &text)) }, .sql_null = false };
                }
                const instant = try @import("antfly_local_sources").sql_scalar.timestampResult(value, self.page.columns[column].type);
                return .{ .value = try wire_values.timestampEnvelope(alloc, instant), .sql_null = false };
            }
            if (value.array) |array| {
                const definition = self.page.columns[column];
                if (value.sql_null or value.value != .null or value.patterns() != null or definition.type != .array or definition.element_type != array.element_type) return error.InvalidSqlBackendResponse;
                return .{ .array = array, .sql_null = false };
            }
            if (value.numeric) |number| {
                const definition = self.page.columns[column];
                if (value.sql_null or value.value != .null or value.patterns() != null or definition.type != .number or definition.element_type != .numeric) return error.InvalidSqlBackendResponse;
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
        if (self.native_cut) |cut| cut.release();
        self.native_cut = null;
        self.adapter.server.txn_sessions.snapshot_retirement.drain();
        if (self.decision_runtime) |*active| active.deinit();
        self.plan.release(self.adapter.server.sqlPlanCacheIo());
        self.admission.release();
        self.staged.deinit(self.alloc);
        self.range_guards.deinit(self.adapter.server.alloc);
    }

    fn close(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        detach(raw);
        self.native_adapter.releaseExecutionOwners();
        self.identity.?.deinit(self.adapter.server.alloc);
        if (self.session_id) |id| self.adapter.server.alloc.free(id);
        self.arena.deinit();
        self.alloc.destroy(self);
    }
};

const ReadRevision = struct { grants: u64, token: u64 };

// These scan clauses bind only to the table layout captured by the guard.
fn tableOnlyPreparedRead(compiled: *const compiler.Compiled) bool {
    if (compiled.statement != .select or compiled.parameter_count != 0 or compiled.uses_current_setting) return false;
    const select = compiled.statement.select;
    if (select.source) |source| {
        if (source.* != .table or source.table.search != null or source.table.graph != null or source.table.graph_search != null) return false;
    } else if (select.table == null) return false;
    if (select.ctes.len != 0 or select.values_arms.len != 0 or select.set_operation != null or
        select.having != null or select.count_all or select.distinct != null or
        select.windows.len != 0 or select.group_by.len != 0 or select.locking != null) return false;
    if (select.predicate) |predicate| switch (predicate.*) {
        .is_null => {},
        .scalar => |expr| {
            if (expr.* != .unary or (expr.unary.op != .is_null and expr.unary.op != .is_not_null) or expr.unary.operand.* != .column) return false;
        },
        else => return false,
    };
    for (select.order_by) |order| {
        if (order.expression) |expression| if (expression.* != .column) return false;
    }
    for (select.columns) |column| {
        if (column.expression) |expression| if (expression.* != .column) return false;
    }
    return true;
}

// Capture the exact executable binding before either kind of reader can touch
// rows. Preserve existing after-write receipt validation on the same binding.
const CursorAdmission = struct {
    out: std.mem.Allocator,
    adapter: *Adapter,
    native: *execution.Adapter,
    credential: *Credential,
    admitted: ReadRevision,
    lease_epoch: ?u128,
    expected_columns: ?[]const wire.Column = null,
    expected_tables: ?[]const u8 = null,
    previous: @FieldType(catalog.Backend, "execution_binding_validator") = null,
    captured: ?RetainedRead = null,

    fn install(self: *@This(), backend: *catalog.Backend) void {
        self.previous = backend.execution_binding_validator;
        backend.execution_binding_validator = .{ .ptr = self, .validate = capture };
    }

    fn capture(raw: *anyopaque, scratch: std.mem.Allocator, binding: describe_sql.BoundStatement) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.previous) |validator| try validator.validate(validator.ptr, scratch, binding);
        if (self.expected_tables) |expected| {
            const revision = self.native.revision orelse return error.CatalogGenerationChanged;
            if (revision != std.mem.readInt(u64, expected[0..8], .little)) {
                const current = (try statementBindingGuard(scratch, revision, binding)) orelse return error.CatalogGenerationChanged;
                defer scratch.free(current);
                if (!std.mem.eql(u8, expected[8..], current[8..])) return error.CatalogGenerationChanged;
            }
        }
        if (self.expected_columns) |expected| {
            if (expected.len != binding.columns.len) return error.CachedPlanResultTypeChanged;
            for (expected, binding.columns) |column, current| {
                const actual: wire.Column = .{
                    .name = current.name,
                    .type = try wireType(current.type),
                    .element_type = current.element_type,
                    .numeric_modifier = current.numeric_modifier,
                    .enum_view = current.enum_view,
                    .domain_view = current.domain_view,
                    .temporal_kind = current.temporal_kind,
                };
                if (!wire.sameColumnType(column, actual) or !std.mem.eql(u8, column.name, actual.name))
                    return error.CachedPlanResultTypeChanged;
            }
        }
        self.captured = try RetainedRead.capture(self.out, self.adapter, self.credential, self.native.identity.* orelse return error.Unauthorized, binding, self.native.context, self.native.revision, self.admitted);
        self.captured.?.lease_epoch = self.lease_epoch;
        // A later native operator may mint another owner proof. Bind its role
        // set under the same manager lock that signs that proof.
        self.native.identity.*.?.bound_policy_roles = self.captured.?.role_digest;
    }
};

const RetainedRead = struct {
    const Domain = @import("antfly_local_sources").system_catalog_domain;
    adapter: *Adapter,
    policies: []const OwnedRead.Policy,
    bindings: []const Domain.CursorBinding,
    validated: ReadRevision,
    lease_epoch: ?u128 = null,
    validated_stamp: ?wire.CatalogStamp = null,
    role_digest: ?[32]u8 = null,
    cut: Cut,
    const Cut = struct { policies: Domain.CursorPolicies, stamp: ?wire.CatalogStamp, revision: ?u64 };

    fn capture(out: std.mem.Allocator, adapter: *Adapter, credential: *Credential, identity: http.AuthenticatedIdentity, binding: describe_sql.BoundStatement, context: operation.RequestContext, revision: ?u64, admitted: ReadRevision) !RetainedRead {
        var temp = std.heap.ArenaAllocator.init(adapter.server.alloc);
        defer temp.deinit();
        var tables: std.ArrayHashMapUnmanaged(BindingEntry, catalog.Table, BindingEntryContext, true) = .empty;
        try collectBindingTables(temp.allocator(), binding, &tables, 0);
        const policies = try out.alloc(OwnedRead.Policy, tables.count());
        const bindings = try out.alloc(Domain.CursorBinding, tables.count());
        for (tables.values(), policies, bindings) |table, *policy, *target| {
            policy.* = try OwnedRead.Policy.capture(out, identity, table);
            const name = policy.name;
            if (name == null and policy.graph_revision == null) return error.InvalidSqlBackendResponse;
            target.* = .{ .target = if (name) |n| .{ .database = n.database.?, .namespace = n.namespace.?, .table = n.table } else null, .physical = policy.resource, .id = policy.id, .version = policy.graph_revision orelse policy.schema_version };
        }
        var result: RetainedRead = .{ .adapter = adapter, .policies = policies, .bindings = bindings, .validated = admitted, .cut = undefined };
        result.cut = try result.readCut(temp.allocator(), context, revision, null);
        if (result.cut.policies.protected) result.role_digest = try usermgr.UserManager.roleDigest(temp.allocator(), identity.roles);
        result.validated = try result.validateAccess(temp.allocator(), credential);
        return result;
    }

    fn readCut(self: *const RetainedRead, allocator: std.mem.Allocator, context: operation.RequestContext, expected: ?u64, known: ?wire.CatalogStamp) !Cut {
        var scratch = std.heap.ArenaAllocator.init(allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        var revision = expected;
        var stamp: ?wire.CatalogStamp = null;
        var hash = std.crypto.hash.Blake3.init(.{});
        var protected = false;
        var offset: usize = 0;
        var internal = context;
        internal.row_policy_install_authority = true;
        while (offset < self.bindings.len) {
            _ = scratch.reset(.retain_capacity);
            const end = @min(offset + 256, self.bindings.len);
            const request: Domain.ResolveMany = .{ .cursor_bindings = self.bindings[offset..end], .cursor_catalog_cut = if (known) |cut| .{ .revision = cut.catalog_revision, .epoch = cut.relation_epoch } else null, .expected_revision = revision, .include_source_epoch = true };
            const reply = try self.bindingBatch(a, internal, request);
            const policies = reply.cursor_policies orelse return error.InvalidSqlBackendResponse;
            try context.checkCatalogIncarnation(reply.source_epoch);
            if (revision) |r| if (r != reply.revision) return error.CatalogGenerationChanged;
            const observed: ?wire.CatalogStamp = if (reply.source_epoch) |epoch| .{ .catalog_revision = reply.revision, .relation_epoch = epoch } else null;
            if (offset != 0 and !std.meta.eql(stamp, observed)) return error.CatalogGenerationChanged;
            stamp = observed;
            revision = reply.revision;
            hash.update(&policies.digest);
            protected = protected or policies.protected;
            offset = end;
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return .{ .policies = .{ .digest = digest, .protected = protected }, .stamp = stamp, .revision = revision };
    }

    fn bindingBatch(self: *const RetainedRead, a: std.mem.Allocator, context: operation.RequestContext, request: Domain.ResolveMany) !Domain.ResolvedMany {
        const source = self.adapter.server.source;
        if (source.systemCatalog(a, context, .{ .resolve_many = request })) |bytes| {
            const reply = try std.json.parseFromSliceLeaky(Domain.ResolvedMany, a, bytes, .{ .ignore_unknown_fields = true });
            if (reply.cursor_policies != null) return reply;
        } else |err| switch (err) {
            error.UnknownField, error.UnsupportedOperation => {},
            else => return err,
        }
        // Older owners expose the same proofs individually. Fence both ends
        // of their batch and require an explicit publication status per table.
        var targets: std.ArrayList(Domain.Target) = .empty;
        for (request.cursor_bindings) |binding| if (binding.target) |target| try targets.append(a, target);
        const legacy: Domain.ResolveMany = .{ .targets = targets.items, .expected_revision = request.expected_revision, .include_source_epoch = true, .include_query_definitions = true };
        const bytes = source.systemCatalog(a, context, .{ .resolve_many = legacy }) catch |err| switch (err) {
            error.UnknownField, error.UnsupportedOperation => blk: {
                if (context.bound_catalog_incarnation != null) return error.CatalogGenerationChanged;
                var old = legacy;
                old.include_source_epoch = false;
                break :blk try source.systemCatalog(a, context, .{ .resolve_many = old });
            },
            else => return err,
        };
        var reply = try std.json.parseFromSliceLeaky(Domain.ResolvedMany, a, bytes, .{ .ignore_unknown_fields = true });
        try context.checkCatalogIncarnation(reply.source_epoch);
        if (reply.tables.len != targets.items.len) return error.InvalidSqlBackendResponse;
        var hash = std.crypto.hash.Blake3.init(.{});
        var protected = false;
        var index: usize = 0;
        for (request.cursor_bindings) |binding| {
            const stamp = if (binding.target != null) table: {
                const row = reply.tables[index] orelse return error.CatalogGenerationChanged;
                index += 1;
                const definition = row.query_definition orelse return error.InvalidSqlBackendResponse;
                if (row.table_id != binding.id or definition.table_id != binding.id or !std.mem.eql(u8, row.name, binding.physical) or
                    try @import("../../tables.zig").schemaVersionAlloc(a, definition.schema_json) != binding.version) return error.CatalogGenerationChanged;
                const raw = try source.systemCatalog(a, context, .{ .policy_publication_status = binding.id });
                break :table try std.json.parseFromSliceLeaky(?@import("antfly_local_sources").system_catalog_policies.PublicationStamp, a, raw, .{ .ignore_unknown_fields = true });
            } else graph: {
                const raw = try source.systemCatalog(a, context, .{ .read = .{ .kind = .graph, .name = binding.physical } });
                const state = try std.json.parseFromSliceLeaky(Domain.State, a, raw, .{ .ignore_unknown_fields = true });
                const row = state.find(.graph, 0, binding.physical) orelse return error.CatalogGenerationChanged;
                if (state.revision != reply.revision or row.id != binding.id or row.graph_revision != binding.version) return error.CatalogGenerationChanged;
                break :graph null;
            };
            try Domain.CursorPolicies.observe(&hash, &protected, binding, stamp);
        }
        const last = try source.systemCatalog(a, context, .{ .resolve_many = .{ .include_source_epoch = reply.source_epoch != null, .expected_revision = reply.revision } });
        const end = try std.json.parseFromSliceLeaky(Domain.ResolvedMany, a, last, .{ .ignore_unknown_fields = true });
        if (end.revision != reply.revision or !std.meta.eql(end.source_epoch, reply.source_epoch)) return error.CatalogGenerationChanged;
        reply.cursor_policies = .{ .digest = undefined, .protected = protected };
        hash.final(&reply.cursor_policies.?.digest);
        return reply;
    }

    fn validateAccess(self: *const RetainedRead, a: std.mem.Allocator, credential: *Credential) !ReadRevision {
        const revision = credential.readRevision();
        if (!std.meta.eql(revision, self.validated)) {
            var fresh = try credential.identity(a);
            defer fresh.deinit(a);
            if (self.role_digest) |digest| {
                if (!std.meta.eql(digest, try usermgr.UserManager.roleDigest(a, fresh.roles))) return error.Forbidden;
            }
            for (self.bindings) |binding| if (binding.target) |target| {
                const logical = try target.resourceNameAlloc(a);
                defer a.free(logical);
                try http.projectCatalogIdentity(a, &fresh, logical, binding.physical);
            };
            try OwnedRead.validateProjectedPolicies(a, &fresh, self.policies);
        }
        return revision;
    }

    fn check(self: *RetainedRead, a: std.mem.Allocator, credential: *Credential, request: wire.Request, materialized: bool) !void {
        try request.check();
        try credential.validate();
        if (self.adapter.server.cfg.user_manager != credential.manager) return error.Unauthorized;
        if (request.session_id) |encoded| {
            const id = try @import("../../distributed_txn.zig").parseTxnIdHex(encoded);
            const epoch = try self.adapter.server.txn_sessions.sqlReaderTenure(a, id, credential.sessionPrincipal(), self.adapter.server.localSessionNodeId());
            if (epoch != self.lease_epoch) return error.SessionLeaseLost;
        }
        const revision = try self.validateAccess(a, credential);
        var context = try requestContext(credential, &request).platformDeadline();
        if (self.validated_stamp orelse self.cut.stamp) |stamp| context.bound_catalog_incarnation = stamp.relation_epoch.incarnation;
        const cut = try self.readCut(a, context, if (materialized) self.cut.revision else null, self.validated_stamp orelse self.cut.stamp);
        if (!std.meta.eql(cut.policies, self.cut.policies)) return error.RowPolicyCatalogChanged;
        if (materialized and !std.meta.eql(self.cut.stamp, cut.stamp)) return error.CatalogGenerationChanged;
        // Advance both hints only after the complete check. The original cut
        // still owns the incarnation, policy digest and materialization fence.
        self.validated = revision;
        self.validated_stamp = cut.stamp;
    }

    fn validateBuffered(raw: *anyopaque, a: std.mem.Allocator, identity: wire.Identity, request: wire.Request, materialized: bool) !void {
        const self: *RetainedRead = @ptrCast(@alignCast(raw));
        var job: StreamJob = .{ .adapter = self.adapter, .alloc = a, .credential = @ptrCast(@alignCast(identity.context)), .request = request, .buffered = self, .materialized = materialized };
        try job.dispatch();
    }
};

const StreamJob = struct {
    buffered: ?*RetainedRead = null,
    materialized: bool = false,
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
        @import("../../protected_future.zig").wait(io, &future);
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
            const resource = try @import("../../../system_catalog/routes.zig").resourceNameAlloc(self.alloc, .{ .kind = .namespace, .database = database, .name = namespace });
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
        if (self.buffered) |buffered| {
            var admission = try self.adapter.server.acquireSqlExecution(false);
            defer admission.release();
            return buffered.check(self.alloc, self.credential, self.request, self.materialized);
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
};

const Credential = struct {
    manager: ?*usermgr.UserManager,
    username: []u8,
    principal: []u8,
    user_instance_id: ?[16]u8 = null,
    verifier_mac: [Mac.mac_length]u8,
    trusted_server: ?*http.ApiHttpServer = null,
    trusted_identity: ?http.AuthenticatedIdentity = null,
    trusted_issuer: ?[]u8 = null,
    trusted_expires: i64 = 0,
    trusted_revision: u64 = 1,
    session_cleanup_complete: bool = true,
    imported_snapshot: ?ImportedSnapshot = null,
    transaction_guard: ?@import("../../transactions.zig").Mutation = null,

    /// A socket owns one native command identity, independently of its transaction.
    native_connection: ?[32]u8 = null,
    notification_interest: bool = false,
    notification_observation: @import("../notifications/hub.zig").Observation = .{},

    fn ensureNativeConnection(self: *Credential, server: *http.ApiHttpServer, identity_value: ?http.AuthenticatedIdentity, request: wire.Request) !?[32]u8 {
        if (!server.source.vtable.supports_sql_command_ownership) return null;
        if (self.native_connection) |id| {
            const store = server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
            try @import("../session/connections.zig").keepWireAlive(store, server.alloc, id, http.transactionPrincipal(identity_value) orelse return error.Unauthorized, server.localSessionNodeId(), try server.sql_connection_admission.executionInstance(server.embedding_provider_runtime.io), @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
            return id;
        }
        const store = server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
        try self.validate();
        var random: [16]u8 = undefined;
        try server.embedding_provider_runtime.io.randomSecure(&random);
        const id = @import("../execution/resource_routing.zig").encode(server.localSessionNodeId(), random);
        const principal = http.transactionPrincipal(identity_value) orelse return error.Unauthorized;
        const now_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
        _ = try @import("../session/connections.zig").mutate(store, .{ .create = .{
            .id = id,
            .principal = principal,
            .owner_node_id = server.localSessionNodeId(),
            .expires_at_ms = now_ms +| @import("../session/connection_record.zig").ttl_ms,
            .wire_epoch = try server.sql_connection_admission.executionInstance(server.embedding_provider_runtime.io),
            .database = request.database orelse "default",
            .namespace = request.namespace orelse "public",
            .baseline = request.setting_baseline orelse &.{},
            .principal_instance = try http.sqlPrincipalInstance(server, identity_value),
        } }, principal, server.localSessionNodeId(), now_ms);
        self.native_connection = id;
        return id;
    }

    fn transactionGuard(self: *const Credential, id: [16]u8) !@import("../../transactions.zig").Mutation {
        const guard = self.transaction_guard orelse return error.SessionLeaseLost;
        if (!std.mem.eql(u8, &id, &guard.id)) return error.SessionLeaseLost;
        return guard;
    }

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

    fn grantRevision(self: *const Credential) u64 {
        return if (self.manager) |m| m.authorization_revision.load(.acquire) else 0;
    }

    fn readRevision(self: *Credential) ReadRevision {
        return .{ .grants = self.grantRevision(), .token = self.trusted_revision };
    }

    fn descriptionRevision(self: *Credential) !u64 {
        if (self.trusted_identity != null) {
            try self.validate();
            return self.trusted_revision;
        }
        return self.manager.?.authorization_revision.load(.acquire);
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

const publication_metadata = @import("../../../metadata/pg_publication.zig");
const publication_control = @import("../../../metadata/changefeed_control.zig");
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
    const owner = try @import("../../changefeed_public.zig").durableOwner(identity);
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
    var previous = try port.readRecord(a, database_key, command.name, maintenance);
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
    try port.mutateRecord(a, .{
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
    kind: enum { describe, execute, probe, notifications, notification_connect, discard_connection },
    notification_control: @import("antfly_local_sources").sql_ast.notifications.DeliveryControl = .{ .poll = 128 },
    notification_page: @import("antfly_local_sources").sql_ast.notifications.Page = .{},
    catalog_stamp: ?wire.CatalogStamp = null,
    description: ?wire.Description = null,
    result: ?wire.Result = null,
    failure: ?anyerror = null,
    compile_diagnostic: compiler.Diagnostic = .{},
    compile_diagnostic_valid: bool = false,
    done: std.Io.Event = .unset,

    fn run(self: *Job) void {
        defer self.done.set(self.request.io);
        self.runInner() catch |err| {
            if (@import("builtin").is_test and self.request.copy_source_statement != null and std.mem.eql(u8, execution.diagnostic(err).code, "XX000")) {
                std.debug.print("COPY predicate execution failed: {s}\n", .{@errorName(err)});
                if (@errorReturnTrace()) |trace| std.debug.dumpErrorReturnTrace(trace);
            }
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

    fn runNotificationConnect(self: *Job) !void {
        const server = self.adapter.server;
        var user: ?http.AuthenticatedIdentity = try self.credential.identity(server.alloc);
        defer user.?.deinit(server.alloc);
        var authority_owner: Authority = .{ .credential = self.credential, .identity = &user, .request = self.request };
        const context = try authority_owner.context();
        const locks = @import("../../../system_catalog/lock_authority.zig");
        var native_port: @import("../session/command_producer.zig").NativePort = .{ .server = server, .context = context };
        const actor = try @import("../recovery/authorization_recovery.zig").actor(server, user);
        const bytes = try native_port.port().call(native_port.port().ptr, self.alloc, .{ .sql_lock_read = .{ .incarnation = @splat(0), .standalone = server.cfg.deployment_mode == .standalone, .actor = actor, .database = 0, .discovery = self.request.database orelse "default" } });
        defer self.alloc.free(bytes);
        var cut = try std.json.parseFromSlice(locks.Snapshot, self.alloc, bytes, .{});
        defer cut.deinit();
        if (!cut.value.notification_capable) return;
        const id = (try self.credential.ensureNativeConnection(server, user, self.request)) orelse return;
        const io = server.embedding_provider_runtime.io;
        var lease = try server.sql_connection_admission.acquire(server.alloc, io, .init(id, self.credential.sessionPrincipal()));
        defer lease.release(io);
        var command: @import("../session/command_execution.zig").Request = .{};
        var finished = false;
        defer if (!finished) command.finish() catch {};
        if (!try command.start(server, context, user, &lease, .{ .native_backend_identity = self.request.backend_pid })) return error.SqlConnectionUnavailable;
        var root: execution.Adapter = .{ .server = server, .identity = &user, .context = command.attach(context), .database = self.request.database orelse "default", .namespace = self.request.namespace orelse "public", .connection_id = id, .backend_pid = self.request.backend_pid };
        if (try @import("../notifications/execution.zig").bindIdentity(&root, self.alloc) == null) return error.TableTopologyProtocolUpgradeRequired;
        try command.finish();
        finished = true;
    }

    fn runNotifications(self: *Job) !void {
        try self.request.io.checkCancel();
        if (self.credential.native_connection == null) return;
        const server = self.adapter.server;
        var user: ?http.AuthenticatedIdentity = try self.credential.identity(server.alloc);
        defer user.?.deinit(server.alloc);
        const id = (try self.credential.ensureNativeConnection(server, user, self.request)) orelse return;
        const io = server.sharedApiIo() orelse return error.SqlConnectionUnavailable;
        var lease = try server.sql_connection_admission.acquire(server.alloc, io, .init(id, self.credential.sessionPrincipal()));
        defer lease.release(io);
        var authority: Authority = .{ .credential = self.credential, .identity = &user, .request = self.request };
        var context = try (try authority.context()).platformDeadline();
        context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 250 * std.time.ns_per_ms);
        const owner: @import("../notifications/delivery.zig").Owner = .{ .server = server, .user = user, .context = context, .connection = id, .observation = &self.credential.notification_observation };
        switch (self.notification_control) {
            .poll => |maximum| {
                self.notification_page = try owner.poll(self.alloc, null, maximum);
                if (!self.notification_page.subscribed and !self.notification_page.deferred) self.credential.notification_interest = false;
            },
            .acknowledge => |cursor| try owner.acknowledge(self.alloc, cursor),
        }
    }

    fn runDiscard(self: *Job) !void {
        const server = self.adapter.server;
        if (!server.source.vtable.supports_sql_command_ownership) return;
        var user: ?http.AuthenticatedIdentity = try self.credential.identity(server.alloc);
        defer user.?.deinit(server.alloc);
        const id = (try self.credential.ensureNativeConnection(server, user, self.request)) orelse return;
        const io = server.embedding_provider_runtime.io;
        var lease = try server.sql_connection_admission.acquire(server.alloc, io, .init(id, self.credential.sessionPrincipal()));
        defer lease.release(io);
        var authority: Authority = .{ .credential = self.credential, .identity = &user, .request = self.request };
        const context = try authority.context();
        var command: @import("../session/command_execution.zig").Request = .{};
        defer command.finish() catch {};
        if (!try command.start(server, context, user, &lease, .{ .discard_connection = true })) return error.SqlConnectionUnavailable;
        var connection = try @import("../session/connections.zig").loadWithCommand(server.txn_sessions.durable.?, self.alloc, id, self.credential.sessionPrincipal(), server.localSessionNodeId(), @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms, command.attach(context).sql_command_fence);
        defer connection.deinit();
        if (connection.value.state != .idle) return error.ActiveSqlTransaction;
        var coordinator: @import("../session/session.zig").Coordinator = .{ .server = server, .identity = &user, .context = command.attach(context) };
        // Once publication starts, only its persisted control may resume. An
        // uncertain reply is never permission to execute DISCARD a second time.
        coordinator.discard(id, connection.value.revision) catch return error.SqlMutationOutcomeUnknown;
        command.finish() catch return error.SqlMutationOutcomeUnknown;
        var settled = (@import("../session/command_producer.zig").settleCompletion(server, self.alloc, context, id, self.credential.sessionPrincipal()) catch return error.SqlMutationOutcomeUnknown) orelse return error.SqlMutationOutcomeUnknown;
        defer settled.deinit();
        if (settled.value.phase != .idle or settled.value.recovery) return error.SqlMutationOutcomeUnknown;
        self.credential.notification_interest = false;
        self.credential.notification_observation = .{};
    }

    fn runInner(self: *Job) !void {
        if (self.kind == .discard_connection) return self.runDiscard();
        if (self.kind == .notification_connect) return self.runNotificationConnect();
        if (self.kind == .notifications) return self.runNotifications();
        // Every SQL completion boundary, including streamed/routine execution
        // and errors, must observe incoming messages beyond an older cache cut.
        if (self.kind == .execute) self.credential.notification_observation.revision = null;
        try validateRequest(self.request);
        if (self.kind == .probe) {
            self.catalog_stamp = try catalogStamp(self.adapter.server, self.alloc, self.credential, self.request);
            return;
        }
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
        var effective = lease.compiled().*;
        var source_lease: ?@TypeOf(lease) = null;
        defer if (source_lease) |*source| source.release(self.adapter.server.sqlPlanCacheIo());
        var copy_filter: ?catalog.CopyFilter = null;
        var copy_freeze = false;
        if (effective.statement == .insert) if (self.request.copy_source_statement) |source_sql| {
            if (!self.request.copy_input) return error.Forbidden;
            source_lease = try self.adapter.server.sqlPlanCache().acquire(self.adapter.server.sqlPlanCacheIo(), .{ .statement = source_sql, .principal = self.credential.principal, .database = self.request.database orelse "default", .namespace = self.request.namespace orelse "public" }, &self.compile_diagnostic);
            const source = source_lease.?.compiled();
            if (source.statement != .copy_from or source.statement.copy_from.target != null or
                !sameCopyTarget(effective.statement.insert.table, source.statement.copy_from.table)) return error.InvalidSqlParameters;
            copy_freeze = source.statement.copy_from.options.freeze;
            if (source.statement.copy_from.predicate) |predicate| {
                copy_filter = .{ .predicate = predicate, .parameter_count = source.parameter_count, .descriptors = self.request.copy_source_descriptors, .inputs = self.request.copy_source_inputs };
            } else if (!copy_freeze) return error.InvalidSqlParameters;
            effective.uses_current_setting = effective.uses_current_setting or source.uses_current_setting;
            effective.uses_sequences = effective.uses_sequences or source.uses_sequences;
            effective.uses_advisory = effective.uses_advisory or source.uses_advisory;
        };
        const compiled = &effective;
        if (self.request.capture_cursor_authority == .require_select and compiled.statement != .select) return error.UnsupportedSqlExecution;
        const write = self.kind == .execute and compiled.requiresWriteAdmission();
        const server = self.adapter.server;
        var admission = try server.acquireSqlExecution(write);
        defer admission.release();
        preparation.release();
        // Sample before loading grants so a concurrent revocation can only
        // cause a cache miss, never attest a descriptor built under old grants.
        const cursor_revision = if (self.request.capture_cursor_authority != .none and compiled.statement == .select) self.credential.readRevision() else null;
        const authority_revision = if (self.kind == .describe) try self.credential.descriptionRevision() else 0;
        var identity: ?http.AuthenticatedIdentity = try self.credential.identity(server.alloc);
        defer identity.?.deinit(server.alloc);
        var authority = Authority{ .server = server, .credential = self.credential, .identity = &identity, .request = self.request };
        var context = try authority.context();
        var connection_lease: ?@import("../session/connection_admission.zig").Lease = null;
        defer if (connection_lease) |*value| value.release(server.embedding_provider_runtime.io);
        var command_scope: @import("../session/command_execution.zig").Request = .{};
        var command_finished = false;
        defer if (!command_finished) command_scope.finish() catch {};
        errdefer if (command_scope.failed(identity, null) catch true) {
            if (self.request.diagnostics) |diagnostic| diagnostic.transaction_status = .failed;
        };
        const native_connection = if (self.kind == .execute) try self.credential.ensureNativeConnection(server, identity, self.request) else null;
        if (native_connection) |id| {
            connection_lease = try server.sql_connection_admission.acquire(server.alloc, server.embedding_provider_runtime.io, .init(id, self.credential.sessionPrincipal()));
            if (try command_scope.start(server, context, identity, &connection_lease.?, .{ .statement = self.request.statement, .parameters = self.request.parameters, .database = self.request.database }))
                context = command_scope.attach(context);
        }
        var native_adapter = execution.Adapter{
            .backend_pid = self.request.backend_pid,
            .notification_interest = &self.credential.notification_interest,
            .copy_input = self.request.copy_input or compiled.statement == .copy_from,
            .copy_filter = copy_filter,
            .copy_freeze_requested = copy_freeze,
            .copy_has_active_cursor = self.request.copy_has_active_cursor,
            .statement_instant = self.request.statement_instant,
            .sequence_state = self.request.sequence_state,
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
            .connection_id = native_connection,
            .connection_mutation = self.credential.transaction_guard,
            .session_namespace = self.request.session_namespace,
            .setting_overlay = self.request.setting_overlay,
            .setting_baseline = self.request.setting_baseline,
            .setting_overlay_source = .connection,
            .expected_setting_epoch = self.request.setting_epoch,
        };
        defer native_adapter.releaseExecutionOwners();
        if (native_connection) |id| {
            var connection = try @import("../session/connections.zig").loadWithCommand(server.txn_sessions.durable.?, server.alloc, id, self.credential.sessionPrincipal(), server.localSessionNodeId(), @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms, context.sql_command_fence);
            defer connection.deinit();
            native_adapter.connection_revision = connection.value.revision;
            native_adapter.connection_generation = connection.value.generation;
        }
        if (self.request.diagnostics) |diagnostic| {
            if (diagnostic.notice_allocator != null) native_adapter.notice_sink = .{ .ptr = diagnostic, .emit = wire.Diagnostic.emitNotice, .reserve = wire.Diagnostic.reserveNotices };
        }
        var captured_bindings: std.ArrayList(@import("../session/prepared.zig").Binding) = .empty;
        if (self.kind == .describe) native_adapter.collect_prepared_bindings = &captured_bindings else native_adapter.prepared_bindings = try viewBindings(self.alloc, self.request.binding_guard);
        // describe/execute capture a single settings view. The adapter's
        // loader checks expected_setting_epoch on that exact captured epoch.
        var guarded = GuardedCatalog{
            .native = native_adapter.backend(),
            .authority = &authority,
            .revision = &native_adapter.revision,
            .expected_guard = self.request.binding_guard,
        };
        var enum_views: std.AutoHashMapUnmanaged(*const catalog.enum_values.View, *const catalog.enum_values.View) = .empty;
        var domain_views: @import("antfly_local_sources").sql_scalar.DomainViews = .empty;
        self.request.parameter_descriptors = try @import("../catalog/type_schema.zig").parameterDescriptorsLeaky(server, self.alloc, identity, context, self.request.parameter_descriptors, self.request.parameter_oids);
        var settings_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer settings_arena.deinit();
        const setting_capture = try guarded.settingCapture(settings_arena.allocator(), &native_adapter, compiled);
        const scratch = self.request.scratch_allocator orelse self.alloc;
        try guarded.indexLargeGuard(scratch);
        defer guarded.indexed_entries.deinit(scratch);
        if (self.kind == .describe) {
            switch (compiled.statement) {
                .begin, .commit, .rollback, .savepoint, .rollback_to_savepoint, .release_savepoint, .set_constraints, .set_transaction, .prepared_transaction, .notification => {
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
            var probe: DescriptionProbe = .{ .resolution_context = &native_adapter.context, .server = server, .credential = self.credential, .request = self.request };
            guarded.probe = &probe;
            var describe_backend = guarded.backend();
            describe_backend.parameter_descriptor_hints = self.request.parameter_descriptors;
            describe_backend.setting_capture = setting_capture;
            if (compiled.statement == .copy_from and self.request.parameter_descriptors.len == 0 and hints.len != 0) {
                const descriptors = try self.alloc.alloc(wire.Parameter, hints.len);
                for (descriptors, hints) |*out, hint| out.* = .{ .kind = hint };
                describe_backend.parameter_descriptor_hints = descriptors;
            }
            const describe_freeze = compiled.statement == .copy_from and compiled.statement.copy_from.options.freeze;
            if (describe_freeze) if (self.request.session_id) |id| {
                native_adapter.active_transaction = try @import("../../distributed_txn.zig").parseTxnIdHex(id);
                native_adapter.active_private_mutation = true;
                native_adapter.copy_freeze_description = true;
            };
            const copy_input = if (compiled.statement == .copy_from) try describe_sql.copyWrite(self.alloc, describe_backend, compiled) else null;
            var description = try describe_sql.describe(self.request.scratch_allocator orelse self.alloc, if (copy_input) |input| input.binding_backend else describe_backend, if (copy_input) |*input| &input.compiled else compiled, if (copy_input != null) &.{} else hints);
            defer description.deinit();
            // COPY lowers to a one-target INSERT. Its binding already carries
            // the catalog revision even when no general read snapshot was pinned.
            const copy_revision = if (copy_input != null)
                ((description.binding.table orelse return error.InvalidSqlBackendResponse).scope orelse return error.InvalidSqlBackendResponse).revision
            else
                null;
            const ddl_catalog = @import("../catalog/catalog.zig");
            const guard = if (ddl_catalog.bindsExisting(compiled.statement, native_adapter.database, native_adapter.namespace)) blk: {
                try probe.sample(self.alloc);
                const bound = try ddl_catalog.describeGuard(server, identity, native_adapter.context, native_adapter.database, native_adapter.namespace, self.alloc, compiled.statement, native_adapter.ddl_search_path);
                break :blk bound.bytes;
            } else if (hasViewBindings(captured_bindings.items)) try encodeViewBindings(self.alloc, captured_bindings.items) else try statementBindingGuard(self.alloc, native_adapter.revision orelse copy_revision, description.binding);
            const bound_guard = if (guard) |bytes| blk: {
                if (probe.catalog) |stamp| {
                    defer self.alloc.free(bytes);
                    break :blk try std.mem.concat(self.alloc, u8, &.{ incarnation_prefix, &stamp.relation_epoch.incarnation, bytes });
                }
                break :blk bytes;
            } else null;
            const columns = try self.alloc.alloc(wire.Column, description.binding.columns.len);
            for (columns, description.binding.columns) |*out, column| out.* = .{ .name = try self.alloc.dupe(u8, column.name), .type = try wireType(column.type), .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .enum_view = try @import("antfly_local_sources").sql_scalar.ownEnumView(self.alloc, &enum_views, column.enum_view), .domain_view = try @import("antfly_local_sources").sql_scalar.ownDomainView(self.alloc, &domain_views, column.domain_view), .temporal_kind = column.temporal_kind };
            const parameter_types = try self.alloc.alloc(wire.Type, description.binding.parameter_types.len);
            for (parameter_types, description.binding.parameter_types) |*out, kind| out.* = if (kind) |value| try wireType(value) else .unknown;
            const copy_columns = if (copy_input) |input| blk: {
                const target = description.binding.table orelse return error.InvalidSqlBackendResponse;
                if (describe_freeze and self.request.copy_freeze_admission) {
                    const txn_id = native_adapter.active_transaction orelse return error.SqlCopyFreezeGenerationRequired;
                    _ = try @import("../execution/private_relation.zig").freezeOrigin(&native_adapter, self.alloc, target, txn_id);
                }
                const selected = try self.alloc.alloc(wire.Column, input.compiled.statement.insert.columns.len);
                for (selected, input.compiled.statement.insert.columns) |*out, name| {
                    const column = try target.column(name);
                    if (column.generated) return error.SqlGeneratedColumnWrite;
                    out.* = .{ .defaulted = column.defaulted, .name = try self.alloc.dupe(u8, name), .type = try wireType(column.type), .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .enum_view = try @import("antfly_local_sources").sql_scalar.ownEnumView(self.alloc, &enum_views, column.enum_view), .domain_view = try @import("antfly_local_sources").sql_scalar.ownDomainView(self.alloc, &domain_views, column.domain_view), .temporal_kind = column.temporal_kind };
                }
                break :blk selected;
            } else null;
            const owned_descriptors = try self.alloc.dupe(wire.Parameter, description.binding.parameter_descriptors);
            for (owned_descriptors) |*descriptor| {
                descriptor.enum_view = try @import("antfly_local_sources").sql_scalar.ownEnumView(self.alloc, &enum_views, descriptor.enum_view);
                if (try @import("antfly_local_sources").sql_scalar.ownDomainView(self.alloc, &domain_views, descriptor.domainView())) |view| descriptor.domain_binding = &view.type_binding;
            }
            const filter_descriptors: []const wire.Parameter = if (description.binding.copy_filter) |filter| try self.alloc.dupe(wire.Parameter, filter.parameter_descriptors) else &.{};
            for (@constCast(filter_descriptors)) |*descriptor| {
                descriptor.enum_view = try @import("antfly_local_sources").sql_scalar.ownEnumView(self.alloc, &enum_views, descriptor.enum_view);
                if (try @import("antfly_local_sources").sql_scalar.ownDomainView(self.alloc, &domain_views, descriptor.domainView())) |view| descriptor.domain_binding = &view.type_binding;
            }
            const filter_types = try self.alloc.alloc(wire.Type, filter_descriptors.len);
            for (filter_descriptors, filter_types) |descriptor, *out| out.* = if (descriptor.kind) |kind| try wireType(kind) else .unknown;
            self.description = .{
                .is_select = compiled.statement == .select,
                .stamp = if (probe.sampled and probe.catalog == null) null else .{ .catalog = probe.catalog, .authority = authority_revision, .reusable = !compiled.uses_current_setting and !describe_freeze },
                .cursor_readable = compiled.statement == .select,
                .copy_out = if (compiled.statement == .copy_to and compiled.statement.copy_to.target == null) try compiled.statement.copy_to.options.clone(self.alloc) else null,
                .copy_in = if (copy_columns != null and compiled.statement.copy_from.target == null) .{ .freeze_admitted = describe_freeze and self.request.copy_freeze_admission, .source_statement = if (compiled.statement.copy_from.predicate != null or describe_freeze) try self.alloc.dupe(u8, self.request.statement) else null, .table = try cloneName(self.alloc, compiled.statement.copy_from.table), .columns = copy_columns.?, .options = try compiled.statement.copy_from.options.clone(self.alloc) } else null,
                .columns = if (@import("../copy/server.zig").target(compiled.statement) != null) &.{} else columns,
                .parameter_types = if (copy_input != null) filter_types else parameter_types,
                .parameter_descriptors = if (copy_input != null) filter_descriptors else owned_descriptors,
                .binding_guard = bound_guard,
                .setting_epoch = if (description.settings) |view| view.epoch else null,
            };
            return;
        }
        if (@import("../catalog/catalog.zig").bindsExisting(compiled.statement, native_adapter.database, native_adapter.namespace)) {
            if (self.request.binding_guard) |bytes| native_adapter.ddl_guard = try @import("../catalog/ddl_binding.zig").Guard.decode((try splitBinding(bytes)).targets);
        }
        const parameters = if (self.request.parameter_cells != null) self.request.parameters else try normalizeParameters(self.alloc, self.request.parameters, self.request.parameter_types);
        var execution_backend = guarded.backend();
        var opening: CursorAdmission = undefined;
        if (cursor_revision) |revision| {
            const tenure = if (self.request.session_id) |hex|
                try server.txn_sessions.sqlReaderTenure(self.alloc, try @import("../../distributed_txn.zig").parseTxnIdHex(hex), self.credential.sessionPrincipal(), server.localSessionNodeId())
            else
                null;
            opening = .{ .lease_epoch = tenure, .out = self.alloc, .adapter = self.adapter, .native = &native_adapter, .credential = self.credential, .admitted = revision };
            opening.install(&execution_backend);
        }
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
        if (native_adapter.result_session_id != null) {
            self.credential.transaction_guard = native_adapter.admitted_mutation orelse return error.SessionLeaseLost;
        } else self.credential.transaction_guard = null;
        if (self.request.diagnostics) |diagnostic| diagnostic.transaction_status = @fromBackingInt(@backingInt(native_adapter.transaction_status));
        errdefer if (native_adapter.result_session_id) |id| {
            _ = command_scope.failed(identity, null) catch true;
            if (context.sql_command_fence == null) {
                const transaction_id = @import("../../distributed_txn.zig").parseTxnIdHex(&id) catch unreachable;
                if (native_adapter.admitted_mutation) |guard| {
                    if (std.mem.eql(u8, &guard.id, &transaction_id)) server.txn_sessions.setSqlFailed(server.alloc, guard, true) catch {};
                }
            }
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
        for (result.output.columns, 0..) |column, index| if (column.type == .datetime and (column.temporal_kind orelse .timestamp_tz) == .timestamp_tz) {
            for (0..result.output.rows.len) |row_index| {
                const row = result.mutableRow(row_index);
                row[index] = try datetimeResult(self.alloc, row[index]);
            }
        };
        const columns = try self.alloc.alloc(wire.Column, result.output.columns.len);
        for (columns, result.output.columns) |*out, column| out.* = .{ .name = column.name, .type = try wireType(column.type), .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .enum_view = try @import("antfly_local_sources").sql_scalar.ownEnumView(self.alloc, &enum_views, column.enum_view), .domain_view = try @import("antfly_local_sources").sql_scalar.ownDomainView(self.alloc, &domain_views, column.domain_view), .temporal_kind = column.temporal_kind };
        const native_owner = if (result.output.native_rows != null) try result.state.arena.allocator().create(NativeResultCells) else null;
        if (native_owner) |owner| owner.* = .{ .output = result.output };
        native_adapter.releaseExecutionOwners();
        command_scope.finish() catch return error.SqlMutationOutcomeUnknown;
        command_finished = true;
        const cursor_authority: ?wire.CursorAuthority = if (cursor_revision != null) authority_result: {
            const capture = opening.captured orelse return error.InvalidSqlBackendResponse;
            const pinned = try self.alloc.create(RetainedRead);
            pinned.* = capture;
            break :authority_result .{ .ptr = pinned, .validate = RetainedRead.validateBuffered };
        } else null;
        self.result = .{
            .cursor_authority = cursor_authority,
            .columns = columns,
            .cells = if (native_owner) |owner| .{ .context = owner, .count = result.output.rowCount(), .width = columns.len, .read = NativeResultCells.read } else null,
            .rows = result.output.rows,
            .sql_nulls = result.output.sql_nulls,
            .rows_affected = result.output.rows_affected,
            .notices = if (native_adapter.result_ddl_notices.len != 0) native_adapter.result_ddl_notices else if (self.request.diagnostics) |diagnostic| if (diagnostic.notice_allocator != null) &.{} else result.output.notices else result.output.notices,
            .command_tag = try commandTag(self.alloc, result.output.command_tag, result.output.completion_rows orelse result.output.rowCount(), result.output.rows_affected),
            .mutation_outcome = if (result.output.mutation_outcome) |outcome| switch (outcome) {
                inline else => |kind| @field(wire.MutationOutcome, @tagName(kind)),
            } else null,
            .transaction_id = native_adapter.outcome_transaction_id,
            .ddl_receipt_json = if (result.output.ddl_receipt) |receipt| try std.json.Stringify.valueAlloc(self.alloc, receipt, .{ .emit_null_optional_fields = false }) else null,
            .catalog_receipt_json = if (result.output.catalog_receipt) |receipt| try std.json.Stringify.valueAlloc(self.alloc, receipt, .{ .emit_null_optional_fields = false }) else null,
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
        normalized.?[index] = .{ .string = try @import("antfly_local_sources").datetime.formatDateTimeSignedNsAlloc(alloc, try wire_values.timestampNanos(value)) };
    }
    return normalized orelse input;
}

fn datetimeResult(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    return wire_values.timestampResult(alloc, value) catch |err| switch (err) {
        error.InvalidResult => error.InvalidSqlBackendResponse,
        else => err,
    };
}

fn wireType(kind: ast.ColumnType) !wire.Type {
    return switch (kind) {
        inline else => |tag| @field(wire.Type, @tagName(tag)),
    };
}

fn commandTag(alloc: std.mem.Allocator, command: []const u8, rows: u64, affected: u64) ![]const u8 {
    if (std.mem.eql(u8, command, "DDL PENDING")) return alloc.dupe(u8, command);
    if (std.mem.eql(u8, command, "SELECT")) return std.fmt.allocPrint(alloc, "SELECT {d}", .{rows});
    if (std.mem.eql(u8, command, "COPY")) return std.fmt.allocPrint(alloc, "COPY {d}", .{affected});
    if (std.mem.eql(u8, command, "INSERT")) return std.fmt.allocPrint(alloc, "INSERT 0 {d}", .{affected});
    if (std.mem.eql(u8, command, "UPDATE") or std.mem.eql(u8, command, "DELETE") or std.mem.eql(u8, command, "MERGE")) return std.fmt.allocPrint(alloc, "{s} {d}", .{ command, affected });
    if (std.mem.eql(u8, command, "REFRESH MATERIALIZED VIEW")) return alloc.dupe(u8, command);
    if (std.mem.startsWith(u8, command, "CREATE ") or std.mem.startsWith(u8, command, "DROP ") or std.mem.startsWith(u8, command, "ALTER ")) return alloc.dupe(u8, command);
    inline for (.{ "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "SET CONSTRAINTS", "TRUNCATE TABLE", "SECURITY LABEL", "LISTEN", "NOTIFY", "UNLISTEN", "LOCK TABLE", "PREPARE TRANSACTION", "COMMIT PREPARED", "ROLLBACK PREPARED" }) |tag| if (std.mem.eql(u8, command, tag)) return alloc.dupe(u8, command);
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
            var owner: NativeResultCells = .{ .output = result.output };
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

const NativeResultCells = @import("wire_cells.zig").OutputCells;

fn releaseResult(raw: *anyopaque) void {
    var result = native.Result{ .state = @ptrCast(@alignCast(raw)), .output = undefined };
    result.deinit();
}

fn requestContext(credential: *const Credential, request: *const wire.Request) operation.RequestContext {
    const deadline = std.math.cast(u64, request.deadline.raw.nanoseconds);
    return .{
        .deadline_ns = deadline,
        .deadline_io = if (deadline != null) io_abi.Borrow.init(&request.io) else null,
        .fanout_io = io_abi.Borrow.init(&request.io),
        .cancellation = operation.CancellationToken.fromAtomic(request.cancel_requested),
        .sql_selinux_ingress = request.selinux_ingress,
        .principal = .{ .kind = .user, .subject = credential.username },
        .destination_authorization_principal = credential.principal,
    };
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
        var native_context = requestContext(self.credential, &self.request);
        if (self.request.binding_guard) |guard| native_context.bound_catalog_incarnation = (try splitBinding(guard)).incarnation;
        native_context.sql_completion = if (self.request.completion != null) .{ .ptr = self, .prepare_transaction = prepareTransaction, .prepare_metadata = prepareMetadata, .reject_metadata = rejectMetadata } else null;
        native_context.row_policy_credential = self.identity;
        native_context.table_write_authorization = .{ .ptr = self, .allows = allowsWrite };
        return native_context.platformDeadline() catch |err| return requestError(self.request, err);
    }

    fn prepareTransaction(raw: *anyopaque, id: [16]u8) !void {
        const self: *Authority = @ptrCast(@alignCast(raw));
        try @import("../session/completion.zig").prepareTransaction(self.credential.trusted_server orelse return error.DurabilityUnavailable, self.request.completion_allocator orelse return error.InvalidArgument, self.request.completion.?, id);
    }
    fn prepareMetadata(raw: *anyopaque, owner: operation.SQLCompletionOwner) !void {
        const self: *Authority = @ptrCast(@alignCast(raw));
        try @import("../session/completion.zig").prepareMetadata(self.credential.trusted_server orelse return error.DurabilityUnavailable, self.request.completion_allocator orelse return error.InvalidArgument, self.request.completion.?, owner);
    }
    fn rejectMetadata(raw: *anyopaque, owner: operation.SQLCompletionOwner) !void {
        const self: *Authority = @ptrCast(@alignCast(raw));
        try @import("../session/completion.zig").rejectMetadata(self.credential.trusted_server orelse return error.DurabilityUnavailable, self.request.completion_allocator orelse return error.InvalidArgument, self.request.completion.?, owner);
    }
    fn importedSnapshot(self: *const Authority) ?ImportedSnapshot {
        const snapshot = self.credential.imported_snapshot orelse return null;
        const encoded = self.request.session_id orelse return null;
        const id = @import("../../distributed_txn.zig").parseTxnIdHex(encoded) catch return null;
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
    const system = @import("../../pgwire_catalog.zig");

    native: catalog.Backend,
    authority: *Authority,
    revision: *const ?u64,
    expected_guard: ?[]const u8,
    allow_revision_advance: bool = false,
    probe: ?*DescriptionProbe = null,
    indexed_entries: BindingEntries = .empty,

    fn indexLargeGuard(self: *GuardedCatalog, a: std.mem.Allocator) !void {
        const envelope = self.expected_guard orelse return;
        const bound = try splitBinding(envelope);
        const bytes = bound.targets;
        self.expected_guard = bytes;
        if (bound.incarnation == null) self.allow_revision_advance = false;
        if (@import("../catalog/ddl_binding.zig").Guard.isDdl(bytes) or std.mem.startsWith(u8, bytes, view_binding_magic)) {
            self.allow_revision_advance = false;
            return;
        }
        var targets = try GuardTargets.init(bytes);
        if (targets.count <= 8) return;
        errdefer self.indexed_entries.deinit(a);
        try self.indexed_entries.ensureTotalCapacity(a, targets.count);
        while (try targets.next()) |target| {
            self.indexed_entries.putAssumeCapacity(.{ .name = target.name, .id = target.id, .version = target.version, .column_lineage = target.column_lineage, .graph_revision = target.graph_revision }, {});
        }
    }

    fn verify(self: *GuardedCatalog, table: catalog.Table) !void {
        const bytes = self.expected_guard orelse return;
        if (std.mem.startsWith(u8, bytes, view_binding_magic) or @import("../catalog/ddl_binding.zig").Guard.isDdl(bytes)) return;
        const revision = if (self.allow_revision_advance)
            std.mem.readInt(u64, bytes[0..8], .little)
        else
            self.revision.* orelse (table.scope orelse return error.InvalidSqlBackendResponse).revision;
        if (self.indexed_entries.count() == 0) return verifyBindingGuard(bytes, revision, table);
        if (std.mem.readInt(u64, bytes[0..8], .little) != revision or !self.indexed_entries.contains(BindingEntry.of(table))) return error.CatalogGenerationChanged;
    }

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
        result.vtable = &.{ .resolve_read = resolveRead, .resolve_graph = resolveGraph, .resolve = resolve, .scan = scan, .supports_scan_order = true, .open_scan = openScan, .open_statement = openStatement, .mutate = mutate, .mutate_prepared = mutatePrepared, .prepare_mutations = prepareMutations, .assign_document_revisions = assignDocumentRevisions, .open_mutation_preparation = openMutationPreparation, .resolve_conflict_owners = resolveConflictOwners, .generate_row_id = generateRowId, .checkpoint = checkpoint, .ddl = ddl };
        return result;
    }
    fn resolveGraph(raw: *anyopaque, alloc: std.mem.Allocator, definition: ast.GraphTable) !catalog.Table {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (self.authority.importedSnapshot() != null) return error.UnsupportedSqlShape;
        if (self.probe) |probe| try probe.sample(alloc);
        const resolve_graph = self.native.vtable.resolve_graph orelse return error.UnsupportedSqlExecution;
        const table = try resolve_graph(self.native.ptr, alloc, definition);
        try self.verify(table);
        if (self.expected_guard != null) for (table.graph.?.dependencies) |dependency| {
            try self.verify(dependency);
        };
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
            for (binding.dependencies) |dependency| try self.checkRead(dependency);
        } else try self.checkRead(table);
    }
    fn copyDependencies(alloc: std.mem.Allocator, tables: []const catalog.Table) ![]catalog.Table {
        if (tables.len == 0) return &.{};
        const copied = try alloc.alloc(catalog.Table, tables.len);
        errdefer alloc.free(copied);
        var count: usize = 0;
        errdefer for (copied[0..count]) |table| alloc.free(table.physical_name);
        for (tables, copied) |table, *out| {
            out.* = table;
            out.physical_name = try alloc.dupe(u8, table.physical_name);
            count += 1;
        }
        return copied;
    }
    fn dependencies(alloc: std.mem.Allocator, table: catalog.Table) ![]catalog.Table {
        const binding = table.graph orelse return &.{};
        return copyDependencies(alloc, binding.dependencies);
    }
    fn freeDependencies(alloc: std.mem.Allocator, tables: []catalog.Table) void {
        if (tables.len == 0) return;
        for (tables) |table| alloc.free(table.physical_name);
        alloc.free(tables);
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
        if (self.probe) |probe| try probe.sample(alloc);
        const table = (try system.resolve(name, action, self.authority.request.database orelse "default")) orelse
            try resolveOnPath(self.native, alloc, name, action, if (self.authority.request.search_path) |*path| path else null);
        if (!system.owns(table)) try self.verify(table);
        return table;
    }
    fn resolveRead(raw: *anyopaque, alloc: std.mem.Allocator, name: ast.Name) !catalog.ReadRelation {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        try self.authority.credential.validate();
        if (self.probe) |probe| try probe.sample(alloc);
        if (try system.resolve(name, .read, self.authority.request.database orelse "default")) |table| return .{ .table = table };
        const relation = try resolveReadOnPath(self.native, alloc, name, if (self.authority.request.search_path) |*path| path else null);
        if (relation == .table) try self.verify(relation.table);
        return relation;
    }
    fn scanPinned(self: *GuardedCatalog, alloc: std.mem.Allocator, snapshot: ImportedSnapshot, table: catalog.Table, request: catalog.Scan) !catalog.Page {
        if (request.search != null or request.index_equality != null or request.index_range != null or
            request.primary_key != null or request.before != null or request.physical_selection != null or request.row_refs != null or
            request.include_document or request.include_primary_digest or request.conditions.len != 0 or request.order.len != 0)
            return error.UnsupportedSqlShape;
        const authority = self.authority;
        const server = authority.server orelse return error.ChangefeedCapabilityUnsupported;
        const context = try @import("../../../pgwire/retained_stream.zig").metadataContext(authority.request);
        const slots = try CatalogReplicationSlots.open(server, .{ .context = authority.credential, .read = Adapter.replicationIdentity }, alloc, authority.request.database orelse "default", context);
        return slots.readExportedSnapshotCatalogPage(alloc, &snapshot.token.name(), table, request.after, @min(request.limit, 256), 4 * 1024 * 1024, context) catch |err| {
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
            return system.scanClasses(alloc, request, try self.catalogClasses(arena.allocator(), request.conditions, true));
        }
        if (system.isNamespaces(table)) {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            return system.scanNamespaces(alloc, request, try self.catalogNamespaces(arena.allocator(), request.conditions));
        }
        if (system.isAttributes(table)) {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            return system.scanAttributes(alloc, request, try self.catalogAttributes(arena.allocator(), request.conditions));
        }
        if (system.isIndexes(table)) {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            return system.scanIndexes(alloc, request, try self.catalogIndexes(a, try self.catalogClasses(a, &.{}, false), request.conditions));
        }
        if (system.owns(table)) return system.scan(alloc, request);
        if (self.authority.importedSnapshot()) |snapshot| return self.scanPinned(alloc, snapshot, table, request);
        return self.native.vtable.scan(self.native.ptr, alloc, table, request);
    }
    fn checkRead(self: *GuardedCatalog, table: catalog.Table) !void {
        try checkpoint(self);
        try self.authority.credential.validate();
        if (table.read_authority != null) return @import("../execution/read_authority.zig").check(table);
        if (!(try http.tablePermissionCurrentlyAllowed(self.authority.identity.*, table.physical_name, .read))) return error.Forbidden;
    }
    const ReadCursor = struct {
        alloc: std.mem.Allocator,
        guard: *GuardedCatalog,
        table: catalog.Table,
        is_graph: bool = false,
        graph_dependencies: []catalog.Table = &.{},
        inner: catalog.Cursor,

        fn checkAuthority(self: *@This()) !void {
            if (!self.is_graph) return self.guard.checkRead(self.table);
            try GuardedCatalog.checkpoint(self.guard);
            if (!try http.resourcePermissionCurrentlyAllowed(self.guard.authority.identity.*, .graph, self.table.physical_name, .read)) return error.Forbidden;
            for (self.graph_dependencies) |table| try self.guard.checkRead(table);
        }
        fn releaseFields(self: *@This()) void {
            self.alloc.free(self.table.physical_name);
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
            if (enabled) try @import("../execution/group_guard.zig").wrap(self.alloc, grouped, .{ .ptr = self, .call = checkGroups });
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
            try self.checkAuthority();
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
                const name = try alloc.dupe(u8, self.table.physical_name);
                errdefer alloc.free(name);
                var table = self.table;
                table.physical_name = name;
                const deps = try GuardedCatalog.copyDependencies(alloc, self.graph_dependencies);
                errdefer GuardedCatalog.freeDependencies(alloc, deps);
                const owner = try alloc.create(ReadCursor);
                owner.* = .{ .alloc = alloc, .guard = self.guard, .table = table, .is_graph = self.is_graph, .graph_dependencies = deps, .inner = child };
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
            const items = self.catalogClasses(arena.allocator(), request.conditions, true) catch |err| {
                arena.deinit();
                return err;
            };
            return system.openRows(system.Class, request, arena, items);
        }
        if (system.isNamespaces(table)) {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            return system.openNamespaces(alloc, request, try self.catalogNamespaces(arena.allocator(), request.conditions));
        }
        if (system.isAttributes(table)) {
            var arena = std.heap.ArenaAllocator.init(alloc);
            const items = self.catalogAttributes(arena.allocator(), request.conditions) catch |err| {
                arena.deinit();
                return err;
            };
            return system.openRows(system.Attribute, request, arena, items);
        }
        if (system.isIndexes(table)) {
            var arena = std.heap.ArenaAllocator.init(alloc);
            const items = self.openIndexRows(arena.allocator(), request.conditions) catch |err| {
                arena.deinit();
                return err;
            };
            return system.openRows(system.PrimaryIndex, request, arena, items);
        }
        return system.open(alloc, request);
    }
    fn publicationPage(self: *GuardedCatalog, alloc: std.mem.Allocator, request: catalog.Scan) !catalog.Page {
        const name = (try system.keyedName(request, "pubname")) orelse return .{ .rows = &.{} };
        if (name.len == 0 or name.len > 63 or std.mem.indexOfScalar(u8, name, 0) != null or request.limit == 0 or request.after != null) return .{ .rows = &.{} };
        const server = self.authority.server orelse return error.UnsupportedReplicationMode;
        const port = server.source.pg_publications orelse return .{ .rows = &.{} };
        const identity = self.authority.identity.* orelse return error.Forbidden;
        const owner = try @import("../../changefeed_public.zig").durableOwner(identity);
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var context = try self.authority.context();
        const database = try CatalogReplicationSlots.readDatabase(server, a, self.authority.request.database orelse "default", context);
        context.principal = .{ .kind = .internal, .subject = "pg-publication-maintenance" };
        var record = (try port.readRecord(a, database, name, context)) orelse return .{ .rows = &.{} };
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
            else => {
                std.log.debug("PostgreSQL slot catalog read failed err={s}", .{@errorName(err)});
                return err;
            },
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
            const transaction = try @import("../../distributed_txn.zig").parseTxnIdHex(encoded);
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

    fn catalogClasses(self: *GuardedCatalog, a: std.mem.Allocator, conditions: []const catalog.Condition, include_indexes: bool) ![]const system.Class {
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
                const page = try std.json.parseFromSliceLeaky(@import("../../../system_catalog/projection.zig").TableListing, scratch, bytes, .{});
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
                    const schema = if (entry.table.schema_json.len == 0) null else try std.json.parseFromSliceLeaky(
                        struct { replica_identity: ?enum { default, full } = null },
                        scratch,
                        entry.table.schema_json,
                        .{ .ignore_unknown_fields = true },
                    );
                    const item: system.Class = .{
                        .oid = 16384 + std.mem.readInt(u32, digest[0..4], .big) % (std.math.maxInt(u32) - 16384),
                        .name = entry.name,
                        .namespace = namespace.oid,
                        .replica_identity = if (schema != null and schema.?.replica_identity == .full) "f" else "d",
                    };
                    if (include_indexes or try item.matches(conditions)) {
                        var retained = item;
                        retained.name = try a.dupe(u8, item.name);
                        retained.logical = .{ .namespace = namespace.name, .id = entry.table.table_id, .revision = page.revision };
                        try result.append(a, retained);
                    }
                }
                const next = page.next_after orelse break;
                if (list.after) |prior| if (std.mem.eql(u8, prior, next) and list.after_table_id == page.next_table_id) return error.InvalidSqlBackendResponse;
                list.after = try a.dupe(u8, next);
                list.after_table_id = page.next_table_id;
            }
        }
        if (include_indexes) {
            const indexes = try self.catalogIndexes(a, result.items, &.{});
            for (indexes) |index| try result.append(a, .{ .oid = index.index, .name = index.name, .namespace = index.namespace, .kind = "i" });
            var kept: usize = 0;
            for (result.items) |item| {
                if (!try item.matches(conditions)) continue;
                result.items[kept] = item;
                kept += 1;
            }
            result.shrinkRetainingCapacity(kept);
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

    const CatalogDefinition = struct { name: []const u8, schema: @import("antfly_local_sources").system_catalog_domain.QueryDefinition };

    fn catalogDefinition(self: *GuardedCatalog, a: std.mem.Allocator, class: system.Class) !?CatalogDefinition {
        const logical = class.logical orelse return null;
        const server = self.authority.server orelse return error.InvalidSqlBackendResponse;
        const context = try self.authority.context();
        const domain = @import("antfly_local_sources").system_catalog_domain;
        const target: domain.Target = .{ .database = self.authority.request.database orelse "default", .namespace = logical.namespace, .table = class.name };
        const bytes = try server.source.systemCatalog(a, context, .{ .resolve_many = .{
            .targets = &.{target},
            .include_query_definitions = true,
            .expected_revision = logical.revision,
        } });
        const snapshot = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{});
        if (snapshot.tables.len != 1 or snapshot.revision != logical.revision) return error.CatalogGenerationChanged;
        const current = snapshot.tables[0] orelse return error.CatalogGenerationChanged;
        if (current.table_id != logical.id) return error.CatalogGenerationChanged;
        try current.requireAccess(context.native_service);
        const schema = current.query_definition orelse return error.InvalidSqlBackendResponse;
        if (schema.table_id != logical.id) return error.CatalogGenerationChanged;
        return .{ .name = current.name, .schema = schema };
    }

    fn catalogSchema(self: *GuardedCatalog, a: std.mem.Allocator, definition: CatalogDefinition) !catalog.Table {
        const server = self.authority.server orelse return error.InvalidSqlBackendResponse;
        return server.sql_schema_cache.resolve(server.sqlPlanCacheIo(), a, definition.schema.schema_json, definition.schema.table_id, definition.name);
    }

    fn openIndexRows(self: *GuardedCatalog, alloc: std.mem.Allocator, conditions: []const catalog.Condition) ![]const system.PrimaryIndex {
        return self.catalogIndexes(alloc, try self.catalogClasses(alloc, &.{}, false), conditions);
    }

    fn catalogIndexes(self: *GuardedCatalog, alloc: std.mem.Allocator, classes: []const system.Class, conditions: []const catalog.Condition) ![]const system.PrimaryIndex {
        var indexes: std.ArrayList(system.PrimaryIndex) = .empty;
        for (classes) |class| {
            if (class.logical == null) continue;
            try checkpoint(self);
            var arena = std.heap.ArenaAllocator.init(if (self.authority.server) |server| server.alloc else alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const definition = (try self.catalogDefinition(a, class)) orelse continue;
            var schema = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(a, definition.schema.schema_json);
            defer schema.deinit(a);
            const constraints = schema.unique_constraints orelse continue;
            for (constraints.value) |constraint| {
                if (!(constraint.primary orelse false)) continue;
                const names = constraint.columns orelse return error.InvalidSqlBackendResponse;
                const table = try self.catalogSchema(a, definition);
                const keys = try a.alloc(i16, names.len);
                for (names, keys) |name, *key| {
                    key.* = for (table.columns, 1..) |column, ordinal| {
                        if (std.mem.eql(u8, column.name, name)) break std.math.cast(i16, ordinal) orelse return error.SqlProgramLimitExceeded;
                    } else return error.InvalidSqlBackendResponse;
                }
                const digest = publication_control.hash(.{ .domain = "antfly:pg-primary-index-oid:v1", .table = definition.schema.table_id, .name = constraint.name });
                var item: system.PrimaryIndex = .{
                    .relation = class.oid,
                    .index = 16384 + std.mem.readInt(u32, digest[0..4], .big) % (std.math.maxInt(u32) - 16384),
                    .namespace = class.namespace,
                    .name = constraint.name,
                    .keys = keys,
                };
                if (!try item.matches(conditions)) continue;
                item.name = try alloc.dupe(u8, item.name);
                item.keys = try alloc.dupe(i16, item.keys);
                try indexes.append(alloc, item);
            }
        }
        return indexes.items;
    }

    fn catalogAttributes(self: *GuardedCatalog, alloc: std.mem.Allocator, conditions: []const catalog.Condition) ![]const system.Attribute {
        const database = self.authority.request.database orelse "default";
        const classes = try self.catalogClasses(alloc, &.{}, false);
        var attributes: std.ArrayList(system.Attribute) = .empty;
        var visited: usize = 0;
        for (classes) |class| {
            try checkpoint(self);
            var needs_class = true;
            for (conditions) |condition| {
                if (condition.op != .eq or !std.mem.eql(u8, condition.column, "attrelid")) continue;
                if (condition.value != .integer or condition.value.integer != class.oid) needs_class = false;
            }
            if (!needs_class) continue;
            var arena = std.heap.ArenaAllocator.init(if (self.authority.server) |server| server.alloc else alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const table = if (try self.catalogDefinition(a, class)) |definition|
                try self.catalogSchema(a, definition)
            else
                (try system.resolve(.{ .namespace = "pg_catalog", .table = class.name }, .read, database)) orelse return error.InvalidSqlBackendResponse;
            for (table.columns, 1..) |column, ordinal| {
                visited += 1;
                if (visited > 32768) return error.SqlProgramLimitExceeded;
                var item: system.Attribute = .{ .relation = class.oid, .ordinal = std.math.cast(u16, ordinal) orelse return error.SqlProgramLimitExceeded, .column = column };
                if (!try item.matches(a, conditions)) continue;
                item.column.name = try alloc.dupe(u8, column.name);
                item.column.path = item.column.name;
                try attributes.append(alloc, item);
            }
        }
        return attributes.items;
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
            const logical = try @import("../../../system_catalog/routes.zig").resourceNameAlloc(a, .{ .kind = .namespace, .database = database, .name = resource.name });
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
        var retained = table;
        retained.physical_name = name;
        const deps = try dependencies(alloc, table);
        errdefer freeDependencies(alloc, deps);
        const cursor = try alloc.create(ReadCursor);
        cursor.* = .{ .alloc = alloc, .guard = self, .table = retained, .is_graph = table.graph != null, .graph_dependencies = deps, .inner = inner };
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

    fn assignDocumentRevisions(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation, output: []?catalog.DocumentRevision) !void {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.checkWrite(table);
        const assign = self.native.vtable.assign_document_revisions orelse return;
        try assign(self.native.ptr, alloc, table, input, output);
    }

    fn checkWrite(self: *GuardedCatalog, table: catalog.Table) !void {
        try checkpoint(self);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
    }
    const MutationPreparation = struct {
        allocator: std.mem.Allocator,
        guard: *GuardedCatalog,
        table: catalog.Table,
        inner: catalog.MutationPreparation,
        fn candidates(raw: *anyopaque, a: std.mem.Allocator, input: []const catalog.Mutation) ![]const catalog.Mutation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.checkWrite(self.table);
            return (self.inner.candidates orelse return error.UnsupportedSqlExecution)(self.inner.ptr, a, input);
        }
        fn prepare(raw: *anyopaque, a: std.mem.Allocator, input: []const catalog.Mutation) ![]const catalog.Mutation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.checkWrite(self.table);
            return self.inner.prepare(self.inner.ptr, a, input);
        }
        fn finish(raw: *anyopaque, a: std.mem.Allocator) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.checkWrite(self.table);
            try self.inner.finish(self.inner.ptr, a);
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inner.close(self.inner.ptr);
            self.allocator.destroy(self);
        }
    };
    fn openMutationPreparation(raw: *anyopaque, a: std.mem.Allocator, table: catalog.Table, key: []const u8, require_visible: bool) !catalog.MutationPreparation {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.checkWrite(table);
        const open = self.native.vtable.open_mutation_preparation orelse return error.UnsupportedSqlExecution;
        const owner = try a.create(MutationPreparation);
        errdefer a.destroy(owner);
        const inner = try open(self.native.ptr, a, table, key, require_visible);
        owner.* = .{ .allocator = a, .guard = self, .table = table, .inner = inner };
        return .{ .ptr = owner, .candidates = if (inner.candidates != null) MutationPreparation.candidates else null, .prepare = MutationPreparation.prepare, .finish = MutationPreparation.finish, .close = MutationPreparation.close };
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
            var retained = scan_request.table;
            retained.physical_name = try alloc.dupe(u8, retained.physical_name);
            wrapper.* = .{ .alloc = alloc, .guard = self, .table = retained, .is_graph = scan_request.table.graph != null, .graph_dependencies = deps, .inner = cursor };
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
        var wire_allocators = allocators;
        wire_allocators.completion = .until_deadline;
        return dispatch_ddl(self.native.ptr, wire_allocators, input);
    }
};

const view_binding_magic = "AVBG2\x00";
test "SQL pgwire view manifests retain durable identities and reject malformed guards" {
    const a = std.testing.allocator;
    const Binding = @import("../session/prepared.zig").Binding;
    const binding = Binding.fromView(.{ .table = "stored" }, 7, 2, @splat(3), 8);
    const guard = try encodeViewBindings(a, &.{binding});
    defer a.free(guard);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const retained = (try viewBindings(arena.allocator(), guard)).?;
    try std.testing.expectEqual(@as(usize, 1), retained.len);
    try Binding.verifyView(retained, binding);
    var renamed = binding;
    renamed.name = "new_name";
    renamed.revision += 1;
    try Binding.verifyView(retained, renamed);
    renamed.schema_version += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, Binding.verifyView(retained, renamed));
    for ([_][]const u8{ view_binding_magic, view_binding_magic ++ "[]", view_binding_magic ++ "null", view_binding_magic ++ "[{}]", view_binding_magic ++ "[1]" }) |invalid|
        try std.testing.expectError(error.CatalogGenerationChanged, viewBindings(arena.allocator(), invalid));
    try std.testing.expect((try viewBindings(arena.allocator(), null)) == null);
    try std.testing.expect((try viewBindings(arena.allocator(), "legacy-table-guard")) == null);
}

test "SQL pgwire view resolver preserves search path and stops on authority errors" {
    const search_path = @import("../../../pgwire/search_path.zig");
    const Fixture = struct {
        calls: usize = 0,
        failure: anyerror = error.TableNotFound,
        query: ast.Select = .{},
        fn read(raw: *anyopaque, _: std.mem.Allocator, name: ast.Name) !catalog.ReadRelation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (std.mem.eql(u8, name.namespace orelse "", "tenant")) return self.failure;
            return .{ .view = .{ .id = 7, .generation = 1, .query = &self.query, .columns = &.{}, .backend = self.backend(), .security_barrier = true } };
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve_read = read, .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = undefined } };
        }
    };
    var fixture: Fixture = .{};
    var path: search_path.Path = .{};
    try path.append(try search_path.Namespace.init("tenant"));
    try path.append(try search_path.Namespace.init("public"));
    const relation = try resolveReadOnPath(fixture.backend(), std.testing.allocator, .{ .table = "stored" }, &path);
    try std.testing.expect(relation == .view);
    try std.testing.expectEqual(@as(u64, 7), relation.view.id);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    for ([_]anyerror{ error.Forbidden, error.CatalogGenerationChanged, error.QueryCanceled }) |failure| {
        fixture = .{ .failure = failure };
        try std.testing.expectError(failure, resolveReadOnPath(fixture.backend(), std.testing.allocator, .{ .table = "stored" }, &path));
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    }
}

fn hasViewBindings(bindings: []const @import("../session/prepared.zig").Binding) bool {
    for (bindings) |binding| if (binding.kind != .table) return true;
    return false;
}
fn encodeViewBindings(a: std.mem.Allocator, bindings: []const @import("../session/prepared.zig").Binding) ![]const u8 {
    if (bindings.len == 0 or bindings.len > 64 or !hasViewBindings(bindings)) return error.InvalidSqlBackendResponse;
    const bytes = try std.json.Stringify.valueAlloc(a, bindings, .{});
    defer a.free(bytes);
    return std.mem.concat(a, u8, &.{ view_binding_magic, bytes });
}
fn viewBindings(a: std.mem.Allocator, guard: ?[]const u8) !?[]const @import("../session/prepared.zig").Binding {
    const bytes = (try splitBinding(guard orelse return null)).targets;
    if (!std.mem.startsWith(u8, bytes, view_binding_magic)) return null;
    if (bytes.len > 256 * 1024) return error.CatalogGenerationChanged;
    const bindings = std.json.parseFromSliceLeaky([]const @import("../session/prepared.zig").Binding, a, bytes[view_binding_magic.len..], .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.CatalogGenerationChanged,
    };
    if (bindings.len == 0 or bindings.len > 64 or !hasViewBindings(bindings)) return error.CatalogGenerationChanged;
    for (bindings) |binding| {
        if ((binding.id == 0 and binding.kind != .cast) or binding.database.len == 0 or (binding.namespace.len == 0 and binding.kind != .cast) or binding.name.len == 0) return error.CatalogGenerationChanged;
        if (binding.kind == .cast) try @import("../session/prepared.zig").Binding.verifyCast(&.{binding}, binding);
        if (binding.kind == .view and (binding.schema_version == 0 or binding.definition_digest == null)) return error.CatalogGenerationChanged;
        if ((binding.kind == .function or binding.kind == .procedure or binding.kind == .operator or binding.kind == .aggregate) and (binding.schema_version == 0 or binding.routine_incarnation == null or std.mem.allEqual(u8, &binding.routine_incarnation.?, 0) or binding.routine_signature == null or binding.routine_namespace_id == null or binding.routine_namespace_id == 0)) return error.CatalogGenerationChanged;
        if (binding.kind == .sequence) {
            const identity: @import("antfly_local_sources").sql_sequence_session.Identity = .{ .id = binding.id, .incarnation = binding.sequence_incarnation orelse return error.CatalogGenerationChanged };
            identity.validate() catch return error.CatalogGenerationChanged;
        }
    }
    return bindings;
}

fn sameCopyTarget(a: ast.Name, b: ast.Name) bool {
    if (!std.mem.eql(u8, a.table, b.table)) return false;
    inline for (.{ "database", "namespace" }) |field| {
        const left = @field(a, field);
        const right = @field(b, field);
        if ((left == null) != (right == null)) return false;
        if (left) |name| if (!std.mem.eql(u8, name, right.?)) return false;
    }
    return true;
}

fn cloneName(a: std.mem.Allocator, name: ast.Name) !ast.Name {
    return .{ .database = if (name.database) |v| try a.dupe(u8, v) else null, .namespace = if (name.namespace) |v| try a.dupe(u8, v) else null, .table = try a.dupe(u8, name.table) };
}

const DescriptionProbe = struct {
    resolution_context: *operation.RequestContext,
    server: *http.ApiHttpServer,
    credential: *Credential,
    request: wire.Request,
    sampled: bool = false,
    catalog: ?wire.CatalogStamp = null,

    fn sample(self: *@This(), alloc: std.mem.Allocator) !void {
        if (self.sampled) return;
        self.sampled = true;
        self.catalog = try catalogStamp(self.server, alloc, self.credential, self.request);
        if (self.catalog) |stamp| self.resolution_context.bound_catalog_incarnation = stamp.relation_epoch.incarnation;
    }
};

fn requestError(request: wire.Request, err: anyerror) anyerror {
    request.check() catch |canceled| return canceled;
    return if (err == error.DeadlineExceeded) error.QueryCanceled else err;
}

fn catalogStamp(server: *http.ApiHttpServer, alloc: std.mem.Allocator, credential: *Credential, request: wire.Request) !?wire.CatalogStamp {
    const domain = @import("antfly_local_sources").system_catalog_domain;
    const context = requestContext(credential, &request).platformDeadline() catch |err| return requestError(request, err);
    const bytes = server.source.systemCatalog(alloc, context, .{ .resolve_many = .{ .include_source_epoch = true } }) catch |err| {
        // Preserve executor shutdown, translate client cancellation and timeout
        // to the recoverable pgwire statement error, and allow older peers.
        return switch (requestError(request, err)) {
            error.UnsupportedOperation, error.UnknownField => null,
            else => |failure| failure,
        };
    };
    defer alloc.free(bytes);
    try request.check();
    var reply = try std.json.parseFromSlice(domain.ResolvedMany, alloc, bytes, .{ .ignore_unknown_fields = true });
    defer reply.deinit();
    const epoch = reply.value.source_epoch orelse return null;
    if (epoch.revision == 0 or std.mem.allEqual(u8, &epoch.incarnation, 0)) return null;
    return .{ .catalog_revision = reply.value.revision, .relation_epoch = epoch };
}

fn statementBindingGuard(alloc: std.mem.Allocator, revision: ?u64, binding: describe_sql.BoundStatement) !?[]const u8 {
    var entries: BindingEntries = .{};
    defer entries.deinit(alloc);
    try collectBindingTables(alloc, binding, &entries, 0);
    if (entries.count() == 0) return null;
    return try encodeBindingGuard(alloc, revision orelse return error.InvalidSqlBackendResponse, entries.keys());
}

// A statement may bind 64 relations, each graph with up to 64 distinct label
// tables. Keep only compact identity fields and deduplicate in insertion order.
const max_binding_entries = 64 * (1 + 64);
const BindingEntry = struct {
    id: u64,
    name: []const u8,
    version: u32,
    column_lineage: u32,
    graph_revision: u64,
    fn of(table: catalog.Table) BindingEntry {
        return .{ .id = table.id, .name = table.physical_name, .version = table.schema_version, .column_lineage = table.column_lineage orelse table.schema_version, .graph_revision = graphRevision(table) };
    }
};
const BindingEntryContext = struct {
    pub fn hash(_: @This(), key: BindingEntry) u32 {
        var h = std.hash.Wyhash.init(key.id);
        h.update(key.name);
        std.hash.autoHash(&h, key.version);
        std.hash.autoHash(&h, key.column_lineage);
        std.hash.autoHash(&h, key.graph_revision);
        return @truncate(h.final());
    }
    pub fn eql(_: @This(), x: BindingEntry, y: BindingEntry, _: usize) bool {
        return x.id == y.id and x.version == y.version and x.column_lineage == y.column_lineage and x.graph_revision == y.graph_revision and std.mem.eql(u8, x.name, y.name);
    }
};
const BindingEntries = std.ArrayHashMapUnmanaged(BindingEntry, void, BindingEntryContext, true);

fn collectBindingTable(alloc: std.mem.Allocator, table: catalog.Table, entries: anytype) !void {
    if (table.id == 0 or GuardedCatalog.system.owns(table)) return;
    const entry = try entries.getOrPut(alloc, BindingEntry.of(table));
    if (entry.found_existing) return;
    if (@TypeOf(entry.value_ptr.*) == catalog.Table) entry.value_ptr.* = table;
    if (entries.count() > max_binding_entries) return error.SqlProgramLimitExceeded;
    if (table.graph) |binding| for (binding.dependencies) |dependency| {
        try collectBindingTable(alloc, dependency, entries);
    };
}

fn graphRevision(table: catalog.Table) u64 {
    return if (table.graph) |binding| binding.graph.revision else 0;
}

fn collectBindingTables(alloc: std.mem.Allocator, binding: describe_sql.BoundStatement, entries: anytype, depth: usize) anyerror!void {
    if (depth >= 64) return error.SqlProgramLimitExceeded;
    if (binding.table) |table| try collectBindingTable(alloc, table, entries);
    if (binding.relation) |relation| for (relation.scans) |scan| {
        try collectBindingTable(alloc, scan.table, entries);
    };
    if (binding.insert_source) |source| try collectBindingTables(alloc, source.*, entries, depth + 1);
    if (binding.joined_mutation) |joined| try collectBindingTables(alloc, joined.input.*, entries, depth + 1);
    if (binding.merge_mutation) |merge| {
        try collectBindingTable(alloc, merge.target, entries);
        try collectBindingTables(alloc, merge.input.*, entries, depth + 1);
    }
    if (binding.window) |window| try collectBindingTables(alloc, window.input.*, entries, depth + 1);
    if (binding.returning) |returning| try collectBindingTables(alloc, returning.*, entries, depth + 1);
}

fn bindingGuard(alloc: std.mem.Allocator, revision: u64, table: catalog.Table) ![]const u8 {
    return encodeBindingGuard(alloc, revision, &.{BindingEntry.of(table)});
}

fn encodeBindingGuard(alloc: std.mem.Allocator, revision: u64, entries: []const BindingEntry) ![]const u8 {
    if (entries.len == 0 or entries.len > max_binding_entries) return error.SqlProgramLimitExceeded;
    var length: usize = 10;
    for (entries) |item| length = try std.math.add(usize, length, 28 + item.name.len);
    const bytes = try alloc.alloc(u8, length);
    std.mem.writeInt(u64, bytes[0..8], revision, .little);
    std.mem.writeInt(u16, bytes[8..10], @intCast(entries.len), .little);
    var offset: usize = 10;
    for (entries) |item| {
        const entry = bytes[offset..];
        std.mem.writeInt(u32, entry[0..4], @intCast(item.name.len), .little);
        std.mem.writeInt(u64, entry[4..12], item.id, .little);
        std.mem.writeInt(u32, entry[12..16], item.version, .little);
        std.mem.writeInt(u64, entry[16..24], item.graph_revision, .little);
        std.mem.writeInt(u32, entry[24..28], item.column_lineage, .little);
        @memcpy(entry[28..][0..item.name.len], item.name);
        offset += 28 + item.name.len;
    }
    return bytes;
}

const GuardTargets = struct {
    rest: []const u8,
    count: u16,
    const Target = struct { id: u64, version: u32, column_lineage: u32, graph_revision: u64, name: []const u8 };

    fn init(guard: []const u8) !GuardTargets {
        if (guard.len < 10) return error.CatalogGenerationChanged;
        const count = std.mem.readInt(u16, guard[8..10], .little);
        if (count == 0 or count > max_binding_entries) return error.CatalogGenerationChanged;
        return .{ .rest = guard[10..], .count = count };
    }
    fn next(self: *GuardTargets) !?Target {
        if (self.count == 0) {
            if (self.rest.len != 0) return error.CatalogGenerationChanged;
            return null;
        }
        const entry = self.rest;
        if (entry.len < 28) return error.CatalogGenerationChanged;
        const len = std.mem.readInt(u32, entry[0..4], .little);
        if (len > entry.len - 28) return error.CatalogGenerationChanged;
        self.rest = entry[28 + len ..];
        self.count -= 1;
        return .{ .id = std.mem.readInt(u64, entry[4..12], .little), .version = std.mem.readInt(u32, entry[12..16], .little), .graph_revision = std.mem.readInt(u64, entry[16..24], .little), .column_lineage = std.mem.readInt(u32, entry[24..28], .little), .name = entry[28..][0..len] };
    }
};

const incarnation_prefix = "epoch-v1" ++ "\x00\x00";
const BoundTargets = struct { incarnation: ?[16]u8 = null, targets: []const u8 };
fn splitBinding(bytes: []const u8) !BoundTargets {
    if (!std.mem.startsWith(u8, bytes, incarnation_prefix)) return .{ .targets = bytes };
    const rest = bytes[incarnation_prefix.len..];
    if (rest.len <= 16) return error.CatalogGenerationChanged;
    return .{ .incarnation = rest[0..16].*, .targets = rest[16..] };
}

fn sameBindingTargets(original: []const u8, current: []const u8) bool {
    const old = splitBinding(original) catch return false;
    const new = splitBinding(current) catch return false;
    if (!std.meta.eql(old.incarnation, new.incarnation)) return false;
    const before = old.targets;
    const after = new.targets;
    if (@import("../catalog/ddl_binding.zig").Guard.isDdl(before) or std.mem.startsWith(u8, before, view_binding_magic)) return std.mem.eql(u8, before, after);
    var left = GuardTargets.init(before) catch return false;
    var right = GuardTargets.init(after) catch return false;
    if (left.count != right.count) return false;
    while (left.next() catch return false) |x| {
        const y = (right.next() catch return false) orelse return false;
        if (x.id != y.id or x.column_lineage != y.column_lineage or x.graph_revision != y.graph_revision or !std.mem.eql(u8, x.name, y.name)) return false;
    }
    return right.rest.len == 0;
}

fn verifyBindingGuard(guard: []const u8, revision: u64, table: catalog.Table) !void {
    var targets = try GuardTargets.init(guard);
    if (std.mem.readInt(u64, guard[0..8], .little) != revision) return error.CatalogGenerationChanged;
    var found = false;
    while (try targets.next()) |target| {
        if (target.id == table.id and (target.graph_revision != 0) == (table.graph != null) and std.mem.eql(u8, target.name, table.physical_name)) {
            if (!BindingEntryContext.eql(.{}, BindingEntry.of(table), .{ .id = target.id, .name = target.name, .version = target.version, .column_lineage = target.column_lineage, .graph_revision = target.graph_revision }, 0)) return error.CatalogGenerationChanged;
            found = true;
        }
    }
    if (!found) return error.CatalogGenerationChanged;
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
    const many = try encodeBindingGuard(std.testing.allocator, 7, &.{ BindingEntry.of(table), BindingEntry.of(other) });
    defer std.testing.allocator.free(many);
    var same_version = table;
    same_version.column_lineage = table.schema_version + 1;
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(guard, 7, same_version));
    const recreated_column = try bindingGuard(std.testing.allocator, 8, replacement);
    defer std.testing.allocator.free(recreated_column);
    try std.testing.expect(!sameBindingTargets(guard, recreated_column));
    replacement.column_lineage = table.schema_version;
    const compatible = try bindingGuard(std.testing.allocator, 8, replacement);
    defer std.testing.allocator.free(compatible);
    try std.testing.expect(sameBindingTargets(guard, compatible));
    try std.testing.expect(!sameBindingTargets(guard, many));
    try std.testing.expect(sameBindingTargets(many, many));
    for (0..many.len) |length| try std.testing.expect(!sameBindingTargets(many, many[0..length]));
    for ([_]catalog.Table{
        .{ .id = table.id + 1, .physical_name = table.physical_name, .schema_version = table.schema_version, .columns = &.{} },
        .{ .id = table.id, .physical_name = "another_name", .schema_version = table.schema_version, .columns = &.{} },
    }) |different| {
        const changed = try bindingGuard(std.testing.allocator, 7, different);
        defer std.testing.allocator.free(changed);
        try std.testing.expect(!sameBindingTargets(guard, changed));
    }
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

test "SQL pgwire binds the maximum distinct label tables plus graph identity" {
    var dependencies: [64]catalog.Table = undefined;
    for (&dependencies, 0..) |*t, i| t.* = .{ .id = i + 1, .physical_name = "label", .schema_version = 3, .columns = &.{} };
    var graph: catalog.graph_relation.Binding = undefined;
    graph.graph.revision = 4;
    graph.dependencies = &dependencies;
    const table: catalog.Table = .{ .id = 1, .physical_name = "graph", .schema_version = 1, .columns = &.{}, .graph = &graph };
    const binding: describe_sql.BoundStatement = .{ .table = table, .action = .read, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty };
    const guard = (try statementBindingGuard(std.testing.allocator, 9, binding)).?;
    defer std.testing.allocator.free(guard);
    try std.testing.expectEqual(@as(u16, 65), (try GuardTargets.init(guard)).count);
    try verifyBindingGuard(guard, 9, table);
    for (dependencies) |dependency| try verifyBindingGuard(guard, 9, dependency);
}

test "SQL pgwire warm catalog probes share preparation admission and constants bypass it" {
    var manager: usermgr.UserManager = undefined;
    manager.authorization_revision = .init(7);
    var server = http.ApiHttpServer.init(std.testing.allocator, .{ .sql_max_concurrent_preparations = 1 }, .{ .ptr = &manager, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    server.cfg.user_manager = &manager;
    var credential: Credential = .{ .manager = &manager, .username = @constCast("alice"), .principal = @constCast("basic:alice"), .verifier_mac = undefined };
    const identity: wire.Identity = .{ .context = &credential, .release = Credential.release };
    var cancel: std.atomic.Value(bool) = .init(false);
    const req: wire.Request = .{ .statement = "SELECT 1", .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } }, .cancel_requested = &cancel };
    var adapter: Adapter = .{ .server = &server };
    var held = server.sql_preparation_admission.tryAcquireLease().?;
    defer held.release();
    try std.testing.expect(try Adapter.descriptionCurrent(&adapter, std.testing.allocator, identity, req, .{ .stamp = .{ .authority = 7 } }));
    const catalog_stamp: wire.CatalogStamp = .{ .catalog_revision = 1, .relation_epoch = .{ .incarnation = @splat(1), .revision = 1 } };
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, Adapter.descriptionCurrent(&adapter, std.testing.allocator, identity, req, .{ .stamp = .{ .authority = 7, .catalog = catalog_stamp } }));
}

test "SQL pgwire warm catalog probe returns recoverable cancellation and preserves shutdown" {
    const Probe = struct {
        cancel: std.atomic.Value(bool) = .init(false),
        client_cancel: bool = true,
        failure: anyerror = error.Canceled,

        fn call(raw: *anyopaque, _: std.mem.Allocator, _: operation.RequestContext, _: @import("../../../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.cancel.store(self.client_cancel, .release);
            return self.failure;
        }
    };
    var probe: Probe = .{};
    var server = http.ApiHttpServer.init(std.testing.allocator, .{}, .{ .ptr = &probe, .vtable = &.{ .status = undefined, .system_catalog = Probe.call } }, null, null);
    defer server.deinit();
    var credential: Credential = .{ .manager = null, .username = @constCast("alice"), .principal = @constCast("basic:alice"), .verifier_mac = undefined };
    const request: wire.Request = .{ .statement = "", .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } }, .cancel_requested = &probe.cancel };
    try std.testing.expectError(error.QueryCanceled, catalogStamp(&server, std.testing.allocator, &credential, request));
    probe.cancel.store(false, .release);
    probe.client_cancel = false;
    try std.testing.expectError(error.Canceled, catalogStamp(&server, std.testing.allocator, &credential, request));
    probe.failure = error.DeadlineExceeded;
    try std.testing.expectError(error.QueryCanceled, catalogStamp(&server, std.testing.allocator, &credential, request));
    probe.failure = error.ResourceTemporarilyUnavailable;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, catalogStamp(&server, std.testing.allocator, &credential, request));
    probe.failure = error.UnsupportedOperation;
    try std.testing.expect(try catalogStamp(&server, std.testing.allocator, &credential, request) == null);
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
    const retained_policies = [_]OwnedRead.Policy{.{ .resource = "private-table-17", .filter = null }};
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
        fn ddl(_: *anyopaque, allocators: catalog.DdlAllocators, _: catalog.Ddl) !catalog.DdlOutcome {
            try std.testing.expectEqual(catalog.DdlAllocators.Completion.until_deadline, allocators.completion);
            return .{};
        }
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
        .native = .{ .ptr = &native_cursor, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .open_scan = NativeCursor.open, .checkpoint = NativeCursor.checkpoint, .ddl = NativeCursor.ddl } },
        .authority = &authority,
        .revision = &revision,
        .expected_guard = null,
    };
    const backend = guarded.backend();
    _ = try backend.vtable.ddl.?(backend.ptr, .{ .result = alloc, .scratch = alloc }, .{ .catalog_ddl = .{ .kind = .database, .action = .create, .name = .{ .table = "test" } } });
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
    var server = http.ApiHttpServer.init(alloc, .{ .user_manager = &manager }, .{ .ptr = undefined, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    var adapter = Adapter{ .server = &server };
    const wire_identity: wire.Identity = .{ .context = credential, .release = Credential.release };
    const cached: wire.Description = .{ .columns = &.{}, .stamp = .{ .authority = try credential.descriptionRevision() } };
    // An empty catalog backend and a zero-allocation budget exercise the
    // warm path for a binding which never resolved a table.
    var no_alloc = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expect(try Adapter.descriptionCurrent(&adapter, no_alloc.allocator(), wire_identity, authority.request, cached));
    try std.testing.expect(!no_alloc.has_induced_failure);
    server.cfg.user_manager = null;
    try std.testing.expectError(error.Unauthorized, Adapter.descriptionCurrent(&adapter, alloc, wire_identity, authority.request, cached));
    server.cfg.user_manager = &manager;
    // SQL roles can change while the same ordinary table grant remains valid.
    try manager.addRoleToUser("alice", "reader");
    var retained: RetainedRead = .{ .adapter = &adapter, .validated = credential.readRevision(), .bindings = &.{}, .policies = &.{}, .cut = .{ .policies = .{ .digest = @splat(0), .protected = true }, .stamp = null, .revision = null } };
    retained.role_digest = try usermgr.UserManager.roleDigest(alloc, &.{"reader"});
    try manager.removeRoleFromUser("alice", "reader");
    try std.testing.expect(try http.tablePermissionCurrentlyAllowed(admitted, "docs", .read));
    try std.testing.expectError(error.Forbidden, retained.check(alloc, credential, authority.request, false));
    retained.validated = credential.readRevision();
    retained.cut.policies.protected = false;
    retained.role_digest = null;
    retained.policies = &retained_policies;
    retained.bindings = &.{.{ .id = 17, .physical = "private-table-17", .version = 1, .target = .{ .database = "default", .namespace = "public", .table = "docs" } }};
    try manager.setRowFilter("alice", "docs", "{\"term\":{\"tenant_id\":\"allowed\"}}");
    try std.testing.expectError(error.Forbidden, retained.check(alloc, credential, authority.request, false));
    try manager.removeRowFilter("alice", "docs");
    const closes_before_revoke = native_cursor.closes;
    try manager.removePermissionFromUser("alice", "docs", .table);
    try std.testing.expect(!try Adapter.descriptionCurrent(&adapter, alloc, wire_identity, authority.request, cached));
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

test "SQL pgwire native prepared streams validate catalog changes without a redundant warm probe" {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const db_mod = local.storage_db_selected_root.db;
    const raft = @import("../../../raft/mod.zig");
    const reads = @import("antfly_source_root").antfly_sources.table_reads;
    const Source = @import("../testing/parity_sources.zig").Tables(1);
    var declaration = try compiler.compile(a, "CREATE TABLE usage_records(id TEXT,status TEXT)", .{});
    defer declaration.deinit();
    const schema = try local.sql_ddl_runtime.createSchemaAlloc(a, declaration.statement.create_table);
    defer a.free(schema);
    var directory = try local.common_test_directory.TestDirectory.init("sql-prepared-admission-native");
    defer directory.cleanup();
    var changed_declaration = try compiler.compile(a, "CREATE TABLE usage_records(id TEXT,status INTEGER)", .{});
    defer changed_declaration.deinit();
    const changed_schema = try local.sql_ddl_runtime.createSchemaAlloc(a, changed_declaration.statement.create_table);
    defer a.free(changed_schema);
    var schema_arena = std.heap.ArenaAllocator.init(a);
    defer schema_arena.deinit();
    const scratch = schema_arena.allocator();
    var changed_layout = try std.json.parseFromSliceLeaky(std.json.Value, scratch, changed_schema, .{});
    try changed_layout.object.put(scratch, "version", .{ .integer = 2 });
    const replacement_schema = try std.json.Stringify.valueAlloc(scratch, changed_layout, .{});
    var db = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } });
    defer db.close();
    try db.setSchemaJson(a, schema);
    try db.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"id\":\"zulu\",\"status\":\"open\"}" },
        .{ .key = "b", .value = "{\"id\":null,\"status\":null}" },
        .{ .key = "c", .value = "{\"id\":\"alpha\",\"status\":\"open\"}" },
        .{ .key = "d", .value = "{\"id\":\"mike\",\"status\":\"open\"}" },
    } });
    var ranges = [_]local.common_topology_records.RangeRecord{.{ .table_id = 7, .group_id = 7, .range_id = 1, .start_key = "", .doc_identity_shard_id = 1, .doc_identity_range_id = 1 }};
    var source: Source = .{ .records = .{.{ .table_id = 7, .name = "usage_records", .schema_json = schema }}, .reads = .{reads.BoundTableReadSource.init("usage_records", 7, &db, raft.read_gate.alreadyReadSafeBarrier())}, .ranges = &ranges };
    var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var store = usermgr.MemoryStore.init(a);
    defer store.deinit();
    var policies = @import("antfly_casbin").MemoryAdapter.init(a);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(a, store.iface(), try usermgr.initDefaultEnforcer(a, policies.iface()));
    defer manager.deinit();
    var permission = try usermgr.Permission.initOwned(a, .table, "usage_records", .read);
    defer permission.deinit(a);
    var user = try manager.createUser("copy-reader", "secret", &.{permission});
    defer user.deinit(a);
    var server = http.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr(), .auth_enabled = true, .user_manager = &manager }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.systemCatalog, .admin_snapshot = Source.snapshot, .free_admin_snapshot = Source.freeSnapshot, .supports_query_definitions = true } }, source.source(), null);
    defer server.deinit();
    var adapter: Adapter = .{ .server = &server };
    // Run extended protocol through the native adapter. Only Parse and Bind
    // sample the catalog stamp on a warm execution. A new revision after
    // Bind re-enters the compatibility path before delivering any rows.
    source.catalog_epoch = .{ .incarnation = @splat(1), .revision = 1 };
    const Frames = struct {
        const Change = enum { none, unrelated, layout };
        var admission_attempts: usize = 0;
        var schema_after_bind: ?[]const u8 = null;
        fn observeAdmission(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !?wire.ReadStream {
            admission_attempts += 1;
            return Adapter.openStream(raw, alloc, identity, request);
        }

        fn write(out: *std.Io.Writer, tag: u8, payload: []const u8) !void {
            try out.writeByte(tag);
            try out.writeInt(u32, @intCast(payload.len + 4), .big);
            try out.writeAll(payload);
        }
        fn bindThenChange(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request, description: wire.Description) !bool {
            const valid = try Adapter.descriptionCurrent(raw, alloc, identity, request, description);
            const owner: *Adapter = @ptrCast(@alignCast(raw));
            const fixture: *Source = @ptrCast(@alignCast(owner.server.source.ptr));
            if (valid) {
                fixture.catalog_revision += 1;
                if (schema_after_bind) |replacement| fixture.records[0].schema_json = replacement;
            }
            return valid;
        }
    };
    var baseline: [2]usize = undefined;
    for ([_][]const u8{
        "SELECT id FROM usage_records",
        "SELECT id FROM usage_records WHERE status IS NOT NULL ORDER BY id",
    }, 0..) |statement, projection| for ([_]bool{ false, true }) |guarded| for ([_]Frames.Change{ .none, .unrelated, .layout }, 0..) |change, scenario| {
        if (change == .layout and (!guarded or projection == 0)) continue;
        Frames.schema_after_bind = if (change == .layout) replacement_schema else null;
        source.records[0].schema_json = schema;
        source.catalog_revision = 7;
        source.catalog_stamp_reads.store(0, .monotonic);
        source.catalog_reads.store(0, .monotonic);
        Frames.admission_attempts = 0;
        var input: std.Io.Writer.Allocating = .init(a);
        defer input.deinit();
        const startup = "user\x00copy-reader\x00database\x00default\x00\x00";
        try input.writer.writeInt(u32, startup.len + 8, .big);
        try input.writer.writeInt(u32, 196608, .big);
        try input.writer.writeAll(startup);
        try Frames.write(&input.writer, 'p', "secret\x00");
        const parse = try std.fmt.allocPrint(a, "plan\x00{s}\x00\x00\x00", .{statement});
        defer a.free(parse);
        try Frames.write(&input.writer, 'P', parse);
        try Frames.write(&input.writer, 'B', "portal\x00plan\x00\x00\x00\x00\x00\x00\x00");
        try Frames.write(&input.writer, 'E', "portal\x00\x00\x00\x00\x00");
        try Frames.write(&input.writer, 'S', "");
        try Frames.write(&input.writer, 'X', "");
        var reader = std.Io.Reader.fixed(input.written());
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        const Protocol = @import("../../../pgwire/protocol.zig");
        var backend = adapter.backend();
        var vtable = backend.vtable.*;
        vtable.open_stream = Frames.observeAdmission;
        vtable.guarded_select_stream = guarded;
        if (change != .none) vtable.description_current = Frames.bindThenChange;
        backend.vtable = &vtable;
        var session: Protocol.Session = .{ .alloc = a, .io = std.testing.io, .source = backend, .reader = &reader, .writer = &output.writer };
        defer session.deinit();
        try session.run();
        var frames: Protocol.Cursor = .{ .bytes = output.written() };
        const expected: []const ?[]const u8 = if (projection == 0)
            &.{ "zulu", null, "alpha", "mike" }
        else
            &.{ "alpha", "mike", "zulu" };
        var rows: usize = 0;
        var errors: usize = 0;
        while (frames.offset < frames.bytes.len) {
            const tag = try frames.int(u8);
            const bytes = try frames.take((try frames.int(u32)) - 4);
            if (tag == 'E') {
                if (change == .layout) {
                    try std.testing.expect(std.mem.indexOf(u8, bytes, "C40001\x00") != null);
                    errors += 1;
                    continue;
                }
                std.debug.print("prepared protocol error: {s}\n", .{bytes});
                return error.UnexpectedPreparedQueryError;
            }
            if (tag == 'D') {
                try std.testing.expect(rows < expected.len);
                var row: Protocol.Cursor = .{ .bytes = bytes };
                try std.testing.expectEqual(@as(u16, 1), try row.int(u16));
                const len = try row.int(i32);
                if (expected[rows]) |value| {
                    try std.testing.expectEqual(@as(i32, @intCast(value.len)), len);
                    try std.testing.expectEqualStrings(value, try row.take(@intCast(len)));
                } else {
                    try std.testing.expectEqual(@as(i32, -1), len);
                }
                try std.testing.expectEqual(bytes.len, row.offset);
                rows += 1;
            }
        }
        if (change == .layout) {
            try std.testing.expectEqual(@as(usize, 1), errors);
            try std.testing.expectEqual(@as(usize, 0), rows);
            try std.testing.expectEqual(@as(usize, 1), Frames.admission_attempts);
            continue;
        }
        try std.testing.expectEqual(expected.len, rows);
        try std.testing.expectEqual(@as(usize, if (guarded) 2 else if (change != .none) 4 else 3), source.catalog_stamp_reads.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 1), Frames.admission_attempts);
        const catalog_calls = source.catalog_reads.load(.monotonic);
        if (!guarded) baseline[scenario] = catalog_calls else {
            try std.testing.expectEqual(baseline[scenario] - @as(usize, if (change != .none) 3 else 1), catalog_calls);
        }
        std.debug.print("PGWIRE_PREPARED_ADMISSION guarded={} stale={} metadata_calls={d} admissions={d}\n", .{ guarded, change != .none, catalog_calls, Frames.admission_attempts });
    };
}

test "SQL pgwire native COPY originals stream PostgreSQL byte contracts over durable rows" {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const db_mod = local.storage_db_selected_root.db;
    const raft = @import("../../../raft/mod.zig");
    const reads = @import("antfly_source_root").antfly_sources.table_reads;
    const Source = @import("../testing/parity_sources.zig").Tables(1);
    const Copy = @import("../../../pgwire/copy_out.zig");
    const Reference = struct { rows: []const []const std.json.Value, profiles: []const struct { options: Copy.Options, sql: []const u8, hex: []const u8 } };
    const reference = try std.json.parseFromSlice(Reference, a, @embedFile("../../../pgwire/fixtures/copy_out_reference.json"), .{});
    defer reference.deinit();
    var declaration = try compiler.compile(a, "CREATE TABLE usage_records(id TEXT,status TEXT,generated TEXT GENERATED ALWAYS AS (id || status) STORED)", .{});
    defer declaration.deinit();
    const schema = try local.sql_ddl_runtime.createSchemaAlloc(a, declaration.statement.create_table);
    defer a.free(schema);
    var directory = try local.common_test_directory.TestDirectory.init("sql-copy-out-native");
    defer directory.cleanup();
    var db = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } });
    defer db.close();
    try db.setSchemaJson(a, schema);
    for (reference.value.rows) |row| {
        const json = try std.json.Stringify.valueAlloc(a, .{ .id = row[0].string, .status = row[1] }, .{});
        defer a.free(json);
        try db.batch(.{ .writes = &.{.{ .key = row[0].string, .value = json }} });
    }
    var ranges = [_]local.common_topology_records.RangeRecord{.{ .table_id = 7, .group_id = 7, .range_id = 1, .start_key = "", .doc_identity_shard_id = 1, .doc_identity_range_id = 1 }};
    var source: Source = .{ .records = .{.{ .table_id = 7, .name = "usage_records", .schema_json = schema }}, .reads = .{reads.BoundTableReadSource.init("usage_records", 7, &db, raft.read_gate.alreadyReadSafeBarrier())}, .ranges = &ranges };
    var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var store = usermgr.MemoryStore.init(a);
    defer store.deinit();
    var policies = @import("antfly_casbin").MemoryAdapter.init(a);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(a, store.iface(), try usermgr.initDefaultEnforcer(a, policies.iface()));
    defer manager.deinit();
    var permission = try usermgr.Permission.initOwned(a, .table, "usage_records", .read);
    defer permission.deinit(a);
    var user = try manager.createUser("copy-reader", "secret", &.{permission});
    defer user.deinit(a);
    var server = http.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr(), .auth_enabled = true, .user_manager = &manager }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.systemCatalog, .admin_snapshot = Source.snapshot, .free_admin_snapshot = Source.freeSnapshot, .supports_query_definitions = true } }, source.source(), null);
    defer server.deinit();
    var adapter: Adapter = .{ .server = &server };
    const credential = try Credential.authenticate(a, &manager, "copy-reader", "secret");
    defer Credential.release(credential, a);
    var canceled: std.atomic.Value(bool) = .init(false);
    // These are the immutable original statements, not rewritten SELECTs.
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    for ([_][]const u8{ "sql-0373", "sql-0374", "sql-0383", "sql-0378" }, [_]usize{ 2, 0, 5, 0 }) |id, profile_index| {
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        const original = try corpus.get(id);
        const req: wire.Request = .{ .statement = original.sql, .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } }, .cancel_requested = &canceled };
        const description = try adapter.backend().vtable.describe(&adapter, arena.allocator(), .{ .context = credential, .release = Credential.release }, req);
        try std.testing.expect(description.copy_out != null);
        const stream = (try OwnedRead.open(&adapter, a, credential, req)).?;
        defer stream.close(stream.context);
        var encoder = try Copy.Encoder.init(a, arena.allocator(), stream.columns, stream.copy_out.?, 4096);
        defer encoder.deinit();
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        try encoder.start(&output.writer);
        var count: usize = 0;
        while (true) {
            var page_arena: std.heap.ArenaAllocator = .init(a);
            defer page_arena.deinit();
            var page = try stream.next(stream.context, page_arena.allocator(), req, 2);
            defer page.result.deinit();
            for (0..page.result.rowCount()) |index| try encoder.row(page_arena.allocator(), &output.writer, page.result, index);
            count += page.result.rowCount();
            if (page.exhausted) break;
        }
        try encoder.finish(&output.writer);
        const profile = reference.value.profiles[profile_index];
        const expected = try arena.allocator().alloc(u8, profile.hex.len / 2);
        _ = try std.fmt.hexToBytes(expected, profile.hex);
        try std.testing.expectEqualSlices(u8, expected, output.written());
        try std.testing.expectEqual(reference.value.rows.len, count);
    }
    // Constant query COPY must preserve native JSONB semantics through the
    // blocking read spool, not fall back to API JSON serialization.
    {
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        const Jsonb = struct { profiles: []const struct { sql: []const u8, hex: []const u8 } };
        const jsonb = try std.json.parseFromSliceLeaky(Jsonb, arena.allocator(), @embedFile("../../../pgwire/fixtures/copy_out_jsonb_reference.json"), .{ .ignore_unknown_fields = true });
        for (jsonb.profiles) |profile| {
            const request: wire.Request = .{ .statement = profile.sql, .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } }, .cancel_requested = &canceled };
            const output_stream = (try OwnedRead.open(&adapter, a, credential, request)).?;
            defer output_stream.close(output_stream.context);
            var encoder = try Copy.Encoder.init(a, arena.allocator(), output_stream.columns, output_stream.copy_out.?, 4096);
            defer encoder.deinit();
            var output: std.Io.Writer.Allocating = .init(a);
            defer output.deinit();
            try encoder.start(&output.writer);
            {
                var page = try output_stream.next(output_stream.context, arena.allocator(), request, 2);
                defer page.result.deinit();
                try std.testing.expectEqual(@as(usize, 1), page.result.rowCount());
                try std.testing.expect(page.exhausted);
                try encoder.row(arena.allocator(), &output.writer, page.result, 0);
            }
            try encoder.finish(&output.writer);
            const expected = try arena.allocator().alloc(u8, profile.hex.len / 2);
            _ = try std.fmt.hexToBytes(expected, profile.hex);
            try std.testing.expectEqualSlices(u8, expected, output.written());
        }
    }
    // COPY uses the same per-page revocation fence as native SELECT cursors.
    const req: wire.Request = .{ .statement = "COPY usage_records TO STDOUT", .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } }, .cancel_requested = &canceled };
    const retained = (try OwnedRead.open(&adapter, a, credential, req)).?;
    defer retained.close(retained.context);
    var page_arena: std.heap.ArenaAllocator = .init(a);
    defer page_arena.deinit();
    {
        var first = try retained.next(retained.context, page_arena.allocator(), req, 2);
        defer first.result.deinit();
        try std.testing.expectEqual(@as(usize, 2), first.result.rowCount());
        try std.testing.expect(!first.exhausted);
    }
    try manager.removePermissionFromUser("copy-reader", "usage_records", .table);
    try std.testing.expectError(error.Forbidden, retained.next(retained.context, page_arena.allocator(), req, 2));
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
    // Exact signed time crosses the legacy native datetime adapter without
    // narrowing, including pre-Unix values and positive values beyond i64.
    for ([_]i128{ -1_000, -31_536_000_000_000_000, 0, 946684800000000000, 18000000000123456000, std.math.maxInt(u64) / 1000 * 1000 }) |nanos| {
        const wire_value = try wire_values.timestampValue(alloc, nanos);
        const bytes = try wire_values.encode(alloc, .datetime, 1, wire_value);
        const decoded = try wire_values.decode(alloc, 1184, 1, bytes);
        const parameters = try normalizeParameters(alloc, &.{decoded}, &.{.datetime});
        const coerced = try describe_sql.coerce(parameters[0], .datetime);
        try std.testing.expect(coerced == .string);
        try std.testing.expectEqual(@as(?i128, nanos), @import("antfly_local_sources").datetime.parseDateTimeToSignedNs(coerced.string));
        // The native SELECT representation is ISO; the adapter normalizes just
        // that owned cell and then produces the exact original binary micros.
        const returned = try datetimeResult(alloc, coerced);
        try std.testing.expectEqualSlices(u8, bytes, try wire_values.encode(alloc, .datetime, 1, returned));
        const text = try wire_values.encode(alloc, .datetime, 0, returned);
        try std.testing.expectEqual(@as(?i128, nanos), (try wire_values.timestampFromValue(.{ .string = text })).unixNanoseconds());
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
    try std.testing.expectEqual(@as(i128, -31_536_000_000_000_000), try wire_values.timestampNanos(try datetimeResult(alloc, .{ .string = "1969-01-01T00:00:00Z" })));
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
                    // Independent PostgreSQL 18 jsonb::text witness: JSONB
                    // canonical order/spacing applies to frozen cells too.
                    const expected = "{\"s\": \"a\\nb\", \"x\": [9007199254740993, 1.25]}";
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

test "SQL pgwire retained mutation preparation fences write authority cancellation and owner lifetime" {
    const a = std.testing.allocator;
    var store = usermgr.MemoryStore.init(a);
    defer store.deinit();
    var policies = @import("antfly_casbin").MemoryAdapter.init(a);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(a, store.iface(), try usermgr.initDefaultEnforcer(a, policies.iface()));
    defer manager.deinit();
    var permission = try usermgr.Permission.initOwned(a, .table, "docs", .write);
    defer permission.deinit(a);
    var user = try manager.createUser("writer", "secret", &.{permission});
    defer user.deinit(a);
    const credential = try Credential.authenticate(a, &manager, "writer", "secret");
    defer Credential.release(credential, a);
    var identity: ?http.AuthenticatedIdentity = try credential.identity(a);
    defer identity.?.deinit(a);
    try std.testing.expect(!try http.tablePermissionCurrentlyAllowed(identity, "docs", .read));
    const Native = struct {
        opens: usize = 0,
        prepares: usize = 0,
        candidate_calls: usize = 0,
        finishes: usize = 0,
        closes: usize = 0,
        fn checkpoint(_: *anyopaque) !void {}
        fn open(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const u8, require_visible: bool) !catalog.MutationPreparation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!require_visible);
            self.opens += 1;
            return .{ .ptr = self, .candidates = candidates, .prepare = prepare, .finish = finish, .close = close };
        }
        fn candidates(raw: *anyopaque, _: std.mem.Allocator, input: []const catalog.Mutation) ![]const catalog.Mutation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.candidate_calls += 1;
            return input;
        }
        fn prepare(raw: *anyopaque, _: std.mem.Allocator, input: []const catalog.Mutation) ![]const catalog.Mutation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.prepares += 1;
            return input;
        }
        fn finish(raw: *anyopaque, _: std.mem.Allocator) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.finishes += 1;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closes += 1;
        }
    };
    var native_owner: Native = .{};
    var canceled: std.atomic.Value(bool) = .init(false);
    var authority: Authority = .{ .credential = credential, .identity = &identity, .request = .{ .statement = "INSERT INTO docs", .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } }, .cancel_requested = &canceled } };
    const revision: ?u64 = 1;
    var guard: GuardedCatalog = .{ .native = .{ .ptr = &native_owner, .vtable = &.{ .checkpoint = Native.checkpoint, .resolve = undefined, .scan = undefined, .mutate = undefined, .open_mutation_preparation = Native.open } }, .authority = &authority, .revision = &revision, .expected_guard = null };
    const backend = guard.backend();
    const table: catalog.Table = .{ .id = 7, .physical_name = "docs", .schema_version = 1, .columns = &.{} };
    const open = backend.vtable.open_mutation_preparation.?;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, open(backend.ptr, failing.allocator(), table, "key", false));
    try std.testing.expectEqual(@as(usize, 0), native_owner.opens);
    {
        const preparation = try open(backend.ptr, a, table, "key", false);
        defer preparation.close(preparation.ptr);
        _ = try preparation.candidates.?(preparation.ptr, a, &.{});
        _ = try preparation.prepare(preparation.ptr, a, &.{});
        try preparation.finish(preparation.ptr, a);
        try manager.removePermissionFromUser("writer", "docs", .table);
        try std.testing.expectError(error.Forbidden, preparation.candidates.?(preparation.ptr, a, &.{}));
        try std.testing.expectError(error.Forbidden, preparation.prepare(preparation.ptr, a, &.{}));
        try std.testing.expectError(error.Forbidden, preparation.finish(preparation.ptr, a));
        try std.testing.expectError(error.Forbidden, open(backend.ptr, a, table, "key", false));
        canceled.store(true, .release);
        try std.testing.expectError(error.QueryCanceled, preparation.prepare(preparation.ptr, a, &.{}));
        try std.testing.expectEqual(@as(usize, 1), native_owner.opens);
        try std.testing.expectEqual(@as(usize, 1), native_owner.candidate_calls);
        try std.testing.expectEqual(@as(usize, 1), native_owner.prepares);
        try std.testing.expectEqual(@as(usize, 1), native_owner.finishes);
        // Cleanup remains permitted after cancellation and authority revocation.
    }
    try std.testing.expectEqual(@as(usize, 1), native_owner.closes);
}

test "SQL pgwire native population completion counts destination rows without returning DataRows" {
    const a = std.testing.allocator;
    const output: @import("antfly_local_sources").sql_runtime.Output = .{ .command_tag = "SELECT", .rows_affected = 257, .completion_rows = 257 };
    try std.testing.expectEqual(@as(usize, 0), output.rowCount());
    const tag = try commandTag(a, output.command_tag, output.completion_rows orelse output.rowCount(), output.rows_affected);
    defer a.free(tag);
    try std.testing.expectEqualStrings("SELECT 257", tag);
    for ([_][]const u8{ "CREATE TABLE AS", "CREATE MATERIALIZED VIEW", "REFRESH MATERIALIZED VIEW" }) |expected| {
        const actual = try commandTag(a, expected, 0, 0);
        defer a.free(actual);
        try std.testing.expectEqualStrings(expected, actual);
    }
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
    const original_revision = try credential.descriptionRevision();
    const durableOwner = @import("../../changefeed_public.zig").durableOwner;
    const owner = try durableOwner(credential.trusted_identity.?);
    credential.trusted_expires = 0;
    try std.testing.expectError(error.Unauthorized, credential.validate());
    try Adapter.refresh(&adapter, a, authenticated, token);
    try std.testing.expectEqual(owner, try durableOwner(credential.trusted_identity.?));
    const other_tenant = try testTrustedToken(a, "renewal-key", "{\"iss\":\"cloudaf\",\"sub\":\"key-1\",\"tenant\":\"other\",\"admin\":true,\"exp\":4102444800}");
    defer a.free(other_tenant);
    try std.testing.expectError(error.Unauthorized, Adapter.refresh(&adapter, a, authenticated, other_tenant));
    try std.testing.expect(original_revision != try credential.descriptionRevision());
    try credential.validate();
    try std.testing.expectEqualStrings("trusted:cloudaf:key-1", credential.sessionPrincipal());
    const other = try testTrustedToken(a, "renewal-key", "{\"iss\":\"cloudaf\",\"sub\":\"key-2\",\"admin\":true,\"exp\":4102444800}");
    defer a.free(other);
    try std.testing.expectError(error.Unauthorized, Adapter.refresh(&adapter, a, authenticated, other));
    try std.testing.expectEqualStrings("trusted:cloudaf:key-1", credential.sessionPrincipal());
    server.cfg.trusted_principal_issuer = "rotated";
    try std.testing.expectError(error.Unauthorized, credential.validate());
}

test "SQL pgwire retained reads track the serving policy even without a catalog revision change on older owners" {
    const Domain = RetainedRead.Domain;
    const Stamp = @import("antfly_local_sources").system_catalog_policies.PublicationStamp;
    const Fake = struct {
        generation: u64 = 1,
        revision: u64 = 10,
        schema_version: u64 = 1,
        compact: bool = false,
        calls: usize = 0,
        binding_lookups: usize = 0,
        fn call(raw: *anyopaque, a: std.mem.Allocator, ctx: operation.RequestContext, input: @import("../../../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expect(ctx.row_policy_install_authority);
            const stamp: Stamp = .{ .table_id = 7, .schema_version = 1, .catalog_epoch = self.generation, .generation = self.generation, .phase = .active };
            return switch (input) {
                .policy_publication_status => std.json.Stringify.valueAlloc(a, stamp, .{}),
                .resolve_many => |request| blk: {
                    if (request.expected_revision) |revision| if (revision != self.revision) return error.CatalogGenerationChanged;
                    var reply: Domain.ResolvedMany = .{ .revision = self.revision, .tables = &.{}, .source_epoch = .{ .incarnation = @splat(1), .revision = 2 } };
                    if (request.cursor_bindings.len != 0) {
                        if (!self.compact) return error.UnknownField;
                        if (request.cursor_catalog_cut == null or request.cursor_catalog_cut.?.revision != self.revision) self.binding_lookups += 1;
                        var hash = std.crypto.hash.Blake3.init(.{});
                        var protected = false;
                        try Domain.CursorPolicies.observe(&hash, &protected, request.cursor_bindings[0], stamp);
                        reply.cursor_policies = .{ .digest = undefined, .protected = protected };
                        hash.final(&reply.cursor_policies.?.digest);
                    } else if (request.targets.len != 0) {
                        const rows = try a.alloc(?Domain.ResolvedTable, 1);
                        rows[0] = .{ .table_id = 7, .name = "physical", .query_definition = .{ .table_id = 7, .schema_json = try std.json.Stringify.valueAlloc(a, .{ .version = self.schema_version }, .{}), .read_schema_json = "", .indexes_json = "{}" } };
                        reply.tables = rows;
                    }
                    break :blk std.json.Stringify.valueAlloc(a, reply, .{});
                },
                else => error.UnexpectedCatalogCall,
            };
        }
    };
    const a = std.testing.allocator;
    var fake: Fake = .{};
    var api = http.ApiHttpServer.init(a, .{}, .{ .ptr = &fake, .vtable = &.{ .status = undefined, .system_catalog = Fake.call } }, null, null);
    defer api.deinit();
    var adapter: Adapter = .{ .server = &api };
    const bindings = [_]Domain.CursorBinding{.{ .id = 7, .physical = "physical", .version = 1, .target = .{ .database = "default", .namespace = "public", .table = "docs" } }};
    var retained: RetainedRead = .{ .adapter = &adapter, .validated = undefined, .policies = &.{}, .bindings = &bindings, .cut = undefined };
    const original = try retained.readCut(a, .{}, 10, null);
    try std.testing.expect(original.policies.protected);
    fake.compact = true;
    fake.calls = 0;
    try std.testing.expectEqualDeep(original, try retained.readCut(a, .{}, 10, original.stamp));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    const casbin = @import("antfly_casbin");
    var store = usermgr.MemoryStore.init(a);
    defer store.deinit();
    var policies = casbin.MemoryAdapter.init(a);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(a, store.iface(), try usermgr.initDefaultEnforcer(a, policies.iface()));
    defer manager.deinit();
    api.cfg.user_manager = &manager;
    defer api.cfg.user_manager = null;
    var grant = try usermgr.Permission.initOwned(a, .table, "docs", .read);
    defer grant.deinit(a);
    var user = try manager.createUser("alice", "secret", &.{grant});
    defer user.deinit(a);
    try manager.addRoleToUser("alice", "reader");
    const credential = try Credential.authenticate(a, &manager, "alice", "secret");
    defer Credential.release(credential, a);
    retained.validated = credential.readRevision();
    retained.role_digest = try usermgr.UserManager.roleDigest(a, &.{"reader"});
    retained.cut = original;
    retained.policies = &.{.{ .resource = "physical", .filter = null }};
    var cancel = std.atomic.Value(bool).init(false);
    const req: wire.Request = .{ .statement = "", .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } }, .cancel_requested = &cancel };
    const session = try api.txn_sessions.beginForPrincipal(a, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .read_committed, .mode = .read_only } }, api.localSessionNodeId(), credential.sessionPrincipal());
    const session_id = std.fmt.bytesToHex(session.txn_id, .lower);
    var attached = req;
    attached.session_id = &session_id;
    try retained.check(a, credential, attached, true);
    try std.testing.expect(api.txn_sessions.remove(a, session.txn_id));
    try std.testing.expectError(error.SqlTransactionNotActive, retained.check(a, credential, attached, false));
    try manager.addRoleToSubject("bob", "auditor");
    fake.revision += 1;
    fake.binding_lookups = 0;
    for (0..3) |_| try retained.check(a, credential, req, false);
    try std.testing.expectEqual(@as(usize, 1), fake.binding_lookups);
    try std.testing.expectEqualDeep(credential.readRevision(), retained.validated);
    try manager.addRoleToSubject("reader", "inherited");
    try std.testing.expectError(error.Forbidden, retained.check(a, credential, req, false));
    try std.testing.expect(!std.meta.eql(credential.readRevision(), retained.validated));
    fake.revision = 10;
    fake.generation += 1;
    const changed = try retained.readCut(a, .{}, 10, original.stamp);
    try std.testing.expect(!std.meta.eql(original.policies, changed.policies));
    fake.compact = false;
    try std.testing.expectEqualDeep(changed, try retained.readCut(a, .{}, 10, original.stamp));
    fake.schema_version += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, retained.readCut(a, .{}, 10, original.stamp));
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

test "SQL pgwire native void public results retain OID nullness and both wire formats" {
    const Harness = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator) !void {
            var marker: u8 = 0;
            const backend: catalog.Backend = .{ .ptr = &marker, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = checkpoint } };
            var compiled = try compiler.compile(a, "SELECT 'ignored'::void AS v, NULL::void AS n, coalesce(NULL::void, 'ignored'::void) AS c, 'ignored'::void || 'suffix' AS t", .{});
            defer compiled.deinit();
            var result = try native.execute(a, backend, &compiled, &.{}, .{ .native_result = true });
            defer result.deinit();
            var public = try native.execute(a, backend, &compiled, &.{}, .{});
            defer public.deinit();
            var owner: NativeResultCells = .{ .output = result.output };
            try std.testing.expectEqual(@as(usize, 1), result.output.rowCount());
            for (result.output.columns, 0..) |column, index| {
                const kind = try wireType(column.type);
                try std.testing.expectEqual(if (index < 3) wire.Type.void else wire.Type.string, kind);
                try std.testing.expectEqual(@as(u32, if (index < 3) 2278 else 25), try wire_values.oid(kind));
                const cell = try NativeResultCells.read(&owner, a, 0, index);
                try std.testing.expectEqual(index == 1, cell.sql_null);
                try std.testing.expectEqual(public.output.sql_nulls.?[0][index], cell.sql_null);
                if (cell.sql_null) continue;
                for ([_]u16{ 0, 1 }) |format| {
                    var storage: [6]u8 = undefined;
                    var output: std.Io.Writer = .fixed(&storage);
                    try wire_values.encodeCellInto(a, &output, .{ .name = column.name, .type = kind }, format, cell, storage.len);
                    try std.testing.expectEqualStrings(if (index < 3) "" else "suffix", output.buffered());
                }
            }
        }
    };
    try Harness.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "pgwire notifications preserve utility completion tags without row count suffixes" {
    for ([_][]const u8{ "LISTEN", "NOTIFY", "UNLISTEN", "LOCK TABLE", "PREPARE TRANSACTION", "COMMIT PREPARED", "ROLLBACK PREPARED" }) |expected| {
        const actual = try commandTag(std.testing.allocator, expected, 0, 0);
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualStrings(expected, actual);
    }
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
    const policies = [_]OwnedRead.Policy{.{ .resource = "table:immutable", .filter = filter }};
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
    const retained = [_]OwnedRead.Policy{.{ .resource = "table:tenant", .filter = filters[1].filter }};
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
    const owner = try @import("../../changefeed_public.zig").durableOwner(admitted);
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

        fn status(_: *anyopaque) !@import("../../../metadata/api.zig").MetadataStatus {
            return error.TestUnexpectedResult;
        }

        fn catalogCall(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../../../system_catalog/server_call.zig").Call) ![]u8 {
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
        var txn: @import("../../transactions.zig").SessionRegistry.SqlState = .{ .metadata = .{ .database = "default", .namespace = "public", .isolation = .repeatable_read, .mode = .read_only }, .owner_node_id = 1, .execution_started = false, .terminal = null, .savepoints = 0 };
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
        credential.imported_snapshot = .{ .transaction = transaction, .token = token };
        defer credential.imported_snapshot = null;
        var wrong_request = authority.request;
        var bound_authority: Authority = .{ .credential = credential, .identity = &identity, .request = wrong_request };
        try std.testing.expect(bound_authority.importedSnapshot() == null);
        const session_hex = std.fmt.bytesToHex(transaction, .lower);
        wrong_request.session_id = &session_hex;
        bound_authority.request = wrong_request;
        try std.testing.expectEqualDeep(token, bound_authority.importedSnapshot().?.token);
        try std.testing.expect(!Authority.allowsWrite(&bound_authority, "physical"));
        try std.testing.expectError(error.HistoryCheckpointConflict, slots.readExportedSnapshotCatalogPage(alloc, &token.name(), sql_table, "bad", 1, 4096, .{}));
        try std.testing.expectError(error.ChangefeedCapabilityUnsupported, slots.readExportedSnapshotCatalogPage(alloc, &token.name(), sql_table, null, 1, 4096, .{}));
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
        var artifacts = try @import("../../../serverless/artifacts/fs_store.zig").FsStore.init(a, path);
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
    try @import("../../changefeed_public.zig").maintainNativeFeed(metadata, undefined, std.testing.io, alloc, id, maintenance);
    try std.testing.expectEqual(publication_control.Phase.deleted, fixture.slot_state.phase);
    fixture.database_id = 99;
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, slots.readOwned(alloc, "docs_slot", .{}));
    try std.testing.expect(fixture.slot_reads > 4);
    try std.testing.expectEqual(@as(usize, 3), fixture.writes);
}
